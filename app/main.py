import socket
from threading import Thread
from typing import Tuple, Optional, List
import time
import argparse
from argparse import Namespace


MAX_BYTES_TO_RECEIVE = 1024
MAX_NUM_UNACCEPTED_CONN = 10
ARGS_IDX = 0
CMD_LEN_IDX = 1
CMD_IDX = 2
PARAM_LEN_IDX = 3
PARAM_IDX = 4
PARAM_ARG_LEN_IDX = 5
PARAM_ARG_IDX = 6
EXTRA_ARGS_CMD_LEN_IDX = 7
EXTRA_ARGS_CMD_IDX = 8
EXTRA_ARGS_CONTENT_LEN_IDX = 9
EXTRA_ARGS_CONTENT_IDX = 10
SECONDS_TO_MS = 1_000


class Parser:
    REDIS_DB = {}

    REPLICATION_ID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
    REPLICATION_OFFSET = 0

    @staticmethod
    def _extract_content(
        cmd_list, content_len_idx, content_idx
    ) -> Tuple[Optional[int], Optional[str]]:
        if len(cmd_list) > content_idx:
            param_len = int(cmd_list[content_len_idx][1:])
            param_content = cmd_list[content_idx]
            assert (
                len(param_content) == param_len
            ), f"Expected parameter of length {param_len} but received {len(param_content)}"
            return param_len, param_content
        else:
            print(
                f"Arguments idx incorrect. Command list has {len(cmd_list)} elements but requested idx is {content_idx}"
            )
            return (None, None)

    @classmethod
    def parse_command(cls, cmd_list: List[str], args) -> bytes:
        """
        Parses the command list and dispatches the command to the appropriate handler.

        Args:
            cmd_list (list): The list of command arguments.

        Returns:
            bytes: The response from the command handler.

        Raises:
            Exception: If the command is not supported.
        """
        _num_elements = int(cmd_list[ARGS_IDX][1:])
        print("Received command list: ", cmd_list)
        cmd = cmd_list[CMD_IDX].strip().upper()
        cmd_len = int(cmd_list[CMD_LEN_IDX][1:])
        assert (
            len(cmd) == cmd_len
        ), f"Expected argument of length {cmd_len} but received {len(cmd)}"

        command_functions = {
            "PING": cls._handle_ping,
            "ECHO": cls._handle_echo,
            "SET": cls._handle_set,
            "GET": cls._handle_get,
            "INFO": cls._handle_info,
        }
        if cmd in command_functions:
            return command_functions[cmd](cmd_list, args)
        else:
            raise Exception(f"Command {cmd} not supported!")

    @classmethod
    def _handle_ping(cls, _cmd_list: List[str], _args) -> bytes:
        """
        Handles the PING command.

        Args:
            cmd_list (list): The list of command arguments.

        Returns:
            bytes: The response "+PONG\r\n".
        """
        return b"+PONG\r\n"

    @classmethod
    def _handle_echo(cls, cmd_list: List[str], _args) -> bytes:
        """
        Handles the ECHO command.

        Args:
            cmd_list (list): The list of command arguments.

        Returns:
            bytes: The response containing the echoed message.
        """
        param_len, param_content = cls._extract_content(
            cmd_list, PARAM_LEN_IDX, PARAM_IDX
        )
        return f"${param_len}\r\n{param_content}\r\n".encode("utf-8")

    @classmethod
    def _handle_set(cls, cmd_list: List[str], _args) -> bytes:
        """
        Handles the SET command.

        Args:
            cmd_list (list): The list of command arguments.

        Returns:
            bytes: The response "+OK\r\n".
        """
        _key_len, key_content = cls._extract_content(cmd_list, PARAM_LEN_IDX, PARAM_IDX)
        _val_len, val_content = cls._extract_content(
            cmd_list, PARAM_ARG_LEN_IDX, PARAM_ARG_IDX
        )
        _extra_args_len, extra_args_content = cls._extract_content(
            cmd_list, EXTRA_ARGS_CMD_LEN_IDX, EXTRA_ARGS_CMD_IDX
        )
        record = {}
        if extra_args_content:
            if extra_args_content.strip().upper() == "PX":
                # Add TTL
                _ttl_len, ttl_content = cls._extract_content(
                    cmd_list, EXTRA_ARGS_CONTENT_LEN_IDX, EXTRA_ARGS_CONTENT_IDX
                )
                record.update(
                    {
                        "value": val_content,
                        "TTL": (time.time() * SECONDS_TO_MS) + float(ttl_content),
                    }
                )
            else:
                raise Exception(
                    f"Extra argument {extra_args_content} not supported for SET"
                )
        else:
            record.update({"value": val_content})
        cls.REDIS_DB.update({key_content: record})
        print("redis_db: ", cls.REDIS_DB)
        return b"+OK\r\n"

    @classmethod
    def _handle_get(cls, cmd_list: List[str], _args) -> bytes:
        """
        Handles the GET command.

        Args:
            cmd_list (list): The list of command arguments.

        Returns:
            bytes: The response containing the value associated with the key, or "$-1\r\n" if the key is not found.
        """
        _key_len, key_content = cls._extract_content(cmd_list, PARAM_LEN_IDX, PARAM_IDX)
        value_struct = cls.REDIS_DB.get(key_content, None)
        if value_struct is None:
            return b"$-1\r\n"
        elif (
            "TTL" in value_struct and value_struct["TTL"] < time.time() * SECONDS_TO_MS
        ):
            return b"$-1\r\n"
        else:
            return (
                f"${len(value_struct['value'])}\r\n{value_struct['value']}\r\n".encode(
                    "utf-8"
                )
            )

    @classmethod
    def _handle_info(cls, _cmd_list: List[str], args: Namespace) -> bytes:
        """
        Handles the INFO command.

        Args:
            cmd_list (list): The list of command arguments.

        Returns:
            bytes: The response containing the role of the server.
        """
        if args.replicaof:
            return b"$10\r\nrole:slave\r\n"
        else:
            return f"$87\r\nrole:master\nmaster_replid:{cls.REPLICATION_ID}\nmaster_repl_offset:{cls.REPLICATION_OFFSET}\r\n".encode(
                "utf-8"
            )


def process_request(client_socket, _client_addr, args):
    """
    Processes the request from a client. It continuously receives data from the client
    and sends a response until the client socket is closed.

    Args:
        client_socket (socket): The client's socket.
        client_addr (tuple): The client's address.

    Returns:
        None
    """
    try:
        while True:
            data = client_socket.recv(MAX_BYTES_TO_RECEIVE)
            if not data:
                break
            data = data.decode("utf-8")
            cmd_list = data.split("\r\n")
            result = Parser.parse_command(cmd_list, args)
            client_socket.send(result)

    except socket.error as e:
        print(f"Socket error: {e}")
    finally:
        client_socket.close()


def main():
    """
    The main function of the server. It creates a server socket, listens for incoming connections,
    and starts a new thread to process each connection.

    Args:
        None

    Returns:
        None
    """

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-p", "--port", help="Redis server port", default=6379, type=int
    )
    parser.add_argument(
        "-r",
        "--replicaof",
        help="Address of Redis master server. Current server will be the replica of the master",
        default=None,
    )
    args = parser.parse_args()
    server_socket = socket.create_server(("localhost", args.port), reuse_port=True)
    print(f"Listening on port {args.port}..")
    server_socket.listen(MAX_NUM_UNACCEPTED_CONN)
    while True:
        (client_socket, _client_add) = server_socket.accept()
        Thread(target=process_request, args=(client_socket, _client_add, args)).start()


if __name__ == "__main__":
    main()
