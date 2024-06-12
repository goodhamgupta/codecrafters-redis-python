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

    RDB_HEX_DUMP = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"

    REPLICA_SOCKETS = []

    def __init__(
        self, client_socket: socket.socket, cmd_list: List[str], args: Namespace
    ):
        """
        Initializes the Parser instance.

        Args:
            client_socket (socket.socket): The socket object for the client connection.
            cmd_list (List[str]): The list of command arguments received from the client.
            args (Namespace): Additional arguments for the parser.
        """
        self.client_socket = client_socket
        self.cmd_list = cmd_list
        self.args = args

    def _extract_content(
        self, content_len_idx, content_idx
    ) -> Tuple[Optional[int], Optional[str]]:
        if len(self.cmd_list) > content_idx:
            param_len = int(self.cmd_list[content_len_idx][1:])
            param_content = self.cmd_list[content_idx]
            assert (
                len(param_content) == param_len
            ), f"Expected parameter of length {param_len} but received {len(param_content)}"
            return param_len, param_content
        else:
            print(
                f"Arguments idx incorrect. Command list has {len(self.cmd_list)} elements but requested idx is {content_idx}"
            )
            return (None, None)

    def parse_command(self) -> bytes:
        """
        Parses the command list and dispatches the command to the appropriate handler.

        Args:
            cmd_list (list): The list of command arguments.

        Returns:
            bytes: The response from the command handler.

        Raises:
            Exception: If the command is not supported.
        """
        _num_elements = int(self.cmd_list[ARGS_IDX][1:])
        print("Received command list: ", self.cmd_list)
        cmd = self.cmd_list[CMD_IDX].strip().upper()
        cmd_len = int(self.cmd_list[CMD_LEN_IDX][1:])
        assert (
            len(cmd) == cmd_len
        ), f"Expected argument of length {cmd_len} but received {len(cmd)}"

        command_functions = {
            "PING": self._handle_ping,
            "ECHO": self._handle_echo,
            "SET": self._handle_set,
            "GET": self._handle_get,
            "INFO": self._handle_info,
            "REPLCONF": self._handle_replconf,
            "PSYNC": self._handle_psync,
        }
        if cmd in command_functions:
            result = command_functions[cmd]()
            if isinstance(result, List):
                # Hack: For now, only PSYNC sends multiple messages. We receive PSYNC only from the replica
                # Hence, we duplicate the current socket to the replica_socket so that we can send WRITE messages
                # to the replica
                print("Setting replica socket..")
                Parser.REPLICA_SOCKETS.append(self.client_socket.dup())
                print("List of replica sockets: ", Parser.REPLICA_SOCKETS)
                print("Replica socket set!")
                # Hack: PSYNC needs to send multiple messages.
                for msg in result:
                    self.client_socket.send(msg)
            else:
                self.client_socket.send(result)
        else:
            raise Exception(f"Command {cmd} not supported!")

    @staticmethod
    def _handle_ping() -> bytes:
        """
        Handles the PING command.

        Args:
            cmd_list (list): The list of command arguments.

        Returns:
            bytes: The response "+PONG\r\n".
        """
        return b"+PONG\r\n"

    def _handle_echo(self) -> bytes:
        """
        Handles the ECHO command.

        Args:
            cmd_list (list): The list of command arguments.

        Returns:
            bytes: The response containing the echoed message.
        """
        param_len, param_content = self._extract_content(PARAM_LEN_IDX, PARAM_IDX)
        return f"${param_len}\r\n{param_content}\r\n".encode("utf-8")

    def _handle_set(self) -> bytes:
        """
        Handles the SET command.

        Args:
            cmd_list (list): The list of command arguments.

        Returns:
            bytes: The response "+OK\r\n".
        """
        _key_len, key_content = self._extract_content(PARAM_LEN_IDX, PARAM_IDX)
        _val_len, val_content = self._extract_content(PARAM_ARG_LEN_IDX, PARAM_ARG_IDX)
        _extra_args_len, extra_args_content = self._extract_content(
            EXTRA_ARGS_CMD_LEN_IDX, EXTRA_ARGS_CMD_IDX
        )
        record = {}
        if extra_args_content:
            if extra_args_content.strip().upper() == "PX":
                # Add TTL
                _ttl_len, ttl_content = self._extract_content(
                    EXTRA_ARGS_CONTENT_LEN_IDX, EXTRA_ARGS_CONTENT_IDX
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
        self.REDIS_DB.update({key_content: record})
        print("redis_db: ", self.REDIS_DB)
        if len(Parser.REPLICA_SOCKETS) > 0:
            for candidate_replica_socket in Parser.REPLICA_SOCKETS:
                print(f"Sending command to replica: {candidate_replica_socket}...")
                replica_command = "\r\n".join(self.cmd_list)
                candidate_replica_socket.send(replica_command.encode("utf-8"))
                print("Message sent to replica!")
        else:
            print("No replica detected.")
        return b"+OK\r\n"

    def _handle_get(self) -> bytes:
        """
        Handles the GET command.

        Args:
            cmd_list (list): The list of command arguments.

        Returns:
            bytes: The response containing the value associated with the key, or "$-1\r\n" if the key is not found.
        """
        _key_len, key_content = self._extract_content(PARAM_LEN_IDX, PARAM_IDX)
        value_struct = self.REDIS_DB.get(key_content, None)
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

    def _handle_info(self) -> bytes:
        """
        Handles the INFO command.

        Args:
            cmd_list (list): The list of command arguments.

        Returns:
            bytes: The response containing the role of the server.
        """
        if self.args.replicaof:
            return b"$10\r\nrole:slave\r\n"
        else:
            return f"$87\r\nrole:master\nmaster_replid:{self.REPLICATION_ID}\nmaster_repl_offset:{self.REPLICATION_OFFSET}\r\n".encode(
                "utf-8"
            )

    @staticmethod
    def _handle_replconf() -> bytes:
        """
        Handles the REPLCONF command.

        Args:
            cmd_list (list): The list of command arguments.
            _args (Namespace): The command line arguments parsed by argparse.

        Returns:
            bytes: The response indicating the REPLCONF command was handled successfully.
        """
        return b"+OK\r\n"

    def _handle_psync(self) -> List[bytes]:
        """
        Handles the PSYNC command.

        Args:
            _cmd_list (list): The list of command arguments.
            _args (Namespace): The command line arguments parsed by argparse.

        Returns:
            bytes: The response indicating the PSYNC command was handled successfully.
        """
        rdb_binary = bytes.fromhex(self.RDB_HEX_DUMP)
        return [
            f"+FULLRESYNC {self.REPLICATION_ID} {self.REPLICATION_OFFSET}\r\n".encode(
                "utf-8"
            ),
            f"${len(rdb_binary)}\r\n".encode("utf-8") + rdb_binary,
        ]


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
            Parser(client_socket, cmd_list, args).parse_command()
    except socket.error as e:
        print(f"Socket error: {e}")
    finally:
        client_socket.close()


class ReplicationHandshake:
    """
    Class to handle the replication handshake process.
    """

    def __init__(self, args: Namespace) -> None:
        """
        Initialize the ReplicationHandshake class.

        Args:
            args (Namespace): The command line arguments parsed by argparse.
        """
        self.args = args

    def connect_to_master(self, master_ip: str, master_port: str) -> socket.socket:
        """
        Connect to the master server.

        Args:
            master_ip (str): The IP address of the master server.
            master_port (str): The port number of the master server.

        Returns:
            socket.socket: The socket object connected to the master server.
        """
        master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        master_socket.connect((master_ip, int(master_port)))
        return master_socket

    def send_message(self, master_socket: socket.socket, message: bytes) -> str:
        """
        Send a REPLCONF command to the master server.

        Args:
            master_socket (socket.socket): The socket object connected to the master server.
            message (bytes): The REPLCONF command to be sent.

        Returns:
            str: The response from the master server.
        """
        master_socket.send(message)
        return master_socket.recv(MAX_BYTES_TO_RECEIVE).decode("utf-8")

    def perform_handshake(self) -> None:
        """
        Perform the replication handshake process.
        """
        if self.args.replicaof:
            (master_ip, master_port) = self.args.replicaof.split(" ")
            master_socket = self.connect_and_ping_master(master_ip, master_port)
            self.perform_replconf_handshake(master_socket)
            self.perform_psync_handshake(master_socket)
        else:
            print("Current server is master. No replication needed..")

    def connect_and_ping_master(
        self, master_ip: str, master_port: str
    ) -> socket.socket:
        """
        Connect to the master server and send a PING command.

        Args:
            master_ip (str): The IP address of the master server.
            master_port (str): The port number of the master server.

        Returns:
            socket.socket: The socket object connected to the master server.
        """
        try:
            master_socket = self.connect_to_master(master_ip, master_port)
            first_handshake_response = self.send_message(
                master_socket, b"*1\r\n$4\r\nPING\r\n"
            )
            print(f"Received response from master: {first_handshake_response}")
            if "PONG" not in first_handshake_response:
                raise Exception(
                    f"Master server did not respond to PING. Response received: {first_handshake_response}"
                )
        except socket.error as e:
            print(f"Failed to connect to master: {e}")
            master_socket.close()
            raise
        return master_socket

    def perform_replconf_handshake(self, master_socket: socket.socket) -> None:
        """
        Perform the REPLCONF handshake process.

        Args:
            master_socket (socket.socket): The socket object connected to the master server.
        """
        try:
            second_handshake_response = self.send_message(
                master_socket,
                b"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n",
            )
            if "OK" not in second_handshake_response:
                raise Exception(
                    f"Master server did not respond to REPLCONF. Response received: {second_handshake_response}"
                )
            final_handshake_response = self.send_message(
                master_socket,
                b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n",
            )
            if "OK" not in final_handshake_response:
                raise Exception(
                    f"Master server did not respond to REPLCONF capabilities. Response received: {final_handshake_response}"
                )
            print("Handshake process complete!")
        except socket.error as e:
            print(f"Failed to connect to master: {e}")
            raise

    def perform_psync_handshake(self, master_socket: socket.socket) -> None:
        """
        Perform the PSYNC handshake process.

        This method sends a PSYNC message to the master server and expects a FULLRESYNC response.
        If the response is not FULLRESYNC, an exception is raised.

        Args:
            master_socket (socket.socket): The socket object connected to the master server.

        Raises:
            Exception: If the response from the master server is not FULLRESYNC.

        Returns:
            None
        """
        try:
            psync_response = self.send_message(
                master_socket, b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"
            )
            if "FULLRESYNC" in psync_response:
                print(
                    "Received FULLRESYNC from master. Replication handshake complete."
                )
            else:
                raise Exception(
                    f"Excepted FULLRESYNC from master. Received: {psync_response}"
                )
        except Exception as e:
            print(f"Failed to connect to master: {e}")


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
    Thread(target=ReplicationHandshake(args).perform_handshake).start()
    while True:
        (client_socket, _client_add) = server_socket.accept()
        Thread(target=process_request, args=(client_socket, _client_add, args)).start()


if __name__ == "__main__":
    main()
