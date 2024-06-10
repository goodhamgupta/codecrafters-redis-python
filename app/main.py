import socket
from threading import Thread


MAX_NUM_UNACCEPTED_CONN = 10
ARGS_IDX = 0
CMD_LEN_IDX = 1
CMD_IDX = 2
PARAM_LEN_IDX = 3
PARAM_IDX = 4


class Parser:
    db = {}

    @staticmethod
    def _extract_content(cmd_list, content_len_idx, content_idx):
        if len(cmd_list) > content_idx:
            param_len = int(cmd_list[content_len_idx][1:])
            param_content = cmd_list[content_idx]
            assert (
                len(param_content) == param_len
            ), f"Expected parameter of length {param_len} but received {len(param_content)}"
            return param_len, param_content
            return f"${param_len}\r\n{param_content}\r\n".encode("utf-8")
        else:
            raise Exception("Invalid command format for ECHO")

    @classmethod
    def parse_command(cls, cmd_list):
        _num_elements = int(cmd_list[ARGS_IDX][1:])
        print("Received command list: ", cmd_list)
        cmd = cmd_list[CMD_IDX].strip().upper()
        cmd_len = int(cmd_list[CMD_LEN_IDX][1:])
        assert (
            len(cmd) == cmd_len
        ), f"Expected argument of length {cmd_len} but received {len(cmd)}"
        if cmd == "PING":
            return b"+PONG\r\n"
        elif cmd == "ECHO":
            param_len, param_content = cls._extract_content(
                cmd_list, PARAM_LEN_IDX, PARAM_IDX
            )
            return f"${param_len}\r\n{param_content}\r\n".encode("utf-8")
        elif cmd == "SET":
            _key_len, key_content = cls._extract_content(
                cmd_list, PARAM_LEN_IDX, PARAM_IDX
            )
            _val_len, val_content = cls._extract_content(
                cmd_list, PARAM_LEN_IDX + 2, PARAM_IDX + 2
            )
            cls.db.update({key_content: val_content})
            print("DB: ", cls.db)
            return b"+OK\r\n"
        elif cmd == "GET":
            _key_len, key_content = cls._extract_content(
                cmd_list, PARAM_LEN_IDX, PARAM_IDX
            )
            if key_content in cls.db:
                value = cls.db[key_content]
                return f"${len(value)}\r\n{value}\r\n".encode("utf-8")
            else:
                return b"$-1\r\n"
        else:
            raise Exception(f"Command {cmd} not supported!")


def process_request(client_socket, client_addr):
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
            data = client_socket.recv(1024)
            if not data:
                break
            data = data.decode("utf-8")
            cmd_list = data.split("\r\n")
            result = Parser.parse_command(cmd_list)
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
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    server_socket.listen(MAX_NUM_UNACCEPTED_CONN)
    while True:
        (client_socket, _client_add) = server_socket.accept()
        Thread(target=process_request, args=(client_socket, _client_add)).start()


if __name__ == "__main__":
    main()
