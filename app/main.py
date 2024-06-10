import socket
from threading import Thread


MAX_NUM_UNACCEPTED_CONN = 10
ARGS_IDX = 0
CMD_LEN_IDX = 1
CMD_IDX = 2
PARAM_LEN_IDX = 3
PARAM_IDX = 4


class Parser:
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
            if len(cmd_list) > PARAM_IDX:
                param_len = int(cmd_list[PARAM_LEN_IDX][1:])
                param_content = cmd_list[PARAM_IDX]
                assert (
                    len(param_content) == param_len
                ), f"Expected parameter of length {param_len} but received {len(param_content)}"
                return f"${param_len}\r\n{param_content}\r\n".encode("utf-8")
            else:
                raise Exception("Invalid command format for ECHO")
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
