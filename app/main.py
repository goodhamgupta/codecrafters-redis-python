import socket


MAX_NUM_UNACCEPTED_CONN = 10


def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    server_socket.listen(MAX_NUM_UNACCEPTED_CONN)
    while server_socket:
        (client_socket, _client_add) = server_socket.accept()
        data = client_socket.recv(1024).decode("utf-8")
        for _cmd in data.split("\n"):
            client_socket.sendall(b"+PONG\r\n")


if __name__ == "__main__":
    main()
