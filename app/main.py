import socket


MAX_NUM_UNACCEPTED_CONN = 10
def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    server_socket.listen(MAX_NUM_UNACCEPTED_CONN)
    while server_socket:
        (client_socket, client_add) = server_socket.accept()
        _data = client_socket.recv(1024).decode("utf-8")
        client_socket.sendall(b"+PONG\r\n")


if __name__ == "__main__":
    main()
