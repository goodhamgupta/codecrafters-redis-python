import socket
from threading import Thread


MAX_NUM_UNACCEPTED_CONN = 10


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
            _data = client_socket.recv(1024)
            if not _data:
                break
            _data = _data.decode("utf-8")
            client_socket.send(b"+PONG\r\n")
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
