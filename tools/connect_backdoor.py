import socket
import argparse

def test_single_connection(host, port):
    try:
        # Crear socket TCP
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
            client.settimeout(5)  # 5 secs timoeut
            print(f"Connecting to {host}:{port}...")
            client.connect((host, port))
            print("¡Connection succesful!")
            client.close()
            print("Connection closed.")
    except socket.error as e:
        print(f"Error connecting to {host}:{port}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test HES backdoor connection")
    parser.add_argument("--host", default="127.0.0.1", help="host IP(default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8081, help="host port (default: 8081)")
    args = parser.parse_args()

    test_single_connection(args.host, args.port)
