import socket
import time
import threading

HOST = "localhost"
PORT = 11211  
DURATION = 10  # Run for 10 seconds
NUM_CLIENTS = 10  # Number of concurrent clients

def benchmark_client(stop_event, results, client_id):
    sock = socket.create_connection((HOST, PORT))
    count = 0
    while not stop_event.is_set():
        try:
            sock.sendall(b"set test 0 0 5\r\ndata\r\n")
            response = sock.recv(1024)
            if b"STORED" in response:
                count += 1
        except Exception as e:
            print(f"Client {client_id} error:", e)
            break

    sock.close()
    results.append(count)

def run_benchmark():
    stop_event = threading.Event()
    results = []
    threads = []

    print(f" Running benchmark with {NUM_CLIENTS} clients for {DURATION} seconds...\n")

    for i in range(NUM_CLIENTS):
        thread = threading.Thread(target=benchmark_client, args=(stop_event, results, i))
        thread.start()
        threads.append(thread)

    time.sleep(DURATION)
    stop_event.set()

    for thread in threads:
        thread.join()

    total_requests = sum(results)
    rps = total_requests / DURATION

    print(f"\n Benchmark completed.")
    print(f"Total Requests: {total_requests}")
    print(f"Requests per Second (RPS): {rps:.2f}")

if __name__ == "__main__":
    run_benchmark()