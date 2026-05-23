import sys
import argparse
import time
import requests
import csv
import threading
from pathlib import Path

BASE_URL = "http://localhost:8080/api/bitcask"
OUTPUT_DIR = Path(__file__).resolve().parent / "snapshots"


def ensure_output_dir():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def handle_view_all(suffix=""):
    try:
        response = requests.get(BASE_URL, timeout=10)
        if response.status_code == 200:
            data = response.json()
            timestamp = int(time.time())
            
            # Formulate filename according to spec requirements
            ensure_output_dir()
            filename = OUTPUT_DIR / f"{timestamp}{suffix}.csv"
            
            with open(filename, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow(["key", "value"]) # Headers
                for key, val in data.items():
                    writer.writerow([key, val])
            
            # Only print status if we aren't running in heavy concurrent noise loops
            if not suffix:
                print(f"Successfully generated storage snapshot: {filename}")
        else:
            print(f"Server Error: Received code {response.status_code}", file=sys.stderr)
    except Exception as e:
        print(f"Network Failure trying to communicate with Bitcask Engine: {e}", file=sys.stderr)

def handle_single_view(key):
    try:
        response = requests.get(f"{BASE_URL}/{key}", timeout=5)
        if response.status_code == 200:
            print(response.text)
        elif response.status_code == 404:
            print(f"Key '{key}' not found or has been removed via Tombstone.", file=sys.stderr)
        else:
            print(f"Server Error: {response.status_code}", file=sys.stderr)
    except Exception as e:
        print(f"Connection Failed: {e}", file=sys.stderr)

def handle_deletion(key):
    try:
        response = requests.delete(f"{BASE_URL}/{key}", timeout=5)
        if response.status_code == 204:
            print(f"Success: Key '{key}' has been safely removed from Bitcask memory.")
        elif response.status_code == 404:
            print(f"Error: Key '{key}' could not be deleted because it does not exist.", file=sys.stderr)
        else:
            print(f"Server Error: {response.status_code}", file=sys.stderr)
    except Exception as e:
        print(f"Connection Failed: {e}", file=sys.stderr)

def handle_perf_test(client_count):
    print(f"Initializing mass concurrency test across {client_count} worker pools...")
    threads = []
    
    for i in range(1, client_count + 1):
        suffix = f"_thread_{i}"
        t = threading.Thread(target=handle_view_all, args=(suffix,))
        threads.append(t)
        
    start_time = time.time()
    
    # Unleash all connection threads simultaneously 
    for t in threads:
        t.start()
        
    # Wait for execution block pool compilation to drop back down to zero
    for t in threads:
        t.join()
        
    duration = time.time() - start_time
    print(f"Performance Test Concluded. Processed {client_count} data downloads in {duration:.3f} seconds.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bitcask Engine API Client Interface")
    parser.add_name = parser.add_argument
    
    parser.add_argument("--view-all", action="store_true")
    parser.add_argument("--view", action="store_true")
    parser.add_argument("--delete", action="store_true")
    parser.add_argument("--key", type=str)
    parser.add_argument("--perf", action="store_true")
    parser.add_argument("--clients", type=int, default=100)
    
    args = parser.parse_args()

    if args.view_all:
        handle_view_all()
    elif args.view and args.key:
        handle_single_view(args.key)
    elif args.delete and args.key:
        handle_deletion(args.key)
    elif args.perf:
        handle_perf_test(args.clients)
    else:
        parser.print_help()