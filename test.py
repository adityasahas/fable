import requests
import json
import sys
import time
import threading

url = "http://137.184.2.115:8080/find_aliases"


def ani():
    c = "|/-\\"
    i = 0
    while lod.is_alive:
        print(f"\rProcessing {c[i % len(c)]}", end="", flush=True)
        i += 1
        time.sleep(0.1)


def get():
    lst = []
    print("Enter URLs (press Enter with empty input when done):")

    fst = input("URL 1: ").strip()
    if not fst:
        print("Error: At least one URL is required")
        return None
    lst.append(fst)

    i = 2
    while True:
        u = input(f"URL {i}: ").strip()
        if not u:
            sys.stdout.write("\x1b[1A\x1b[2K")
            break
        lst.append(u)
        i += 1
    return lst


def main():
    global lod
    while True:
        n = input("\nEnter netloc (e.g., domain.com) or press Enter to quit: ").strip()
        if not n:
            break

        u = get()
        if u is None:
            continue

        dat = [{"netloc_dir": n, "urls": u}]

        print("\nSending request...")

        lod = threading.Event()
        lod.is_alive = True
        t = threading.Thread(target=ani)
        t.daemon = True
        t.start()

        try:
            r = requests.post(url, json=dat)
            r.raise_for_status()
            res = r.json()

            lod.is_alive = False
            time.sleep(0.2)
            print("\r" + " " * 20 + "\r", end="")

            print("\nResults:")
            print(json.dumps(res, indent=2))

        except requests.exceptions.RequestException as e:
            lod.is_alive = False
            time.sleep(0.2)
            print("\r" + " " * 20 + "\r", end="")
            print(f"Error: {e}")

        a = input("\nTest another netloc? (y/n): ").lower()
        if a != "y":
            break


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        lod.is_alive = False
        print("\nOperation cancelled")
