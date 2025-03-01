# /// script
# dependencies = [
#   "requests"
# ]
# ///


import sys
import requests
import random
import string

if __name__ == "__main__":
    url = sys.argv[1]
    count = int(sys.argv[2])
    print(f"inserting {count} keys at {url}")
    for i in range(0, count):
        d = "".join(random.choices(string.printable, k=32))
        r = requests.put(f"{url}/d/key{i}", data=f"{i}/{count} {d}")
        r.raise_for_status()
