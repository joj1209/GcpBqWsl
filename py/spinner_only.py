#!/usr/bin/env python3
import os
import sys
import time

spinner = "|/-\\"
interval = float(os.getenv("SPINNER_SEC", "0.2"))

i = 0
try:
    while True:
        sys.stdout.write("\r" + spinner[i % len(spinner)])
        sys.stdout.flush()
        i += 1
        time.sleep(interval)
except KeyboardInterrupt:
    sys.stdout.write("\r \r\n")
    sys.stdout.flush()
