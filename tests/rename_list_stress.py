#!/usr/bin/env python3
"""Mount-local rename/overwrite versus LIST; no external consistency barrier."""

import concurrent.futures
import errno
import os
import pathlib
import sys
import threading
import time


def run(root, workers, iterations):
    stop = threading.Event()
    listed = threading.Event()

    def scan():
        while not stop.is_set():
            with os.scandir(root) as entries:
                for _ in entries:
                    pass
            listed.set()

    def worker(index):
        for iteration in range(iterations):
            base = root / f"rename-list-{index}-{iteration}"
            source = base.with_suffix(".source")
            target = base.with_suffix(".target")
            payload = f"worker={index} iteration={iteration}".encode()
            source.write_bytes(payload)
            target.write_bytes(b"old destination")
            # O_PATH pins kernel inode identity without a FUSE read/write open.
            # Retiring the overwritten inode must not remove the new occupant.
            old = os.open(target, os.O_PATH | os.O_CLOEXEC)
            pin = os.open(source, os.O_PATH | os.O_CLOEXEC)
            try:
                source_inode = os.fstat(pin).st_ino
                old_inode = os.fstat(old).st_ino
                deadline = time.monotonic() + 2
                while True:
                    try:
                        os.rename(source, target)
                        break
                    except OSError as error:
                        # FUSE RELEASE for a just-closed writer can arrive later.
                        if error.errno != errno.EBUSY or time.monotonic() >= deadline:
                            raise
                        time.sleep(0.001)
                assert target.stat().st_ino == source_inode, "rename replaced source inode identity"
                assert os.fstat(old).st_ino == old_inode, "overwrite changed retired inode identity"
                assert target.read_bytes() == payload, "rename returned incorrect file data"
            finally:
                os.close(pin)
                os.close(old)
            assert target.stat().st_ino == source_inode, "forget removed destination slot occupant"
            target.unlink()

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers + 1) as pool:
        scanner = pool.submit(scan)
        try:
            if not listed.wait(10):
                scanner.result(timeout=0)
                raise RuntimeError("initial directory scan did not finish")
            futures = [pool.submit(worker, index) for index in range(workers)]
            for future in futures:
                future.result()
        finally:
            stop.set()
            scanner.result()
    print(f"rename/LIST stress passed: {workers} workers x {iterations} overwrites")


if __name__ == "__main__":
    run(pathlib.Path(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]))
