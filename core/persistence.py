"""Atomic bytes and cross-process serialization for local state and cards."""
from contextlib import contextmanager, asynccontextmanager
import asyncio
import json
import os
from pathlib import Path
import tempfile
import threading
import time

_registry_guard = threading.Lock()
_registry = {}
_depth = threading.local()


@contextmanager
def file_lock(path, timeout=30.0):
    path = str(Path(path).resolve())
    with _registry_guard:
        lock = _registry.setdefault(path, threading.RLock())
    if not lock.acquire(timeout=timeout):
        raise TimeoutError("Timed out waiting for state lock")
    depths = getattr(_depth, "paths", {})
    _depth.paths = depths
    handle, acquired = None, False
    try:
        if depths.get(path, 0) == 0:
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            handle = open(path, "a+b")
            if os.fstat(handle.fileno()).st_size == 0:
                handle.write(b"\0")
                handle.flush()
            deadline = time.monotonic() + timeout
            while True:
                try:
                    handle.seek(0)
                    if os.name == "nt":
                        import msvcrt
                        msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                    else:
                        import fcntl
                        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    acquired = True
                    break
                except OSError:
                    if time.monotonic() >= deadline:
                        raise TimeoutError("Timed out waiting for interprocess state lock")
                    time.sleep(0.025)
        depths[path] = depths.get(path, 0) + 1
        try:
            yield
        finally:
            depths[path] -= 1
    finally:
        if handle is not None:
            try:
                if acquired:
                    handle.seek(0)
                    if os.name == "nt":
                        import msvcrt
                        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                    else:
                        import fcntl
                        fcntl.flock(handle, fcntl.LOCK_UN)
            finally:
                handle.close()
        lock.release()


def atomic_write_bytes(path, data):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, temp = tempfile.mkstemp(prefix="." + target.name + ".", suffix=".tmp", dir=target.parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, target)
        if os.name != "nt":
            directory = os.open(target.parent, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
    finally:
        if os.path.exists(temp):
            os.unlink(temp)


def atomic_write_json(path, data):
    atomic_write_bytes(path, (json.dumps(data, ensure_ascii=False, indent=2) + "\n").encode("utf-8"))


@asynccontextmanager
async def async_file_lock(path, timeout=30.0):
    """Task-safe OS lock: unlike an RLock it cannot be reentered by another coroutine."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    handle = open(target, "a+b")
    acquired = False
    try:
        if os.fstat(handle.fileno()).st_size == 0:
            handle.write(b"\0")
            handle.flush()
        deadline = time.monotonic() + timeout
        while True:
            try:
                handle.seek(0)
                if os.name == "nt":
                    import msvcrt
                    msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                acquired = True
                break
            except OSError:
                if time.monotonic() >= deadline:
                    raise TimeoutError("Timed out waiting for crawler configuration lock")
                await asyncio.sleep(0.025)
        yield
    finally:
        try:
            if acquired:
                handle.seek(0)
                if os.name == "nt":
                    import msvcrt
                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(handle, fcntl.LOCK_UN)
        finally:
            handle.close()
