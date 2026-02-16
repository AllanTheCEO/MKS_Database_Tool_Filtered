import sys
import time
import os
import shutil
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import pyarrow.parquet as pq
import ingest

test_stand = r"test_stand_here"
destination = r"_destination_here"
DATABASE = "database_here"

watch_dirs = ["list_of_dirs_to_watch_here"
             ]

class MirrorEventHandler(FileSystemEventHandler):
    def __init__(self, src_root: str, dest_root: str):
        super().__init__()
        self.src_root = os.path.abspath(src_root)
        self.dest_root = os.path.abspath(dest_root)

    def _relative_dest_path(self, src_path: str) -> str:
        rel = os.path.relpath(src_path, self.src_root)
        rel = os.path.join(self.test_stand(src_path), rel)
        return os.path.join(self.dest_root, rel)
    
    def test_stand(self, path: str) -> str:
        parts = os.path.normpath(path).split(os.sep)
        for part in parts:
            if part.startswith("prefix_here") or part.startswith("prefix_here"):
                return part
        return "unknown_test_stand"

    def _copy_file(self, src_path: str):
        dest_path = self._relative_dest_path(src_path)
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)

        max_attempts = 5
        delay = 0.2  

        for attempt in range(1, max_attempts + 1):
            try:
                shutil.copy2(src_path, dest_path)
                print(f"    Copied to: {dest_path}")
                return  
            except PermissionError as e:
                winerr = getattr(e, "winerror", None)
                if winerr == 32 and attempt < max_attempts:
                    print(
                        f"    [WARN] {src_path} is in use (WinError 32), "
                        f"retrying {attempt}/{max_attempts}..."
                    )
                    time.sleep(delay)
                    continue
                else:
                    print(
                        f"    [ERROR] Failed to copy {src_path} -> {dest_path} "
                        f"after {attempt} attempts: {e}"
                    )
                    return
            except Exception as e:
                print(f"    [ERROR] Failed to copy {src_path} -> {dest_path}: {e}")
                return


    def _ensure_dir(self, src_path: str):
        dest_path = self._relative_dest_path(src_path)
        try:
            os.makedirs(dest_path, exist_ok=True)
            print(f"    Ensured directory exists in dest: {dest_path}")
        except Exception as e:
            print(f"    [ERROR] Failed to create directory {dest_path}: {e}")

    def on_created(self, event):
        if event.is_directory:
            print(f"[DIR CREATED] {event.src_path}")
            self._ensure_dir(event.src_path)
        else:
            print(f"[FILE CREATED] {event.src_path}")
            self._copy_file(event.src_path)

    def on_modified(self, event):
        if event.is_directory:
            print(f"[DIR MODIFIED] {event.src_path}")
            self._ensure_dir(event.src_path)
        else:
            print(f"[FILE MODIFIED] {event.src_path}")
            self._copy_file(event.src_path)

    def on_deleted(self, event):
        dest_path = self._relative_dest_path(event.src_path)
        if os.path.isdir(dest_path):
            print(f"[DIR DELETED]  {event.src_path}")
            if os.path.isdir(dest_path):
                try:
                    shutil.rmtree(dest_path)
                    print(f"    Deleted directory from dest: {dest_path}")
                except Exception as e:
                    print(f"    [ERROR] Failed to delete directory {dest_path}: {e}")
        else:
            print(f"[FILE DELETED] {event.src_path}")
            if os.path.isfile(dest_path):
                try:
                    os.remove(dest_path)
                    print(f"    Deleted file from dest: {dest_path}")
                except Exception as e:
                    print(f"    [ERROR] Failed to delete file {dest_path}: {e}")
                    
    def on_moved(self, event):
        old_dest = self._relative_dest_path(event.src_path)
        new_dest = self._relative_dest_path(event.dest_path)

        if event.is_directory:
            print(f"[DIR MOVED]   {event.src_path} -> {event.dest_path}")
        else:
            print(f"[FILE MOVED]  {event.src_path} -> {event.dest_path}")

        os.makedirs(os.path.dirname(new_dest), exist_ok=True)

        try:
            if os.path.exists(old_dest):
                shutil.move(old_dest, new_dest)
                print(f"    Moved in dest: {old_dest} -> {new_dest}")
            else:
                if event.is_directory:
                    os.makedirs(new_dest, exist_ok=True)
                    print(f"    Created moved dir in dest: {new_dest}")
                else:
                    self._copy_file(event.dest_path)
        except Exception as e:
            print(f"    [ERROR] Failed to move in dest {old_dest} -> {new_dest}: {e}")

def copy_file(src_path: str, dest_root: str, src_root: str):
    dest_path = os.path.join(dest_root, os.path.relpath(src_path, src_root))
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    shutil.copy2(src_path, dest_path)

def main(src_dirs, dest_dir: str):
    observer = Observer()

    if isinstance(src_dirs, str):
        src_dirs = [src_dirs]

    for src in src_dirs:
        handler = MirrorEventHandler(src, dest_dir)
        observer.schedule(handler, path=src, recursive=True)
        print(f"Watching: {os.path.abspath(src)}")

    print(f"Mirroring changes into: {os.path.abspath(dest_dir)}")
    print("Press Ctrl+C to stop.\n")

    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping watcher...")
        observer.stop()
    observer.join()

if __name__ == "__main__":
    main(watch_dirs, destination)