"""Thin file-share abstraction over a Unity Catalog volume (or a local directory in tests).

The SSIS packages used File System Tasks against C:\\WWI\\...; on Databricks the equivalent is
plain POSIX access to /Volumes/<catalog>/<schema>/landing, which works on serverless compute.
"""

from __future__ import annotations

import hashlib
import os
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass(frozen=True)
class FileInfo:
    path: str
    name: str
    sizeBytes: int
    modifiedAtUtc: datetime


class FileOps:
    def __init__(self, root: str):
        self.root = root.rstrip("/")

    def join(self, *parts: str) -> str:
        return "/".join([self.root, *[p.strip("/") for p in parts if p]])

    def ensureDir(self, path: str) -> str:
        os.makedirs(path, exist_ok=True)
        return path

    def exists(self, path: str) -> bool:
        return os.path.exists(path)

    def listFiles(self, folder: str, recursive: bool = False) -> list[FileInfo]:
        if not os.path.isdir(folder):
            return []
        found: list[FileInfo] = []
        if recursive:
            walker = ((dirpath, filenames) for dirpath, _dirs, filenames in os.walk(folder))
        else:
            walker = [(folder, [n for n in os.listdir(folder) if os.path.isfile(os.path.join(folder, n))])]
        for dirpath, filenames in walker:
            for name in sorted(filenames):
                full = os.path.join(dirpath, name)
                stat = os.stat(full)
                found.append(
                    FileInfo(
                        path=full,
                        name=name,
                        sizeBytes=stat.st_size,
                        modifiedAtUtc=datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).replace(tzinfo=None),
                    )
                )
        return found

    def move(self, sourcePath: str, destinationFolder: str) -> str:
        self.ensureDir(destinationFolder)
        destination = os.path.join(destinationFolder, os.path.basename(sourcePath))
        shutil.move(sourcePath, destination)
        return destination

    def delete(self, path: str) -> None:
        if os.path.exists(path):
            os.remove(path)

    def readLines(self, path: str) -> list[str]:
        with open(path, "rb") as handle:
            raw = handle.read()
        return raw.decode("utf-8", errors="replace").splitlines()

    def writeText(self, path: str, text: str) -> str:
        self.ensureDir(os.path.dirname(path))
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(text)
        return path

    def sha256(self, path: str) -> str:
        digest = hashlib.sha256()
        with open(path, "rb") as handle:
            for chunk in iter(lambda: handle.read(1 << 20), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def usage(self, path: str) -> tuple[int, int]:
        """(total bytes, free bytes) of the filesystem backing ``path``."""
        target = path if os.path.exists(path) else self.root
        stat = shutil.disk_usage(target)
        return stat.total, stat.free
