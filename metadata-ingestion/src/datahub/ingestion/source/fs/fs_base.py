from dataclasses import dataclass
from typing import Iterable


@dataclass
class FileStatus:
    def __init__(self, path: str, size, is_file):
        self.path = path
        self.size = size
        self.is_file = is_file

    def __str__(self):
        return f"FileStatus({self.path}, {self.size}, {self.is_file})"


class FileSystem:

    @classmethod
    def get(cls, path: str):
        from datahub.ingestion.source.fs import fs_factory
        return fs_factory.get_fs(path)

    def create(self, path: str):
        raise NotImplementedError()

    def open(self, path: str):
        raise NotImplementedError()

    def file_status(self, path: str) -> FileStatus:
        raise NotImplementedError()

    def list(self, path) -> Iterable[FileStatus]:
        raise NotImplementedError()
