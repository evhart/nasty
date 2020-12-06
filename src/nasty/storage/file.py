#
# Copyright 2019-2020 Lukas Schmelzeisen
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from logging import getLogger
from pathlib import Path
from tempfile import mkdtemp
from typing import Iterable, Optional, Sequence

from .._util.json_ import (
    read_json,
    read_json_lines,
    read_lines_file,
    write_json,
    write_jsonl_lines,
)
from ..batch.batch_entry import BatchEntry
from ..tweet.tweet import Tweet, TweetId
from ..tweet.tweet_stream import TweetStream
from .storage import Storage

logger = getLogger(__name__)


class FileStorage(Storage):
    def __init__(self, path: Optional[Path] = None, use_lzma: bool = True):
        super().__init__()

        if not path:
            self.path = Path(mkdtemp())
        else:
            self.path = path

        self.use_lzma = use_lzma

        logger.debug("  Saving results to '{}'.".format(self.path))
        Path.mkdir(self.path, exist_ok=True, parents=True)

    def entry_exists(self, entry: BatchEntry) -> bool:
        meta_file = self.path / entry.meta_file_name
        data_file = self.path / entry.data_file_name

        if meta_file.exists():
            prev_execution_entry = read_json(meta_file, BatchEntry)

            if data_file.exists():
                logger.debug("  Request entry files already exist.")
                entry.completed_at = prev_execution_entry.completed_at
                return True

            logger.debug(
                "  Entry request has stray files associated from previous "
                "execution: {}".format(prev_execution_entry.exception)
            )

        return False

    def read_entry(self, entry: BatchEntry) -> Optional[BatchEntry]:
        meta_file = self.path / entry.meta_file_name
        data_file = self.path / entry.data_file_name

        if meta_file.exists():
            execution_entry = read_json(meta_file, BatchEntry)

            if data_file.exists():
                entry.completed_at = execution_entry.completed_at

            return execution_entry

        return None

    def write_entry(self, entry: BatchEntry) -> None:
        meta_file = self.path / entry.meta_file_name
        write_json(meta_file, entry)

    def entries(self) -> Sequence[BatchEntry]:
        entries: Sequence[BatchEntry] = [
            read_json(meta_file, BatchEntry)
            for meta_file in self.path.iterdir()
            if meta_file.name.endswith(".meta.json")
        ]
        return entries

    def write_data(self, entry: BatchEntry, tweet_stream: TweetStream) -> None:
        data_file = self.path / entry.data_file_name
        write_jsonl_lines(data_file, tweet_stream, use_lzma=self.use_lzma)

    def read_data(self, entry: BatchEntry) -> Iterable[Tweet]:
        data_file = self.path / entry.data_file_name
        ids_file = self.path / entry.ids_file_name

        if not data_file.exists() and ids_file.exists():
            raise ValueError("Tweet data not available. Did you forget to unidify?")

        yield from read_json_lines(data_file, Tweet, use_lzma=self.use_lzma)

    def read_data_ids(self, entry: BatchEntry) -> Iterable[TweetId]:
        data_file = self.path / entry.data_file_name
        ids_file = self.path / entry.ids_file_name

        if ids_file.exists():
            yield from read_lines_file(ids_file)
        else:
            yield from (
                tweet.id for tweet in read_json_lines(data_file, Tweet, use_lzma=True)
            )
