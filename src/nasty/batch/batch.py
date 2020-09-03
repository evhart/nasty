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

import hashlib
import json
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from logging import getLogger
from os import getenv
from pathlib import Path
from tempfile import mkdtemp
from typing import Iterator, List, Optional, Sequence, Union, overload
from uuid import uuid4

from .._util.json_ import (
    JsonSerializedException,
    read_json,
    read_json_lines,
    write_json,
    write_jsonl_lines,
)
from ..request.request import Request
from ..storage.file import FileStorage
from ..storage.storage import Storage
from ._execute_result import _ExecuteResult
from .batch_entry import BatchEntry
from .batch_results import BatchResults

logger = getLogger(__name__)


class Batch:
    def __init__(self) -> None:
        self._entries: List[BatchEntry] = []

    def append(self, request: Request) -> None:
        id_ = hashlib.md5(json.dumps(request.to_json()).encode("utf-8")).hexdigest()
        self._entries.append(
            BatchEntry(request, id_=id_, completed_at=None, exception=None)
        )

    def dump(self, file: Path) -> None:
        logger.debug("Dumping batch to file '{}'.".format(file))
        write_jsonl_lines(
            file, self._entries, overwrite_existing=True
        )  # TODO Dump to storage

    def load(self, file: Path) -> None:
        logger.debug("Loading batch from file '{}'.".format(file))
        self._entries += read_json_lines(file, BatchEntry)  # TODO Read from storage

    def execute(
        self, storage: Optional[Union[Path, Storage]] = None
    ) -> Optional[BatchResults]:
        logger.debug(
            "Started executing batch of {:d} requests.".format(len(self._entries))
        )

        if not storage:
            storage = FileStorage()
        if isinstance(storage, Path) or isinstance(storage, str):
            if isinstance(storage, str):
                storage = Path(storage)
            storage = FileStorage(path=storage)

        num_workers = int(getenv("NASTY_NUM_WORKERS", default="1"))
        with ThreadPoolExecutor(max_workers=num_workers) as pool:
            futures = (
                pool.submit(self._execute_entry, entry, storage)
                for entry in self._entries
            )
            result_counter = Counter(
                future.result() for future in as_completed(futures)
            )

        logger.info(
            "Executing batch completed. "
            "{:d} successful, {:d} skipped, {:d} failed.".format(
                result_counter[_ExecuteResult.SUCCESS],
                result_counter[_ExecuteResult.SKIP],
                result_counter[_ExecuteResult.FAIL],
            )
        )
        if result_counter[_ExecuteResult.FAIL]:
            logger.error("Some requests failed!")
            return None
        return BatchResults(storage)

    @classmethod
    def _execute_entry(cls, entry: BatchEntry, storage: Storage) -> _ExecuteResult:
        logger.debug("Executing request: {}".format(entry.request.to_json()))

        if storage.entry_exists(entry):
            prev_execution_entry = storage.read_entry(entry)
            if prev_execution_entry:
                entry.completed_at = prev_execution_entry.completed_at

            if entry.completed_at:
                logger.debug(
                    "  Skipping request, because files already exist and entry completed."
                )
                return _ExecuteResult.SKIP
            else:
                logger.debug("  Executing request, because entry is not completed.")

        result = _ExecuteResult.SUCCESS
        try:
            tweet_stream = entry.request.request()
            storage.write_data(entry, tweet_stream)
            entry.completed_at = datetime.now()

        except Exception as e:
            logger.exception("  Request execution failed with exception.")
            entry.exception = JsonSerializedException.from_exception(e)
            result = _ExecuteResult.FAIL

        storage.write_entry(entry)
        return result

    def __len__(self) -> int:
        return len(self._entries)

    def __contains__(self, item: object) -> bool:
        return item in self._entries

    @overload
    def __getitem__(self, _index: int) -> BatchEntry:
        ...

    @overload  # noqa: F811
    def __getitem__(self, _slice: slice) -> Sequence[BatchEntry]:
        ...

    def __getitem__(  # noqa: F811
        self, index_or_slice: Union[int, slice]
    ) -> Union[BatchEntry, Sequence[BatchEntry]]:
        return self._entries[index_or_slice]

    def __iter__(self) -> Iterator[BatchEntry]:
        return iter(self._entries)

    def __repr__(self) -> str:
        return repr(self._entries)
