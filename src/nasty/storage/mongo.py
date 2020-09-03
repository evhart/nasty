from logging import getLogger
from pathlib import Path
from typing import Iterator, List, Optional, Sequence, Union, overload, Iterable

from .._util.json_ import (
    JsonSerializedException,
    read_json,
    read_json_lines,
    write_json,
    write_jsonl_lines,
    read_lines_file,
)
from tempfile import mkdtemp
from ..batch.batch_entry import BatchEntry
from ..tweet.tweet_stream import TweetStream
from .storage import Storage
from ..tweet.tweet import Tweet, TweetId
from overrides import overrides

from pymongo import MongoClient

import json
from pymongo.operations import UpdateOne

logger = getLogger(__name__)


class MongoStorage(Storage):
    def __init__(
        self,
        host: str = "localhost",
        port: int = 27017,
        tz_aware: bool = False,
        database: Optional[str] = None,
    ):
        super().__init__()
        self._client = MongoClient(host=host, port=port, tz_aware=tz_aware)

        if database:
            self._database = self._client[database]
        else:
            self._database = self._client

        self._entries = self._database["entries"]
        self._data = self._database["data"]

        logger.debug("  Saving results to '{}'.".format(self._database))

    def entry_exists(self, entry: BatchEntry) -> bool:
        if self._entries.count_documents({"id": entry.id}, limit=1) != 0:
            prev_execution_entry = BatchEntry.from_json(
                self._entries.find_one({"id": entry.id})
            )

            if self._data.count_documents({"entry_id": entry.id}, limit=1) != 0:
                logger.debug("  Request entry files already exist.")
                entry.completed_at = prev_execution_entry.completed_at
                return True

            logger.debug(
                "  Entry request has stray files associated from previous "
                "execution: {}".format(prev_execution_entry.exception)
            )

        return False

    def read_entry(self, entry: BatchEntry) -> Optional[BatchEntry]:
        if self._entries.count_documents({"id": entry.id}, limit=1) != 0:
            execution_entry: BatchEntry = BatchEntry.from_json(
                self._entries.find_one({"id": entry.id})
            )

            if self._data.count_documents({"entry_id": entry.id}, limit=1) != 0:
                entry.completed_at = execution_entry.completed_at

            return execution_entry

        return None

    def write_entry(self, entry: BatchEntry) -> None:
        self._entries.update_one(
            {"_id": entry.id}, {"$set": entry.to_json()}, upsert=True
        )

    def entries(self) -> Sequence[BatchEntry]:
        entries: Sequence[BatchEntry] = [
            BatchEntry.from_json(j) for j in self._entries.find({}, {"_id": False})
        ]
        return entries

    def write_data(self, entry: BatchEntry, tweet_stream: TweetStream) -> None:
        upserts = []
        for tweet in tweet_stream:
            j = tweet.to_json()
            j["_id"] = tweet.id
            j["entry_id"] = entry.id

            upserts.append(
                UpdateOne({"_id": tweet.id}, {"$setOnInsert": j}, upsert=True)
            )

        if upserts:
            self._data.bulk_write(upserts)

    def read_data(self, entry: BatchEntry) -> Iterable[Tweet]:
        if (
            self._entries.count_documents({"id": entry.id}, limit=1) != 1
            and self._data.count_documents({"entry_id": entry.id}, limit=1) != 1
        ):
            raise ValueError("Tweet data not available. Did you forget to unidify?")

        for j in self._data.find(
            {"entry_id": entry.id}, {"_id": False, "entry_id": False}
        ):
            yield Tweet(j)

    def read_data_ids(self, entry: BatchEntry) -> Iterable[TweetId]:

        for j in self._data.find(
            {"entry_id": entry.id}, {"_id": False, "entry_id": False}
        ):
            yield j.id
