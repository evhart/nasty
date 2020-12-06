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
from abc import abstractmethod
from typing import Iterable, Optional, Sequence

from ..batch.batch_entry import BatchEntry
from ..tweet.tweet import Tweet, TweetId
from ..tweet.tweet_stream import TweetStream


class Storage(object):
    @abstractmethod
    def entry_exists(self, entry: BatchEntry) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def read_entry(self, entry: BatchEntry) -> Optional[BatchEntry]:
        raise NotImplementedError()

    @abstractmethod
    def write_entry(self, entry: BatchEntry) -> None:
        raise NotImplementedError()

    @abstractmethod
    def entries(self) -> Sequence[BatchEntry]:
        raise NotImplementedError()

    @abstractmethod
    def write_data(self, entry: BatchEntry, tweet_stream: TweetStream) -> None:
        raise NotImplementedError()

    @abstractmethod
    def read_data(self, entry: BatchEntry) -> Iterable[Tweet]:
        raise NotImplementedError()

    @abstractmethod
    def read_data_ids(self, entry: BatchEntry) -> Iterable[TweetId]:
        raise NotImplementedError()
