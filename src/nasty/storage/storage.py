from abc import abstractmethod
from typing import Optional, Sequence, Iterable
from ..batch.batch_entry import BatchEntry
from ..tweet.tweet_stream import TweetStream
from ..tweet.tweet import Tweet, TweetId


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
