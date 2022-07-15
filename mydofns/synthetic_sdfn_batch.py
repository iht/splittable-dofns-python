import random

import apache_beam as beam
import typing

from apache_beam import RestrictionProvider
from apache_beam.io.iobase import RestrictionTracker
from apache_beam.io.restriction_trackers import OffsetRange, OffsetRestrictionTracker
from apache_beam.runners.sdf_utils import RestrictionTrackerView


class MyFile(typing.NamedTuple):
    id: int
    start: int
    end: int


class GenerateFilesDoFn(beam.DoFn):
    NUM_FILES = 20
    MAX_FILE_SIZE = 30
    MIN_FILE_SIZE = 2

    def process(self, ignored_element, *args, **kwargs):
        for k in range(self.NUM_FILES):
            yield MyFile(id=k, start=0, end=random.randint(self.MIN_FILE_SIZE, self.MAX_FILE_SIZE))


class ProcessFilesSplittableDoFn(beam.DoFn, RestrictionProvider):
    def process(self,
                element: MyFile,
                tracker: RestrictionTrackerView = beam.DoFn.RestrictionParam(),
                **unused_kwargs) -> typing.Iterable[typing.Tuple[int, str]]:
        restriction: OffsetRange = tracker.current_restriction()
        for position in range(restriction.start, restriction.stop + 1):
            if tracker.try_claim(position):
                line = f"This is the line at position {position}"
                yield element.id, line
            else:
                return

    def create_tracker(self, restriction: OffsetRange) -> RestrictionTracker:
        return OffsetRestrictionTracker(restriction)

    def initial_restriction(self, element: MyFile) -> OffsetRange:
        return OffsetRange(start=element.start, stop=element.end)

    def restriction_size(self, element: MyFile, restriction: OffsetRange):
        return restriction.size()
