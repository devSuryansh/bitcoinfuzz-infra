"""Decode a Docker log byte stream into complete text lines.

Docker's multiplexed log API can split a single libFuzzer line across
chunks, or pack several lines into one chunk. Callers that treat each
chunk as a line will drop or corrupt pulse data under load.
"""

from typing import Iterable, Iterator, Union

LogChunk = Union[bytes, str]


def iter_log_lines(stream: Iterable[LogChunk]) -> Iterator[str]:
    """Yield complete lines from a Docker log chunk stream.

    Buffers until a newline arrives. A leftover fragment is yielded when
    the stream ends so a final line without a trailing newline is not lost.
    """
    buffer = ""
    for chunk in stream:
        if not chunk:
            continue
        if isinstance(chunk, bytes):
            text = chunk.decode("utf-8", errors="replace")
        else:
            text = chunk
        buffer += text
        while "\n" in buffer:
            line, buffer = buffer.split("\n", 1)
            line = line.rstrip("\r")
            if line:
                yield line
    leftover = buffer.rstrip("\r")
    if leftover:
        yield leftover
