"""Streaming delimited-file writer.

Rows are written as they are produced, flushed in chunks, and never collected
into a list, so peak memory is a function of the chunk size rather than the
row count. The writer also computes the output checksum incrementally, which
is what the determinism self-check compares.

Value formatting is fixed here rather than at each call site so that the same
Python value always serialises to the same bytes: dates as ``YYYY-MM-DD``,
timestamps as ``YYYY-MM-DD HH:MM:SS``, decimals with an explicit scale, ``None``
as an empty field, and booleans as ``Y``/``N``.

Malformed rows are written through :meth:`DelimitedWriter.write_raw`, which
bypasses formatting entirely - a deliberately broken file feed row must be
able to have the wrong field count or a bad encoding.
"""

from __future__ import annotations

import datetime
import hashlib
import os

from . import schema


def format_value(value, column: schema.Column, lenient: bool = False) -> str:
    """Serialise one value for its column.

    ``lenient`` is used by the file feeds, where a deliberately broken value -
    a date of ``0000-00-00`` or an amount of ``1,234.56`` - has to survive
    into the file exactly as the upstream system sent it rather than being
    coerced or rejected here.
    """
    if value is None:
        return ""
    if lenient and isinstance(value, str) and column.type != schema.STRING:
        return value
    if column.type == schema.DATE:
        if isinstance(value, (datetime.date, datetime.datetime)):
            return value.strftime("%Y-%m-%d")
        return str(value)
    if column.type == schema.TIMESTAMP:
        if isinstance(value, datetime.datetime):
            return value.strftime("%Y-%m-%d %H:%M:%S")
        if isinstance(value, datetime.date):
            return value.strftime("%Y-%m-%d 00:00:00")
        return str(value)
    if column.type == schema.DECIMAL:
        return "%.*f" % (column.scale or 2, float(value))
    if column.type == schema.INTEGER:
        return "%d" % int(value)
    if column.type == schema.FLAG:
        if isinstance(value, bool):
            return "Y" if value else "N"
        return str(value)
    return str(value)


class DelimitedWriter:
    """Chunked, checksummed writer for one table's extract file."""

    def __init__(self, path: str, spec: schema.TableSpec, chunk_rows: int = 50000):
        self.path = path
        self.spec = spec
        self.chunk_rows = max(chunk_rows, 1)
        self.rows_written = 0
        self.bytes_written = 0
        self._buffer = []
        self._buffered = 0
        self._digest = hashlib.sha256()
        directory = os.path.dirname(path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        self._handle = open(path, "wb")
        if spec.header:
            self._emit(spec.delimiter.join(column.name for column in spec.columns))

    # -- writing --------------------------------------------------------

    def write(self, values) -> None:
        columns = self.spec.columns
        if len(values) != len(columns):
            raise ValueError("%s: expected %d values, received %d"
                             % (self.spec.key, len(columns), len(values)))
        lenient = self.spec.allows_defects
        line = self.spec.delimiter.join(
            format_value(value, column, lenient) for value, column in zip(values, columns))
        self._emit(line)
        self.rows_written += 1

    def write_raw(self, line, count_row: bool = True) -> None:
        """Write an already-serialised line, bypassing all formatting.

        Accepts ``bytes`` so a feed can carry a deliberately wrong encoding.
        """
        self._emit(line)
        if count_row:
            self.rows_written += 1

    def _emit(self, line) -> None:
        if isinstance(line, bytes):
            payload = line + self.spec.line_terminator.encode("ascii")
        else:
            payload = (line + self.spec.line_terminator).encode(self.spec.encoding, "strict")
        self._buffer.append(payload)
        self._buffered += 1
        if self._buffered >= self.chunk_rows:
            self.flush()

    def flush(self) -> None:
        if not self._buffer:
            return
        blob = b"".join(self._buffer)
        self._handle.write(blob)
        self._digest.update(blob)
        self.bytes_written += len(blob)
        self._buffer = []
        self._buffered = 0

    def close(self) -> dict:
        self.flush()
        self._handle.close()
        return {
            "rows": self.rows_written,
            "bytes": self.bytes_written,
            "sha256": self._digest.hexdigest(),
        }

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.flush()
        self._handle.close()
        return False


class ProgressReporter:
    """Row-count progress on a fixed interval, written to a stream."""

    def __init__(self, stream, label: str, every: int = 250000, quiet: bool = False):
        self.stream = stream
        self.label = label
        self.every = max(every, 1)
        self.quiet = quiet
        self._last = 0

    def tick(self, rows: int) -> None:
        if self.quiet:
            return
        if rows - self._last >= self.every:
            self._last = rows
            self.stream.write("    %-46s %12d rows\n" % (self.label, rows))
            self.stream.flush()

    def done(self, rows: int) -> None:
        if self.quiet:
            return
        self.stream.write("    %-46s %12d rows (complete)\n" % (self.label, rows))
        self.stream.flush()
