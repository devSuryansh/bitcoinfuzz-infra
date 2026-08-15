"""Tests for Docker log chunk reassembly."""

from metrics_agent.log_lines import iter_log_lines
from metrics_agent.parser import FuzzStats, parse_line


class TestIterLogLines:
    def test_complete_chunks(self):
        stream = [b"hello\n", b"world\n"]
        assert list(iter_log_lines(stream)) == ["hello", "world"]

    def test_split_across_chunks(self):
        pulse = (
            b"#2097152 pulse  cov: 847 ft: 3201 corp: 412/2Mb "
            b"exec/s: 14832 rss: 387Mb"
        )
        stream = [pulse[:20], pulse[20:] + b"\n"]
        lines = list(iter_log_lines(stream))
        assert len(lines) == 1
        assert isinstance(parse_line(lines[0]), FuzzStats)

    def test_multiple_lines_in_one_chunk(self):
        stream = [b"a\nb\nc\n"]
        assert list(iter_log_lines(stream)) == ["a", "b", "c"]

    def test_crlf(self):
        stream = [b"hello\r\n", b"world\r\n"]
        assert list(iter_log_lines(stream)) == ["hello", "world"]

    def test_leftover_without_newline(self):
        stream = [b"partial"]
        assert list(iter_log_lines(stream)) == ["partial"]

    def test_empty_chunks_ignored(self):
        stream = [b"", b"ok\n", b""]
        assert list(iter_log_lines(stream)) == ["ok"]

    def test_already_decoded_strings(self):
        stream = ["hello\n", "world\n"]
        assert list(iter_log_lines(stream)) == ["hello", "world"]
