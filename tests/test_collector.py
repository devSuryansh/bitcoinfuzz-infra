"""Tests for Prometheus collector crash baseline and series cleanup."""

import asyncio
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from prometheus_client import CollectorRegistry

from metrics_agent.agent import MetricsAgent
from metrics_agent.collector import MetricsCollector


def _crash_value(collector: MetricsCollector, target: str, campaign_id: str) -> float:
    return collector.crash_total.labels(
        target=target, campaign_id=campaign_id
    )._value.get()


def _status_samples(collector: MetricsCollector) -> list:
    return list(collector.container_status.collect())[0].samples


class TestCrashCounting:
    def test_inc_crash_counts_one(self):
        collector = MetricsCollector(registry=CollectorRegistry())
        collector.inc_crash("script", "c1")
        collector.inc_crash("script", "c1")
        assert _crash_value(collector, "script", "c1") == 2.0

    def test_no_series_until_first_crash(self):
        """An idle campaign must not publish a zero-valued crash series."""
        collector = MetricsCollector(registry=CollectorRegistry())
        samples = list(collector.crash_total.collect())[0].samples
        assert samples == []


class TestRemoveCampaign:
    def test_removes_container_status_series(self):
        collector = MetricsCollector(registry=CollectorRegistry())
        collector.set_container_status("script", "c1", True)
        collector.set_container_status("script", "c1", False)
        assert any(s.labels.get("campaign_id") == "c1" for s in _status_samples(collector))

        collector.remove_campaign("script", "c1")
        assert not any(
            s.labels.get("campaign_id") == "c1" for s in _status_samples(collector)
        )


@pytest.mark.asyncio
async def test_agent_stop_removes_status_series(tmp_path: Path):
    """Clean stop must drop the series, not leave status=0 for CampaignCrashed."""
    collector = MetricsCollector(registry=CollectorRegistry())
    agent = MetricsAgent(
        campaign_id="c1",
        target_name="script",
        container_id="ctr",
        data_dir=tmp_path,
        collector=collector,
        docker_client=None,
    )
    await agent.start()
    assert any(s.labels.get("campaign_id") == "c1" for s in _status_samples(collector))

    await agent.stop()
    assert not any(
        s.labels.get("campaign_id") == "c1" for s in _status_samples(collector)
    )


@pytest.mark.asyncio
async def test_parse_log_stream_does_not_block_event_loop(tmp_path: Path):
    """A blocked Docker follow iterator must not freeze other asyncio tasks."""
    release = threading.Event()

    class BlockingStream:
        def __iter__(self):
            return self

        def __next__(self):
            release.wait(timeout=5)
            raise StopIteration

    docker_client = MagicMock()
    container = MagicMock()
    container.logs.return_value = BlockingStream()
    docker_client.containers.get.return_value = container

    collector = MetricsCollector(registry=CollectorRegistry())
    agent = MetricsAgent(
        campaign_id="c1",
        target_name="script",
        container_id="ctr",
        data_dir=tmp_path,
        collector=collector,
        docker_client=docker_client,
    )
    await agent.start()
    try:
        started = time.monotonic()
        await asyncio.sleep(0)
        # A blocking follow-iterator would hold the loop until release.wait
        # times out (5s). to_thread lets this resume immediately.
        assert time.monotonic() - started < 1.0
    finally:
        release.set()
        await agent.stop()
