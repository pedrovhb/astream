import pytest
import asyncio
from datetime import timedelta
from astream.on_time import OnTime


@pytest.mark.asyncio
async def test_ontime_with_timedelta_period():
    ontime = OnTime(timedelta(seconds=1), start=False)
    assert ontime.period == timedelta(seconds=1)
    assert ontime.iteration_count == 0
    assert ontime.is_done == False

    # Start the OnTime instance
    ontime.start()
    assert ontime.iteration_count == 1
    assert ontime.is_done == False

    # Wait for the next iteration
    await ontime
    assert ontime.iteration_count == 2
    assert ontime.is_done == False

    # Wait for the next iteration
    await ontime
    assert ontime.iteration_count == 3
    assert ontime.is_done == False

    # Stop the OnTime instance
    ontime.stop()
    assert ontime.iteration_count == 3
    assert ontime.is_done == True


@pytest.mark.asyncio
async def test_ontime_with_count():
    ontime = OnTime(timedelta(seconds=1), count=3, start=False)
    assert ontime.period == timedelta(seconds=1)
    assert ontime.iteration_count == 0
    assert ontime.is_done == False

    # Start the OnTime instance
    ontime.start()
    assert ontime.iteration_count == 1
    assert ontime.is_done == False

    # Wait for the next iteration
    await ontime
    assert ontime.iteration_count == 2
    assert ontime.is_done == False

    # Wait for the next iteration
    await ontime
    assert ontime.iteration_count == 3
    assert ontime.is_done == False

    # Wait for the next iteration
    await ontime
    assert ontime.iteration_count == 4

    # Wait for the next iteration
    await ontime.wait_done()
    assert ontime.iteration_count == 4
    assert ontime.is_done == True


@pytest.mark.asyncio
async def test_ontime_with_float_period():
    ontime = OnTime(1.0, start=False)
    assert ontime.period == timedelta(seconds=1)
    assert ontime.iteration_count == 0
    assert ontime.is_done == False

    # Start the OnTime instance
    ontime.start()
    assert ontime.iteration_count == 1
    assert ontime.is_done == False

    # Wait for the next iteration
    await ontime
    assert ontime.iteration_count == 2
    assert ontime.is_done == False

    # Wait for the next iteration
    await ontime
    assert ontime.iteration_count == 3
    assert ontime.is_done == False

    # Stop the OnTime instance
    ontime.stop()
    assert ontime.iteration_count == 3
    assert ontime.is_done
