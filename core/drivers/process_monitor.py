"""Bounded streaming of owned child output with immediate stop propagation."""
import asyncio

from ..contracts import DriverError


async def monitor_process(process, on_line, terminate, timeout, input_payload=None):
    tails = {'stdout': bytearray(), 'stderr': bytearray()}
    tasks = []
    async def consume(stream, channel):
        while True:
            try:
                line = await stream.readline()
            except ValueError as exc:
                raise DriverError('Child output line exceeds the supported bound') from exc
            if not line: break
            if len(line) > 65536: raise DriverError('Child output line exceeds the supported bound')
            tails[channel].extend(line)
            del tails[channel][:-65536]
            on_line(line.decode('utf-8', errors='replace').strip(), channel)
    try:
        if input_payload is not None and process.stdin is not None:
            try:
                process.stdin.write(input_payload)
                await process.stdin.drain()
                process.stdin.close()
            except OSError:
                pass
        tasks = [asyncio.create_task(consume(process.stdout, 'stdout')),
                 asyncio.create_task(consume(process.stderr, 'stderr')),
                 asyncio.create_task(process.wait())]
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=timeout)
        return bytes(tails['stdout']), bytes(tails['stderr'])
    except BaseException:
        await terminate(process)
        raise
    finally:
        for task in tasks:
            if not task.done(): task.cancel()
        if tasks: await asyncio.gather(*tasks, return_exceptions=True)
