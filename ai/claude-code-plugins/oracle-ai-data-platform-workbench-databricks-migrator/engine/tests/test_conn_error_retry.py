"""PR-37: _create_message_with_529_retry must retry transient transport
failures (anthropic.APIConnectionError / APITimeoutError) with capped
backoff, and surface a clear RuntimeError once conn_max_retries is exhausted.

The waits are monkeypatched away so the suite stays fast.

Run: python -m pytest tests/test_conn_error_retry.py -v
"""
import asyncio
import os
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import anthropic  # noqa: E402
import agent_migrate  # noqa: E402


def _conn_err():
    # APIConnectionError requires the originating request object.
    import httpx
    return anthropic.APIConnectionError(
        request=httpx.Request("POST", "https://api.anthropic.com/v1/messages"))


class TestConnErrorRetry(unittest.TestCase):

    def _run(self, coro):
        return asyncio.run(coro)

    def test_retries_then_succeeds(self):
        """Two transport blips, then success — caller sees the response."""
        calls = {"n": 0}

        def create_fn():
            calls["n"] += 1
            if calls["n"] <= 2:
                raise _conn_err()
            return "RESPONSE"

        async def go():
            loop = asyncio.get_running_loop()
            with mock.patch("asyncio.sleep", new=mock.AsyncMock()):
                return await agent_migrate._create_message_with_529_retry(
                    loop, create_fn)

        self.assertEqual(self._run(go()), "RESPONSE")
        self.assertEqual(calls["n"], 3)

    def test_exhaustion_raises_runtime_error(self):
        """Persistent transport failure exhausts conn_max_retries and raises
        RuntimeError (not the bare SDK exception), chaining the original."""
        def create_fn():
            raise _conn_err()

        async def go():
            loop = asyncio.get_running_loop()
            with mock.patch("asyncio.sleep", new=mock.AsyncMock()):
                await agent_migrate._create_message_with_529_retry(
                    loop, create_fn, conn_max_retries=2)

        with self.assertRaises(RuntimeError) as ctx:
            self._run(go())
        self.assertIn("connection error persisted", str(ctx.exception))
        self.assertIsInstance(ctx.exception.__cause__, anthropic.APIConnectionError)

    def test_timeout_error_also_retried(self):
        """APITimeoutError takes the same conn branch."""
        import httpx
        calls = {"n": 0}

        def create_fn():
            calls["n"] += 1
            if calls["n"] == 1:
                raise anthropic.APITimeoutError(
                    request=httpx.Request("POST", "https://api.anthropic.com/v1/messages"))
            return "OK"

        async def go():
            loop = asyncio.get_running_loop()
            with mock.patch("asyncio.sleep", new=mock.AsyncMock()):
                return await agent_migrate._create_message_with_529_retry(
                    loop, create_fn)

        self.assertEqual(self._run(go()), "OK")
        self.assertEqual(calls["n"], 2)

    def test_non_transient_errors_still_raise_immediately(self):
        """A non-matching exception must not enter any retry branch."""
        def create_fn():
            raise ValueError("boom")

        async def go():
            loop = asyncio.get_running_loop()
            await agent_migrate._create_message_with_529_retry(loop, create_fn)

        with self.assertRaises(ValueError):
            self._run(go())


if __name__ == "__main__":
    unittest.main()
