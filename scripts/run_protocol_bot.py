#!/usr/bin/env python3
"""Compatibility entrypoint; installed users use wekeeper-bot check/run."""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from chatlog_keeper.bot.protocol_runtime import open_runtime
from chatlog_keeper.bot.runtime_cli import source_main


def main(argv=None):
    return source_main(argv, opener=open_runtime)


if __name__ == "__main__":
    raise SystemExit(main())
