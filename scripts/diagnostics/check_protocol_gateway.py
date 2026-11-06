#!/usr/bin/env python3
"""Run one fixed native quote probe; no model, business writes or UI automation."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import stat
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from chatlog_keeper.bot.protocol_acceptance import execute_one, inspect_one, verify_spec, assess_recipient
from chatlog_keeper.bot.protocol_gateway import ProtocolGateway


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, required=True)
    parser.add_argument("--case", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--observation", type=Path, help="Check an independent recipient record without contacting the gateway")
    parser.add_argument("--execute-probe", action="store_true", help="Actually submit this fixed test reply")
    parser.add_argument("--status", action="store_true", help="Only query the original send key; never submit")
    parser.add_argument("--server", default="http://127.0.0.1:6174")
    parser.add_argument("--token-file", type=Path)
    parser.add_argument("--journal", type=Path)
    args = parser.parse_args()
    spec = json.loads(args.spec.read_text())
    verify_spec(spec)
    if sum(bool(mode) for mode in (args.observation, args.execute_probe, args.status)) != 1:
        parser.error("choose exactly one of --execute-probe, --status or --observation")
    if args.output.exists():
        parser.error("output exists; retain earlier evidence and select a new report path")
    if args.observation:
        result = assess_recipient(spec, args.case, json.loads(args.observation.read_text()))
    else:
        if not args.token_file or not args.journal:
            parser.error("gateway operations require --token-file and retained --journal")
        mode = args.token_file.stat()
        if (args.token_file.is_symlink() or not stat.S_ISREG(mode.st_mode)
                or mode.st_uid != os.geteuid() or mode.st_mode & 0o077):
            parser.error("token file must be an owner-only regular file")
        with ProtocolGateway(token=args.token_file.read_text().strip(), account_id=spec["account_id"],
                             journal_path=args.journal, base_url=args.server) as gateway:
            result = (inspect_one if args.status else execute_one)(gateway, spec, args.case)
    args.output.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    with args.output.open("x") as file:
        os.chmod(args.output, 0o600)
        json.dump(result, file, ensure_ascii=False, indent=2)
        file.write("\n")
    print(json.dumps({"report": str(args.output), "full_acceptance_passed": result.get("full_acceptance_passed", False),
                      "phone_full_received": result.get("phone_full_received", False)}))


if __name__ == "__main__":
    main()
