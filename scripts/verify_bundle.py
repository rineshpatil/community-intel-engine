#!/usr/bin/env python3
"""Structurally verify the Lambda bundle.

The bundle is cross-compiled for linux/x86_64, so it cannot be imported on a
macOS or ARM dev machine. Verification is therefore structural, not import
based: check the wheels target Lambda's platform, the dependencies we import
are present, boto3 is excluded, and the handler paths the CDK stack names
actually resolve.
"""
import pathlib
import sys

OUT = pathlib.Path(__file__).resolve().parent.parent / "build" / "lambda"
DEPS = ("pydantic", "pydantic_core", "pydantic_settings", "praw", "prawcore")
HANDLERS = (
    "community_intel/handlers/webhook.py",
    "community_intel/handlers/reddit_poll.py",
)
MAX_MB = 250

fails: list[str] = []


def check(label: str, ok: bool, detail: str = "") -> None:
    print(f"{'PASS' if ok else 'FAIL'}  {label}{' — ' + detail if detail else ''}")
    if not ok:
        fails.append(label)


def main() -> int:
    if not OUT.is_dir():
        print(f"FAIL  bundle missing at {OUT} — run scripts/build_lambda.sh")
        return 1

    sos = list(OUT.rglob("*.so"))
    linux = [p for p in sos if "x86_64-linux-gnu" in p.name]
    check("compiled wheels target linux x86_64",
          bool(sos) and len(sos) == len(linux), f"{len(linux)}/{len(sos)} .so files")

    for pkg in DEPS:
        check(f"dependency bundled: {pkg}", (OUT / pkg).is_dir())

    check("boto3 excluded (runtime provides it)", not (OUT / "boto3").exists())

    for handler in HANDLERS:
        check(f"handler present: {handler}", (OUT / handler).is_file())

    mb = sum(f.stat().st_size for f in OUT.rglob("*") if f.is_file()) / 1e6
    check(f"under {MAX_MB}MB unzipped limit", mb < MAX_MB, f"{mb:.1f}MB")

    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
