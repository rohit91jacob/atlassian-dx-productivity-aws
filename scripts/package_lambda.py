"""Build the extractor Lambda deployment package: dist/extract_lambda.zip.

The extractor needs only the standard library plus boto3, which the python3.12 Lambda runtime
provides, so the zip is just this package's source (a few dozen KB). Entries are written in
sorted order with a fixed timestamp, so the same commit always produces the same zip (and the
same Terraform ``source_code_hash``).
"""

from __future__ import annotations

import argparse
import hashlib
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PACKAGE = ROOT / "src" / "dx_productivity"
# The extract path never imports these (tests/aws/test_aws_mode.py enforces it).
EXCLUDE = {"report", "transform", "verify"}
FIXED_TIME = (2026, 1, 1, 0, 0, 0)


def build(out: Path) -> str:
    out.parent.mkdir(parents=True, exist_ok=True)
    files = sorted(
        p
        for p in PACKAGE.rglob("*.py")
        if "__pycache__" not in p.parts and p.relative_to(PACKAGE).parts[0] not in EXCLUDE
    )
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for path in files:
            info = zipfile.ZipInfo(path.relative_to(PACKAGE.parent).as_posix(), FIXED_TIME)
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o644 << 16
            zf.writestr(info, path.read_bytes())
    return hashlib.sha256(out.read_bytes()).hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, default=ROOT / "dist" / "extract_lambda.zip")
    args = parser.parse_args()
    digest = build(args.out)
    print(f"{args.out} ({args.out.stat().st_size / 1024:.1f} KB) sha256={digest}")


if __name__ == "__main__":
    main()
