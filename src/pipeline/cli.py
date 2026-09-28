import argparse
from pathlib import Path

from pipeline.run import run


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="pipeline")
    commands = parser.add_subparsers(dest="command", required=True)
    run_parser = commands.add_parser("run", help="process every batch in --incoming not yet processed")
    run_parser.add_argument("--incoming", type=Path, required=True)
    run_parser.add_argument("--warehouse", type=Path, required=True)
    args = parser.parse_args(argv)
    run(args.incoming, args.warehouse)
    return 0
