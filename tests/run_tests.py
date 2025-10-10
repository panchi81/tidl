"""Test runner and utilities for tidl project."""

import sys
from pathlib import Path

import pytest


def run_tests(test_path: str = "tests/", verbose: bool = True) -> int:
    """Run tests using pytest.

    Args:
        test_path: Path to tests directory or specific test file
        verbose: Whether to run in verbose mode

    Returns:
        Exit code (0 for success, non-zero for failure)

    """
    args = [test_path]

    if verbose:
        args.append("-v")

    # Add current directory to Python path for imports
    project_root = Path(__file__).parent
    sys.path.insert(0, str(project_root))

    return pytest.main(args)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run tidl tests")
    parser.add_argument("test_path", nargs="?", default="tests/", help="Path to tests directory or specific test file")
    parser.add_argument("-q", "--quiet", action="store_true", help="Run in quiet mode (less verbose output)")
    parser.add_argument("--coverage", action="store_true", help="Run with coverage reporting")

    args = parser.parse_args()

    # Add coverage if requested
    if args.coverage:
        sys.exit(run_tests(args.test_path, verbose=not args.quiet))
    else:
        sys.exit(run_tests(args.test_path, verbose=not args.quiet))
