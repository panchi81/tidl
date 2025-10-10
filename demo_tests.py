#!/usr/bin/env python3
"""Demo script showing how to use the new test directory structure.

This script demonstrates the organized test structure and how to run different types of tests.
"""

import subprocess
from pathlib import Path


def run_command(cmd: list[str], description: str) -> None:
    """Run a command and display the results."""
    print(f"\n{'=' * 60}")
    print(f"🧪 {description}")
    print(f"{'=' * 60}")
    print(f"Command: {' '.join(cmd)}")
    print("-" * 60)

    try:
        result = subprocess.run(cmd, check=False, capture_output=True, text=True, cwd=Path(__file__).parent)
        if result.stdout:
            print(result.stdout)
        if result.stderr:
            print("STDERR:", result.stderr)
        print(f"Exit code: {result.returncode}")
    except Exception as e:
        print(f"Error running command: {e}")


def main():
    """Demonstrate the test structure."""
    project_root = Path(__file__).parent

    print("🎯 TIDL Test Directory Structure Demo")
    print("=" * 60)  # Show test structure
    print("\n📁 Test Directory Structure:")
    tests_dir = project_root / "tests"
    for item in sorted(tests_dir.iterdir()):
        if item.is_file():
            print(f"  📄 {item.name}")

    # Show how to run different types of tests
    commands = [
        # List all tests
        (["uv", "run", "pytest", "tests/", "--collect-only", "-q"], "List all available tests"),
        # Run a simple test
        (
            ["uv", "run", "pytest", "tests/test_config.py::test_pytest_config_working", "-v"],
            "Run a specific simple test",
        ),
        # Run all client tests
        (["uv", "run", "pytest", "tests/test_client.py", "-v"], "Run all client tests"),
        # Run tests by pattern
        (["uv", "run", "pytest", "tests/", "-k", "config", "-v"], "Run tests matching 'config' pattern"),
        # Show test coverage (if available)
        (["uv", "run", "pytest", "tests/", "--tb=short"], "Run all tests with short traceback format"),
    ]

    for cmd, description in commands:
        run_command(cmd, description)

    print(f"\n{'=' * 60}")
    print("✅ Test structure demonstration complete!")
    print("=" * 60)
    print("\n📖 To run tests manually:")
    print(f"   cd {project_root}")
    print("   uv run pytest tests/")
    print("\n📚 See tests/README.md for more information")


if __name__ == "__main__":
    main()
