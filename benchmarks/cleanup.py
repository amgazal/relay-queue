"""Remove only the explicitly named Relay benchmark project and its volumes."""
import re
import subprocess
import sys
from pathlib import Path

project = sys.argv[1] if len(sys.argv) == 2 else ""
if not re.fullmatch("relay-bench-[0-9a-f]{12}", project):
    sys.exit("supply the exact relay-bench-<12 hex digits> from the run log")
subprocess.run(["docker", "compose", "-p", project, "--env-file", "/dev/null",
                "-f", "compose.yaml", "-f", "benchmarks/compose.yaml", "--profile", "demo",
                "down", "--volumes"], cwd=Path(__file__).resolve().parents[1], check=True)
