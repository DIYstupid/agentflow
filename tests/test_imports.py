import subprocess
import sys


def test_graph_and_runtime_import_cleanly_in_either_order():
    commands = (
        "import graph; import runtime; assert graph.GraphExecutor; assert runtime.TaskManager",
        "import runtime; import graph; assert runtime.TaskManager; assert graph.GraphExecutor",
    )
    for command in commands:
        result = subprocess.run(
            [sys.executable, "-c", command],
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, result.stderr
