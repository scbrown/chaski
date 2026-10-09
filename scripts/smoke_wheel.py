"""Install a wheel into an isolated venv and prove its actual console command."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile
import venv


def smoke(wheel: Path) -> None:
    with tempfile.TemporaryDirectory(prefix="chaski-wheel-") as directory:
        root = Path(directory)
        environment = root / "venv"
        venv.EnvBuilder(with_pip=True).create(environment)
        python = environment / "bin" / "python"
        program = environment / "bin" / "chaski"
        install_env = dict(os.environ, PIP_CONFIG_FILE=os.devnull, PIP_INDEX_URL="https://pypi.org/simple",
                           PIP_EXTRA_INDEX_URL="", PIP_CACHE_DIR=str(root / "pip-cache"))
        subprocess.run([str(python), "-m", "pip", "install", "--disable-pip-version-check", str(wheel.resolve())],
                       cwd=root, env=install_env, check=True)

        modules = "import chaski, emitter, graph_sink, incremental, alertmanager_sink, command_sink, external_event"
        subprocess.run([str(python), "-c", modules], cwd=root, check=True)

        def run(*args: str) -> str:
            return subprocess.check_output([str(program), *args], cwd=root, text=True)

        version = run("--version").strip()
        assert version == f"chaski {wheel.name.split('-')[1]}", version
        assert "The chaski CLI" in run("--help")
        proof = root / "proof"
        assert json.loads(run("events", "--directory", str(proof))) == []
        result = json.loads(run("verify-event", "--directory", str(proof), "--marker", "wheel-event-proof"))
        assert result["verified"] is True and result["events"] == 1, result
        assert json.loads(run("events", "--directory", str(proof))) == ["wheel-event-proof"]
        print("PASS: installed wheel version, help, negative control, real event delivery and reader proof")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("wheel", type=Path)
    smoke(parser.parse_args().wheel)
