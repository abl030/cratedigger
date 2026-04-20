"""Ephemeral slskd container for integration tests.

Spins up a real slskd instance in Docker on a random port, connects to
the Soulseek network with provided credentials, and tears down on exit.

Usage:
    from ephemeral_slskd import EphemeralSlskd

    slskd = EphemeralSlskd("tests/.slskd-creds.json")
    slskd.start()        # Starts container, waits for API + Soulseek login
    # ... run tests against slskd.host_url with slskd.api_key ...
    slskd.stop()

Or as a context manager:
    with EphemeralSlskd("tests/.slskd-creds.json") as s:
        client = slskd_api.SlskdClient(host=s.host_url, api_key=s.api_key)
"""

import atexit
import json
import os
import shutil
import socket
import subprocess
import tempfile
import time
import urllib.request
import urllib.error


def _find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


SLSKD_IMAGE = "slskd/slskd:0.21.4"


def _docker_cmd():
    """Return docker command with correct socket. Handles podman-wrapped docker."""
    env = dict(os.environ)
    # If the default socket doesn't work, try the real docker socket
    if os.path.exists("/var/run/docker.sock"):
        env["DOCKER_HOST"] = "unix:///var/run/docker.sock"
    return env


class EphemeralSlskd:
    def __init__(self, creds_file):
        self.creds_file = creds_file
        self.port = None
        self.api_key = None
        self.host_url = None
        self.download_dir = None
        self.container_id = None
        self._tmpdir = None
        self._started = False

    def start(self):
        if self._started:
            return

        if not shutil.which("docker"):
            raise RuntimeError("docker not found on PATH")

        if not os.path.exists(self.creds_file):
            raise RuntimeError(f"Credentials file not found: {self.creds_file}")

        with open(self.creds_file) as f:
            creds = json.load(f)

        username = creds["username"]
        password = creds["password"]
        self.api_key = creds.get("api_key", "cratedigger-test-key")

        self.port = _find_free_port()
        self._tmpdir = tempfile.mkdtemp(prefix="cratedigger_test_slskd_")
        self.download_dir = os.path.join(self._tmpdir, "downloads")
        os.makedirs(self.download_dir)

        docker_env = _docker_cmd()

        # Write slskd YAML config
        config_dir = os.path.join(self._tmpdir, "config")
        os.makedirs(config_dir)
        config_path = os.path.join(config_dir, "slskd.yml")
        with open(config_path, "w") as f:
            f.write(f"""soulseek:
  username: {username}
  password: {password}
  description: cratedigger integration test
web:
  port: 5030
  authentication:
    disabled: false
    api_keys:
      cratedigger_test:
        key: {self.api_key}
        role: administrator
        cidr: 0.0.0.0/0,::/0
directories:
  downloads: /downloads
flags:
  no_share_scan: true
  no_version_check: true
""")

        # Pull image if not present (suppress output)
        subprocess.run(
            ["docker", "pull", SLSKD_IMAGE],
            capture_output=True, timeout=120, env=docker_env,
        )

        # Start container
        result = subprocess.run(
            [
                "docker", "run", "-d", "--rm",
                "-p", f"127.0.0.1:{self.port}:5030",
                "-v", f"{self.download_dir}:/downloads",
                "-v", f"{config_dir}:/app",
                SLSKD_IMAGE,
            ],
            capture_output=True, text=True, timeout=30, env=docker_env,
        )
        if result.returncode != 0:
            raise RuntimeError(f"docker run failed: {result.stderr}")

        self.container_id = result.stdout.strip()
        self.host_url = f"http://127.0.0.1:{self.port}"

        # Wait for API readiness (first boot can be slow)
        self._wait_for_api(timeout=60)
        self._started = True
        atexit.register(self.stop)

    def _wait_for_api(self, timeout=30):
        """Wait for slskd HTTP API to respond."""
        assert self.api_key is not None
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                req = urllib.request.Request(
                    f"{self.host_url}/api/v0/application",
                    headers={"X-API-Key": self.api_key},
                )
                with urllib.request.urlopen(req, timeout=2) as resp:
                    if resp.status == 200:
                        return
            except (urllib.error.URLError, OSError, TimeoutError):
                pass
            time.sleep(0.5)
        self.stop()
        raise RuntimeError(f"slskd API not ready after {timeout}s")

    def wait_for_soulseek(self, timeout=60):
        """Wait for Soulseek network connection (logged in)."""
        assert self.api_key is not None
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                req = urllib.request.Request(
                    f"{self.host_url}/api/v0/server",
                    headers={"X-API-Key": self.api_key},
                )
                with urllib.request.urlopen(req, timeout=2) as resp:
                    data = json.loads(resp.read())
                    if data.get("isConnected") and data.get("isLoggedIn"):
                        return True
            except (urllib.error.URLError, OSError, TimeoutError, json.JSONDecodeError):
                pass
            time.sleep(1)
        return False

    def stop(self):
        if not self._started:
            return
        self._started = False

        if self.container_id:
            subprocess.run(
                ["docker", "stop", self.container_id],
                capture_output=True, timeout=15, env=_docker_cmd(),
            )
            self.container_id = None

        if self._tmpdir:
            # Container creates files as root — need sudo or ignore
            subprocess.run(
                ["docker", "run", "--rm", "-v", f"{self._tmpdir}:/cleanup",
                 "alpine", "rm", "-rf", "/cleanup"],
                capture_output=True, timeout=15, env=_docker_cmd(),
            )
            shutil.rmtree(self._tmpdir, ignore_errors=True)
            self._tmpdir = None

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *args):
        self.stop()
