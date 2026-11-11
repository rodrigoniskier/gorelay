#!/usr/bin/env python3
"""Start the optional loopback dashboard demo without printing credentials.
Requires the compose PostgreSQL container and a locally built demo image.
Existing containers and volumes are never removed by this script. By default it
runs the demo as a container, except on Docker Desktop, where `--network host`
does not reach the host's loopback: on a Linux host (including WSL) it copies the
static binary out of the image and runs it natively instead (a throw-away container created and removed
by this script is the only container it ever deletes).
"""
import argparse
import collections
import os
import pathlib
import json
import subprocess
import sys
import tempfile
from urllib.parse import quote

IMAGE = "gorelay:dashboard-demo"
CONTAINER = "gorelay-dashboard-demo"

HINTS = (
    (("28P01", "password authentication failed"),
     "PostgreSQL rejected the password. A compose volume keeps the password it was first "
     "created with, so a changed POSTGRES_PASSWORD in .env is ignored. Either put the old "
     "password back in .env, or set the database password to the .env value without losing "
     "data:  docker compose exec postgres psql -U <user> -d <db>  then  \\password <user>"),
    (("connection refused", "no such host"),
     "PostgreSQL is not reachable. Check `docker compose ps` and that POSTGRES_PORT in .env "
     "is the published loopback port."),
    (("checksum mismatch", "unknown migrations", "history has a gap"),
     "The schema was created by a different GoRelay build. Use a fresh database or schema "
     "instead of editing migrations or dropping data you may need."),
)


def docker_desktop():
    try:
        out = subprocess.check_output(["docker", "info", "--format", "{{.OperatingSystem}}"],
                                      stderr=subprocess.DEVNULL, timeout=30)
    except (OSError, subprocess.SubprocessError):
        return False
    return "docker desktop" in out.decode(errors="replace").lower()


def run_app(command, env, kept=None):
    """Run the app, mirroring its stderr live, and explain a failed start."""
    tail = collections.deque(maxlen=50)
    proc = subprocess.Popen(command, env=env, stderr=subprocess.PIPE, text=True, errors="replace")
    try:
        for line in proc.stderr:
            sys.stderr.write(line)
            sys.stderr.flush()
            tail.append(line)
        code = proc.wait()
    except KeyboardInterrupt:
        # The terminal's Ctrl+C reaches the child too; Go joins its workers on shutdown.
        code = proc.wait()
        return
    if code == 0:
        return
    text = "".join(tail)
    for needles, hint in HINTS:
        if any(n in text for n in needles):
            print(f"\nHint: {hint}", file=sys.stderr)
            break
    if kept:
        print(f"\nThe stopped container {kept} is kept; inspect it, then remove it yourself "
              "before retrying.", file=sys.stderr)
    raise SystemExit(f"GoRelay exited with status {code}. The reason is in the ERROR line above; "
                     "see docs/TROUBLESHOOTING.md.")


def extract_binary(directory):
    """Copy the static binary out of the demo image using a throw-away container."""
    name = f"gorelay-demo-extract-{os.getpid()}"
    target = pathlib.Path(directory) / "gorelay"
    try:
        subprocess.run(["docker", "create", "--name", name, IMAGE], check=True,
                       stdout=subprocess.DEVNULL)
        try:
            subprocess.run(["docker", "cp", f"{name}:/gorelay", str(target)], check=True,
                           stdout=subprocess.DEVNULL)
        finally:
            subprocess.run(["docker", "rm", name], stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL)
    except (OSError, subprocess.CalledProcessError) as exc:
        raise SystemExit(f"Could not extract the binary from image {IMAGE} ({exc}). Build it "
                         f"first: docker build -t {IMAGE} .")
    target.chmod(0o700)
    return target


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--native', type=pathlib.Path,
                    help='run an explicitly built Go binary on this host (WSL)')
parser.add_argument('--docker', action='store_true',
                    help='always run the demo as a container, even on Docker Desktop')
args = parser.parse_args()
try:
    ps_lines = subprocess.check_output([
        "docker", "compose", "ps", "--format", "json", "postgres"
    ]).decode().splitlines()
except (OSError, subprocess.CalledProcessError) as exc:
    raise SystemExit(f"Could not query Docker Compose ({exc}). Is Docker running?")
if not ps_lines:
    raise SystemExit("The PostgreSQL container is not running. Start it first: "
                     "docker compose up -d --wait postgres")
info = json.loads(ps_lines[0])
container = info["Name"]
config = json.loads(subprocess.check_output(["docker", "inspect", container]))[0]
env = dict(item.split("=", 1) for item in config["Config"]["Env"])
port = next((item["HostPort"] for item in config["NetworkSettings"]["Ports"]["5432/tcp"]
             if item["HostIp"] == "127.0.0.1"), None)
if port is None:
    raise SystemExit("PostgreSQL must publish its port only on loopback")
subprocess.run(["docker", "exec", container, "psql", "-U", env["POSTGRES_USER"],
                "-d", env["POSTGRES_DB"], "-v", "ON_ERROR_STOP=1", "-c",
                "CREATE SCHEMA IF NOT EXISTS gorelay_dashboard_demo"], check=True,
               stdout=subprocess.DEVNULL)
dsn = ("postgres://" + quote(env["POSTGRES_USER"], safe="") + ":" +
       quote(env["POSTGRES_PASSWORD"], safe="") + "@127.0.0.1:" + port + "/" +
       quote(env["POSTGRES_DB"], safe="") +
       "?sslmode=disable&search_path=gorelay_dashboard_demo")
# Existing HTTP target variables cannot alter the built-in fixed recipient.
appenv = os.environ.copy()
appenv.update(DATABASE_URL=dsn, GORELAY_ADDR='127.0.0.1:18084',
              GORELAY_DASHBOARD_DEMO='1', GORELAY_WORKERS='4',
              GORELAY_RETRY_BASE_DELAY='1s', GORELAY_RETRY_MAX_DELAY='4s')
use_native = args.native is not None
if not use_native and not args.docker and docker_desktop():
    if sys.platform.startswith("linux"):
        print("Docker Desktop detected: running the demo natively, because `--network host` "
              "would open the port inside Docker's VM, not on this machine.", file=sys.stderr)
        use_native = True
    else:
        # The image binary is a Linux executable; it cannot run on a macOS/Windows host.
        print("Docker Desktop on a non-Linux host detected: the container's `--network host` "
              "port may not be reachable from your browser, and the image's Linux binary "
              "cannot run here. If the dashboard does not open, build a host binary with "
              "`go build -o gorelay ./cmd/gorelay` and run `--native ./gorelay`.",
              file=sys.stderr)
if use_native:
    if args.native:
        binary = args.native.resolve(strict=True)
        if not binary.is_file():
            raise SystemExit('native binary must be a file')
        run_app([str(binary)], appenv)
    else:
        with tempfile.TemporaryDirectory(prefix="gorelay-demo-") as directory:
            run_app([str(extract_binary(directory))], appenv)
else:
    # Never remove or reuse an existing container: it may hold a stale configuration.
    if subprocess.run(["docker", "container", "inspect", CONTAINER],
                      stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0:
        raise SystemExit(f"Container {CONTAINER} already exists. Review it "
                         "(docker inspect / docker logs) and see docs/TROUBLESHOOTING.md; "
                         "this script will not delete or silently reuse it.")
    # DATABASE_URL is passed by name only so the password does not appear in `ps`
    # output or the command line (it remains in the container environment).
    run_app(["docker", "run", "--name", CONTAINER, "--network", "host",
             "-e", "DATABASE_URL", "-e", "GORELAY_ADDR=127.0.0.1:18084",
             "-e", "GORELAY_DASHBOARD_DEMO=1", "-e", "GORELAY_WORKERS=4",
             "-e", "GORELAY_RETRY_BASE_DELAY=1s", "-e", "GORELAY_RETRY_MAX_DELAY=4s",
             IMAGE], appenv, kept=CONTAINER)
