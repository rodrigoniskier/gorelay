#!/usr/bin/env python3
"""Start the optional Linux Docker loopback demo without printing credentials.
Requires the compose PostgreSQL container and a locally built demo image.
Existing containers and volumes are never removed by this script.
"""
import argparse
import os
import pathlib
import json
import subprocess
from urllib.parse import quote

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--native', type=pathlib.Path, help='run an explicitly built Go binary on this host (WSL)')
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
if args.native:
    binary = args.native.resolve(strict=True)
    if not binary.is_file():
        raise SystemExit('native binary must be a file')
    appenv = os.environ.copy()
    appenv.update(DATABASE_URL=dsn, GORELAY_ADDR='127.0.0.1:18084',
                  GORELAY_DASHBOARD_DEMO='1', GORELAY_WORKERS='4',
                  GORELAY_RETRY_BASE_DELAY='1s', GORELAY_RETRY_MAX_DELAY='4s')
    # Existing HTTP target variables cannot alter the built-in fixed recipient.
    try:
        subprocess.run([str(binary)], env=appenv, check=True)
    except KeyboardInterrupt:
        pass  # The child receives SIGINT and Go joins its workers on shutdown.
else:
    # Never remove or reuse an existing container: it may hold a stale configuration.
    if subprocess.run(["docker", "container", "inspect", "gorelay-dashboard-demo"],
                      stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0:
        raise SystemExit("Container gorelay-dashboard-demo already exists. Review it "
                         "(docker inspect / docker logs) and see docs/TROUBLESHOOTING.md; "
                         "this script will not delete or silently reuse it.")
    # DATABASE_URL is passed by name only so the password does not appear in `ps`
    # output or the command line (it remains in the container environment).
    dockerenv = os.environ.copy()
    dockerenv["DATABASE_URL"] = dsn
    subprocess.run(["docker", "run", "--name", "gorelay-dashboard-demo", "--network", "host",
                    "-e", "DATABASE_URL", "-e", "GORELAY_ADDR=127.0.0.1:18084",
                    "-e", "GORELAY_DASHBOARD_DEMO=1", "-e", "GORELAY_WORKERS=4",
                    "-e", "GORELAY_RETRY_BASE_DELAY=1s", "-e", "GORELAY_RETRY_MAX_DELAY=4s",
                    "gorelay:dashboard-demo"], check=True, env=dockerenv)
