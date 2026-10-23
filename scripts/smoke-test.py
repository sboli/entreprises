#!/usr/bin/env python3
"""Run a built container against small generated stocks; never fetch the real registry."""
import argparse
import csv
import io
import json
import os
from pathlib import Path
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
import zipfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def docker(*args, check=True):
    return subprocess.run(["docker", *args], check=check, text=True, capture_output=True)


def stock_zip(header, rows, filename):
    text = io.StringIO(newline="")
    writer = csv.DictWriter(text, fieldnames=header.split(","))
    writer.writeheader()
    writer.writerows(rows)
    data = io.BytesIO()
    with zipfile.ZipFile(data, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(filename, text.getvalue())
    return data.getvalue()


def stocks():
    establishments = []
    for i in range(1, 55):
        establishments.append({
            "siren": "012345678", "siret": f"012345678{i:05d}",
            "statutDiffusionEtablissement": "P" if i == 54 else "O",
            "etablissementSiege": str(i == 2).lower(),
            "etatAdministratifEtablissement": "F" if i == 53 else "A",
            "enseigne1Etablissement": "Boutique, café", "enseigne2Etablissement": "Alias Secret",
            "numeroVoieEtablissement": "7", "typeVoieEtablissement": "RUE",
            "libelleVoieEtablissement": "DU PORT", "codePostalEtablissement": "97110",
            "libelleCommuneEtablissement": "Pointe-à-Pitre", "activitePrincipaleEtablissement": "47.11A",
        })
    est = stock_zip(
        "siren,siret,statutDiffusionEtablissement,etablissementSiege,etatAdministratifEtablissement,denominationUsuelleEtablissement,enseigne1Etablissement,enseigne2Etablissement,enseigne3Etablissement,numeroVoieEtablissement,indiceRepetitionEtablissement,typeVoieEtablissement,libelleVoieEtablissement,codePostalEtablissement,libelleCommuneEtablissement,activitePrincipaleEtablissement",
        establishments, "StockEtablissement_utf8.csv")
    legal = stock_zip(
        "siren,statutDiffusionUniteLegale,etatAdministratifUniteLegale,denominationUniteLegale,nomUniteLegale,prenom1UniteLegale,denominationUsuelle1UniteLegale,denominationUsuelle2UniteLegale,denominationUsuelle3UniteLegale,sigleUniteLegale,categorieJuridiqueUniteLegale",
        [{"siren": "012345678", "statutDiffusionUniteLegale": "O", "etatAdministratifUniteLegale": "A", "denominationUniteLegale": "Société Étoile", "categorieJuridiqueUniteLegale": "5710"}],
        "StockUniteLegale_utf8.csv")
    return est, legal


def wait_for(predicate, label, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.1)
    raise AssertionError(f"timed out waiting for {label}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default="entreprises:local-test")
    parser.add_argument("--signup-dir", type=Path, help="Also test external signup handlers against this container")
    args = parser.parse_args()
    archives = stocks()
    state = {"fail": False, "requests": 0}

    class FixtureHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            state["requests"] += 1
            if state["fail"]:
                self.send_error(503)
                return
            if self.path == "/est":
                self.send_response(307)
                self.send_header("Location", "/est.zip")
                self.end_headers()
                return
            if self.path not in ("/est.zip", "/legal.zip"):
                self.send_error(404)
                return
            body = archives[0 if self.path == "/est.zip" else 1]
            self.send_response(200)
            self.send_header("Content-Type", "application/zip")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("0.0.0.0", 0), FixtureHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    volume = "entreprises-smoke-" + uuid.uuid4().hex
    container = None
    # A synthetic key used only by this temporary local container.
    key = uuid.uuid4().hex
    docker("volume", "create", volume)
    try:
        fixture_base = f"http://host.docker.internal:{server.server_port}"
        container = docker(
            "run", "-d", "--read-only", "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
            "--tmpfs", "/tmp:rw,noexec,nosuid,size=64m", "--memory", "1g", "--cpus", "2",
            "--add-host", "host.docker.internal:host-gateway", "-p", "127.0.0.1::8080",
            "-v", volume + ":/data", "-e", "API_KEY=" + key,
            "-e", "ESTABLISHMENTS_URL=" + fixture_base + "/est",
            "-e", "LEGAL_UNITS_URL=" + fixture_base + "/legal.zip",
            "-e", "DUCKDB_MEMORY_LIMIT=64MiB", "-e", "DUCKDB_IMPORT_MEMORY_LIMIT=256MiB",
            "-e", "DUCKDB_THREADS=1",
            "-e", "REFRESH_INTERVAL=5s", "-e", "RETRY_INTERVAL=1s", "-e", "REFRESH_TIMEOUT=20s",
            args.image).stdout.strip()
        address = docker("port", container, "8080/tcp").stdout.strip()
        base = "http://" + address

        def request(path, auth=True):
            headers = {"Authorization": "Bearer " + key} if auth else {}
            try:
                with urllib.request.urlopen(urllib.request.Request(base + path, headers=headers), timeout=3) as response:
                    return response.status, json.load(response)
            except urllib.error.HTTPError as error:
                return error.code, json.load(error)

        def ready():
            try:
                return request("/readyz", False)[0] == 200
            except (OSError, ValueError):
                return False

        wait_for(ready, "automatic first import")
        assert state["requests"] >= 3, "download redirect was not followed"
        assert request("/api/status", False)[0] == 401
        status, first = request("/api/companies?q=etoile")
        assert status == 200 and len(first["companies"]) == 50 and first["next_cursor"]
        assert first["companies"][0]["legal_name"] == "Société Étoile"
        cursor = first["next_cursor"]
        status, second = request("/api/companies?q=etoile&cursor=" + urllib.parse.quote(cursor))
        assert status == 200 and len(second["companies"]) == 2 and second["next_cursor"] is None
        assert request("/api/companies?q=012%20345%20678")[1]["companies"][0]["siret"] == "01234567800002"
        detail = request("/api/pappers-company?siret=01234567800001")[1]["company"]
        assert detail["address"] == "7 RUE DU PORT" and detail["details_loaded"]
        assert request("/api/companies/01234567800054")[0] == 404
        assert request("/api/companies/01234567800053")[1]["company"]["active"] is False
        assert docker("exec", container, "/usr/local/bin/entreprises", "healthcheck").returncode == 0
        print("PASS: non-root read-only container, automatic bootstrap, redirect, auth, search, pagination, details, privacy, health", flush=True)

        if args.signup_dir:
            env = dict(os.environ, REGISTRY_TEST_URL=base, REGISTRY_TEST_KEY=key)
            subprocess.run(["node", "--test", "signup/tests/entreprises.integration.test.js"], cwd=args.signup_dir, env=env, check=True)

        version = request("/api/status")[1]["snapshot"]
        # The short test interval exercises the same scheduler as the seven-day default.
        wait_for(lambda: request("/api/status")[1]["snapshot"] != version, "scheduled refresh")
        assert request("/api/companies?q=etoile&cursor=" + urllib.parse.quote(cursor))[0] == 409
        state["fail"] = True
        version = request("/api/status")[1]["snapshot"]
        wait_for(lambda: bool(request("/api/status")[1]["last_error"]), "failed scheduled download")
        assert request("/api/status")[1]["snapshot"] == version
        assert request("/api/companies?q=etoile")[0] == 200
        state["fail"] = False
        wait_for(lambda: request("/api/status")[1]["snapshot"] != version, "retry recovery")
        print("PASS: scheduled refresh, stale-cursor rejection, old snapshot survives failure, automatic retry", flush=True)

        version = request("/api/status")[1]["snapshot"]
        docker("restart", "--time", "10", container)
        # Docker may assign a new ephemeral host port after restart.
        base = "http://" + docker("port", container, "8080/tcp").stdout.strip()
        wait_for(ready, "restart readiness")
        assert request("/api/status")[1]["snapshot"] == version
        assert request("/api/companies?q=etoile")[0] == 200
        print("PASS: restart loads the persisted snapshot", flush=True)
        lock_attempt = docker("exec", container, "/usr/local/bin/entreprises", check=False)
        assert lock_attempt.returncode != 0 and "data directory already in use" in lock_attempt.stderr
        docker("stop", "--time", "10", container)
        inspection = json.loads(docker("inspect", container).stdout)[0]
        assert inspection["State"]["ExitCode"] == 0, inspection["State"]
        print("PASS: volume ownership lock and graceful shutdown", flush=True)
    except BaseException:
        if container:
            print(docker("logs", container, check=False).stderr)
        raise
    finally:
        if container:
            docker("rm", "-f", container, check=False)
        docker("volume", "rm", volume, check=False)
        server.shutdown()
        server.server_close()


if __name__ == "__main__":
    main()
