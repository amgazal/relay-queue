"""Exercise the candidate checkout in an isolated Compose project; retain its volumes."""
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import tempfile
import time
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[1]


def run(work):
    project = "relay-smoke-" + uuid.uuid4().hex[:10]
    env = dict(os.environ, COMPOSE_PROJECT_NAME=project, POSTGRES_PORT="0",
               POSTGRES_USER="relay", POSTGRES_DB="relay", POSTGRES_PASSWORD=secrets.token_hex(20),
               RELAY_API_PORT="0", RELAY_API_KEY=secrets.token_hex(32),
               RELAY_DEMO_SIGNING_SECRET=secrets.token_hex(32),
               RELAY_LEASE="3s", RELAY_REAP_INTERVAL="250ms", RELAY_DEMO_FAIL_FIRST="false")
    env["RELAY_WEBHOOK_DESTINATIONS"] = json.dumps({"local-demo": {
        "url": "https://receiver:8082/events", "secret": env["RELAY_DEMO_SIGNING_SECRET"]}})

    def compose(*args, capture=False):
        return subprocess.run(["docker", "compose", "--profile", "demo", *args],
                              check=True, cwd=work, env=env, text=True,
                              stdout=subprocess.PIPE if capture else None).stdout

    def sql(statement):
        return compose("exec", "-T", "db", "psql", "-XAt", "-U", "relay", "-d", "relay",
                       "-v", "ON_ERROR_STOP=1", "-c", statement, capture=True).strip()

    def until(predicate, label):
        for _ in range(300):
            if predicate():
                return
            time.sleep(0.2)
        raise RuntimeError("timed out: " + label)

    def api(path, body=None):
        headers = {"Authorization": "Bearer " + env["RELAY_API_KEY"], "Content-Type": "application/json"}
        request = urllib.request.Request(base + path, headers=headers,
                                         data=None if body is None else json.dumps(body).encode())
        with urllib.request.urlopen(request, timeout=6) as response:
            return json.load(response)

    def submit(kind, payload, attempts=3):
        return api("/v1/jobs", {"type": kind, "payload": payload, "max_attempts": attempts})["id"]

    def state(job, status):
        until(lambda: api("/v1/jobs/" + job)["status"] == status, status)

    def url():
        return "http://" + compose("port", "api", "8080", capture=True).strip()

    print("Testing isolated project", project, flush=True)
    try:
        compose("config", "-q")
        compose("build")
        compose("up", "-d", "--wait")
        base = url()
        assert api("/readyz")["status"] == "ready"
        assert sql("select count(*) from schema_migrations") == "5"
        state(submit("demo.sleep", {}), "succeeded")
        failed = submit("demo.fail", {}, 2)
        state(failed, "dead")
        assert api("/v1/jobs/" + failed)["attempt_count"] == 2

        schedule = api("/v1/schedules", {"name": "smoke", "type": "demo.sleep", "payload": {},
                                         "cron": "* * * * *"})["id"]
        sql(f"update schedules set next_run=now()-interval '1 minute' where id='{schedule}'")
        until(lambda: sql(f"select count(*) from jobs where schedule_id='{schedule}' and state='succeeded'") != "0",
              "scheduled execution")

        event = str(uuid.uuid4())
        payload = {"destination": "local-demo", "event_id": event, "event_type": "smoke", "data": {}}
        state(submit("webhook.deliver", payload), "succeeded")
        interrupted = submit("demo.wait", {"duration": "3s"})
        state(interrupted, "running")
        compose("kill", "-s", "SIGKILL", "worker")
        compose("up", "-d", "--wait", "--scale", "worker=2")
        base = url()
        state(interrupted, "succeeded")
        assert sql(f"select string_agg(outcome,',' order by attempt_number) from job_attempts where job_id='{interrupted}'") == "expired,succeeded"
        batch = [submit("demo.wait", {"duration": "500ms"}) for _ in range(12)]
        for job in batch:
            state(job, "succeeded")
        ids = ",".join("'" + job + "'" for job in batch)
        assert sql(f"select count(distinct worker_id) from job_attempts where job_id in ({ids})") == "2"
        compose("restart", "db")
        compose("up", "-d", "--wait", "--scale", "worker=2")
        compose("restart", "api")
        compose("up", "-d", "--wait", "--scale", "worker=2")
        base = url()
        state(submit("demo.sleep", {}), "succeeded")
        compose("run", "--rm", "--no-deps", "migrate")
        drain = submit("demo.wait", {"duration": "2s"})
        state(drain, "running")
        compose("stop", "worker")
        state(drain, "succeeded")
        assert api("/v1/jobs/" + drain)["attempt_count"] == 1
        compose("down")
        compose("up", "-d", "--wait")
        base = url()
        state(interrupted, "succeeded")
        state(submit("webhook.deliver", payload), "succeeded")
        assert sql(f"select count(*) from demo_received_events where event_id='{event}'") == "1"
        for service in ("api", "worker", "receiver"):
            image = compose("images", "-q", service, capture=True).strip()
            subprocess.run(["docker", "image", "inspect", "--format",
                            service + " {{.Architecture}} {{.Size}} bytes", image], check=True)
        print("PASS: migrations, jobs, scheduling, webhook, scaling, crash recovery, graceful stop and persistence")
    finally:
        compose("down")


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as directory:
        work = Path(directory)
        files = subprocess.check_output(["git", "ls-files", "-co", "--exclude-standard", "-z"], cwd=ROOT)
        for filename in filter(None, os.fsdecode(files).split("\0")):
            source = ROOT / filename
            if source.is_file():
                (work / filename).parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source, work / filename)
        run(work)
