"""Bounded benchmarks against fresh, uniquely named local Compose projects."""
import argparse
import concurrent.futures
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import secrets
import subprocess
import sys
import threading
import time
import urllib.request
import uuid

from verify import require, verify

ROOT = Path(__file__).resolve().parents[1]
K6 = "grafana/k6:1.3.0"


def command(args, env=None, data=None, timeout=300):
    p = subprocess.run(args, cwd=ROOT, env=env, input=data, text=True,
                       capture_output=True, timeout=timeout)
    if p.returncode:
        # k6 log lines contain job IDs; do not echo generated credentials.
        raise RuntimeError(f"{args[0]} exited {p.returncode}: {p.stderr[-2000:]}")
    return p.stdout.strip()


def percentile(values, p):
    ordered = sorted(values)
    index = (len(ordered)-1)*p
    low = math.floor(index)
    return round(ordered[low] + (ordered[math.ceil(index)]-ordered[low])*(index-low), 3)


class Lab:
    def __init__(self):
        self.project = "relay-bench-" + uuid.uuid4().hex[:12]
        self.env = dict(os.environ, COMPOSE_PROJECT_NAME=self.project, POSTGRES_PORT="0",
                        POSTGRES_DB="relay", POSTGRES_USER="relay", POSTGRES_PASSWORD=secrets.token_hex(16),
                        RELAY_API_PORT="0", RELAY_API_KEY=secrets.token_hex(32),
                        RELAY_DEMO_SIGNING_SECRET=secrets.token_hex(32), RELAY_DEMO_FAIL_FIRST="false",
                        RELAY_CONCURRENCY="4", RELAY_LEASE="8s", RELAY_REAP_INTERVAL="500ms",
                        RELAY_POLL_INTERVAL="20ms", RELAY_HANDLER_TIMEOUT="90s",
                        RELAY_MAX_ACTIVE_JOBS="0", RELAY_SCHEDULER_INTERVAL="1s")
        self.env["RELAY_WEBHOOK_DESTINATIONS"] = json.dumps({"demo": {
            "url": "https://receiver:8082/events", "secret": self.env["RELAY_DEMO_SIGNING_SECRET"]}})
        self.compose = ["docker", "compose", "-p", self.project, "--env-file", "/dev/null",
                        "-f", "compose.yaml", "-f", "benchmarks/compose.yaml", "--profile", "demo"]

    def dc(self, *args):
        return command(self.compose + list(args), self.env)

    def sql(self, statement):
        return command(self.compose + ["exec", "-T", "db", "psql", "-XqAt", "-U", "relay",
                                      "-v", "ON_ERROR_STOP=1"], self.env, statement, timeout=30)

    def api(self, kind, prefix, payload=None, priority=0, maximum=5):
        body = dict(type=kind, payload=payload or {}, idempotency_key=prefix, priority=priority,
                    max_attempts=maximum)
        req = urllib.request.Request(self.base+"/v1/jobs", data=json.dumps(body).encode(),
                                     headers={"Authorization": "Bearer "+self.env["RELAY_API_KEY"],
                                              "Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=8) as response:
            require(response.status == 201, "unexpected control submission")
            return json.load(response)["id"]

    def wait(self, predicate, seconds=180):
        end = time.monotonic()+seconds
        while time.monotonic()<end:
            if predicate():
                return
            time.sleep(.2)
        raise TimeoutError("benchmark condition did not hold before deadline")

    def scale(self, n):
        self.dc("up", "-d", "--wait", "--scale", f"worker={n}", "worker")
        self.base = "http://" + self.dc("port", "api", "8080")

    def load(self, prefix, n, kind, rate=0, seconds=0):
        args = ["docker", "run", "--rm", "--network", self.project+"_default",
                "-v", str(ROOT/"benchmarks/load.js")+":/load.js:ro"]
        extra = dict(COUNT=str(n), PREFIX=prefix, KIND=kind, RATE=str(rate), SECONDS=str(seconds))
        for key in [*extra, "RELAY_API_KEY"]:
            args.extend(["-e", key])
        p = subprocess.run(args+[K6, "run", "--quiet", "--log-output=stdout", "--log-format=raw",
                                 "/load.js"], env=self.env | extra, text=True, capture_output=True,
                           timeout=260)
        if p.returncode:
            raise RuntimeError(f"k6 failed: {p.stderr[-1000:]} {p.stdout[-1000:]}")
        rows = [json.loads(line) for line in p.stdout.splitlines() if line.startswith("[")]
        require(sum(r[1]==201 for r in rows)==n, "incomplete k6 workload")
        ids = {i: ident for i,status,ident,_,_ in rows if status==201}
        require(len(ids)==n and len(set(ids.values()))==n and
                all(ids.get(i)==ident for i,_,ident,_,_ in rows), "unexpected replay identity")
        return rows

    def urls(self, replicas):
        return ["http://"+self.dc("port","api","9091")+"/metrics"]+[
            "http://"+self.dc("port","--index",str(i),"worker","9092")+"/metrics"
            for i in range(1,replicas+1)]

    def sample(self, urls):
        values = {}
        for url in urls:
            with urllib.request.urlopen(url,timeout=3) as resp:
                for line in resp.read().decode().splitlines():
                    key, _, value = line.rpartition(" ")
                    if key in ("relay_active_handlers","relay_jobs_completed_total","relay_failed_attempts_total",
                               "relay_expired_attempts_recovered_total","relay_handler_duration_seconds_sum",
                               "relay_handler_duration_seconds_count",'relay_queue_depth{state="queued"}',
                               'relay_http_request_duration_seconds_sum{route="POST /v1/jobs"}',
                               'relay_http_request_duration_seconds_count{route="POST /v1/jobs"}'):
                        values[key]=values.get(key,0)+float(value)
        values["lock_waiters"]=int(self.sql("select count(*) from pg_stat_activity where wait_event_type='Lock'"))
        return values

    def history(self, ident):
        return self.sql(f"select outcome from job_attempts where job_id='{ident}' order by attempt_number").splitlines()


def audit(lab, prefix, ids):
    # Only compare IDs returned by successful HTTP submissions to the durable run ledger.
    rows = lab.sql(f"select id from jobs where starts_with(idempotency_key,'{prefix}-')").splitlines()
    result = dict(matched=len(set(rows)&ids),unexpected_ids=len(set(rows)-ids),
                  accepted=len(ids),ledger_sha256=hashlib.sha256("\n".join(sorted(ids)).encode()).hexdigest())
    result.update(json.loads(lab.sql(f"""
    with j as (select * from jobs where starts_with(idempotency_key,'{prefix}-')),
    a as (select a.*,lag(finished_at) over(partition by job_id order by attempt_number) as prev
          from job_attempts a join j on j.id=a.job_id),
    checks as (select j.id,j.state,j.attempt_count,j.max_attempts,count(a.job_id) n,
      max(attempt_number) last_n, count(*) filter(where prev>started_at) overlap_n,
      bool_and(finished_at is not null and outcome<>'running') closed,
      count(*) filter(where outcome='expired') expired,
      bool_or(attempt_number=attempt_count and ((state='succeeded' and outcome='succeeded')
        or (state='dead' and outcome in ('failed','expired')))) final_ok
      from j left join a on a.job_id=j.id group by j.id,j.state,j.attempt_count,j.max_attempts)
    select json_build_object('succeeded',count(*) filter(where state='succeeded'),'dead',count(*) filter(where state='dead'),
    'attempts',sum(n),'extra_attempts',sum(greatest(n-1,0)),'expired',sum(expired),'overlapping_attempts',sum(overlap_n),
    'invalid_history',count(*) filter(where n<>attempt_count or n>max_attempts or last_n<>n or not closed or not final_ok))
    from checks;""")))
    return result


def measured(lab, mode, replicas, n, idx, rate, seconds, ledger_dir=None):
    prefix = "r"+uuid.uuid4().hex
    kind = {"sustained":"demo.sleep","webhook":"webhook.deliver"}.get(mode,"demo.noop")
    lab.scale(replicas)
    # Each measured run warms its processes with 64 unique jobs and drains them.
    warm_start=time.monotonic()
    lab.load(prefix+"warm",64,kind)
    lab.wait(lambda: lab.sql("select count(*) from jobs where state in ('queued','running')")=="0")
    warm_s=round(time.monotonic()-warm_start,3)
    signals={}
    control=set()
    if mode=="chaos":
        # Grace proof before the kill. Only this project's workers are affected.
        grace=lab.api("demo.wait",prefix+"-drain",{"duration":"4s"})
        control.add(grace)
        lab.wait(lambda:lab.sql(f"select state from jobs where id='{grace}'")=="running")
        lab.dc("stop","worker")
        signals["graceful_history"]=lab.history(grace)
        lab.sql("""create table bench_deliveries(id uuid);
          create function bench_receipt() returns trigger language plpgsql as $$
          begin insert into bench_deliveries values(new.event_id); return new; end $$;
          create trigger bench_receipt before insert on demo_received_events for each row execute function bench_receipt();
          create function bench_gate() returns trigger language plpgsql as $$
          begin perform pg_sleep(60); return new; end $$;
          create trigger bench_gate before update on jobs for each row
          when (new.kind='webhook.deliver' and new.state='succeeded' and new.attempt_count=1)
          execute function bench_gate();""")
        event=str(uuid.uuid4())
        hook=lab.api("webhook.deliver",prefix+"-hook",dict(destination="demo",event_id=event,event_type="bench",data={}),100)
        control.update([hook,lab.api("demo.fail",prefix+"-dead",maximum=1)])
    urls=[] if mode=="chaos" else lab.urls(replicas)
    samples=[]
    begin=time.monotonic()
    def collect(stop):
        while not stop.is_set():
            snap=lab.sample(urls)
            snap["elapsed_s"]=round(time.monotonic()-begin,3)
            samples.append(snap)
            stop.wait(1)
    stop=threading.Event()
    with concurrent.futures.ThreadPoolExecutor(2) as executor:
        monitor=executor.submit(collect,stop) if urls else None
        # Capture one live database resource sample without blocking the request generator.
        def resources():
            containers=lab.dc("ps","-q","db","worker").splitlines()
            lines=command(["docker","stats","--no-stream","--format","{{json .}}",*containers])
            return [{k: sample[k] for k in ("Name","CPUPerc","MemUsage")} for sample in map(json.loads,lines.splitlines())]
        stats=executor.submit(resources)
        try:
            rows=lab.load(prefix,n,kind,rate,seconds)
            ids={r[2] for r in rows if r[1]==201}|control
            if ledger_dir is not None:
                ledger_dir.mkdir(parents=True, exist_ok=True)
                ledger = ledger_dir / f"{lab.project}-{mode}-{replicas}-{idx}.txt"
                ledger.write_text("\n".join(sorted(ids)))
            if mode=="chaos":
                lab.dc("start","worker")
                lab.wait(lambda: lab.sql("select count(*) from pg_stat_activity where wait_event='PgSleep'")=="1",30)
                require(lab.sql(f"select count(*) from demo_received_events where event_id='{event}'")=="1","no accepted effect before kill")
                lab.dc("kill","-s","SIGKILL","worker")
                # PostgreSQL may still be sleeping on a disconnected socket. End the test-only
                # backend so its uncommitted completion rolls back immediately.
                lab.sql("select pg_terminate_backend(pid) from pg_stat_activity where wait_event='PgSleep'")
                lab.scale(replicas)
            lab.wait(lambda: lab.sql(f"select count(*) from jobs where starts_with(idempotency_key,'{prefix}-') and state in ('queued','running')")=="0")
        finally:
            stop.set()
        if monitor:
            monitor.result()
            final=lab.sample(urls);final["elapsed_s"]=round(time.monotonic()-begin,3);samples.append(final)
        usage=stats.result()
    elapsed=time.monotonic()-begin
    r=audit(lab,prefix,ids)
    if mode=="webhook":
        signals["receiver_effects"]=int(lab.sql(f"""select count(*) from jobs j join demo_received_events e on
          e.event_id=(j.payload->>'event_id')::uuid where starts_with(j.idempotency_key,'{prefix}-')"""))
    if mode=="chaos":
        signals.update(webhook_history=lab.history(hook),accepted_deliveries=int(lab.sql("select count(*) from bench_deliveries")),
                       receiver_effects=int(lab.sql(f"select count(*) from demo_received_events where event_id='{event}'")))
    timing=json.loads(lab.sql(f"""select json_build_object('latency',percentile_cont(array[0.5,0.95,0.99])
      within group(order by extract(epoch from completed_at-created_at)*1000),
      'span',extract(epoch from max(completed_at)-min(created_at)),
      'drain',greatest(0,extract(epoch from max(completed_at)-max(created_at))))
      from jobs where starts_with(idempotency_key,'{prefix}-')"""))
    span=(max(r[4] for r in rows)-min(r[4]-r[3] for r in rows))/1000
    r.update(signals,mode=mode,replicas=replicas,repeat=idx,expected=n+len(control),load_jobs=n,
             errors=sum(x[1] not in (200,201) for x in rows),replays=sum(x[1]==200 for x in rows),
             elapsed_s=round(elapsed,3),deadline_s=300,http_requests=len(rows),http_span_s=round(span,3),
             request_rps=round(len(rows)/span,2),admitted_rps=round(n/span,2),
             enqueue_ms=[percentile([row[3] for row in rows if row[1]==201],p) for p in (.5,.95,.99)],
             e2e_ms=[round(v,3) for v in timing["latency"]],completion_rps=round(r["succeeded"]/float(timing["span"]),2),
             drain_s=round(float(timing["drain"]),3),samples=samples,resources=usage,warmup_s=warm_s,http_rejections=sum(x[1]==429 for x in rows))
    verify(r)
    return r


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("mode",choices=["chaos","scale","api","sustained","webhook","all"])
    parser.add_argument("--jobs",type=int,default=2000)
    parser.add_argument("--chaos-jobs",type=int,default=10000)
    parser.add_argument("--repeats",type=int,default=3)
    parser.add_argument("--rate",type=int,default=100)
    parser.add_argument("--seconds",type=int,default=20)
    parser.add_argument("--output",type=Path,default=Path("benchmarks/local.json"))
    parser.add_argument("--ledger-dir",type=Path,help="retain complete accepted UUID sets outside the compact report")
    a=parser.parse_args()
    require(1<=a.jobs<=20000 and 1<=a.chaos_jobs<=20000 and 1<=a.repeats<=5 and
            1<=a.seconds<=120 and 1<=a.rate<=500 and a.rate*a.seconds<=30000,"workload outside bounds")
    lab=Lab()
    print("Isolated project:",lab.project,flush=True)
    try:
        lab.dc("config","-q")
        lab.dc("up","--build","-d","--wait")
        info=json.loads(command(["docker","info","--format","{{json .}}"]))
        hardware=dict(architecture=info["Architecture"],docker_cpus=info["NCPU"],docker_bytes=info["MemTotal"],
                      engine=info["ServerVersion"],host=platform.platform(),host_cpus=os.cpu_count())
        if sys.platform=="darwin":
            hardware["host_bytes"]=int(command(["sysctl","-n","hw.memsize"]))
            hardware["cpu"]=command(["sysctl","-n","machdep.cpu.brand_string"])
        sizes_sql="""select relname,pg_table_size(oid),pg_indexes_size(oid) from pg_class where relname in ('jobs','job_attempts') order by relname"""
        report=dict(initial_relations=lab.sql(sizes_sql),project=lab.project,command=["python3","benchmarks/run.py",*sys.argv[1:]],hardware=hardware,
                    go=command(["docker","run","--rm","golang:1.27","go","version"]),
                    postgres=lab.sql("select version()"),k6=K6,commit=command(["git","rev-parse","HEAD"]),
                    settings=json.loads(lab.sql("""select json_object_agg(name,setting||coalesce(unit,'')) from pg_settings where name in
                    ('shared_buffers','work_mem','max_connections','fsync','synchronous_commit','max_wal_size','effective_cache_size','autovacuum')""")),runs=[])
        for mode in (["api","scale","sustained","webhook","chaos"] if a.mode=="all" else [a.mode]):
            for i in range(1, (1 if mode=="chaos" else a.repeats)+1):
                workers=[1,2,4,8] if mode=="scale" else [4]
                if i%2==0: workers.reverse()
                for w in workers:
                    n=a.chaos_jobs if mode=="chaos" else a.rate*a.seconds if mode=="sustained" else a.jobs
                    r=measured(lab,mode,w,n,i,a.rate if mode=="sustained" else 0,a.seconds,a.ledger_dir)
                    report["runs"].append(r)
                    a.output.write_text(json.dumps(report,indent=2)+"\n")
                    print(mode,w,i,n,r["completion_rps"],flush=True)
        report["relations"]=lab.sql("""select relname,pg_table_size(oid),pg_indexes_size(oid) from pg_class
          where relname in ('jobs','job_attempts') order by relname""")
        report["plans"]=lab.sql("""begin;
          insert into jobs(kind,payload) select 'demo.noop','{}' from generate_series(1,500);
          analyze jobs;
          explain(analyze,buffers) select id from jobs where state='queued' and available_at<=statement_timestamp() and attempt_count<max_attempts
          order by priority desc,available_at,id limit 1 for update skip locked;
          explain(analyze,buffers) select state,count(*) from jobs group by state;
          explain(analyze,buffers) insert into jobs(kind,payload) values('demo.noop','{}');
          explain(analyze,buffers) insert into job_attempts(job_id,attempt_number,worker_id,lock_token)
            select id,1,'plan',gen_random_uuid() from jobs where kind='demo.noop' and attempt_count=0 limit 1;
          rollback;""")
        a.output.write_text(json.dumps(report,indent=2)+"\n")
    finally:
        lab.dc("down","--volumes")


if __name__=="__main__":
    main()
