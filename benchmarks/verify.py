"""Stable, hardware-independent checks on a saved Relay report."""
import json
import sys


def require(condition, message):
    if not condition:
        raise ValueError(message)


def verify(r):
    controls = 3 if r["mode"] == "chaos" else 0
    require(r["load_jobs"] > 0 and r["expected"] == r["load_jobs"] + controls,
            "accepted ledger does not cover the submitted workload")
    require(r["http_requests"] == r["load_jobs"] + r["replays"] and r["http_rejections"] == 0,
            "HTTP counts do not reconcile with admissions and replays")
    require(r["expected"] == r["accepted"] == r["matched"] == r["succeeded"] + r["dead"],
            "accepted IDs and terminal jobs differ")
    require(r["unexpected_ids"] == r["invalid_history"] == r["overlapping_attempts"] == 0,
            "identity, attempt ownership or numbering invariant failed")
    require(r["attempts"] == r["accepted"] + r["extra_attempts"], "inconsistent attempt count")
    require(r["dead"] == (1 if r["mode"] == "chaos" else 0), "unexpected terminal outcome")
    require(r["errors"] == 0 and r["replays"] == (r["load_jobs"] + 9)//10,
            "unexpected HTTP outcomes")
    require(r["elapsed_s"] <= r["deadline_s"], "recovery deadline exceeded")
    if r["mode"] == "webhook":
        require(r["receiver_effects"]==r["accepted"], "missing webhook effects")
    if r["mode"] == "chaos":
        require(r["expired"] >= 1 and r["webhook_history"] == ["expired", "succeeded"],
                "missing crash/recovery evidence")
        require(r["accepted_deliveries"] == 2 and r["receiver_effects"] == 1,
                "receiver deduplication failed")
        require(r["graceful_history"] == ["succeeded"], "graceful shutdown abandoned work")


if __name__ == "__main__":
    require(len(sys.argv)>1,"supply at least one result file")
    for path in sys.argv[1:]:
        report = json.load(open(path))
        require(bool(report.get("plans") and report.get("relations")), "incomplete report")
        command=report["command"]
        mode=command[2]
        repeats=int(command[command.index("--repeats")+1]) if "--repeats" in command else 3
        modes=["api","scale","sustained","webhook","chaos"] if mode=="all" else [mode]
        expected={(name,i,w) for name in modes for i in range(1,(1 if name=="chaos" else repeats)+1)
                  for w in ([1,2,4,8] if name=="scale" else [4])}
        actual=[(r["mode"],r["repeat"],r["replicas"]) for r in report["runs"]]
        require(len(actual)==len(expected) and set(actual)==expected,"missing or repeated measured run")
        for run in report["runs"]:
            verify(run)
        print("Verified", len(report["runs"]), "runs in", path)
