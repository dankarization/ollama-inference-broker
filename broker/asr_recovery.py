"""Local, exact-fence operator recovery after a verified executor restart.

This is deliberately not an unauthenticated HTTP mutation. The operator must
stop dispatch, prove the old executor process has exited and inspect GPU
activity before running it with the existing private worker credential.
"""
import argparse
import os
from pathlib import Path
import sqlite3
import time

from .asr import MODEL, WhisperHTTP
from .__main__ import storage_token


def recover(database, worker, request_id, job_id, *, executor_stopped=False, dispatch_stopped=False):
    if not executor_stopped or not dispatch_stopped:
        raise ValueError("operator must confirm old executor exited and dispatch stopped")
    path = Path(database)
    if not path.is_file() or path.is_symlink():
        raise ValueError("existing local database required")
    if os.name != "nt" and path.stat().st_uid != os.getuid():
        raise ValueError("database must belong to the operator")
    status = worker.status()
    if not (status.get("model") == MODEL and status.get("active_jobs") == 0
            and status.get("model_loaded") is False
            and status.get("gpu_release_confirmed") is True):
        raise ValueError("worker has not confirmed idle GPU release")
    with sqlite3.connect(str(path), timeout=5) as db:
        db.execute("PRAGMA synchronous=FULL")
        db.execute("BEGIN IMMEDIATE")
        row = db.execute("SELECT request_id,job_id FROM gpu_executor_fences WHERE resource='mainpc-gpu'").fetchone()
        if row != (request_id, job_id):
            raise ValueError("exact fence identity mismatch")
        if db.execute("SELECT 1 FROM jobs WHERE state IN ('running','cancel_requested') LIMIT 1").fetchone():
            raise ValueError("active broker work prevents recovery")
        db.execute("CREATE TABLE IF NOT EXISTS asr_fence_recoveries "
                   "(request_id TEXT,job_id TEXT,recovered_at REAL,worker_request_id TEXT)")
        db.execute("INSERT INTO asr_fence_recoveries VALUES(?,?,?,?)",
                   (request_id, job_id, time.time(), status.get("last_request_id")))
        cursor = db.execute("DELETE FROM gpu_executor_fences WHERE resource='mainpc-gpu' AND request_id=? AND job_id=?",
                            (request_id, job_id))
        if cursor.rowcount != 1:
            raise ValueError("fence changed during recovery")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database",required=True,type=Path)
    parser.add_argument("--request-id",required=True)
    parser.add_argument("--job-id",required=True)
    parser.add_argument("--worker-url",required=True)
    parser.add_argument("--worker-token-file",required=True)
    parser.add_argument("--confirm-old-executor-exited",action="store_true")
    parser.add_argument("--confirm-dispatch-stopped",action="store_true")
    args=parser.parse_args()
    try:
        if Path(args.worker_token_file).is_symlink():
            raise ValueError("worker credential must be a private regular file")
        token=storage_token(args.worker_token_file)
        recover(args.database,WhisperHTTP(args.worker_url,token),args.request_id,args.job_id,
                executor_stopped=args.confirm_old_executor_exited,dispatch_stopped=args.confirm_dispatch_stopped)
    except Exception as error:
        print("ASR fence recovery refused ("+type(error).__name__+")")
        return 1
    print("Exact ASR fence recovered; operator decision recorded durably")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
