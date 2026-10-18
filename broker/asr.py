"""Opt-in Whisper executor with private artifacts and durable GPU fencing."""
from __future__ import annotations
import hashlib
import hmac
import http.client
import json
import os
import re
import threading
import time
import uuid
from pathlib import Path
from urllib.parse import urlsplit

PROFILE = SOURCE = "stt-whisper"
MODEL = "whisper-medium-int8-mainpc-cuda"
MAX_AUDIO_BYTES = 64 * 1024 * 1024
MAX_RESULT_BYTES = 2 * 1024 * 1024
KEY = re.compile(r"^[A-Za-z0-9_-]{16,100}$")
SHA = re.compile(r"^[a-f0-9]{64}$")

def validate_payload(payload):
    if not isinstance(payload, dict) or set(payload) != {"artifact", "sha256", "bytes"}:
        raise ValueError("invalid ASR artifact fields")
    if not isinstance(payload["artifact"], str) or not SHA.fullmatch(payload["artifact"]):
        raise ValueError("invalid ASR artifact")
    if not isinstance(payload["sha256"], str) or not SHA.fullmatch(payload["sha256"]):
        raise ValueError("invalid ASR digest")
    if type(payload["bytes"]) is not int or not 0 < payload["bytes"] <= MAX_AUDIO_BYTES:
        raise ValueError("invalid ASR size")
    return dict(payload)

class Spool:
    """Bounded owner-only files; no caller-controlled paths or URLs."""
    def __init__(self, root, max_bytes=512 * 1024 * 1024):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        if os.name != "nt" and (self.root.stat().st_mode & 0o077
                                  or self.root.stat().st_uid != os.getuid()):
            raise ValueError("ASR spool must be owner-only")
        self.max_bytes, self.lock = max_bytes, threading.Lock()
        self.admission_locks = [threading.RLock() for _ in range(64)]

    def admission_lock(self, key):
        return self.admission_locks[int(hashlib.sha256(key.encode()).hexdigest()[:2], 16) % 64]

    def sync_directory(self):
        if os.name != "nt":
            descriptor = os.open(self.root, os.O_RDONLY)
            try: os.fsync(descriptor)
            finally: os.close(descriptor)

    def accounted_bytes(self):
        files = [p for p in self.root.iterdir() if p.is_file()]
        used = sum(p.stat().st_size for p in files)
        # Every admitted audio can produce one bounded transcript. Reserve
        # its maximum result until the actual result replaces that reserve.
        used += sum(MAX_RESULT_BYTES for p in files if p.suffix == ".audio"
                    and not self.path(p.stem, ".json").exists())
        return used

    def path(self, artifact, suffix):
        if not isinstance(artifact, str) or not SHA.fullmatch(artifact):
            raise ValueError("invalid ASR artifact")
        path = self.root / (artifact + suffix)
        if path.is_symlink():
            raise ValueError("ASR symlink rejected")
        return path

    def stage(self, key, stream, size, connection):
        if not KEY.fullmatch(key) or not 0 < size <= MAX_AUDIO_BYTES:
            raise ValueError("invalid ASR upload")
        artifact = hashlib.sha256(key.encode()).hexdigest()
        dest = self.path(artifact, ".audio")
        temp = self.root / (uuid.uuid4().hex + ".part")
        with self.lock:
            exists = dest.exists()
            if not exists and self.accounted_bytes() + size + MAX_RESULT_BYTES > self.max_bytes:
                raise ValueError("ASR spool capacity reached")
            digest, deadline = hashlib.sha256(), time.monotonic() + 60
            if exists:
                # Validate an idempotent retry by streaming its digest. No
                # duplicate file or capacity reservation is needed.
                remaining = size
                while remaining:
                    budget = deadline - time.monotonic()
                    if budget <= 0: raise ValueError("ASR upload deadline")
                    connection.settimeout(min(10, budget))
                    block = stream.read(min(64 * 1024, remaining))
                    if not block: raise ValueError("ASR incomplete upload")
                    digest.update(block); remaining -= len(block)
                if dest.stat().st_size != size or hashlib.sha256(dest.read_bytes()).hexdigest() != digest.hexdigest():
                    raise ValueError("ASR idempotency conflict")
                return {"artifact": artifact, "sha256": digest.hexdigest(), "bytes": size}
            try:
                with temp.open("xb") as out:
                    if os.name != "nt": os.chmod(temp, 0o600)
                    remaining = size
                    while remaining:
                        budget = deadline - time.monotonic()
                        if budget <= 0: raise ValueError("ASR upload deadline")
                        connection.settimeout(min(10, budget))
                        block = stream.read(min(64 * 1024, remaining))
                        if not block: raise ValueError("ASR incomplete upload")
                        digest.update(block)
                        out.write(block)
                        remaining -= len(block)
                    out.flush()
                    os.fsync(out.fileno())
                checksum = digest.hexdigest()
                if dest.exists():
                    if hashlib.sha256(dest.read_bytes()).hexdigest() != checksum:
                        raise ValueError("ASR idempotency conflict")
                else:
                    os.replace(temp, dest)
                    self.sync_directory()
                return {"artifact": artifact, "sha256": checksum, "bytes": size}
            finally:
                temp.unlink(missing_ok=True)

    def audio(self, payload):
        payload = validate_payload(payload)
        path = self.path(payload["artifact"], ".audio")
        if path.stat().st_size != payload["bytes"]:
            raise ValueError("ASR artifact size changed")
        if hashlib.sha256(path.read_bytes()).hexdigest() != payload["sha256"]:
            raise ValueError("ASR artifact hash changed")
        return path

    def save_result(self, artifact, result):
        encoded = json.dumps(result, ensure_ascii=False).encode("utf-8")
        if len(encoded) > MAX_RESULT_BYTES: raise ValueError("ASR result too large")
        path, temp = self.path(artifact, ".json"), self.path(artifact, ".result-part")
        with self.lock:
            previous_bytes = path.stat().st_size if path.exists() else 0
            reserved = (MAX_RESULT_BYTES if not path.exists()
                        and self.path(artifact, ".audio").exists() else 0)
            if self.accounted_bytes() - reserved - previous_bytes + len(encoded) > self.max_bytes:
                raise ValueError("ASR spool capacity reached")
            try:
                with temp.open("wb") as out:
                    if os.name != "nt": os.chmod(temp, 0o600)
                    out.write(encoded)
                    out.flush()
                    os.fsync(out.fileno())
                os.replace(temp, path)
                self.sync_directory()
            finally:
                temp.unlink(missing_ok=True)
        return {"artifact": artifact, "model": MODEL, "raw_text_stored": True}

    def result(self, artifact):
        return json.loads(self.path(artifact, ".json").read_text(encoding="utf-8"))

    def discard(self, artifact):
        with self.lock:
            for suffix in (".audio", ".json"):
                self.path(artifact, suffix).unlink(missing_ok=True)
            self.sync_directory()

    def discard_if_unowned(self, broker, key, artifact):
        """Remove only a newly staged artifact with no durable job reference."""
        with broker.lock, self.lock:
            row = broker.db.execute(
                "SELECT 1 FROM jobs WHERE source=? AND external_id=? LIMIT 1",
                (SOURCE, key),
            ).fetchone()
            if row is not None:
                return False
            for suffix in (".audio", ".json"):
                self.path(artifact, suffix).unlink(missing_ok=True)
            self.sync_directory()
            return True

    def prune(self, broker, now=None):
        """Expire abandoned staging after a day, preserving queued work and fences."""
        now = time.time() if now is None else now
        with broker.lock, self.lock:
            if broker.executor_fence():
                return
            keep = {hashlib.sha256(row[0].encode()).hexdigest() for row in broker.db.execute(
                "SELECT external_id FROM jobs WHERE source=? AND state IN "
                "('queued','running','cancel_requested') AND external_id IS NOT NULL", (SOURCE,))}
            for path in self.root.iterdir():
                if path.is_symlink() or not path.is_file() or now - path.stat().st_mtime < 86400:
                    continue
                if path.stem in keep:
                    continue
                if ((SHA.fullmatch(path.stem) and path.suffix in {".audio", ".json", ".result-part"})
                        or (re.fullmatch(r"[a-f0-9]{32}", path.stem) and path.suffix == ".part")):
                    lane = self.admission_locks[int(path.stem[:2], 16) % len(self.admission_locks)]
                    # Never wait in inverse lock order (admission -> broker).
                    # An active same-key upload/ACK owns this artifact until
                    # its durable decision, so concurrent pruning skips it.
                    if not lane.acquire(blocking=False):
                        continue
                    try: path.unlink()
                    finally: lane.release()
            self.sync_directory()

class WhisperHTTP:
    def __init__(self, url, token, timeout=900):
        self.url = urlsplit(url)
        if (self.url.scheme not in {"http", "https"} or not self.url.hostname
                or self.url.username or self.url.password or self.url.query
                or self.url.fragment or self.url.path not in {"", "/"}):
            raise ValueError("invalid configured Whisper endpoint")
        if len(token) < 32: raise ValueError("Whisper token too short")
        self.token, self.timeout = token, timeout

    def request(self, path, audio=None, request_id=None, previous_request_id=None):
        cls = http.client.HTTPSConnection if self.url.scheme == "https" else http.client.HTTPConnection
        conn = cls(self.url.hostname, self.url.port, timeout=self.timeout if audio else 5)
        try:
            headers = {"Authorization": "Bearer " + self.token}
            if audio:
                headers.update({"Content-Type": "application/octet-stream",
                                "Content-Length": str(audio.stat().st_size),
                                "X-Filename": "audio.bin", "X-Request-Id": request_id})
                if previous_request_id:
                    headers['X-Previous-Request-Id'] = previous_request_id
                with audio.open("rb") as stream:
                    conn.request("POST", path, body=stream, headers=headers)
            else:
                conn.request("GET", path, headers=headers)
            response = conn.getresponse()
            raw = response.read(MAX_RESULT_BYTES + 1)
            if response.status != 200 or len(raw) > MAX_RESULT_BYTES:
                raise RuntimeError("Whisper request failed")
            result = json.loads(raw)
            if not isinstance(result, dict): raise ValueError("Whisper invalid response")
            return result
        except Exception:
            raise RuntimeError("Whisper transport unavailable") from None
        finally:
            conn.close()

    def status(self): return self.request("/v1/status")
    def transcribe(self, audio, request_id, previous_request_id=None):
        return self.request("/v1/transcribe", audio, request_id, previous_request_id)

    def release(self, request_id):
        cls = http.client.HTTPSConnection if self.url.scheme == "https" else http.client.HTTPConnection
        conn = cls(self.url.hostname, self.url.port, timeout=10)
        try:
            conn.request("POST", "/v1/release", headers={"Authorization": "Bearer " + self.token,
                                                       "X-Request-Id": request_id})
            response = conn.getresponse()
            raw = response.read(MAX_RESULT_BYTES + 1)
            if response.status != 200:
                raise RuntimeError("Whisper release rejected")
            result = json.loads(raw)
            if result.get("gpu_released") is not True:
                raise RuntimeError("Whisper release unconfirmed")
            return result
        except Exception:
            raise RuntimeError("Whisper release unavailable") from None
        finally:
            conn.close()

class WhisperExecutor:
    def __init__(self, spool, worker):
        self.spool, self.worker = spool, worker

    def allocation_pending(self):
        """Detect resident/unconfirmed work independently of its reusability."""
        try:
            status=self.worker.status()
            return (status.get("model_loaded") is True or status.get("active_jobs",0)>0
                    or status.get("gpu_release_confirmed") is False)
        except Exception:
            return False

    def available(self, ollama):
        try:
            # Leave resident models and their callers alone; wait for natural expiry.
            if ollama.ps(timeout_seconds=5).get("models"):
                return False
            status = self.worker.status()
            return (status.get("status", "ready") == "ready"
                    and status.get("model") == MODEL and status.get("active_jobs") == 0
                    and status.get("model_loaded") in {False, True}
                    and status.get("model_usable",True) is True
                    and status.get("request_tracking") is True)
        except Exception:
            return False

    def can_release(self, request_id):
        try:
            status = self.worker.status()
            return (status.get("model") == MODEL and status.get("active_jobs") == 0
                    and status.get("model_loaded") is False
                    and status.get("last_request_id") == request_id
                    and status.get("last_request_gpu_released") is True)
        except Exception:
            return False

    def can_reuse(self, fence=None, *, for_release=False):
        """Return true only for an idle, compatible warm Whisper owner."""
        try:
            status = self.worker.status()
            return (status.get("model") == MODEL and status.get("active_jobs") == 0
                    and status.get("model_loaded") is True
                    and status.get("request_tracking") is True
                    and (for_release or status.get("model_usable",True) is True)
                    and (fence is None or status.get("last_request_id") == fence["request_id"]))
        except Exception:
            return False

    def release_idle(self, broker):
        """Explicit, non-preemptive release for an Ollama handoff.

        Callers must choose this policy explicitly; it never interrupts an
        active inference and clears the fence only after worker confirmation.
        """
        fence = broker.executor_fence()
        if not fence or not self.can_reuse(fence, for_release=True):
            return False
        self.worker.release(fence["request_id"])
        if self.can_release(fence["request_id"]):
            broker.clear_executor_fence(fence["request_id"])
            return broker.executor_fence() is None
        return False

    def run(self, broker, row):
        payload = validate_payload(json.loads(row["payload"]))
        audio = self.spool.audio(payload)
        status = self.worker.status()
        if (status.get("status", "ready") != "ready"
                or status.get("model") != MODEL or status.get("active_jobs") != 0
                or status.get("model_loaded") not in (False, True)
                or status.get("request_tracking") is not True):
            raise RuntimeError("Whisper worker not idle or incompatible")
        # Do not evict models: an out-of-band caller may still own them.
        if broker.ollama.ps().get("models"):
            raise RuntimeError("GPU occupied by Ollama; retry after idle expiry")
        request_id = str(uuid.uuid4())
        fence = broker.executor_fence()
        warm = status.get("model_loaded") is True
        if warm:
            if not fence or not self.can_reuse(fence):
                raise RuntimeError("Whisper warm ownership mismatch")
            broker.replace_executor_fence(request_id, row["id"], fence["request_id"])
        else:
            # Idle unload can finish after the scheduler admitted warm reuse.
            # Retire the old fence only with request-specific release proof.
            if fence and not broker.reconcile_executor_fence():
                raise RuntimeError("Whisper previous GPU release unconfirmed")
            broker.set_executor_fence(request_id, row["id"])
        try:
            result = (self.worker.transcribe(audio, request_id, previous_request_id=fence['request_id'])
                      if warm else self.worker.transcribe(audio, request_id))
            if (result.get("model") != MODEL or result.get("device") != "cuda"
                    or not isinstance(result.get("text"), str) or not result["text"].strip()):
                raise RuntimeError("invalid Whisper result")
            # Transcript completion and GPU ownership are independent. Keep
            # the fence until request-specific unload proof, including cold
            # requests that leave the model warm for the idle window.
            return self.spool.save_result(payload["artifact"], result)
        finally:
            if self.can_release(request_id): broker.clear_executor_fence(request_id)

class ASRAPI:
    def __init__(self, broker, executor, token):
        if len(token) < 32: raise ValueError("ASR token too short")
        worker_token = getattr(executor.worker, 'token', None)
        if isinstance(worker_token, str) and hmac.compare_digest(token.encode(), worker_token.encode()):
            raise ValueError('ASR and worker credentials must be different')
        self.broker, self.executor, self.token = broker, executor, token
        self.slots = threading.BoundedSemaphore(2)

    def handle(self, handler):
        handler.close_connection = True
        auth = handler.headers.get("Authorization", "")
        if not hmac.compare_digest(auth.encode(), ("Bearer " + self.token).encode()):
            handler._json(401, {"error": "ASR authentication required"})
            return
        path = urlsplit(handler.path).path
        if not self.slots.acquire(blocking=False):
            handler._json(429, {"error": "ASR admission busy"})
            return
        try:
            if path == "/v1/transcriptions" and handler.command == "POST":
                if handler.headers.get("Transfer-Encoding"):
                    raise ValueError("ASR requires Content-Length")
                key = handler.headers.get("Idempotency-Key", "")
                size = int(handler.headers.get("Content-Length", "0"))
                self.executor.spool.prune(self.broker)
                with self.executor.spool.admission_lock(key):
                    with self.broker.lock:
                        existing = self.broker.db.execute(
                            "SELECT id,state FROM jobs WHERE source=? AND external_id=? LIMIT 1",
                            (SOURCE, key)).fetchone()
                        acknowledged = self.broker.db.execute(
                            "SELECT 1 FROM asr_receipts WHERE external_id=?", (key,)).fetchone()
                        purged = self.broker.db.execute(
                            "SELECT 1 FROM job_retention_tombstones WHERE source=? AND external_id=?",
                            (SOURCE, key)).fetchone()
                    if acknowledged or purged:
                        handler._json(410, {"error": "ASR artifact acknowledged"}); return
                    if existing and existing['state'] in {'completed','failed','cancelled'}:
                        artifact = hashlib.sha256(key.encode()).hexdigest()
                        if (not self.executor.spool.path(artifact,'.audio').is_file()
                                or (existing['state']=='completed'
                                    and not self.executor.spool.path(artifact,'.json').is_file())):
                            handler._json(410, {"error":"ASR terminal artifact expired"}); return
                    payload = self.executor.spool.stage(key, handler.rfile, size, handler.connection)
                    try:
                        job = self.broker.submit(PROFILE, "transcribe", payload, SOURCE, external_id=key)
                    except Exception:
                        self.executor.spool.discard_if_unowned(self.broker, key, payload["artifact"])
                        raise
                handler._json(202, {"id": job["id"], "state": job["state"]})
                return
            parts = path.strip("/").split("/")
            if len(parts) not in {3, 4} or parts[:2] != ["v1", "transcriptions"]:
                handler._json(404, {"error": "not found"}); return
            job = self.broker.status(parts[2])
            if job is None or job.get("kind") != "transcribe":
                handler._json(404, {"error": "not found"}); return
            if handler.command == "GET" and len(parts) == 3:
                with self.broker.lock:
                    if self.broker.db.execute("SELECT 1 FROM asr_receipts WHERE job_id=?", (job["id"],)).fetchone():
                        handler._json(410, {"error": "ASR artifact acknowledged"}); return
                fence = self.broker.executor_fence()
                response = {"id": job["id"], "state": job["state"],
                            "gpu_release_pending": bool(fence and fence["job_id"] == job["id"])}
                if job["state"] == "completed":
                    try: response["result"] = self.executor.spool.result(job["result"]["artifact"])
                    except (OSError, KeyError):
                        handler._json(410, {"error": "ASR artifact unavailable"}); return
                handler._json(200, response)
            elif handler.command == "POST" and len(parts) == 4 and parts[3] == "cancel":
                job = self.broker.cancel(job["id"])
                handler._json(200, {"id": job["id"], "state": job["state"]})
            elif handler.command == "POST" and len(parts) == 4 and parts[3] == "ack":
                with self.executor.spool.admission_lock(job["external_id"]):
                    # Receipt is durable before deletion. It is independent
                    # of the model's ownership fence and survives retries.
                    with self.broker.lock, self.broker.db:
                        job = self.broker.status(job["id"])
                        if job is None or job["state"] not in {"completed", "cancelled", "failed"}:
                            raise ValueError("ASR job is not terminal")
                        self.broker.db.execute("INSERT OR IGNORE INTO asr_receipts(job_id,acknowledged_at,external_id) VALUES(?,?,?)",
                                               (job["id"], time.time(), job['external_id']))
                    artifact = (job.get("result") or job.get("payload", {})).get("artifact")
                    if artifact: self.executor.spool.discard(artifact)
                handler._json(200, {"acknowledged": True})
            elif handler.command == 'POST' and len(parts) == 4 and parts[3] == 'retry':
                with self.executor.spool.admission_lock(job['external_id']):
                    # Same-key admission excludes ACK/pruning while hashing;
                    # unrelated broker operations need not wait for disk IO.
                    try: self.executor.spool.audio(job['payload'])
                    except (OSError, ValueError, KeyError):
                        handler._json(410, {'error':'ASR artifact unavailable'}); return
                    with self.broker.lock:
                        if self.broker.executor_fence():
                            handler._json(409, {'error':'GPU release pending'}); return
                        if self.broker.db.execute('SELECT 1 FROM asr_receipts WHERE job_id=?', (job['id'],)).fetchone():
                            handler._json(410, {'error':'ASR artifact acknowledged'}); return
                        retried = self.broker.retry(job['id'])
                handler._json(202, {'id':retried['id'], 'state':retried['state']})
            else: handler._json(404, {"error": "not found"})
        except Exception as error:
            # Keep broker admission semantics visible to authenticated ASR
            # clients without importing service.py (which imports this module).
            error_name = type(error).__name__
            if error_name == "SourceAdmissionBlocked":
                handler._json(403, {"error": str(error)})
            elif error_name == "SourceQueueFull":
                handler._json(429, {"error": str(error)})
            else:
                handler._json(400, {"error": "ASR request rejected"})
        finally:
            self.slots.release()
