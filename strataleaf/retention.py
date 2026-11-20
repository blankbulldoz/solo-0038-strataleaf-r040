import math
from dataclasses import asdict
from .codec import name
from .errors import Conflict, InvalidDocument, NotFound
from .mvcc import checked_revision
from .transaction import commit_operations

SWEEP_FAILURE_LIMIT = 100


class Retention:
    def __init__(self, store):
        self.store = store

    def pin(self, pin_name, revision=None):
        name(pin_name)
        with self.store.atomic() as connection:
            revision = checked_revision(self.store, revision)
            if connection.execute("SELECT 1 FROM pins WHERE name=?", (pin_name,)).fetchone():
                raise Conflict("read pin already exists")
            connection.execute("INSERT INTO pins VALUES (?,?)", (pin_name, revision))
        return revision

    def unpin(self, pin_name):
        with self.store.atomic() as connection:
            if not connection.execute("DELETE FROM pins WHERE name=?", (pin_name,)).rowcount:
                raise NotFound("unknown read pin")

    def compact(self, target):
        with self.store.atomic(writable=True) as connection:
            target = checked_revision(self.store, target)
            blockers = [row[0] for row in connection.execute("SELECT rev FROM pins UNION ALL SELECT ack FROM consumers")]
            boundary = min([target] + blockers)
            prior = int(self.store.get("floor"))
            if boundary < prior:
                raise Conflict("retention metadata violates current floor")
            removed = connection.execute("""DELETE FROM versions WHERE rev<? AND EXISTS
              (SELECT 1 FROM versions newer WHERE newer.collection=versions.collection
               AND newer.key=versions.key AND newer.rev>versions.rev AND newer.rev<=?)""", (boundary, boundary)).rowcount
            logs = connection.execute("DELETE FROM commits WHERE rev<=?", (boundary,)).rowcount
            self.store.set("floor", boundary)
            return {"requested": target, "floor": boundary, "removed_versions": removed,
                    "removed_commits": logs, "blocked": boundary < target}

    def sweep(self, now, limit=1000):
        """Apply expiry deletions fairly across collections.

        Time passing never hides documents: only an explicit sweep call creates real delete
        commits. Candidates are re-checked against their revisions inside the writer
        transaction, so a document renewed or rewritten after the scan is skipped instead of
        being misdeleted. One stale key only records a failure and never blocks unrelated
        collections. A persistent round-robin turn guarantees every collection with expired
        documents gets an explainable share of each small quota, even when other collections
        churn at high frequency.
        """
        if isinstance(now, bool) or not isinstance(now, (int, float)) or not math.isfinite(now):
            raise InvalidDocument("expiry sweep requires a finite timestamp")
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 10000:
            raise InvalidDocument("sweep limit must be between 1 and 10000")
        with self.store.read() as connection:
            turn = connection.execute("SELECT value FROM meta WHERE key='sweep_turn'").fetchone()
            turn = turn[0] if turn is not None else ""
            eligible = [row[0] for row in connection.execute("""SELECT DISTINCT v.collection FROM versions v
              WHERE v.body IS NOT NULL AND v.expires<=? AND NOT EXISTS
              (SELECT 1 FROM versions n WHERE n.collection=v.collection AND n.key=v.key AND n.rev>v.rev)
              ORDER BY v.collection""", (now,)).fetchall()]
            if not eligible:
                return {"expired": 0, "revision": None, "scanned": 0, "remaining": 0,
                        "turn": turn or None, "next_turn": turn or None,
                        "collections": [], "conflicts": [], "failure_reports": 0}
            # Round-robin order starts after the collection served last time.
            ordered = sorted(eligible)
            start = 0
            if turn in ordered:
                start = ordered.index(turn)
            rotation = ordered[start:] + ordered[:start]
            base, extra = divmod(limit, len(rotation))
            quotas = {collection: base + (1 if index < extra else 0)
                      for index, collection in enumerate(rotation)}
            candidates = []
            scanned_per = {}
            for collection in rotation:
                rows = connection.execute("""SELECT v.* FROM versions v WHERE v.collection=?
                  AND v.body IS NOT NULL AND v.expires<=?
                  AND NOT EXISTS (SELECT 1 FROM versions n WHERE n.collection=v.collection
                   AND n.key=v.key AND n.rev>v.rev)
                  ORDER BY v.expires,v.key LIMIT ?""", (collection, now, quotas[collection])).fetchall()
                scanned_per[collection] = len(rows)
                candidates.extend({"collection": row["collection"], "key": row["key"],
                                   "expected": row["rev"], "expires": row["expires"]} for row in rows)
        operations = [{"collection": item["collection"], "key": item["key"], "document": None,
                       "expected": item["expected"], "expires_at": None} for item in candidates]
        # Seam between the read snapshot and the writer transaction. Nothing registers it in
        # production; examples use it to race a real writer and prove precondition re-checking.
        hook = getattr(self, "after_scan_hook", None)
        if hook is not None:
            hook(now, candidates)
        result, conflicts = commit_operations(self.store, operations, tolerant=True, now=now) if operations else (None, [])
        conflict_keys = {(item.collection, item.key) for item in conflicts}
        deleted_per = {collection: 0 for collection in eligible}
        for item in candidates:
            if (item["collection"], item["key"]) not in conflict_keys:
                deleted_per[item["collection"]] += 1
        next_turn = rotation[-1]
        with self.store.atomic(writable=True) as connection:
            for item in candidates:
                collection = item["collection"]
                is_conflict = (collection, item["key"]) in conflict_keys
                connection.execute("""INSERT INTO sweep_progress VALUES (?,?,?,?,?,?,?)
                  ON CONFLICT(collection) DO UPDATE SET last_key=excluded.last_key,
                   last_expires=excluded.last_expires,
                   scanned_total=sweep_progress.scanned_total+excluded.scanned_total,
                   deleted_total=sweep_progress.deleted_total+excluded.deleted_total,
                   conflict_total=sweep_progress.conflict_total+excluded.conflict_total,
                   last_sweep=excluded.last_sweep""",
                                   (collection, item["key"], item["expires"], 1,
                                    0 if is_conflict else 1, 1 if is_conflict else 0, now))
            for failure in conflicts:
                connection.execute("INSERT INTO sweep_failures(observed_at,collection,key,expected_rev,found_rev,reason) VALUES (?,?,?,?,?,?)",
                                   (now, failure.collection, failure.key, failure.expected_rev, failure.found_rev, failure.reason))
            connection.execute("""DELETE FROM sweep_failures WHERE id NOT IN
              (SELECT id FROM sweep_failures ORDER BY id DESC LIMIT ?)""", (SWEEP_FAILURE_LIMIT,))
            connection.execute("INSERT OR IGNORE INTO meta VALUES ('sweep_turn','')")
            self.store.set("sweep_turn", next_turn)
            remaining = connection.execute("""SELECT COUNT(*) FROM versions v WHERE v.body IS NOT NULL AND v.expires<=?
              AND NOT EXISTS (SELECT 1 FROM versions n WHERE n.collection=v.collection
               AND n.key=v.key AND n.rev>v.rev)""", (now,)).fetchone()[0]
            progress_rows = connection.execute("SELECT * FROM sweep_progress WHERE collection IN (%s) ORDER BY collection"
                                               % ",".join("?" for _ in eligible), eligible).fetchall()
        per_collection = []
        for row in progress_rows:
            per_collection.append({"collection": row["collection"], "scanned": scanned_per.get(row["collection"], 0),
                                   "deleted": deleted_per.get(row["collection"], 0),
                                   "last_key": row["last_key"], "last_expires": row["last_expires"],
                                   "scanned_total": row["scanned_total"], "deleted_total": row["deleted_total"],
                                   "conflict_total": row["conflict_total"], "last_sweep": row["last_sweep"]})
        return {"expired": result.count if result is not None else 0,
                "revision": result.revision if result is not None else None,
                "scanned": len(candidates), "remaining": remaining,
                "turn": turn or None, "next_turn": next_turn,
                "collections": per_collection,
                "conflicts": [asdict(item) for item in conflicts],
                "failure_reports": len(conflicts)}

    def sweep_report(self):
        """Return the durable sweep state so progress and failures survive reopening."""
        with self.store.read() as connection:
            turn = connection.execute("SELECT value FROM meta WHERE key='sweep_turn'").fetchone()
            progress = [dict(row) for row in connection.execute("SELECT * FROM sweep_progress ORDER BY collection")]
            failures = [dict(row) for row in connection.execute(
                "SELECT * FROM sweep_failures ORDER BY id DESC LIMIT ?", (SWEEP_FAILURE_LIMIT,))]
            total_failures = connection.execute("SELECT COUNT(*) FROM sweep_failures").fetchone()[0]
        return {"turn": turn[0] if turn is not None and turn[0] else None,
                "collections": progress, "failures": failures,
                "failure_count": total_failures, "failure_limit": SWEEP_FAILURE_LIMIT}
