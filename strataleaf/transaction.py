import math
from dataclasses import dataclass
from .codec import digest, dumps, key as checked_key, loads, name
from .errors import Conflict, InvalidDocument, NotFound
from .indexes import refresh_document
from .mvcc import latest_row
from .schema import validate_document


@dataclass(frozen=True)
class CommitResult:
    revision: int
    count: int
    replayed: bool = False


@dataclass(frozen=True)
class SweepConflict:
    collection: str
    key: str
    expected_rev: int
    found_rev: int
    reason: str


def _conflict_reason(old, expected, now):
    """Classify why a candidate revision no longer matches at sweep commit time."""
    if old is None or old["body"] is None:
        return "deleted after scan"
    if old["rev"] != expected and old["expires"] is not None and old["expires"] > now:
        return "renewed after scan"
    return "rewritten after scan"


def commit_operations(store, operations, *, request_key=None, tolerant=False, now=None):
    """Commit one revision containing the accepted operations in a single atomic boundary.

    Strict mode (regular batches) aborts the whole revision on the first failed precondition.
    Tolerant mode (expiry sweep) instead skips stale candidates, reports them as conflicts, and
    still commits every operation whose precondition still holds. No write is applied until every
    precondition and schema has been re-checked inside the writer transaction, so partial effects
    are impossible on abort. Returns (CommitResult|None, conflicts); the result is None in
    tolerant mode when every candidate was stale.
    """
    if not operations or len(operations) > 10000:
        raise InvalidDocument("batch must contain between 1 and 10000 operations")
    fingerprint = digest(operations)
    conflicts = []
    with store.atomic(writable=True) as connection:
        if request_key is not None:
            prior = connection.execute("SELECT * FROM receipts WHERE request_key=?", (request_key,)).fetchone()
            if prior:
                if prior["request_hash"] != fingerprint:
                    raise Conflict("request key was used with different operations")
                return CommitResult(prior["rev"], prior["count"], True), []
        head = int(store.get("head"))
        accepted = []
        for op in operations:
            schema = connection.execute("SELECT schema FROM collections WHERE name=?", (op["collection"],)).fetchone()
            if schema is None:
                if tolerant:
                    conflicts.append(SweepConflict(op["collection"], op["key"], op["expected"] or 0, 0,
                                                   "collection removed after scan"))
                    continue
                raise NotFound("unknown collection: " + op["collection"])
            old = latest_row(connection, op["collection"], op["key"], head)
            current = old["rev"] if old is not None else 0
            if op["expected"] is not None and op["expected"] != current:
                if tolerant:
                    conflicts.append(SweepConflict(op["collection"], op["key"], op["expected"], current,
                                                   _conflict_reason(old, op["expected"], now)))
                    continue
                raise Conflict(f"revision conflict for {op['collection']}/{op['key']}: expected {op['expected']}, found {current}")
            if op["document"] is not None:
                validate_document(op["document"], loads(schema[0]))
            accepted.append(op)
        if not accepted:
            return None, conflicts
        revision = head + 1
        for op in accepted:
            body = dumps(op["document"]) if op["document"] is not None else None
            connection.execute("INSERT INTO versions VALUES (?,?,?,?,?)",
                               (op["collection"], op["key"], revision, body, op["expires_at"]))
            refresh_document(connection, op["collection"], op["key"], op["document"])
        payload = {"revision": revision, "operations": accepted}
        connection.execute("INSERT INTO commits VALUES (?,?,?,?)",
                           (revision, dumps(payload), request_key, fingerprint))
        if request_key is not None:
            connection.execute("INSERT INTO receipts VALUES (?,?,?,?)",
                               (request_key, revision, fingerprint, len(accepted)))
        store.set("head", revision)
    return CommitResult(revision, len(accepted)), conflicts

class WriteBatch:
    def __init__(self, store, request_key=None):
        self.store = store
        self.request_key = checked_key(request_key) if request_key is not None else None
        self.operations = []
        self.state = "open"

    def _append(self, operation):
        if self.state != "open":
            raise Conflict("batch is already " + self.state)
        identity = (operation["collection"], operation["key"])
        if any((op["collection"], op["key"]) == identity for op in self.operations):
            raise InvalidDocument("one operation per document per batch")
        expected = operation["expected"]
        if expected is not None and (isinstance(expected, bool) or not isinstance(expected, int) or expected < 0):
            raise InvalidDocument("expected revision must be nonnegative or None")
        self.operations.append(operation)
        return self

    def put(self, collection, key, document, expected=None, expires_at=None):
        name(collection)
        checked_key(key)
        if not isinstance(document, dict):
            raise InvalidDocument("document must be an object")
        if expires_at is not None and (isinstance(expires_at, bool) or not isinstance(expires_at, (int, float)) or not math.isfinite(expires_at)):
            raise InvalidDocument("expires_at must be a finite timestamp")
        return self._append({"collection": collection, "key": key, "document": loads(dumps(document)),
                             "expected": expected, "expires_at": expires_at})

    def delete(self, collection, key, expected=None):
        return self._append({"collection": name(collection), "key": checked_key(key), "document": None,
                             "expected": expected, "expires_at": None})

    def abort(self):
        if self.state != "open":
            raise Conflict("batch is already " + self.state)
        self.operations.clear()
        self.state = "aborted"

    def commit(self):
        if self.state != "open":
            raise Conflict("batch is already " + self.state)
        if not self.operations or len(self.operations) > 10000:
            raise InvalidDocument("batch must contain between 1 and 10000 operations")
        try:
            result, _ = commit_operations(self.store, self.operations, request_key=self.request_key)
        except BaseException:
            self.state = "aborted"
            raise
        self.state = "committed"
        return result
