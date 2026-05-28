"""Behavioral tests for the CSV-summarization ingest path.

Run from knowledge-base/scripts:

    python -m _test_clients.test_csv_summary

Exits 0 on all-pass, 1 on any failure.

Covers:
  read_csv_preview        - pure CSV structure reader (headers, row count, sample)
  build_csv_summary_prompt - pure prompt builder (mentions filename, headers, count, sample)
  ingest_csv              - summarizes via Flash, embeds the SUMMARY (not raw rows),
                            upserts one csv_summary doc, returns 1
  ingest_file routing     - .csv dispatches to ingest_csv, NOT ingest_text_file
"""

import os
import sys
import tempfile
from pathlib import Path

HERE = os.path.dirname(os.path.abspath(__file__))
PARENT = os.path.dirname(HERE)
if PARENT not in sys.path:
    sys.path.insert(0, PARENT)

import mmrag


FAILURES = []


def _check(label, cond, detail=""):
    if cond:
        print(f"  PASS  {label}")
    else:
        print(f"  FAIL  {label}: {detail}")
        FAILURES.append(label)


def _write_csv(text):
    f = tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False, newline="")
    f.write(text)
    f.close()
    return Path(f.name)


# --- Stubs for the integration test (fault_injection does not stub embed) ---

class _StubResp:
    def __init__(self, text):
        self.text = text
        self.usage_metadata = None


class _StubEmbedResult:
    class _E:
        def __init__(self, v):
            self.values = v

    def __init__(self, vec):
        self.embeddings = [self._E(vec)]


class _StubModels:
    def __init__(self, summary):
        self._summary = summary
        self.generate_calls = []
        self.embed_calls = []

    def generate_content(self, model=None, contents=None, **kw):
        self.generate_calls.append(contents)
        return _StubResp(self._summary)

    def embed_content(self, model=None, contents=None, config=None, **kw):
        self.embed_calls.append(contents)
        return _StubEmbedResult([0.0] * 768)


class _StubClient:
    def __init__(self, summary):
        self.models = _StubModels(summary)


class _StubCollection:
    def __init__(self):
        self.upserts = []
        self._ids = set()

    def get(self, ids=None, include=None):
        if ids is None:
            return {"ids": list(self._ids), "metadatas": []}
        return {"ids": [i for i in ids if i in self._ids]}

    def upsert(self, ids=None, embeddings=None, documents=None, metadatas=None):
        for i in ids:
            self._ids.add(i)
        self.upserts.append({
            "ids": ids, "embeddings": embeddings,
            "documents": documents, "metadatas": metadatas,
        })


def test_read_csv_preview_basic():
    p = _write_csv("name,email,stage\nAlice,a@x.com,lead\nBob,b@x.com,won\nCy,c@x.com,lost\n")
    try:
        headers, row_count, sample = mmrag.read_csv_preview(p, max_sample_rows=2)
        _check("read_csv_preview headers", headers == ["name", "email", "stage"], f"got {headers}")
        _check("read_csv_preview row_count excludes header", row_count == 3, f"got {row_count}")
        _check("read_csv_preview sample capped", len(sample) == 2, f"got {len(sample)} rows")
        _check("read_csv_preview sample content", sample[0] == ["Alice", "a@x.com", "lead"], f"got {sample[0]}")
    finally:
        p.unlink()


def test_read_csv_preview_empty():
    p = _write_csv("")
    try:
        headers, row_count, sample = mmrag.read_csv_preview(p)
        _check("read_csv_preview empty headers", headers == [], f"got {headers}")
        _check("read_csv_preview empty count", row_count == 0, f"got {row_count}")
    finally:
        p.unlink()


def test_read_csv_preview_header_only():
    p = _write_csv("col_a,col_b\n")
    try:
        headers, row_count, sample = mmrag.read_csv_preview(p)
        _check("read_csv_preview header-only headers", headers == ["col_a", "col_b"], f"got {headers}")
        _check("read_csv_preview header-only count==0", row_count == 0, f"got {row_count}")
        _check("read_csv_preview header-only sample empty", sample == [], f"got {sample}")
    finally:
        p.unlink()


def test_build_csv_summary_prompt_mentions_everything():
    prompt = mmrag.build_csv_summary_prompt(
        "deal.csv", ["name", "amount", "stage"], 1234, [["A", "5", "won"]]
    )
    _check("prompt names file", "deal.csv" in prompt, "filename missing")
    _check("prompt lists all headers", all(h in prompt for h in ["name", "amount", "stage"]), "a header missing")
    _check("prompt has row count", "1234" in prompt, "row count missing")
    _check("prompt has sample value", "won" in prompt, "sample missing")


def test_build_csv_summary_prompt_hardening():
    # Fix 1: must instruct structure-only / no value reproduction, isolate untrusted data,
    # and must NOT use the leaky "notable values" wording.
    prompt = mmrag.build_csv_summary_prompt("contacts.csv", ["name", "email"], 10, [["Jo", "j@x.com"]])
    low = prompt.lower()
    _check("prompt forbids reproducing personal data",
           ("do not reproduce" in low or "do not include" in low or "not reproduce" in low)
           and ("personal" in low or "email" in low or "pii" in low),
           "no PII/value-reproduction instruction")
    _check("prompt isolates untrusted sample with a delimiter",
           "untrusted" in low and ("---" in prompt or "begin" in low),
           "no untrusted-data delimiter")
    _check("prompt does not use leaky 'notable values' wording",
           "notable patterns or values" not in low,
           "still asks to surface notable values")


def test_build_csv_summary_prompt_truncates_long_cells():
    # Fix 4: a huge cell value must be truncated so the prompt stays bounded.
    big = "X" * 5000
    prompt = mmrag.build_csv_summary_prompt("f.csv", ["c"], 1, [[big]])
    _check("long cell truncated in prompt", big not in prompt, "full 5000-char cell leaked into prompt")
    _check("prompt stays bounded", len(prompt) < 4000, f"prompt too long: {len(prompt)}")


def test_build_csv_summary_prompt_caps_columns():
    # Final-review fix: bound the COLUMN axis too, not just rows/cell-length, so a very wide
    # CSV can't inflate the prompt. Cap columns shown and indicate how many were omitted.
    headers = [f"col{i}" for i in range(100)]
    row = [f"val{i}" for i in range(100)]
    prompt = mmrag.build_csv_summary_prompt("wide.csv", headers, 5, [row])
    _check("caps columns shown", "col0" in prompt and "col99" not in prompt, "did not cap columns")
    _check("indicates omitted columns", "more" in prompt.lower(), "no '+N more' indicator")
    _check("caps cells per sample row", "val99" not in prompt, "rendered all cells in a wide row")
    _check("wide prompt stays bounded", len(prompt) < 8000, f"prompt too long: {len(prompt)}")


def test_read_csv_preview_strips_bom():
    # Fix 2: Excel/Windows CSVs carry a UTF-8 BOM that must not glue onto the first header.
    p = _write_csv("﻿name,email\nA,a@x.com\n")
    try:
        headers, _, _ = mmrag.read_csv_preview(p)
        _check("BOM stripped from first header", headers[:1] == ["name"], f"got {headers[:1]!r}")
    finally:
        p.unlink()


def test_ingest_csv_header_only_skips():
    # Fix 3: header-only CSV (0 data rows) must skip Flash, not hallucinate a summary.
    p = _write_csv("col_a,col_b\n")
    client = _StubClient("should not be called")
    coll = _StubCollection()
    try:
        n = mmrag.ingest_csv(client, {}, coll, p)
        _check("header-only returns 0", n == 0, f"got {n}")
        _check("header-only calls no Flash", len(client.models.generate_calls) == 0,
               f"got {len(client.models.generate_calls)}")
    finally:
        p.unlink()


def test_ingest_csv_skips_flash_when_already_exists():
    # Fix 6: re-ingest (no --force) must short-circuit BEFORE the billed Flash call.
    p = _write_csv("a,b\n1,2\n")
    client = _StubClient("summary")
    coll = _StubCollection()
    coll._ids.add(mmrag.file_id(p))  # pretend already ingested
    try:
        n = mmrag.ingest_csv(client, {}, coll, p)
        _check("already-exists returns 0", n == 0, f"got {n}")
        _check("already-exists calls no Flash", len(client.models.generate_calls) == 0,
               f"got {len(client.models.generate_calls)}")
    finally:
        p.unlink()


def test_ingest_csv_empty_flash_response_skips():
    # Fix 6: if Flash returns empty text, do not embed a blank doc.
    p = _write_csv("a,b\n1,2\n")
    client = _StubClient("   ")  # whitespace-only
    coll = _StubCollection()
    try:
        n = mmrag.ingest_csv(client, {"embedding_dimensions": 768}, coll, p)
        _check("empty-Flash returns 0", n == 0, f"got {n}")
        _check("empty-Flash upserts nothing", len(coll.upserts) == 0, f"got {len(coll.upserts)}")
    finally:
        p.unlink()


def test_ingest_csv_embeds_summary_not_rows():
    summary = "This is the field catalog for the HubSpot deal object: name, amount, stage. 1234 rows."
    p = _write_csv("name,amount,stage\nAcme,5000,won\nBeta,300,lost\n")
    client = _StubClient(summary)
    coll = _StubCollection()
    try:
        n = mmrag.ingest_csv(client, {"embedding_dimensions": 768}, coll, p)
        _check("ingest_csv returns 1", n == 1, f"got {n}")
        _check("ingest_csv called Flash once", len(client.models.generate_calls) == 1,
               f"got {len(client.models.generate_calls)}")
        _check("ingest_csv upserted one doc", len(coll.upserts) == 1, f"got {len(coll.upserts)}")
        if coll.upserts:
            doc = coll.upserts[0]["documents"][0]
            meta = coll.upserts[0]["metadatas"][0]
            _check("ingest_csv embeds the SUMMARY text", doc == summary, f"got {doc!r}")
            _check("ingest_csv does NOT embed raw row 'Acme'", "Acme" not in doc, "raw rows leaked into doc")
            _check("ingest_csv metadata type=csv_summary", meta.get("type") == "csv_summary", f"got {meta.get('type')}")
            _check("ingest_csv metadata has filename", meta.get("filename") == p.name, f"got {meta.get('filename')}")
            _check("ingest_csv metadata records row_count", meta.get("row_count") == 2, f"got {meta.get('row_count')}")
    finally:
        p.unlink()


def test_ingest_csv_empty_returns_zero():
    p = _write_csv("")
    client = _StubClient("unused")
    coll = _StubCollection()
    try:
        n = mmrag.ingest_csv(client, {}, coll, p)
        _check("ingest_csv empty returns 0", n == 0, f"got {n}")
        _check("ingest_csv empty calls no Flash", len(client.models.generate_calls) == 0,
               f"got {len(client.models.generate_calls)}")
    finally:
        p.unlink()


def test_ingest_file_routes_csv_to_ingest_csv():
    # Route .csv through ingest_csv, NOT ingest_text_file. Verify by monkeypatching.
    p = _write_csv("a,b\n1,2\n")
    routed = {"csv": 0, "text": 0}
    orig_csv = mmrag.ingest_csv
    orig_text = mmrag.ingest_text_file
    mmrag.ingest_csv = lambda c, cfg, col, fp: routed.__setitem__("csv", routed["csv"] + 1) or 1
    mmrag.ingest_text_file = lambda c, cfg, col, fp: routed.__setitem__("text", routed["text"] + 1) or 1
    try:
        mmrag.ingest_file(None, {}, _StubCollection(), p)
        _check("ingest_file routes .csv to ingest_csv", routed["csv"] == 1, f"csv={routed['csv']}")
        _check("ingest_file does NOT send .csv to ingest_text_file", routed["text"] == 0, f"text={routed['text']}")
    finally:
        mmrag.ingest_csv = orig_csv
        mmrag.ingest_text_file = orig_text
        p.unlink()


def main():
    print("test_csv_summary")
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            print(f"\n{name}:")
            try:
                fn()
            except Exception as e:
                print(f"  FAIL  {name}: raised {type(e).__name__}: {e}")
                FAILURES.append(name)
    print()
    if FAILURES:
        print(f"FAILED ({len(FAILURES)}): {', '.join(FAILURES)}")
        sys.exit(1)
    print("ALL PASS")
    sys.exit(0)


if __name__ == "__main__":
    main()
