"""
Official bulk CSV importer for shokuba.mhlw.go.jp.

This crawler is registered in NetHarvest-Scripts so Docker/Prefect can schedule
it from sites.yml. It downloads the official ZIP, extracts the CSV, stores a
SQLite snapshot, detects NEW/UPDATE/DELETE rows, and yields the changed rows to
the normal NetHarvest pipeline.
"""

import csv
import hashlib
import io
import json
import sqlite3
import sys
import time
import zipfile
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator, Iterable

_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.const.schema import Schema
from src.framework.static import StaticCrawler


SOURCE_URL = "https://shokuba.mhlw.go.jp/shokuba/utilize/download010?lang=JA"
DOWNLOAD_PAGE_URL = "https://shokuba.mhlw.go.jp/shokuba/utilize/utilize010.do"

CSV_CORP_NO = "\u6cd5\u4eba\u756a\u53f7"
CSV_COMPANY_NAME = "\u4f01\u696d\u540d"
CSV_PREF = "\u90fd\u9053\u5e9c\u770c"
CSV_ADDRESS = "\u6240\u5728\u5730"
CSV_TEL = "\u96fb\u8a71"
CSV_HP = "\u4f01\u696d\u30db\u30fc\u30e0\u30da\u30fc\u30b8"
CSV_BIZ = "\u696d\u7a2e"
CSV_SUMMARY = "\u4e8b\u696d\u6982\u8981"
CSV_EMP = "\u4f01\u696d\u898f\u6a21"
CSV_REP = "\u4ee3\u8868\u8005"

CHANGE_NEW = "NEW"
CHANGE_UPDATE = "UPDATE"
CHANGE_DELETE = "DELETE"
CHANGE_SKIPPED = "SKIPPED"

EXTRA_COLUMNS = [
    "change_type",
    "record_key",
    "row_sha256",
    "old_row_sha256",
    "csv_sha256",
    "zip_sha256",
    "import_log_id",
    "rows_read",
    "new_count",
    "updated_count",
    "deleted_count",
    "skipped_same_file",
    "csv_filename",
    "sqlite_path",
    "raw_prefecture",
    "raw_industry",
    "row_json",
    "old_row_json",
]


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def make_run_id() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def strip_code_prefix(value: str) -> str:
    clean = (value or "").strip()
    if ":" not in clean:
        return clean
    _, label = clean.split(":", 1)
    return label.strip() or clean


def canonical_json(data: dict[str, Any] | None) -> str:
    if data is None:
        return ""
    normalized = {str(key): "" if value is None else str(value) for key, value in data.items()}
    return json.dumps(normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def row_sha256(row: dict[str, Any]) -> str:
    return hashlib.sha256(canonical_json(row).encode("utf-8")).hexdigest()


def record_key(row: dict[str, Any]) -> str:
    corp_no = (row.get(CSV_CORP_NO) or "").strip()
    if corp_no:
        return f"corp:{corp_no}"

    name = (row.get(CSV_COMPANY_NAME) or "").strip()
    address = (row.get(CSV_ADDRESS) or "").strip()
    fallback = hashlib.sha256(f"{name}\n{address}".encode("utf-8")).hexdigest()
    return f"name_addr:{fallback}"


def detect_csv_encoding(data: bytes) -> str:
    sample = data[:65536]
    for encoding in ("utf-8-sig", "utf-8", "cp932"):
        try:
            sample.decode(encoding)
            return encoding
        except UnicodeDecodeError:
            continue
    return "cp932"


def iter_csv_rows(csv_data: bytes) -> Iterable[dict[str, str]]:
    encoding = detect_csv_encoding(csv_data)
    text = io.TextIOWrapper(io.BytesIO(csv_data), encoding=encoding, newline="")
    reader = csv.DictReader(text)
    if not reader.fieldnames:
        raise RuntimeError("CSV header could not be read.")
    for row in reader:
        yield {key: "" if value is None else value for key, value in row.items()}


@contextmanager
def connect_db(db_path: Path) -> Iterable[sqlite3.Connection]:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS import_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_started_at TEXT NOT NULL,
            run_finished_at TEXT,
            status TEXT NOT NULL,
            source_url TEXT NOT NULL,
            zip_sha256 TEXT,
            csv_sha256 TEXT,
            csv_filename TEXT,
            rows_read INTEGER NOT NULL DEFAULT 0,
            new_count INTEGER NOT NULL DEFAULT 0,
            updated_count INTEGER NOT NULL DEFAULT 0,
            deleted_count INTEGER NOT NULL DEFAULT 0,
            skipped_same_file INTEGER NOT NULL DEFAULT 0,
            error_message TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS companies (
            record_key TEXT PRIMARY KEY,
            corporation_number TEXT,
            company_name TEXT,
            prefecture TEXT,
            address TEXT,
            tel TEXT,
            homepage TEXT,
            row_sha256 TEXT NOT NULL,
            row_json TEXT NOT NULL,
            is_deleted INTEGER NOT NULL DEFAULT 0,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            deleted_at TEXT,
            source_csv_sha256 TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_companies_corporation_number
            ON companies(corporation_number);

        CREATE INDEX IF NOT EXISTS idx_companies_is_deleted
            ON companies(is_deleted);

        CREATE TABLE IF NOT EXISTS company_changes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            import_log_id INTEGER NOT NULL,
            change_type TEXT NOT NULL,
            record_key TEXT NOT NULL,
            corporation_number TEXT,
            company_name TEXT,
            old_row_sha256 TEXT,
            new_row_sha256 TEXT,
            old_row_json TEXT,
            new_row_json TEXT,
            changed_at TEXT NOT NULL,
            FOREIGN KEY(import_log_id) REFERENCES import_log(id)
        );

        CREATE INDEX IF NOT EXISTS idx_company_changes_import_log_id
            ON company_changes(import_log_id);

        CREATE INDEX IF NOT EXISTS idx_company_changes_change_type
            ON company_changes(change_type);
        """
    )


def create_import_log(conn: sqlite3.Connection) -> int:
    timestamp = now_utc()
    cursor = conn.execute(
        """
        INSERT INTO import_log (
            run_started_at, status, source_url, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?)
        """,
        (timestamp, "RUNNING", SOURCE_URL, timestamp, timestamp),
    )
    return int(cursor.lastrowid)


def finish_import_log(
    conn: sqlite3.Connection,
    import_log_id: int,
    *,
    status: str,
    zip_hash: str = "",
    csv_hash: str = "",
    csv_filename: str = "",
    rows_read: int = 0,
    new_count: int = 0,
    updated_count: int = 0,
    deleted_count: int = 0,
    skipped_same_file: bool = False,
    error_message: str = "",
) -> None:
    timestamp = now_utc()
    conn.execute(
        """
        UPDATE import_log
        SET run_finished_at = ?,
            status = ?,
            zip_sha256 = ?,
            csv_sha256 = ?,
            csv_filename = ?,
            rows_read = ?,
            new_count = ?,
            updated_count = ?,
            deleted_count = ?,
            skipped_same_file = ?,
            error_message = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (
            timestamp,
            status,
            zip_hash,
            csv_hash,
            csv_filename,
            rows_read,
            new_count,
            updated_count,
            deleted_count,
            1 if skipped_same_file else 0,
            error_message,
            timestamp,
            import_log_id,
        ),
    )


def latest_success_hash(conn: sqlite3.Connection) -> str | None:
    row = conn.execute(
        """
        SELECT csv_sha256
        FROM import_log
        WHERE status = 'SUCCESS' AND skipped_same_file = 0 AND csv_sha256 IS NOT NULL
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    return str(row["csv_sha256"]) if row else None


def load_active_companies(conn: sqlite3.Connection) -> dict[str, sqlite3.Row]:
    rows = conn.execute(
        """
        SELECT record_key, row_sha256, row_json, corporation_number, company_name
        FROM companies
        WHERE is_deleted = 0
        """
    ).fetchall()
    return {str(row["record_key"]): row for row in rows}


def insert_change(
    conn: sqlite3.Connection,
    import_log_id: int,
    change_type: str,
    key: str,
    row: dict[str, str] | None,
    old_row: sqlite3.Row | None,
    new_hash: str | None,
) -> None:
    timestamp = now_utc()
    conn.execute(
        """
        INSERT INTO company_changes (
            import_log_id, change_type, record_key, corporation_number, company_name,
            old_row_sha256, new_row_sha256, old_row_json, new_row_json, changed_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            import_log_id,
            change_type,
            key,
            (row or {}).get(CSV_CORP_NO) or (old_row["corporation_number"] if old_row else None),
            (row or {}).get(CSV_COMPANY_NAME) or (old_row["company_name"] if old_row else None),
            old_row["row_sha256"] if old_row else None,
            new_hash,
            str(old_row["row_json"]) if old_row else None,
            canonical_json(row),
            timestamp,
        ),
    )


def upsert_company(
    conn: sqlite3.Connection,
    key: str,
    row: dict[str, str],
    hash_value: str,
    csv_hash: str,
) -> None:
    timestamp = now_utc()
    conn.execute(
        """
        INSERT INTO companies (
            record_key, corporation_number, company_name, prefecture, address,
            tel, homepage, row_sha256, row_json, is_deleted, first_seen_at,
            last_seen_at, deleted_at, source_csv_sha256, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, NULL, ?, ?, ?)
        ON CONFLICT(record_key) DO UPDATE SET
            corporation_number = excluded.corporation_number,
            company_name = excluded.company_name,
            prefecture = excluded.prefecture,
            address = excluded.address,
            tel = excluded.tel,
            homepage = excluded.homepage,
            row_sha256 = excluded.row_sha256,
            row_json = excluded.row_json,
            is_deleted = 0,
            last_seen_at = excluded.last_seen_at,
            deleted_at = NULL,
            source_csv_sha256 = excluded.source_csv_sha256,
            updated_at = excluded.updated_at
        """,
        (
            key,
            row.get(CSV_CORP_NO, ""),
            row.get(CSV_COMPANY_NAME, ""),
            strip_code_prefix(row.get(CSV_PREF, "")),
            row.get(CSV_ADDRESS, ""),
            row.get(CSV_TEL, ""),
            row.get(CSV_HP, ""),
            hash_value,
            canonical_json(row),
            timestamp,
            timestamp,
            csv_hash,
            timestamp,
            timestamp,
        ),
    )


def touch_company_seen(conn: sqlite3.Connection, key: str, csv_hash: str) -> None:
    timestamp = now_utc()
    conn.execute(
        """
        UPDATE companies
        SET last_seen_at = ?,
            source_csv_sha256 = ?,
            updated_at = ?
        WHERE record_key = ?
        """,
        (timestamp, csv_hash, timestamp, key),
    )


def mark_deleted(conn: sqlite3.Connection, key: str, csv_hash: str) -> None:
    timestamp = now_utc()
    conn.execute(
        """
        UPDATE companies
        SET is_deleted = 1,
            deleted_at = ?,
            source_csv_sha256 = ?,
            updated_at = ?
        WHERE record_key = ?
        """,
        (timestamp, csv_hash, timestamp, key),
    )


def make_output_item(
    *,
    change_type: str,
    import_log_id: int,
    sqlite_path: Path,
    zip_hash: str,
    csv_hash: str,
    csv_filename: str,
    rows_read: int,
    new_count: int,
    updated_count: int,
    deleted_count: int,
    row: dict[str, str] | None = None,
    key: str = "",
    hash_value: str = "",
    old_row: sqlite3.Row | None = None,
    skipped_same_file: bool = False,
) -> dict[str, Any]:
    row = row or {}
    old_data = json.loads(str(old_row["row_json"])) if old_row and old_row["row_json"] else {}
    display_row = row or old_data
    pref = strip_code_prefix(display_row.get(CSV_PREF, ""))
    industry = strip_code_prefix(display_row.get(CSV_BIZ, ""))
    summary = display_row.get(CSV_SUMMARY, "") or industry
    old_json = str(old_row["row_json"]) if old_row else ""

    return {
        Schema.URL: SOURCE_URL,
        Schema.NAME: display_row.get(CSV_COMPANY_NAME, old_row["company_name"] if old_row else ""),
        Schema.PREF: pref,
        Schema.ADDR: display_row.get(CSV_ADDRESS, ""),
        Schema.TEL: display_row.get(CSV_TEL, ""),
        Schema.CO_NUM: display_row.get(CSV_CORP_NO, old_row["corporation_number"] if old_row else ""),
        Schema.REP_NM: display_row.get(CSV_REP, ""),
        Schema.EMP_NUM: display_row.get(CSV_EMP, ""),
        Schema.LOB: summary,
        Schema.HP: display_row.get(CSV_HP, ""),
        Schema.CAT_SITE: industry,
        "change_type": change_type,
        "record_key": key,
        "row_sha256": hash_value,
        "old_row_sha256": old_row["row_sha256"] if old_row else "",
        "csv_sha256": csv_hash,
        "zip_sha256": zip_hash,
        "import_log_id": str(import_log_id),
        "rows_read": str(rows_read),
        "new_count": str(new_count),
        "updated_count": str(updated_count),
        "deleted_count": str(deleted_count),
        "skipped_same_file": "1" if skipped_same_file else "0",
        "csv_filename": csv_filename,
        "sqlite_path": str(sqlite_path),
        "raw_prefecture": row.get(CSV_PREF, ""),
        "raw_industry": row.get(CSV_BIZ, ""),
        "row_json": canonical_json(row),
        "old_row_json": old_json,
    }


def get_output_dir() -> Path:
    root = Path(__file__).resolve().parent.parent.parent.parent
    output_dir = root / "output" / "shokuba_mhlw_import"
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def download_zip(session: Any, timeout: int, delay: float) -> tuple[bytes, str]:
    time.sleep(delay)
    session.get(DOWNLOAD_PAGE_URL, timeout=(timeout, 60))
    response = session.get(
        SOURCE_URL,
        timeout=(timeout, 300),
        headers={"Referer": DOWNLOAD_PAGE_URL},
    )
    response.raise_for_status()
    zip_data = response.content
    if not zip_data.startswith(b"PK"):
        raise RuntimeError("Expected a ZIP response from the official download endpoint.")
    return zip_data, sha256_bytes(zip_data)


def extract_csv(zip_data: bytes, output_dir: Path, run_id: str) -> tuple[bytes, str, str]:
    raw_dir = output_dir / "raw" / run_id
    extracted_dir = output_dir / "extracted" / run_id
    raw_dir.mkdir(parents=True, exist_ok=True)
    extracted_dir.mkdir(parents=True, exist_ok=True)

    zip_path = raw_dir / "shokuba_download.zip"
    zip_path.write_bytes(zip_data)

    with zipfile.ZipFile(io.BytesIO(zip_data)) as archive:
        csv_names = [name for name in archive.namelist() if name.lower().endswith(".csv")]
        if not csv_names:
            raise RuntimeError("Downloaded ZIP does not contain a CSV file.")
        csv_name = Path(csv_names[0]).name
        csv_data = archive.read(csv_names[0])

    (extracted_dir / csv_name).write_bytes(csv_data)
    return csv_data, sha256_bytes(csv_data), csv_name


class ShokubaMhlwImportScraper(StaticCrawler):
    """Official Shokuba CSV monthly diff importer."""

    DELAY = 3.0
    EXTRA_COLUMNS = EXTRA_COLUMNS

    def parse(self, url: str) -> Generator[dict, None, None]:
        output_dir = get_output_dir()
        sqlite_path = output_dir / "shokuba.sqlite3"
        run_id = make_run_id()

        with connect_db(sqlite_path) as conn:
            init_db(conn)
            import_log_id = create_import_log(conn)

            try:
                zip_data, zip_hash = download_zip(self.session, self.TIMEOUT, self.DELAY)
                csv_data, csv_hash, csv_filename = extract_csv(zip_data, output_dir, run_id)

                if latest_success_hash(conn) == csv_hash:
                    finish_import_log(
                        conn,
                        import_log_id,
                        status="SKIPPED",
                        zip_hash=zip_hash,
                        csv_hash=csv_hash,
                        csv_filename=csv_filename,
                        skipped_same_file=True,
                    )
                    yield make_output_item(
                        change_type=CHANGE_SKIPPED,
                        import_log_id=import_log_id,
                        sqlite_path=sqlite_path,
                        zip_hash=zip_hash,
                        csv_hash=csv_hash,
                        csv_filename=csv_filename,
                        rows_read=0,
                        new_count=0,
                        updated_count=0,
                        deleted_count=0,
                        skipped_same_file=True,
                    )
                    return

                active = load_active_companies(conn)
                seen_keys: set[str] = set()
                rows_read = 0
                new_count = 0
                updated_count = 0

                for row in iter_csv_rows(csv_data):
                    rows_read += 1
                    key = record_key(row)
                    seen_keys.add(key)
                    hash_value = row_sha256(row)
                    old_row = active.get(key)

                    if old_row is None:
                        new_count += 1
                        insert_change(conn, import_log_id, CHANGE_NEW, key, row, None, hash_value)
                        upsert_company(conn, key, row, hash_value, csv_hash)
                        yield make_output_item(
                            change_type=CHANGE_NEW,
                            import_log_id=import_log_id,
                            sqlite_path=sqlite_path,
                            zip_hash=zip_hash,
                            csv_hash=csv_hash,
                            csv_filename=csv_filename,
                            rows_read=rows_read,
                            new_count=new_count,
                            updated_count=updated_count,
                            deleted_count=0,
                            row=row,
                            key=key,
                            hash_value=hash_value,
                        )
                    elif str(old_row["row_sha256"]) != hash_value:
                        updated_count += 1
                        insert_change(conn, import_log_id, CHANGE_UPDATE, key, row, old_row, hash_value)
                        upsert_company(conn, key, row, hash_value, csv_hash)
                        yield make_output_item(
                            change_type=CHANGE_UPDATE,
                            import_log_id=import_log_id,
                            sqlite_path=sqlite_path,
                            zip_hash=zip_hash,
                            csv_hash=csv_hash,
                            csv_filename=csv_filename,
                            rows_read=rows_read,
                            new_count=new_count,
                            updated_count=updated_count,
                            deleted_count=0,
                            row=row,
                            key=key,
                            hash_value=hash_value,
                            old_row=old_row,
                        )
                    else:
                        touch_company_seen(conn, key, csv_hash)

                    if rows_read % 10000 == 0:
                        self.logger.info("Processed rows: %d", rows_read)

                deleted_count = 0
                for key, old_row in active.items():
                    if key not in seen_keys:
                        deleted_count += 1
                        insert_change(conn, import_log_id, CHANGE_DELETE, key, None, old_row, None)
                        mark_deleted(conn, key, csv_hash)
                        yield make_output_item(
                            change_type=CHANGE_DELETE,
                            import_log_id=import_log_id,
                            sqlite_path=sqlite_path,
                            zip_hash=zip_hash,
                            csv_hash=csv_hash,
                            csv_filename=csv_filename,
                            rows_read=rows_read,
                            new_count=new_count,
                            updated_count=updated_count,
                            deleted_count=deleted_count,
                            key=key,
                            old_row=old_row,
                        )

                finish_import_log(
                    conn,
                    import_log_id,
                    status="SUCCESS",
                    zip_hash=zip_hash,
                    csv_hash=csv_hash,
                    csv_filename=csv_filename,
                    rows_read=rows_read,
                    new_count=new_count,
                    updated_count=updated_count,
                    deleted_count=deleted_count,
                )
            except Exception as exc:
                finish_import_log(
                    conn,
                    import_log_id,
                    status="FAILED",
                    error_message=f"{type(exc).__name__}: {exc}",
                )
                raise
