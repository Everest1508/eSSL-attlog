"""
Data manager utility for handling SQLite3 storage
"""
import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
import logging

logger = logging.getLogger(__name__)

# Unique keys per table used for upsert logic
_UNIQUE_KEYS: Dict[str, tuple] = {
    "attendance":   ("PIN", "Time", "machine_id"),
    "operations":   ("userPin", "codeNumber", "machine_id"),
    "users":        ("PIN",),
    "fingerprints": ("pin", "fingerId"),
    "faces":        ("pin", "faceId"),
}


class DataManager:
    """Manages SQLite3 storage for attendance and related data"""

    def __init__(self, base_path: str = "data"):
        self.db_path = Path(base_path) / "essl.db"
        self.db_path.parent.mkdir(exist_ok=True)
        self._init_db()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @contextmanager
    def _conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init_db(self):
        with self._conn() as conn:
            for table in _UNIQUE_KEYS:
                conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table} (
                        id         INTEGER PRIMARY KEY AUTOINCREMENT,
                        data       TEXT NOT NULL,
                        machine_id TEXT,
                        received_at TEXT
                    )
                """)

    def _load_data(self, table: str) -> List[Dict[str, Any]]:
        with self._conn() as conn:
            rows = conn.execute(f"SELECT data FROM {table}").fetchall()
        return [json.loads(r["data"]) for r in rows]

    def _upsert(self, table: str, records: List[Dict[str, Any]], machine_id: Optional[str]):
        if not records:
            return
        ts = datetime.now().isoformat()
        keys = _UNIQUE_KEYS[table]

        with self._conn() as conn:
            new_count = updated_count = 0
            for record in records:
                record["machine_id"] = machine_id
                record["received_at"] = ts

                # Build WHERE clause from unique key fields
                where_parts = " AND ".join(
                    f"json_extract(data, '$.{k}') = ?" for k in keys
                )
                where_vals = [str(record.get(k, "")) for k in keys]

                row = conn.execute(
                    f"SELECT id FROM {table} WHERE {where_parts}", where_vals
                ).fetchone()

                blob = json.dumps(record, default=str)
                if row:
                    conn.execute(
                        f"UPDATE {table} SET data=?, machine_id=?, received_at=? WHERE id=?",
                        (blob, machine_id, ts, row["id"])
                    )
                    updated_count += 1
                else:
                    conn.execute(
                        f"INSERT INTO {table} (data, machine_id, received_at) VALUES (?, ?, ?)",
                        (blob, machine_id, ts)
                    )
                    new_count += 1

        logger.info(f"Synced {table}: {new_count} new, {updated_count} updated")

    # ------------------------------------------------------------------
    # Public API (same interface as before)
    # ------------------------------------------------------------------

    def get_all_data(self, data_type: str) -> List[Dict[str, Any]]:
        return self._load_data(data_type)

    def clear_data(self, data_type: str):
        with self._conn() as conn:
            conn.execute(f"DELETE FROM {data_type}")
        logger.info(f"Cleared all data from {data_type}")

    def sync_attendance_records(self, records: List[Dict], machine_id: Optional[str] = None):
        self._upsert("attendance", records, machine_id)

    def sync_operations(self, operations: List[Dict], machine_id: Optional[str] = None):
        self._upsert("operations", operations, machine_id)

    def sync_users(self, users: List[Dict], machine_id: Optional[str] = None):
        self._upsert("users", users, machine_id)

    def sync_fingerprints(self, fingerprints: List[Dict], machine_id: Optional[str] = None):
        self._upsert("fingerprints", fingerprints, machine_id)

    def sync_faces(self, faces: List[Dict], machine_id: Optional[str] = None):
        self._upsert("faces", faces, machine_id)

    # Backward-compat aliases
    def append_attendance_records(self, records, machine_id=None):
        return self.sync_attendance_records(records, machine_id)

    def append_operations(self, operations, machine_id=None):
        return self.sync_operations(operations, machine_id)

    def append_users(self, users, machine_id=None):
        return self.sync_users(users, machine_id)

    def append_fingerprints(self, fingerprints, machine_id=None):
        return self.sync_fingerprints(fingerprints, machine_id)

    def append_faces(self, faces, machine_id=None):
        return self.sync_faces(faces, machine_id)


# Global instance
data_manager = DataManager()
