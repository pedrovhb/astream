from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import AsyncIterator

from astream.stream import transformer, Stream

try:
    import sqlite_utils
except ImportError:
    raise ImportError("sqlite_utils is not installed. Install with `pip install sqlite-utils`")


def _get_sqlite_table(
    filename_or_conn: Path
    | str
    | sqlite_utils.Database
    | sqlite_utils.db.Table
    | sqlite3.Connection,
    table_name: str = "data",
    add_db_extension: bool = True,
) -> sqlite_utils.db.Table:
    if isinstance(filename_or_conn, sqlite_utils.Database):
        return filename_or_conn[table_name]
    elif isinstance(filename_or_conn, sqlite_utils.db.Table):
        return filename_or_conn
    elif isinstance(filename_or_conn, (str, Path)):
        filename_or_conn = Path(filename_or_conn)
        if add_db_extension and not filename_or_conn.name.lower().endswith(".db"):
            filename_or_conn = str(filename_or_conn) + ".db"
        db = sqlite_utils.Database(filename_or_conn)
        return db[table_name]
    elif isinstance(filename_or_conn, sqlite3.Connection):
        db = sqlite_utils.Database(filename_or_conn)
        return db[table_name]
    else:
        raise TypeError(
            f"source must be a sqlite_utils.Database, sqlite_utils.db.Table, "
            f"str, or Path, not {type(filename_or_conn)}"
        )


@transformer
async def to_sqlite(
    data: Stream[dict[str, object]],
    filename_or_conn: Path | str | sqlite_utils.Database | sqlite_utils.db.Table,
    table_name: str = "data",
    add_db_extension: bool = True,
    upsert: bool = False,
) -> AsyncIterator[None]:
    table = _get_sqlite_table(filename_or_conn, table_name, add_db_extension)
    async for item in data:
        if upsert:
            table.upsert(item)
        else:
            table.insert(item)
    yield None
