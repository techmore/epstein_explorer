#!/usr/bin/env python3
"""
# pip install flask
Web GUI for training_data.db (compatible with the TUI schema).

Features:
- Top words / top entities
- Boolean search (AND/OR/NOT)
- Add/remove keywords from active filter
- Matching files with hit counts
- Snippet viewer per file
- File path view + optional local open (macOS)
- Entity co-occurrence view

Run:
  python app.py --root /Users/techmore/Documents/training_data
Then open:
  http://127.0.0.1:5000
"""

from __future__ import annotations

import argparse
import os
import re
import shlex
import sqlite3
import subprocess
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

from flask import Flask, abort, redirect, render_template, request, url_for

DB_PATH = Path(os.getenv("TRAINING_DATA_DB", str(Path.home() / "training_data.db"))).expanduser()
DEFAULT_ROOT = Path.cwd()

# If set to 1, enables /open-file to call `open` on macOS host.
ENABLE_LOCAL_OPEN = os.getenv("ENABLE_LOCAL_OPEN", "0") == "1"


def create_app(root: Path) -> Flask:
    app = Flask(__name__)
    app.config["ROOT_PATH"] = root.resolve()
    app.config["DB_PATH"] = DB_PATH

    def get_conn() -> sqlite3.Connection:
        if not app.config["DB_PATH"].exists() or app.config["DB_PATH"].stat().st_size == 0:
            abort(503, f"SQLite DB is missing or empty: {app.config['DB_PATH']}")
        conn = sqlite3.connect(str(app.config["DB_PATH"]))
        conn.row_factory = sqlite3.Row
        return conn

    @app.context_processor
    def inject_globals() -> Dict[str, str]:
        return {
            "db_path": str(app.config["DB_PATH"]),
            "root_path": str(app.config["ROOT_PATH"]),
        }

    @app.route("/")
    def home():
        q = request.args.get("q", "").strip()
        limit = int(request.args.get("limit", "50") or "50")
        with get_conn() as conn:
            top_words = query_top_words(conn, limit=limit)
            top_entities = query_top_entities(conn, limit=limit)
            recent_stats = query_stats(conn)
        return render_template(
            "index.html",
            q=q,
            top_words=top_words,
            top_entities=top_entities,
            stats=recent_stats,
            limit=limit,
        )

    @app.route("/search")
    def search():
        q = request.args.get("q", "").strip()
        limit = int(request.args.get("limit", "300") or "300")
        with get_conn() as conn:
            rows = query_matching_files(conn, q, limit=limit)
        return render_template("search.html", q=q, rows=rows, limit=limit)

    @app.route("/snippets")
    def snippets():
        q = request.args.get("q", "").strip()
        top_folder = request.args.get("top_folder", "")
        rel_path = request.args.get("rel_path", "")
        file_name = request.args.get("file_name", "")
        limit = int(request.args.get("limit", "120") or "120")

        if not file_name:
            abort(400, "file_name is required")

        with get_conn() as conn:
            rows = query_snippets_for_file(conn, top_folder, rel_path, file_name, q, limit=limit)

        abs_path = file_abs_from_parts(app.config["ROOT_PATH"], top_folder, rel_path, file_name)
        return render_template(
            "snippets.html",
            q=q,
            rows=rows,
            top_folder=top_folder,
            rel_path=rel_path,
            file_name=file_name,
            abs_path=str(abs_path),
            limit=limit,
            exists=abs_path.exists(),
        )

    @app.route("/correlations")
    def correlations():
        limit = int(request.args.get("limit", "80") or "80")
        with get_conn() as conn:
            rows = query_entity_correlations(conn, limit=limit)
        return render_template("correlations.html", rows=rows, limit=limit)

    @app.route("/filter/add")
    def add_filter_keyword():
        q = request.args.get("q", "").strip()
        kw = request.args.get("kw", "").strip()
        target = request.args.get("target", "search")

        if kw:
            q = f"{q} AND {kw}".strip() if q else kw

        if target == "home":
            return redirect(url_for("home", q=q))
        return redirect(url_for("search", q=q))

    @app.route("/filter/remove")
    def remove_filter_keyword():
        q = request.args.get("q", "").strip()
        kw = request.args.get("kw", "").strip()
        target = request.args.get("target", "search")

        if kw and q:
            try:
                toks = shlex.split(q)
            except Exception:
                toks = q.split()
            lowered = kw.lower()
            toks = [t for t in toks if t.lower() not in {lowered, f"-{lowered}"}]
            q = " ".join(toks).strip()

        if target == "home":
            return redirect(url_for("home", q=q))
        return redirect(url_for("search", q=q))

    @app.route("/open-file")
    def open_file():
        # Local convenience for macOS only (optional / disabled by default).
        if not ENABLE_LOCAL_OPEN:
            abort(403, "Local open is disabled. Set ENABLE_LOCAL_OPEN=1 to enable.")

        top_folder = request.args.get("top_folder", "")
        rel_path = request.args.get("rel_path", "")
        file_name = request.args.get("file_name", "")

        if not file_name:
            abort(400, "file_name is required")

        abs_path = file_abs_from_parts(app.config["ROOT_PATH"], top_folder, rel_path, file_name)
        root_path = app.config["ROOT_PATH"].resolve()
        try:
            abs_path.resolve().relative_to(root_path)
        except ValueError:
            abort(403, f"Path is outside configured root: {abs_path}")
        if not abs_path.exists():
            abort(404, f"File not found: {abs_path}")

        subprocess.run(["open", str(abs_path)], check=False)
        return redirect(
            url_for(
                "snippets",
                top_folder=top_folder,
                rel_path=rel_path,
                file_name=file_name,
                q=request.args.get("q", ""),
            )
        )

    @app.route("/health")
    def health():
        exists = app.config["DB_PATH"].exists()
        size = app.config["DB_PATH"].stat().st_size if exists else 0
        return {
            "ok": True,
            "db_exists": exists,
            "db_size": size,
            "db_path": str(app.config["DB_PATH"]),
            "root": str(app.config["ROOT_PATH"]),
        }

    @app.route("/admin/rebuild-fts")
    def rebuild_fts():
        with get_conn() as conn:
            ensure_snippets_fts(conn, rebuild=True)
        return redirect(url_for("home"))

    return app


def ensure_snippets_fts(conn: sqlite3.Connection, rebuild: bool = False) -> bool:
    """
    Create/repair the FTS5 side index used for snippet search.
    """
    try:
        had_fts = table_exists(conn, "snippets_fts")
        conn.execute(
            """
            CREATE VIRTUAL TABLE IF NOT EXISTS snippets_fts
            USING fts5(sentence, content='snippets', content_rowid='id')
            """
        )
        conn.execute(
            """
            CREATE TRIGGER IF NOT EXISTS snippets_ai AFTER INSERT ON snippets BEGIN
                INSERT INTO snippets_fts(rowid, sentence) VALUES (new.id, new.sentence);
            END
            """
        )
        conn.execute(
            """
            CREATE TRIGGER IF NOT EXISTS snippets_ad AFTER DELETE ON snippets BEGIN
                INSERT INTO snippets_fts(snippets_fts, rowid, sentence)
                VALUES('delete', old.id, old.sentence);
            END
            """
        )
        conn.execute(
            """
            CREATE TRIGGER IF NOT EXISTS snippets_au AFTER UPDATE ON snippets BEGIN
                INSERT INTO snippets_fts(snippets_fts, rowid, sentence)
                VALUES('delete', old.id, old.sentence);
                INSERT INTO snippets_fts(rowid, sentence) VALUES (new.id, new.sentence);
            END
            """
        )
        if rebuild or not had_fts:
            conn.execute("INSERT INTO snippets_fts(snippets_fts) VALUES('rebuild')")
        conn.commit()
        return True
    except sqlite3.Error:
        conn.rollback()
        return False


def parse_filter_expr(expr: str) -> List[Dict[str, List[str]]]:
    """
    Parse simple boolean logic:
    - Terms default to AND within group
    - OR splits groups
    - NOT term (or -term) excludes term
    """
    expr = (expr or "").strip()
    if not expr:
        return []

    try:
        tokens = shlex.split(expr)
    except Exception:
        tokens = expr.split()

    groups: List[Dict[str, List[str]]] = [{"must": [], "not": []}]
    i = 0
    while i < len(tokens):
        tok = tokens[i]
        upper = tok.upper()

        if upper == "OR":
            groups.append({"must": [], "not": []})
        elif upper == "AND":
            pass
        elif upper == "NOT":
            i += 1
            if i < len(tokens):
                term = tokens[i].strip().lower()
                if term:
                    groups[-1]["not"].append(term)
        elif tok.startswith("-") and len(tok) > 1:
            groups[-1]["not"].append(tok[1:].strip().lower())
        else:
            term = tok.strip().lower()
            if term:
                groups[-1]["must"].append(term)
        i += 1

    return [g for g in groups if g["must"] or g["not"]]


def quote_fts_term(term: str) -> str:
    return '"' + term.replace('"', '""') + '"'


def fts_query_for_filter(expr: str) -> str | None:
    groups = parse_filter_expr(expr)
    if not groups:
        return None

    fts_groups: List[str] = []
    for group in groups:
        must = [quote_fts_term(term) for term in group["must"]]
        excluded = [quote_fts_term(term) for term in group["not"]]
        if not must:
            continue
        group_sql = " AND ".join(must)
        for term in excluded:
            group_sql = f"{group_sql} NOT {term}"
        fts_groups.append(f"({group_sql})")

    if not fts_groups:
        return None
    return " OR ".join(fts_groups)


def sql_where_for_filter(expr: str, column_sql: str = "LOWER(sentence)") -> Tuple[str, List[str]]:
    groups = parse_filter_expr(expr)
    if not groups:
        return "1=1", []

    all_group_sql: List[str] = []
    params: List[str] = []

    for g in groups:
        parts: List[str] = []
        for term in g["must"]:
            parts.append(f"{column_sql} LIKE ?")
            params.append(f"%{term}%")
        for term in g["not"]:
            parts.append(f"{column_sql} NOT LIKE ?")
            params.append(f"%{term}%")
        if not parts:
            parts = ["1=1"]
        all_group_sql.append("(" + " AND ".join(parts) + ")")

    return "(" + " OR ".join(all_group_sql) + ")", params


def query_stats(conn: sqlite3.Connection) -> Dict[str, int]:
    words = conn.execute("SELECT COUNT(*) AS c FROM words").fetchone()["c"]
    entities = conn.execute("SELECT COUNT(*) AS c FROM entities").fetchone()["c"]
    pairs = conn.execute("SELECT COUNT(*) AS c FROM cooccurrences").fetchone()["c"]
    files = conn.execute("SELECT COUNT(*) AS c FROM file_index").fetchone()["c"]
    return {"words": words, "entities": entities, "pairs": pairs, "files": files}


def table_exists(conn: sqlite3.Connection, name: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone() is not None


def query_top_words(conn: sqlite3.Connection, limit: int = 50) -> Sequence[sqlite3.Row]:
    if table_exists(conn, "word_totals"):
        return conn.execute(
            """
            SELECT word, total_count, file_count
            FROM word_totals
            ORDER BY total_count DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

    return conn.execute(
        """
        SELECT word, SUM(count) AS total_count, COUNT(*) AS file_count
        FROM words
        GROUP BY word
        ORDER BY total_count DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()


def query_top_entities(conn: sqlite3.Connection, limit: int = 50) -> Sequence[sqlite3.Row]:
    if table_exists(conn, "entity_totals"):
        return conn.execute(
            """
            SELECT entity, label, total_count, file_count
            FROM entity_totals
            ORDER BY total_count DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

    return conn.execute(
        """
        SELECT entity, label, COUNT(*) AS total_count,
               COUNT(DISTINCT top_folder || '/' || rel_path || '/' || file_name) AS file_count
        FROM entities
        GROUP BY entity, label
        ORDER BY total_count DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()


def query_matching_files(conn: sqlite3.Connection, filter_expr: str, limit: int = 300) -> Sequence[sqlite3.Row]:
    fts_query = fts_query_for_filter(filter_expr)
    if fts_query and ensure_snippets_fts(conn):
        try:
            return conn.execute(
                """
                SELECT s.top_folder, s.rel_path, s.file_name, COUNT(*) AS hits
                FROM snippets_fts
                JOIN snippets s ON s.id = snippets_fts.rowid
                WHERE snippets_fts MATCH ?
                GROUP BY s.top_folder, s.rel_path, s.file_name
                ORDER BY hits DESC, s.top_folder, s.rel_path, s.file_name
                LIMIT ?
                """,
                (fts_query, limit),
            ).fetchall()
        except sqlite3.Error:
            pass

    where_sql, params = sql_where_for_filter(filter_expr, column_sql="LOWER(sentence)")
    return conn.execute(
        f"""
        SELECT top_folder, rel_path, file_name, COUNT(*) AS hits
        FROM snippets
        WHERE {where_sql}
        GROUP BY top_folder, rel_path, file_name
        ORDER BY hits DESC, top_folder, rel_path, file_name
        LIMIT ?
        """,
        (*params, limit),
    ).fetchall()


def query_snippets_for_file(
    conn: sqlite3.Connection,
    top_folder: str,
    rel_path: str,
    file_name: str,
    filter_expr: str,
    limit: int = 120,
) -> Sequence[sqlite3.Row]:
    fts_query = fts_query_for_filter(filter_expr)
    if fts_query and ensure_snippets_fts(conn):
        try:
            return conn.execute(
                """
                SELECT s.sentence
                FROM snippets_fts
                JOIN snippets s ON s.id = snippets_fts.rowid
                WHERE snippets_fts MATCH ?
                  AND s.top_folder = ? AND s.rel_path = ? AND s.file_name = ?
                ORDER BY s.id
                LIMIT ?
                """,
                (fts_query, top_folder, rel_path, file_name, limit),
            ).fetchall()
        except sqlite3.Error:
            pass

    where_sql, params = sql_where_for_filter(filter_expr, column_sql="LOWER(sentence)")
    return conn.execute(
        f"""
        SELECT sentence
        FROM snippets
        WHERE top_folder = ? AND rel_path = ? AND file_name = ? AND ({where_sql})
        ORDER BY id
        LIMIT ?
        """,
        (top_folder, rel_path, file_name, *params, limit),
    ).fetchall()


def query_entity_correlations(conn: sqlite3.Connection, limit: int = 80) -> Sequence[sqlite3.Row]:
    if table_exists(conn, "cooccurrence_totals"):
        return conn.execute(
            """
            SELECT entity1, entity2, total_count, total_files AS files
            FROM cooccurrence_totals
            ORDER BY total_count DESC, total_files DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

    return conn.execute(
        """
        SELECT entity1, entity2, SUM(count) AS total_count, SUM(file_count) AS files
        FROM cooccurrences
        GROUP BY entity1, entity2
        ORDER BY total_count DESC, files DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()


def file_abs_from_parts(root: Path, top_folder: str, rel_path: str, file_name: str) -> Path:
    if top_folder == "__ROOT__":
        return root / file_name
    if rel_path:
        return root / top_folder / rel_path / file_name
    return root / top_folder / file_name


def main() -> int:
    parser = argparse.ArgumentParser(description="Web GUI for training_data.db")
    parser.add_argument("--root", type=str, default=str(DEFAULT_ROOT), help="PDF root folder")
    parser.add_argument("--host", type=str, default="127.0.0.1", help="Host bind")
    parser.add_argument("--port", type=int, default=5000, help="Port bind")
    parser.add_argument("--debug", action="store_true", help="Enable Flask debug")
    args = parser.parse_args()

    root = Path(args.root).expanduser().resolve()
    app = create_app(root)

    # Fast startup check for DB; app still starts even if DB missing.
    if not DB_PATH.exists():
        print(f"[WARN] DB not found yet: {DB_PATH}")

    app.run(host=args.host, port=args.port, debug=args.debug)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
