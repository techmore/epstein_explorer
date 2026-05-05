#!/usr/bin/env python3
"""
Standalone SQLite query TUI for training_data.db.

This tool is read-only and independent from the indexer.
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

from prompt_toolkit import PromptSession
from rich.console import Console
from rich.table import Table

DEFAULT_DB_PATH = Path(os.getenv("TRAINING_DATA_DB", str(Path.home() / "training_data.db"))).expanduser()
TUI_TABLE_LIMIT = 50
DEFAULT_RESULT_LIMIT = 1000
DEFAULT_PAGE_SIZE = 50
MATCH_FILE_LIMIT = 300
SNIPPETS_PER_VIEW = 80
DEFAULT_MIN_FILE_COUNT = 20
DEFAULT_MAX_DF_RATIO = 0.70

COMMON_STOPWORDS = {
    "a", "an", "and", "are", "as", "at", "be", "been", "being", "but", "by",
    "can", "could", "did", "do", "does", "doing", "for", "from", "had", "has",
    "have", "having", "he", "her", "here", "hers", "herself", "him", "himself",
    "his", "how", "i", "if", "in", "into", "is", "it", "its", "itself", "just",
    "may", "me", "might", "more", "most", "my", "myself", "no", "nor", "not",
    "of", "on", "or", "our", "ours", "ourselves", "she", "should", "so", "some",
    "such", "than", "that", "the", "their", "theirs", "them", "themselves",
    "then", "there", "these", "they", "this", "those", "to", "too", "us", "very",
    "was", "we", "were", "what", "when", "where", "which", "while", "who", "why",
    "will", "with", "would", "you", "your", "yours", "yourself", "yourselves",
}
NOISE_TERMS = {
    "subject", "sent", "pm", "am", "gmail.com", "e-mail", "mailto", "re", "fw",
}

console = Console()


def paginate_rows(
    session: PromptSession,
    title: str,
    columns: Sequence[Tuple[str, str]],
    rows: Sequence[Tuple],
    page_size: int,
) -> int | None:
    if not rows:
        console.print("[yellow]No rows.[/yellow]")
        return None

    page_size = max(1, int(page_size))
    total = len(rows)
    page = 0
    pages = (total + page_size - 1) // page_size

    while True:
        start = page * page_size
        end = min(total, start + page_size)
        table = Table(title=f"{title} [{start + 1}-{end}/{total}] page {page + 1}/{pages}")
        for col_name, justify in columns:
            table.add_column(col_name, justify=justify)
        for i, row in enumerate(rows[start:end], start + 1):
            table.add_row(str(i), *[str(v) for v in row])
        console.print(table)

        if pages == 1:
            cmd = session.prompt("Enter 'v <row>' to view row, or Enter to continue... ").strip().lower()
            if cmd.startswith("v "):
                parts = cmd.split()
                if len(parts) == 2 and parts[1].isdigit():
                    idx = int(parts[1]) - 1
                    if 0 <= idx < total:
                        return idx
            return None

        cmd = session.prompt("Page: [n]ext [p]rev [g]oto [v row] [q]uit > ").strip().lower()
        if cmd in {"q", "quit", ""}:
            return None
        if cmd in {"n", "next"}:
            if page < pages - 1:
                page += 1
            continue
        if cmd in {"p", "prev"}:
            if page > 0:
                page -= 1
            continue
        if cmd.startswith("g"):
            parts = cmd.split()
            if len(parts) == 2 and parts[1].isdigit():
                candidate = int(parts[1]) - 1
                if 0 <= candidate < pages:
                    page = candidate
            continue
        if cmd.startswith("v "):
            parts = cmd.split()
            if len(parts) == 2 and parts[1].isdigit():
                idx = int(parts[1]) - 1
                if 0 <= idx < total:
                    return idx
            continue


def ensure_query_indexes(conn: sqlite3.Connection) -> None:
    had_fts = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='snippets_fts'"
    ).fetchone() is not None
    conn.execute("CREATE INDEX IF NOT EXISTS idx_words_word ON words(word)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_entities_entity_label ON entities(entity, label)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_entities_file ON entities(top_folder, rel_path, file_name)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_coocc_pair ON cooccurrences(entity1, entity2)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_snippets_sentence ON snippets(sentence)")
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
    if not had_fts:
        conn.execute("INSERT INTO snippets_fts(snippets_fts) VALUES('rebuild')")
    conn.commit()


def rebuild_snippets_fts(conn: sqlite3.Connection) -> None:
    ensure_query_indexes(conn)
    conn.execute("INSERT INTO snippets_fts(snippets_fts) VALUES('rebuild')")
    conn.commit()


def rebuild_query_cache(conn: sqlite3.Connection) -> None:
    console.print("[bold]Building query cache...[/bold] this can take a while on large DBs.")
    rebuild_snippets_fts(conn)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS word_totals (
            word TEXT PRIMARY KEY,
            total_count INTEGER NOT NULL,
            file_count INTEGER NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS entity_totals (
            entity TEXT NOT NULL,
            label TEXT NOT NULL,
            total_count INTEGER NOT NULL,
            file_count INTEGER NOT NULL,
            PRIMARY KEY (entity, label)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS cooccurrence_totals (
            entity1 TEXT NOT NULL,
            entity2 TEXT NOT NULL,
            total_count INTEGER NOT NULL,
            total_files INTEGER NOT NULL,
            PRIMARY KEY (entity1, entity2)
        )
        """
    )
    conn.execute("DELETE FROM word_totals")
    conn.execute(
        """
        INSERT INTO word_totals(word, total_count, file_count)
        SELECT word, SUM(count) AS total_count, COUNT(*) AS file_count
        FROM words
        GROUP BY word
        """
    )
    conn.execute("DELETE FROM entity_totals")
    conn.execute(
        """
        INSERT INTO entity_totals(entity, label, total_count, file_count)
        SELECT entity, label, COUNT(*) AS total_count,
               COUNT(DISTINCT top_folder || '/' || rel_path || '/' || file_name) AS file_count
        FROM entities
        GROUP BY entity, label
        """
    )
    conn.execute("DELETE FROM cooccurrence_totals")
    conn.execute(
        """
        INSERT INTO cooccurrence_totals(entity1, entity2, total_count, total_files)
        SELECT entity1, entity2, SUM(count) AS total_count, SUM(file_count) AS total_files
        FROM cooccurrences
        GROUP BY entity1, entity2
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_word_totals_total ON word_totals(total_count DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_entity_totals_total ON entity_totals(total_count DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_coocc_totals_total ON cooccurrence_totals(total_count DESC)")
    conn.execute("ANALYZE")
    conn.commit()
    console.print("[green]Query cache ready.[/green]")


def file_abs_from_parts(root: Path, top_folder: str, rel_path: str, file_name: str) -> Path:
    if top_folder == "__ROOT__":
        return root / file_name
    if rel_path:
        return root / top_folder / rel_path / file_name
    return root / top_folder / file_name


def parse_filter_expr(expr: str) -> List[Dict[str, List[str]]]:
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
                term = tokens[i].lower().strip()
                if term:
                    groups[-1]["not"].append(term)
        elif tok.startswith("-") and len(tok) > 1:
            groups[-1]["not"].append(tok[1:].lower().strip())
        else:
            term = tok.lower().strip()
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

    group_sql = []
    params: List[str] = []
    for g in groups:
        parts = []
        for term in g["must"]:
            parts.append(f"{column_sql} LIKE ?")
            params.append(f"%{term}%")
        for term in g["not"]:
            parts.append(f"{column_sql} NOT LIKE ?")
            params.append(f"%{term}%")
        if not parts:
            parts = ["1=1"]
        group_sql.append("(" + " AND ".join(parts) + ")")
    return "(" + " OR ".join(group_sql) + ")", params


def get_top_words(conn: sqlite3.Connection, limit: int = TUI_TABLE_LIMIT) -> List[Tuple[str, int, int]]:
    cache_exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='word_totals'"
    ).fetchone()
    if cache_exists:
        rows = conn.execute(
            """
            SELECT word, total_count, file_count
            FROM word_totals
            ORDER BY total_count DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT word, SUM(count) AS total_count, COUNT(*) AS file_count
            FROM words
            GROUP BY word
            ORDER BY total_count DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

    return [(str(word), int(total), int(fcount)) for word, total, fcount in rows]


def get_top_content_words(
    conn: sqlite3.Connection,
    limit: int = TUI_TABLE_LIMIT,
    min_file_count: int = DEFAULT_MIN_FILE_COUNT,
    max_df_ratio: float = DEFAULT_MAX_DF_RATIO,
) -> List[Tuple[str, int, int]]:
    total_files_row = conn.execute("SELECT COUNT(*) FROM file_index").fetchone()
    total_files = int(total_files_row[0]) if total_files_row and total_files_row[0] else 0
    if total_files <= 0:
        return []

    max_file_count = max(1, int(total_files * max_df_ratio))
    stopword_params = list(COMMON_STOPWORDS | NOISE_TERMS)
    placeholders = ", ".join("?" for _ in stopword_params)

    cache_exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='word_totals'"
    ).fetchone()
    source = "word_totals" if cache_exists else (
        "SELECT word, SUM(count) AS total_count, COUNT(*) AS file_count FROM words GROUP BY word"
    )

    if cache_exists:
        sql = f"""
            SELECT word, total_count, file_count
            FROM {source}
            WHERE file_count BETWEEN ? AND ?
              AND LENGTH(word) >= 3
              AND LOWER(word) NOT IN ({placeholders})
              AND word NOT LIKE '%@%'
              AND word NOT LIKE '%.com'
              AND word NOT LIKE '%.org'
              AND word NOT LIKE '%.net'
            ORDER BY total_count DESC
            LIMIT ?
        """
        params = (min_file_count, max_file_count, *stopword_params, limit)
    else:
        sql = f"""
            SELECT word, total_count, file_count
            FROM ({source})
            WHERE file_count BETWEEN ? AND ?
              AND LENGTH(word) >= 3
              AND LOWER(word) NOT IN ({placeholders})
              AND word NOT LIKE '%@%'
              AND word NOT LIKE '%.com'
              AND word NOT LIKE '%.org'
              AND word NOT LIKE '%.net'
            ORDER BY total_count DESC
            LIMIT ?
        """
        params = (min_file_count, max_file_count, *stopword_params, limit)

    rows = conn.execute(sql, params).fetchall()
    filtered_rows = [
        (w, t, f) for (w, t, f) in rows
        if re.search(r"[A-Za-z]", w) and w.lower() not in COMMON_STOPWORDS and w.lower() not in NOISE_TERMS
    ]

    return [(str(word), int(total), int(fcount)) for word, total, fcount in filtered_rows[:limit]]


def get_top_entities(conn: sqlite3.Connection, limit: int = TUI_TABLE_LIMIT) -> List[Tuple[str, str, int, int]]:
    cache_exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='entity_totals'"
    ).fetchone()
    if cache_exists:
        rows = conn.execute(
            """
            SELECT entity, label, total_count, file_count
            FROM entity_totals
            ORDER BY total_count DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    else:
        rows = conn.execute(
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

    return [(str(entity), str(label), int(count), int(fcount)) for entity, label, count, fcount in rows]


def query_matching_files(
    conn: sqlite3.Connection,
    filter_expr: str,
    limit: int = MATCH_FILE_LIMIT,
) -> List[Tuple[str, str, str, int]]:
    fts_query = fts_query_for_filter(filter_expr)
    if fts_query:
        try:
            rows = conn.execute(
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
            return rows
        except sqlite3.Error:
            pass

    where_sql, params = sql_where_for_filter(filter_expr, column_sql="LOWER(sentence)")
    rows = conn.execute(
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
    return rows


def show_matching_files(rows: Sequence[Tuple[str, str, str, int]]) -> None:
    table = Table(title=f"Matching Files ({len(rows)})")
    table.add_column("#", justify="right")
    table.add_column("Top Folder")
    table.add_column("Rel Path")
    table.add_column("File")
    table.add_column("Hits", justify="right")
    for i, (top, relp, fname, hits) in enumerate(rows, 1):
        table.add_row(str(i), top, relp or ".", fname, str(hits))
    console.print(table)


def show_snippets_for_file(
    conn: sqlite3.Connection,
    top_folder: str,
    rel_path: str,
    file_name: str,
    filter_expr: str,
    limit: int = SNIPPETS_PER_VIEW,
) -> None:
    fts_query = fts_query_for_filter(filter_expr)
    if fts_query:
        try:
            rows = conn.execute(
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
            rows = []
        else:
            table = Table(title=f"Context Snippets: {file_name} ({len(rows)})")
            table.add_column("#", justify="right")
            table.add_column("Snippet")
            for i, (snippet,) in enumerate(rows, 1):
                table.add_row(str(i), snippet)
            console.print(table)
            return

    where_sql, params = sql_where_for_filter(filter_expr, column_sql="LOWER(sentence)")
    rows = conn.execute(
        f"""
        SELECT sentence
        FROM snippets
        WHERE top_folder = ? AND rel_path = ? AND file_name = ? AND ({where_sql})
        ORDER BY id
        LIMIT ?
        """,
        (top_folder, rel_path, file_name, *params, limit),
    ).fetchall()

    table = Table(title=f"Context Snippets: {file_name} ({len(rows)})")
    table.add_column("#", justify="right")
    table.add_column("Snippet")
    for i, (snippet,) in enumerate(rows, 1):
        table.add_row(str(i), snippet)
    console.print(table)


def get_entity_correlations(conn: sqlite3.Connection, limit: int = TUI_TABLE_LIMIT) -> List[Tuple[str, str, int, int]]:
    cache_exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='cooccurrence_totals'"
    ).fetchone()
    if cache_exists:
        rows = conn.execute(
            """
            SELECT entity1, entity2, total_count, total_files
            FROM cooccurrence_totals
            ORDER BY total_count DESC, total_files DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT entity1, entity2, SUM(count) AS total_count, SUM(file_count) AS files
            FROM cooccurrences
            GROUP BY entity1, entity2
            ORDER BY total_count DESC, files DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

    return [(str(e1), str(e2), int(cnt), int(files)) for e1, e2, cnt, files in rows]


def get_entity_neighbors(
    conn: sqlite3.Connection,
    entity_like: str,
    limit: int = TUI_TABLE_LIMIT,
) -> List[Tuple[str, int, int]]:
    pattern = f"%{entity_like.lower()}%"
    rows = conn.execute(
        """
        SELECT other_entity, SUM(cnt) AS total_count, SUM(files) AS total_files
        FROM (
            SELECT entity2 AS other_entity, count AS cnt, file_count AS files
            FROM cooccurrences
            WHERE LOWER(entity1) LIKE ?
            UNION ALL
            SELECT entity1 AS other_entity, count AS cnt, file_count AS files
            FROM cooccurrences
            WHERE LOWER(entity2) LIKE ?
        ) t
        GROUP BY other_entity
        ORDER BY total_count DESC, total_files DESC
        LIMIT ?
        """,
        (pattern, pattern, limit),
    ).fetchall()
    return [(str(name), int(cnt), int(files)) for name, cnt, files in rows]


def get_multi_entity_file_comentions(
    conn: sqlite3.Connection,
    entity_terms: List[str],
    limit: int = TUI_TABLE_LIMIT,
) -> List[Tuple[str, str, str, int, int]]:
    terms = [t.strip().lower() for t in entity_terms if t.strip()]
    if len(terms) < 2:
        return []
    placeholders = ", ".join("?" for _ in terms)
    like_terms = [f"%{t}%" for t in terms]
    rows = conn.execute(
        f"""
        WITH matched AS (
            SELECT top_folder, rel_path, file_name, LOWER(entity) AS ent_norm
            FROM entities
            WHERE {" OR ".join("LOWER(entity) LIKE ?" for _ in terms)}
        ),
        per_file AS (
            SELECT top_folder, rel_path, file_name,
                   COUNT(DISTINCT ent_norm) AS matched_entities,
                   COUNT(*) AS total_mentions
            FROM matched
            GROUP BY top_folder, rel_path, file_name
        )
        SELECT top_folder, rel_path, file_name, matched_entities, total_mentions
        FROM per_file
        WHERE matched_entities >= ?
        ORDER BY matched_entities DESC, total_mentions DESC, top_folder, rel_path, file_name
        LIMIT ?
        """,
        (*like_terms, len(terms), limit),
    ).fetchall()
    return [(str(t), str(r), str(f), int(m), int(c)) for t, r, f, m, c in rows]


def search_keyword_snippets(
    conn: sqlite3.Connection,
    keyword_expr: str,
    limit: int = TUI_TABLE_LIMIT,
) -> List[Tuple[str, str, str, str]]:
    fts_query = fts_query_for_filter(keyword_expr)
    if fts_query:
        try:
            rows = conn.execute(
                """
                SELECT s.top_folder, s.rel_path, s.file_name, s.sentence
                FROM snippets_fts
                JOIN snippets s ON s.id = snippets_fts.rowid
                WHERE snippets_fts MATCH ?
                ORDER BY s.id DESC
                LIMIT ?
                """,
                (fts_query, limit),
            ).fetchall()
            return [(str(t), str(r), str(f), str(s)) for t, r, f, s in rows]
        except sqlite3.Error:
            pass

    where_sql, params = sql_where_for_filter(keyword_expr, column_sql="LOWER(sentence)")
    rows = conn.execute(
        f"""
        SELECT top_folder, rel_path, file_name, sentence
        FROM snippets
        WHERE {where_sql}
        ORDER BY id DESC
        LIMIT ?
        """,
        (*params, limit),
    ).fetchall()
    return [(str(t), str(r), str(f), str(s)) for t, r, f, s in rows]


def get_entity_file_mentions(
    conn: sqlite3.Connection,
    entity: str,
    label: str,
    limit: int = TUI_TABLE_LIMIT,
) -> List[Tuple[str, str, str, int]]:
    rows = conn.execute(
        """
        SELECT top_folder, rel_path, file_name, COUNT(*) AS mentions
        FROM entities
        WHERE entity = ? AND label = ?
        GROUP BY top_folder, rel_path, file_name
        ORDER BY mentions DESC, top_folder, rel_path, file_name
        LIMIT ?
        """,
        (entity, label, limit),
    ).fetchall()
    return [(str(t), str(r), str(f), int(m)) for t, r, f, m in rows]


def get_entity_snippets(
    conn: sqlite3.Connection,
    entity: str,
    label: str,
    limit: int = 100,
) -> List[Tuple[str]]:
    rows = conn.execute(
        """
        SELECT sentence_snippet
        FROM entities
        WHERE entity = ? AND label = ?
        ORDER BY rowid DESC
        LIMIT ?
        """,
        (entity, label, limit),
    ).fetchall()
    return [(str(s),) for (s,) in rows]


def get_word_file_hits(
    conn: sqlite3.Connection,
    word: str,
    limit: int = TUI_TABLE_LIMIT,
) -> List[Tuple[str, str, str, int]]:
    rows = conn.execute(
        """
        SELECT top_folder, rel_path, file_name, count
        FROM words
        WHERE word = ?
        ORDER BY count DESC, top_folder, rel_path, file_name
        LIMIT ?
        """,
        (word, limit),
    ).fetchall()
    return [(str(t), str(r), str(f), int(c)) for t, r, f, c in rows]


def entity_detail_view(
    session: PromptSession,
    conn: sqlite3.Connection,
    root: Path,
    entity: str,
    label: str,
    result_limit: int,
    page_size: int,
) -> None:
    while True:
        console.print(f"\n[bold]Entity Detail[/bold] {entity} [{label}]")
        console.print("1) Top files by mention count")
        console.print("2) Recent mention snippets")
        console.print("3) Back")
        pick = session.prompt("Select option: ").strip().lower()
        if pick in {"3", "b", "back", "q"}:
            return
        if pick == "1":
            rows = get_entity_file_mentions(conn, entity, label, limit=result_limit)
            idx = paginate_rows(
                session,
                f"Files for entity '{entity}'",
                [("#", "right"), ("Top Folder", "left"), ("Rel Path", "left"), ("File", "left"), ("Mentions", "right")],
                rows,
                page_size=page_size,
            )
            if idx is not None:
                top, relp, fname, _ = rows[idx]
                open_now = session.prompt("Open this file? [y/N]: ").strip().lower()
                if open_now in {"y", "yes"}:
                    open_file_default(root, top, relp, fname)
        elif pick == "2":
            rows = get_entity_snippets(conn, entity, label, limit=max(100, page_size * 5))
            paginate_rows(
                session,
                f"Snippets for entity '{entity}'",
                [("#", "right"), ("Snippet", "left")],
                rows,
                page_size=page_size,
            )


def word_detail_view(
    session: PromptSession,
    conn: sqlite3.Connection,
    root: Path,
    word: str,
    result_limit: int,
    page_size: int,
) -> None:
    while True:
        console.print(f"\n[bold]Word Detail[/bold] {word}")
        console.print("1) Top files by word count")
        console.print("2) Back")
        pick = session.prompt("Select option: ").strip().lower()
        if pick in {"2", "b", "back", "q"}:
            return
        if pick == "1":
            rows = get_word_file_hits(conn, word, limit=result_limit)
            idx = paginate_rows(
                session,
                f"Files for word '{word}'",
                [("#", "right"), ("Top Folder", "left"), ("Rel Path", "left"), ("File", "left"), ("Count", "right")],
                rows,
                page_size=page_size,
            )
            if idx is not None:
                top, relp, fname, _ = rows[idx]
                open_now = session.prompt("Open this file? [y/N]: ").strip().lower()
                if open_now in {"y", "yes"}:
                    open_file_default(root, top, relp, fname)


def open_file_default(root: Path, top_folder: str, rel_path: str, file_name: str) -> None:
    target = file_abs_from_parts(root, top_folder, rel_path, file_name)
    if not target.exists():
        console.print(f"[red]File not found:[/red] {target}")
        return

    import platform
    import shutil

    system = platform.system()
    if system == "Darwin":
        subprocess.run(["open", str(target)], check=False)
    else:
        opener = shutil.which("xdg-open")
        if not opener:
            console.print("[red]Cannot open file:[/red] xdg-open not found on this system")
            return
        subprocess.run([opener, str(target)], check=False)
    console.print(f"[green]Opened:[/green] {target}")


def run_tui(conn: sqlite3.Connection, root: Path, db_path: Path) -> None:
    session = PromptSession()
    active_filter = ""
    last_file_matches: List[Tuple[str, str, str, int]] = []
    result_limit = DEFAULT_RESULT_LIMIT
    page_size = DEFAULT_PAGE_SIZE

    while True:
        console.print("\n[bold]PDF Training Query TUI[/bold]")
        console.print(f"DB: {db_path}")
        console.print(f"Root: {root}")
        console.print(f"Active filter: [cyan]{active_filter or '(none)'}[/cyan]")
        console.print(f"Result limit: [cyan]{result_limit}[/cyan]  Page size: [cyan]{page_size}[/cyan]")
        console.print(
            "\n"
            "1) Browse top content words\n"
            "2) Browse top entities\n"
            "3) Set filter query (AND/OR/NOT)\n"
            "4) Add keyword to filter (AND)\n"
            "5) Remove keyword from filter\n"
            "6) Show matching files + hit count\n"
            "7) View context snippets for selected match\n"
            "8) Open selected file in default viewer\n"
            "9) Top co-mentions (pairs)\n"
            "10) Rebuild query cache/indexes\n"
            "11) Browse raw top words\n"
            "12) Query display settings\n"
            "13) Co-mentions for one entity\n"
            "14) Co-mentions for 2+ entities (file intersection)\n"
            "15) General keyword search (snippets)\n"
            "16) Quit\n"
        )

        choice = session.prompt("Select option: ").strip().lower()

        if choice in {"16", "q", "quit", "exit"}:
            console.print("Goodbye.")
            break

        if choice == "1":
            rows = get_top_content_words(conn, limit=result_limit)
            idx = paginate_rows(
                session,
                f"Top Content Words (Top {result_limit})",
                [("#", "right"), ("Word", "left"), ("Total Count", "right"), ("Files", "right")],
                rows,
                page_size=page_size,
            )
            if idx is not None:
                word, _cnt, _files = rows[idx]
                word_detail_view(session, conn, root, word, result_limit, page_size)

        elif choice == "2":
            rows = get_top_entities(conn, limit=result_limit)
            idx = paginate_rows(
                session,
                f"Top Entities (Top {result_limit})",
                [("#", "right"), ("Entity", "left"), ("Label", "left"), ("Mentions", "right"), ("Files", "right")],
                rows,
                page_size=page_size,
            )
            if idx is not None:
                entity, label, _cnt, _files = rows[idx]
                entity_detail_view(session, conn, root, entity, label, result_limit, page_size)

        elif choice == "3":
            active_filter = session.prompt("Enter filter query: ").strip()
            console.print(f"Filter set: [cyan]{active_filter or '(none)'}[/cyan]")

        elif choice == "4":
            kw = session.prompt("Keyword to add: ").strip()
            if kw:
                if active_filter:
                    active_filter = f"{active_filter} AND {kw}"
                else:
                    active_filter = kw
                console.print(f"Filter now: [cyan]{active_filter}[/cyan]")

        elif choice == "5":
            kw = session.prompt("Keyword to remove: ").strip()
            if kw and active_filter:
                try:
                    toks = shlex.split(active_filter)
                except Exception:
                    toks = active_filter.split()
                toks = [t for t in toks if t.lower() != kw.lower() and t.lower() != f"-{kw.lower()}"]
                active_filter = " ".join(toks).strip()
            console.print(f"Filter now: [cyan]{active_filter or '(none)'}[/cyan]")

        elif choice == "6":
            last_file_matches = query_matching_files(conn, active_filter, limit=max(result_limit, MATCH_FILE_LIMIT))
            show_matching_files(last_file_matches)
            session.prompt("Press Enter to continue...")

        elif choice == "7":
            if not last_file_matches:
                last_file_matches = query_matching_files(conn, active_filter, limit=MATCH_FILE_LIMIT)
            if not last_file_matches:
                console.print("[yellow]No matching files.[/yellow]")
                continue
            show_matching_files(last_file_matches[:50])
            pick = session.prompt("Select file # for snippets: ").strip()
            if pick.isdigit():
                idx = int(pick) - 1
                if 0 <= idx < len(last_file_matches):
                    top, relp, fname, _hits = last_file_matches[idx]
                    show_snippets_for_file(conn, top, relp, fname, active_filter, limit=SNIPPETS_PER_VIEW)
                    session.prompt("Press Enter to continue...")

        elif choice == "8":
            if not last_file_matches:
                last_file_matches = query_matching_files(conn, active_filter, limit=MATCH_FILE_LIMIT)
            if not last_file_matches:
                console.print("[yellow]No matching files.[/yellow]")
                continue
            show_matching_files(last_file_matches[:50])
            pick = session.prompt("Select file # to open: ").strip()
            if pick.isdigit():
                idx = int(pick) - 1
                if 0 <= idx < len(last_file_matches):
                    top, relp, fname, _hits = last_file_matches[idx]
                    open_file_default(root, top, relp, fname)

        elif choice == "9":
            rows = get_entity_correlations(conn, limit=result_limit)
            paginate_rows(
                session,
                f"Entity Correlations (Top {result_limit})",
                [("#", "right"), ("Entity 1", "left"), ("Entity 2", "left"), ("Co-mentions", "right"), ("Files", "right")],
                rows,
                page_size=page_size,
            )

        elif choice == "10":
            rebuild_query_cache(conn)
            session.prompt("Press Enter to continue...")

        elif choice == "11":
            rows = get_top_words(conn, limit=result_limit)
            idx = paginate_rows(
                session,
                f"Top Words (Top {result_limit})",
                [("#", "right"), ("Word", "left"), ("Total Count", "right"), ("Files", "right")],
                rows,
                page_size=page_size,
            )
            if idx is not None:
                word, _cnt, _files = rows[idx]
                word_detail_view(session, conn, root, word, result_limit, page_size)

        elif choice == "12":
            new_limit = session.prompt(f"Result limit [{result_limit}]: ").strip()
            new_page = session.prompt(f"Page size [{page_size}]: ").strip()
            if new_limit.isdigit() and int(new_limit) > 0:
                result_limit = int(new_limit)
            if new_page.isdigit() and int(new_page) > 0:
                page_size = int(new_page)
            console.print(f"[green]Updated:[/green] result_limit={result_limit}, page_size={page_size}")

        elif choice == "13":
            probe = session.prompt("Entity/name contains: ").strip()
            if not probe:
                console.print("[yellow]No entity term entered.[/yellow]")
                continue
            rows = get_entity_neighbors(conn, probe, limit=result_limit)
            paginate_rows(
                session,
                f"Co-mentions for '{probe}' (Top {result_limit})",
                [("#", "right"), ("Co-mentioned Entity", "left"), ("Co-mentions", "right"), ("Files", "right")],
                rows,
                page_size=page_size,
            )

        elif choice == "14":
            probe = session.prompt("Enter 2+ entities (comma-separated): ").strip()
            terms = [t.strip() for t in probe.split(",") if t.strip()]
            if len(terms) < 2:
                console.print("[yellow]Enter at least 2 entity terms.[/yellow]")
                continue
            rows = get_multi_entity_file_comentions(conn, terms, limit=result_limit)
            idx = paginate_rows(
                session,
                f"Files mentioning all: {', '.join(terms)} (Top {result_limit})",
                [("#", "right"), ("Top Folder", "left"), ("Rel Path", "left"), ("File", "left"), ("Matched", "right"), ("Mentions", "right")],
                rows,
                page_size=page_size,
            )
            if idx is not None:
                top, relp, fname, _m, _c = rows[idx]
                open_now = session.prompt("Open this file? [y/N]: ").strip().lower()
                if open_now in {"y", "yes"}:
                    open_file_default(root, top, relp, fname)

        elif choice == "15":
            probe = session.prompt("Keyword/boolean query: ").strip()
            if not probe:
                console.print("[yellow]No keyword query entered.[/yellow]")
                continue
            rows = search_keyword_snippets(conn, probe, limit=result_limit)
            idx = paginate_rows(
                session,
                f"Keyword snippets for: {probe} (Top {result_limit})",
                [("#", "right"), ("Top Folder", "left"), ("Rel Path", "left"), ("File", "left"), ("Snippet", "left")],
                rows,
                page_size=page_size,
            )
            if idx is not None:
                top, relp, fname, _snippet = rows[idx]
                open_now = session.prompt("Open this file? [y/N]: ").strip().lower()
                if open_now in {"y", "yes"}:
                    open_file_default(root, top, relp, fname)

        else:
            console.print("[yellow]Unknown option.[/yellow]")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Launch a standalone query TUI for an existing training_data SQLite DB."
    )
    parser.add_argument(
        "--db",
        type=str,
        default=str(DEFAULT_DB_PATH),
        help=f"Path to SQLite DB (default: {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--root",
        type=str,
        default=str(Path.cwd()),
        help="Root path used to reconstruct/open file paths from DB parts (default: current directory).",
    )
    parser.add_argument(
        "--prepare-only",
        action="store_true",
        help="Build query indexes/cache and exit.",
    )
    args = parser.parse_args()

    db_path = Path(args.db).expanduser().resolve()
    root = Path(args.root).expanduser().resolve()

    if not db_path.exists():
        console.print(f"[red]DB not found:[/red] {db_path}")
        return 1
    if not root.exists() or not root.is_dir():
        console.print(f"[red]Invalid root folder:[/red] {root}")
        return 1

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        ensure_query_indexes(conn)
        if args.prepare_only:
            rebuild_query_cache(conn)
            return 0
        run_tui(conn, root, db_path)
    except KeyboardInterrupt:
        console.print("\nInterrupted. Exiting cleanly.")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
