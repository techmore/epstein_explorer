# pip install pdfplumber PyPDF2 nltk spacy tqdm rich prompt_toolkit
#!/usr/bin/env python3
"""
Standalone PDF indexer + terminal UI for macOS.

- Indexes PDF text into SQLite: ~/training_data.db
- Extracts words, entities, co-occurrences, and snippets
- Supports incremental/resumable indexing
- Launches an interactive TUI-style menu after indexing

Notes:
- Uses spaCy model "en_core_web_sm" for NER. If missing, this script will try to download it.
- Uses NLTK sentence/word tokenizers. If missing, this script will download required tokenizer data.
"""

from __future__ import annotations

import argparse
import os
import re
import shlex
import sqlite3
import subprocess
import sys
import time
from collections import Counter
from itertools import combinations
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

import nltk
import pdfplumber
import spacy
from prompt_toolkit import PromptSession
from PyPDF2 import PdfReader
from rich.console import Console
from rich.table import Table
from tqdm import tqdm

# -----------------------------
# Configuration
# -----------------------------

DB_PATH = Path.home() / "training_data.db"

# Skip common junk/system/temp names and extension patterns.
IGNORED_DIRS = {
    ".git",
    "node_modules",
    "__pycache__",
    ".venv",
    "venv",
    ".mypy_cache",
    ".pytest_cache",
}
IGNORED_FILE_NAMES = {
    ".ds_store",
    "thumbs.db",
}
TEMP_FILE_PREFIXES = ("~$", ".~", "._")
TEMP_FILE_SUFFIXES = (".tmp", ".temp", ".swp", ".part", ".crdownload")

MAX_SNIPPETS_PER_FILE = 5000  # Safety cap for very large files.
MAX_SNIPPET_LEN = 500         # Sentence snippet length cap for display/context.
TUI_TABLE_LIMIT = 50
MATCH_FILE_LIMIT = 300
SNIPPETS_PER_VIEW = 80

console = Console()


# -----------------------------
# NLP setup
# -----------------------------

def ensure_nlp_resources() -> "spacy.language.Language":
    """
    Ensure NLTK tokenizers + spaCy model are available.
    Returns loaded spaCy model.
    """
    try:
        nltk.data.find("tokenizers/punkt")
    except LookupError:
        nltk.download("punkt", quiet=True)

    # Some NLTK installs expect punkt_tab.
    try:
        nltk.data.find("tokenizers/punkt_tab")
    except LookupError:
        try:
            nltk.download("punkt_tab", quiet=True)
        except Exception:
            pass

    try:
        return spacy.load("en_core_web_sm")
    except Exception:
        # Attempt automatic download if model is missing.
        from spacy.cli import download as spacy_download
        spacy_download("en_core_web_sm")
        return spacy.load("en_core_web_sm")


# -----------------------------
# DB setup
# -----------------------------

def init_db(conn: sqlite3.Connection) -> None:
    """
    Create required tables + helper tables and indexes.
    """
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")

    # Required table: words
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS words (
            word TEXT,
            count INT,
            top_folder TEXT,
            rel_path TEXT,
            file_name TEXT,
            PRIMARY KEY (word, top_folder, rel_path, file_name)
        )
        """
    )

    # Required table: entities
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS entities (
            entity TEXT,
            label TEXT,
            top_folder TEXT,
            rel_path TEXT,
            file_name TEXT,
            sentence_snippet TEXT
        )
        """
    )

    # Required table: cooccurrences
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS cooccurrences (
            entity1 TEXT,
            entity2 TEXT,
            count INT,
            file_count INT,
            top_folder TEXT,
            PRIMARY KEY (entity1, entity2, top_folder)
        )
        """
    )

    # Tracks indexed files for incremental updates.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS file_index (
            abs_path TEXT PRIMARY KEY,
            mtime REAL,
            size INT,
            last_indexed REAL,
            top_folder TEXT,
            rel_path TEXT,
            file_name TEXT
        )
        """
    )

    # Stores file-level pair contributions so we can subtract on re-index.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS cooccurrence_file (
            entity1 TEXT,
            entity2 TEXT,
            count INT,
            top_folder TEXT,
            rel_path TEXT,
            file_name TEXT,
            PRIMARY KEY (entity1, entity2, top_folder, rel_path, file_name)
        )
        """
    )

    # Snippet table for search/filter + context view.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS snippets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            top_folder TEXT,
            rel_path TEXT,
            file_name TEXT,
            sentence TEXT
        )
        """
    )

    # FTS5 keeps keyword search from scanning every snippet on large DBs.
    had_snippets_fts = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='snippets_fts'"
    ).fetchone() is not None
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
    if not had_snippets_fts:
        conn.execute("INSERT INTO snippets_fts(snippets_fts) VALUES('rebuild')")

    # Helpful indexes
    conn.execute("CREATE INDEX IF NOT EXISTS idx_words_word ON words(word)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_words_file ON words(top_folder, rel_path, file_name)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_entities_entity ON entities(entity)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_entities_label ON entities(label)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_entities_file ON entities(top_folder, rel_path, file_name)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_snippets_file ON snippets(top_folder, rel_path, file_name)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_file_index_file ON file_index(top_folder, rel_path, file_name)")
    conn.commit()


# -----------------------------
# File discovery and filtering
# -----------------------------

def should_ignore_file(name: str) -> bool:
    lower = name.lower()
    if lower in IGNORED_FILE_NAMES:
        return True
    if any(lower.startswith(p.lower()) for p in TEMP_FILE_PREFIXES):
        return True
    if any(lower.endswith(s.lower()) for s in TEMP_FILE_SUFFIXES):
        return True
    return False


def discover_pdf_files(root: Path) -> List[Path]:
    """
    Recursively discover PDFs while pruning ignored directories/files.
    """
    pdfs: List[Path] = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in IGNORED_DIRS and not d.startswith(".git")]
        for fname in filenames:
            if should_ignore_file(fname):
                continue
            if fname.lower().endswith(".pdf"):
                pdfs.append(Path(dirpath) / fname)
    return pdfs


def split_file_parts(root: Path, file_path: Path) -> Tuple[str, str, str]:
    """
    Return (top_folder, rel_path, file_name) where:
    - top_folder is first path segment under root
    - rel_path is directory path under top_folder (can be "")
    """
    rel = file_path.relative_to(root)
    parts = rel.parts
    if len(parts) == 1:
        top_folder = "__ROOT__"
        rel_path = ""
        file_name = parts[0]
    else:
        top_folder = parts[0]
        file_name = parts[-1]
        rel_path = str(Path(*parts[1:-1])) if len(parts) > 2 else ""
    return top_folder, rel_path, file_name


def file_abs_from_parts(root: Path, top_folder: str, rel_path: str, file_name: str) -> Path:
    if top_folder == "__ROOT__":
        return root / file_name
    if rel_path:
        return root / top_folder / rel_path / file_name
    return root / top_folder / file_name


# -----------------------------
# Text extraction / normalization
# -----------------------------

def extract_pdf_text(path: Path) -> str:
    """
    Extract reasonably clean text:
    - Prefer pdfplumber
    - Crop top/bottom margins to reduce headers/footers
    - Fallback to PyPDF2 if needed
    """
    pages_text: List[str] = []

    try:
        with pdfplumber.open(str(path)) as pdf:
            for page in pdf.pages:
                try:
                    # Crop 5% top and bottom to reduce repetitive headers/footers.
                    h = page.height
                    w = page.width
                    cropped = page.within_bbox((0, h * 0.05, w, h * 0.95))
                    text = cropped.extract_text(x_tolerance=2, y_tolerance=2) or ""
                    if text.strip():
                        pages_text.append(text)
                except Exception:
                    # Fall back to full page extraction for this page.
                    text = page.extract_text() or ""
                    if text.strip():
                        pages_text.append(text)
        if pages_text:
            return "\n".join(pages_text)
    except Exception:
        pass

    # Fallback extractor
    try:
        reader = PdfReader(str(path))
        for page in reader.pages:
            text = page.extract_text() or ""
            if text.strip():
                pages_text.append(text)
    except Exception:
        return ""

    return "\n".join(pages_text)


def clean_text(text: str) -> str:
    text = text.replace("\x00", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def normalize_token(token: str) -> str:
    """
    Lowercase and strip punctuation around token.
    Keeps internal punctuation when meaningful (e.g. can't -> cant after strip pass).
    """
    token = token.strip()
    token = re.sub(r"^\W+|\W+$", "", token)
    token = token.lower()
    return token


def map_entity_label(spacy_label: str) -> str | None:
    """
    Map spaCy labels to required categories.
    """
    if spacy_label == "PERSON":
        return "PERSON"
    if spacy_label in {"GPE", "LOC", "FAC"}:
        return "LOCATION"
    if spacy_label == "ORG":
        return "ORGANIZATION"
    if spacy_label == "PRODUCT":
        return "PRODUCT"
    if spacy_label == "EVENT":
        return "EVENT"
    if spacy_label == "WORK_OF_ART":
        return "WORK_OF_ART"
    if spacy_label == "LAW":
        return "LAW"
    return None


# -----------------------------
# Incremental indexing helpers
# -----------------------------

def should_process_file(conn: sqlite3.Connection, path: Path) -> bool:
    """
    True if file is new or changed by mtime/size since last index.
    """
    st = path.stat()
    row = conn.execute(
        "SELECT mtime, size FROM file_index WHERE abs_path = ?",
        (str(path),),
    ).fetchone()
    if row is None:
        return True
    old_mtime, old_size = row
    if int(old_size) != int(st.st_size):
        return True
    if abs(float(old_mtime) - float(st.st_mtime)) > 1e-6:
        return True
    return False


def remove_file_records(
    conn: sqlite3.Connection,
    top_folder: str,
    rel_path: str,
    file_name: str,
) -> None:
    """
    Remove all prior per-file data and subtract old co-occurrence contribution.
    """
    old_pairs = conn.execute(
        """
        SELECT entity1, entity2, count
        FROM cooccurrence_file
        WHERE top_folder = ? AND rel_path = ? AND file_name = ?
        """,
        (top_folder, rel_path, file_name),
    ).fetchall()

    for e1, e2, cnt in old_pairs:
        conn.execute(
            """
            UPDATE cooccurrences
            SET count = count - ?, file_count = file_count - 1
            WHERE entity1 = ? AND entity2 = ? AND top_folder = ?
            """,
            (int(cnt), e1, e2, top_folder),
        )

    conn.execute(
        "DELETE FROM cooccurrences WHERE count <= 0 OR file_count <= 0"
    )
    conn.execute(
        """
        DELETE FROM cooccurrence_file
        WHERE top_folder = ? AND rel_path = ? AND file_name = ?
        """,
        (top_folder, rel_path, file_name),
    )
    conn.execute(
        "DELETE FROM words WHERE top_folder = ? AND rel_path = ? AND file_name = ?",
        (top_folder, rel_path, file_name),
    )
    conn.execute(
        "DELETE FROM entities WHERE top_folder = ? AND rel_path = ? AND file_name = ?",
        (top_folder, rel_path, file_name),
    )
    conn.execute(
        "DELETE FROM snippets WHERE top_folder = ? AND rel_path = ? AND file_name = ?",
        (top_folder, rel_path, file_name),
    )


def remove_missing_files(conn: sqlite3.Connection, existing_paths: set[str]) -> int:
    """
    Remove DB entries for files that no longer exist on disk.
    """
    rows = conn.execute(
        "SELECT abs_path, top_folder, rel_path, file_name FROM file_index"
    ).fetchall()
    removed = 0
    for abs_path, top_folder, rel_path, file_name in rows:
        if abs_path not in existing_paths:
            with conn:
                remove_file_records(conn, top_folder, rel_path, file_name)
                conn.execute("DELETE FROM file_index WHERE abs_path = ?", (abs_path,))
            removed += 1
    return removed


# -----------------------------
# Indexing core
# -----------------------------

def index_single_file(
    conn: sqlite3.Connection,
    nlp: "spacy.language.Language",
    root: Path,
    file_path: Path,
) -> Tuple[int, int, int]:
    """
    Index one file.
    Returns: (word_rows, entity_rows, pair_rows)
    """
    top_folder, rel_path, file_name = split_file_parts(root, file_path)
    st = file_path.stat()

    with conn:
        # Remove prior version of this file first (for clean re-index).
        remove_file_records(conn, top_folder, rel_path, file_name)

        raw_text = extract_pdf_text(file_path)
        text = clean_text(raw_text)

        if not text:
            # Still update file_index so we don't repeatedly retry unchanged empty/bad files.
            conn.execute(
                """
                INSERT INTO file_index(abs_path, mtime, size, last_indexed, top_folder, rel_path, file_name)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(abs_path) DO UPDATE SET
                    mtime=excluded.mtime,
                    size=excluded.size,
                    last_indexed=excluded.last_indexed,
                    top_folder=excluded.top_folder,
                    rel_path=excluded.rel_path,
                    file_name=excluded.file_name
                """,
                (str(file_path), st.st_mtime, st.st_size, time.time(), top_folder, rel_path, file_name),
            )
            return (0, 0, 0)

        # Sentence segmentation for snippets + co-occurrence window.
        try:
            sentences = nltk.sent_tokenize(text)
        except Exception:
            # Fallback sentence split if punkt fails unexpectedly.
            sentences = re.split(r"(?<=[.!?])\s+", text)

        sentences = [s.strip() for s in sentences if s and s.strip()]

        # Insert snippets (bounded by MAX_SNIPPETS_PER_FILE for DB size safety).
        snippet_rows = [
            (top_folder, rel_path, file_name, s[:MAX_SNIPPET_LEN])
            for s in sentences[:MAX_SNIPPETS_PER_FILE]
        ]
        if snippet_rows:
            conn.executemany(
                "INSERT INTO snippets(top_folder, rel_path, file_name, sentence) VALUES (?, ?, ?, ?)",
                snippet_rows,
            )

        # Tokenize words with NLTK and build per-file counts.
        word_counter: Counter[str] = Counter()
        for sent in sentences:
            try:
                tokens = nltk.word_tokenize(sent)
            except Exception:
                tokens = sent.split()

            for tok in tokens:
                norm = normalize_token(tok)
                if not norm:
                    continue
                if not re.search(r"[a-zA-Z]", norm):
                    continue
                word_counter[norm] += 1

        if word_counter:
            conn.executemany(
                """
                INSERT INTO words(word, count, top_folder, rel_path, file_name)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(word, top_folder, rel_path, file_name)
                DO UPDATE SET count=excluded.count
                """,
                [(w, int(c), top_folder, rel_path, file_name) for w, c in word_counter.items()],
            )

        # NER + sentence-level co-occurrence.
        entity_rows: List[Tuple[str, str, str, str, str, str]] = []
        pair_counter: Counter[Tuple[str, str]] = Counter()

        # Use spaCy in batches to keep memory stable on large docs.
        for doc, sent in zip(nlp.pipe(sentences, batch_size=64), sentences):
            ents_in_sent: List[str] = []
            for ent in doc.ents:
                mapped = map_entity_label(ent.label_)
                if not mapped:
                    continue
                ent_text = ent.text.strip()
                if len(ent_text) < 2:
                    continue

                entity_rows.append(
                    (ent_text, mapped, top_folder, rel_path, file_name, sent[:MAX_SNIPPET_LEN])
                )
                ents_in_sent.append(ent_text)

            unique_ents = sorted(set(ents_in_sent), key=lambda x: x.lower())
            for e1, e2 in combinations(unique_ents, 2):
                a, b = (e1, e2) if e1.lower() <= e2.lower() else (e2, e1)
                pair_counter[(a, b)] += 1

        if entity_rows:
            conn.executemany(
                """
                INSERT INTO entities(entity, label, top_folder, rel_path, file_name, sentence_snippet)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                entity_rows,
            )

        # Save file-level pairs and roll up to top_folder aggregate cooccurrences.
        if pair_counter:
            conn.executemany(
                """
                INSERT INTO cooccurrence_file(entity1, entity2, count, top_folder, rel_path, file_name)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(entity1, entity2, top_folder, rel_path, file_name)
                DO UPDATE SET count=excluded.count
                """,
                [(a, b, int(cnt), top_folder, rel_path, file_name) for (a, b), cnt in pair_counter.items()],
            )

            for (a, b), cnt in pair_counter.items():
                conn.execute(
                    """
                    INSERT INTO cooccurrences(entity1, entity2, count, file_count, top_folder)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(entity1, entity2, top_folder)
                    DO UPDATE SET
                        count = cooccurrences.count + excluded.count,
                        file_count = cooccurrences.file_count + excluded.file_count
                    """,
                    (a, b, int(cnt), 1, top_folder),
                )

        # Mark file as indexed.
        conn.execute(
            """
            INSERT INTO file_index(abs_path, mtime, size, last_indexed, top_folder, rel_path, file_name)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(abs_path) DO UPDATE SET
                mtime=excluded.mtime,
                size=excluded.size,
                last_indexed=excluded.last_indexed,
                top_folder=excluded.top_folder,
                rel_path=excluded.rel_path,
                file_name=excluded.file_name
            """,
            (str(file_path), st.st_mtime, st.st_size, time.time(), top_folder, rel_path, file_name),
        )

    return (len(word_counter), len(entity_rows), len(pair_counter))


def run_indexing(conn: sqlite3.Connection, nlp: "spacy.language.Language", root: Path) -> None:
    """
    Full indexing pass (incremental):
    - Discover files
    - Remove deleted files from DB
    - Process only new/changed files
    """
    console.print(f"[bold]Scanning:[/bold] {root}")
    files = discover_pdf_files(root)
    existing = {str(p) for p in files}
    removed = remove_missing_files(conn, existing)

    processed = 0
    skipped = 0
    errors = 0

    progress = tqdm(files, desc="Indexing PDFs", unit="file")
    for file_path in progress:
        try:
            if not should_process_file(conn, file_path):
                skipped += 1
                continue
            w, e, p = index_single_file(conn, nlp, root, file_path)
            processed += 1
            progress.set_postfix(words=w, entities=e, pairs=p)
        except KeyboardInterrupt:
            raise
        except Exception:
            errors += 1
            continue

    console.print(
        f"[bold]Index complete.[/bold] processed={processed} skipped={skipped} removed={removed} errors={errors}"
    )


# -----------------------------
# Filter/search utilities
# -----------------------------

def parse_filter_expr(expr: str) -> List[Dict[str, List[str]]]:
    """
    Parse simple boolean logic:
    - Terms default to AND within a group
    - Use OR to separate groups
    - Use NOT term or -term for exclusion
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

    # Remove empty groups
    return [g for g in groups if g["must"] or g["not"]]


def sql_where_for_filter(expr: str, column_sql: str = "LOWER(sentence)") -> Tuple[str, List[str]]:
    """
    Convert parsed filter expression into SQL WHERE clause + params.
    """
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


# -----------------------------
# TUI views
# -----------------------------

def show_top_words(conn: sqlite3.Connection, limit: int = TUI_TABLE_LIMIT) -> None:
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

    table = Table(title=f"Top Words (Top {limit})")
    table.add_column("#", justify="right")
    table.add_column("Word")
    table.add_column("Total Count", justify="right")
    table.add_column("Files", justify="right")

    for i, (word, total, fcount) in enumerate(rows, 1):
        table.add_row(str(i), word, str(total), str(fcount))
    console.print(table)


def show_top_entities(conn: sqlite3.Connection, limit: int = TUI_TABLE_LIMIT) -> None:
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

    table = Table(title=f"Top Entities (Top {limit})")
    table.add_column("#", justify="right")
    table.add_column("Entity")
    table.add_column("Label")
    table.add_column("Mentions", justify="right")
    table.add_column("Files", justify="right")

    for i, (entity, label, count, fcount) in enumerate(rows, 1):
        table.add_row(str(i), entity, label, str(count), str(fcount))
    console.print(table)


def query_matching_files(
    conn: sqlite3.Connection, filter_expr: str, limit: int = MATCH_FILE_LIMIT
) -> List[Tuple[str, str, str, int]]:
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


def show_entity_correlations(conn: sqlite3.Connection, limit: int = TUI_TABLE_LIMIT) -> None:
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

    table = Table(title=f"Entity Correlations (Top {limit})")
    table.add_column("#", justify="right")
    table.add_column("Entity 1")
    table.add_column("Entity 2")
    table.add_column("Co-mentions", justify="right")
    table.add_column("Files", justify="right")

    for i, (e1, e2, cnt, files) in enumerate(rows, 1):
        table.add_row(str(i), e1, e2, str(cnt), str(files))
    console.print(table)


def open_file_in_macos(root: Path, top_folder: str, rel_path: str, file_name: str) -> None:
    target = file_abs_from_parts(root, top_folder, rel_path, file_name)
    if not target.exists():
        console.print(f"[red]File not found:[/red] {target}")
        return
    subprocess.run(["open", str(target)], check=False)
    console.print(f"[green]Opened:[/green] {target}")


# -----------------------------
# TUI main loop
# -----------------------------

def run_tui(conn: sqlite3.Connection, nlp: "spacy.language.Language", root: Path) -> None:
    session = PromptSession()
    active_filter = ""
    last_file_matches: List[Tuple[str, str, str, int]] = []

    while True:
        console.print("\n[bold]PDF Training Data TUI[/bold]")
        console.print(f"DB: {DB_PATH}")
        console.print(f"Root: {root}")
        console.print(f"Active filter: [cyan]{active_filter or '(none)'}[/cyan]")
        console.print(
            "\n"
            "1) Browse top words\n"
            "2) Browse top entities\n"
            "3) Set filter query (AND/OR/NOT)\n"
            "4) Add keyword to filter (AND)\n"
            "5) Remove keyword from filter\n"
            "6) Show matching files + hit count\n"
            "7) View context snippets for selected match\n"
            "8) Open selected file in default viewer\n"
            "9) Entity correlation view\n"
            "10) Refresh / re-index\n"
            "11) Quit\n"
        )

        choice = session.prompt("Select option: ").strip().lower()

        if choice in {"11", "q", "quit", "exit"}:
            console.print("Goodbye.")
            break

        if choice == "1":
            show_top_words(conn)
            session.prompt("Press Enter to continue...")

        elif choice == "2":
            show_top_entities(conn)
            session.prompt("Press Enter to continue...")

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
                # Basic token remove; keeps rest of expression.
                try:
                    toks = shlex.split(active_filter)
                except Exception:
                    toks = active_filter.split()
                toks = [t for t in toks if t.lower() != kw.lower() and t.lower() != f"-{kw.lower()}"]
                active_filter = " ".join(toks).strip()
            console.print(f"Filter now: [cyan]{active_filter or '(none)'}[/cyan]")

        elif choice == "6":
            last_file_matches = query_matching_files(conn, active_filter, limit=MATCH_FILE_LIMIT)
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
                    open_file_in_macos(root, top, relp, fname)

        elif choice == "9":
            show_entity_correlations(conn)
            session.prompt("Press Enter to continue...")

        elif choice == "10":
            run_indexing(conn, nlp, root)

        else:
            console.print("[yellow]Unknown option.[/yellow]")


# -----------------------------
# Main
# -----------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Index PDFs into SQLite and launch a terminal UI."
    )
    parser.add_argument(
        "--root",
        type=str,
        default=str(Path.cwd()),
        help="Root folder containing top-level training-data folders (default: current directory).",
    )
    parser.add_argument(
        "--skip-index",
        action="store_true",
        help="Skip indexing pass and launch TUI immediately.",
    )
    args = parser.parse_args()

    root = Path(args.root).expanduser().resolve()
    if not root.exists() or not root.is_dir():
        console.print(f"[red]Invalid root folder:[/red] {root}")
        return 1

    console.print("[bold]Loading NLP resources...[/bold]")
    nlp = ensure_nlp_resources()

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    init_db(conn)

    try:
        if not args.skip_index:
            run_indexing(conn, nlp, root)
        run_tui(conn, nlp, root)
    except KeyboardInterrupt:
        console.print("\nInterrupted. Exiting cleanly.")
    finally:
        conn.commit()
        conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
