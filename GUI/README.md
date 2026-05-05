# Web GUI (SQLite-compatible)

# pip install flask

This app reads the same SQLite database created by your TUI indexer:

- `~/training_data.db`
- Uses tables: `words`, `entities`, `cooccurrences`, `snippets`, `file_index`

## Run local

```bash
cd /Users/techmore/Documents/training_data/GUI
python3 app.py --root /Users/techmore/Documents/training_data
# open http://127.0.0.1:5000
```

If you are using the project venv:

```bash
source /Users/techmore/Documents/training_data/.venv/bin/activate
python -m pip install flask
python app.py --root /Users/techmore/Documents/training_data
```

By default the GUI reads `~/training_data.db`. Override that path with:

```bash
TRAINING_DATA_DB=/path/to/training_data.db python app.py --root /path/to/pdf/root
```

## Features

- Top words / top entities (frequency-sorted)
- Search with simple boolean logic (`AND`/`OR`/`NOT`)
- Add keyword to active filter
- Remove keyword from active filter
- Matching files with hit counts
- Context snippets for selected file
- File path display
- Optional macOS local open-file button
- Entity correlation view
- FTS-backed snippet search when the `snippets_fts` side index exists

## Rebuild FTS

For an existing database created before FTS support was added, open this local URL once after starting the app:

```text
http://127.0.0.1:5000/admin/rebuild-fts
```

## Optional local open support (macOS)

Disabled by default. Enable it only on trusted local machine:

```bash
ENABLE_LOCAL_OPEN=1 python3 app.py --root /Users/techmore/Documents/training_data
```

## Deploy (DigitalOcean VM)

Basic run:

```bash
python3 app.py --host 0.0.0.0 --port 5000 --root /path/to/pdf/root
```

Then reverse proxy with Nginx and firewall the port.
