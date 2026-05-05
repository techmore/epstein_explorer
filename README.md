# Training Data Indexer

Python tools for indexing large PDF training-data corpora into SQLite, querying the resulting database from a TUI, and browsing it from a local Flask GUI.

## What Is In Git

This repository should contain the application code only:

- `pdf_training_indexer_tui.py`
- `pdf_training_indexer_tui_parallel.py`
- `pdf_training_query_tui.py`
- `GUI/`
- project documentation

## What Is Not In Git

The source datasets and generated databases are intentionally excluded from Git because the workspace contains more than one million files and multiple files above GitHub's normal 100 MiB file limit.

Excluded dataset roots:

- `DataSet 1/`
- `DataSet 4/`
- `DataSet 6/`
- `VOL00002/`
- `VOL00003/`
- `VOL00005/`
- `VOL00007/`
- `VOL00008/`
- `VOL00010/`
- `VOL00011/`
- `VOL00012/`
- `dataset9-more-complete/`

Excluded generated artifacts:

- `~/training_data.db`
- `*.db`, `*.sqlite`, `*.sqlite3`
- `*.zst`

## Oversized Files Audit

Audit root: `/Users/techmore/Documents/training_data`

GitHub normal Git pushes reject files above 100 MiB. The local audit found 63 files above that threshold:

- 52 PDF files
- 9 MP4 files
- 1 MOV file
- 1 ZST archive

Total size above 100 MiB: about 21.2 GiB.

| Size | Path |
| ---: | --- |
| 6070.0 MiB | `VOL00010/NATIVES/0001/EFTA01600798.mp4` |
| 2254.8 MiB | `techmore_tui_v1.db.zst` |
| 959.4 MiB | `VOL00010/NATIVES/0001/EFTA01600824.mov` |
| 728.6 MiB | `VOL00010/IMAGES/0172/EFTA01671962.pdf` |
| 684.5 MiB | `VOL00010/NATIVES/0001/EFTA01683314.mp4` |
| 657.5 MiB | `dataset9-more-complete/EFTA00171107.pdf` |
| 447.0 MiB | `dataset9-more-complete/EFTA01150379.pdf` |
| 425.7 MiB | `VOL00010/IMAGES/0172/EFTA01661868.pdf` |
| 307.3 MiB | `dataset9-more-complete/EFTA00249282.pdf` |
| 294.3 MiB | `dataset9-more-complete/EFTA00249534.pdf` |
| 291.7 MiB | `dataset9-more-complete/EFTA00249026.pdf` |
| 265.3 MiB | `VOL00010/IMAGES/0175/EFTA01736184.pdf` |
| 263.0 MiB | `dataset9-more-complete/EFTA00255244.pdf` |
| 258.6 MiB | `dataset9-more-complete/EFTA00170259.pdf` |
| 257.1 MiB | `VOL00010/NATIVES/0001/EFTA01683321.mp4` |
| 251.9 MiB | `dataset9-more-complete/EFTA00255457.pdf` |
| 248.2 MiB | `dataset9-more-complete/EFTA00256125.pdf` |
| 236.3 MiB | `dataset9-more-complete/EFTA00227381.pdf` |
| 231.3 MiB | `dataset9-more-complete/EFTA01160176.pdf` |
| 229.7 MiB | `dataset9-more-complete/EFTA00253426.pdf` |
| 225.7 MiB | `dataset9-more-complete/EFTA00106645.pdf` |
| 225.0 MiB | `dataset9-more-complete/EFTA00254812.pdf` |
| 221.8 MiB | `dataset9-more-complete/EFTA00255028.pdf` |
| 220.6 MiB | `dataset9-more-complete/EFTA00253962.pdf` |
| 220.4 MiB | `dataset9-more-complete/EFTA00253680.pdf` |
| 213.3 MiB | `dataset9-more-complete/EFTA01011526.pdf` |
| 203.8 MiB | `VOL00010/IMAGES/0172/EFTA01684802.pdf` |
| 197.1 MiB | `VOL00010/NATIVES/0001/EFTA01683316.mp4` |
| 187.2 MiB | `VOL00010/IMAGES/0172/EFTA01670642.pdf` |
| 178.3 MiB | `dataset9-more-complete/EFTA00252729.pdf` |
| 175.8 MiB | `dataset9-more-complete/EFTA00230786.pdf` |
| 175.4 MiB | `dataset9-more-complete/EFTA00231917.pdf` |
| 173.2 MiB | `dataset9-more-complete/EFTA00251705.pdf` |
| 163.1 MiB | `VOL00010/NATIVES/0001/EFTA01683320.mp4` |
| 157.9 MiB | `VOL00010/NATIVES/0001/EFTA01688316.mp4` |
| 153.3 MiB | `VOL00010/NATIVES/0001/EFTA01683315.mp4` |
| 153.1 MiB | `dataset9-more-complete/EFTA00184224.pdf` |
| 151.6 MiB | `dataset9-more-complete/EFTA00048963.pdf` |
| 148.2 MiB | `dataset9-more-complete/EFTA00254649.pdf` |
| 145.2 MiB | `dataset9-more-complete/EFTA00254398.pdf` |
| 143.9 MiB | `dataset9-more-complete/EFTA00250488.pdf` |
| 131.0 MiB | `VOL00011/IMAGES/0332/EFTA02727130.pdf` |
| 128.4 MiB | `dataset9-more-complete/EFTA00191587.pdf` |
| 125.2 MiB | `dataset9-more-complete/EFTA00046963.pdf` |
| 124.9 MiB | `dataset9-more-complete/EFTA00253174.pdf` |
| 123.7 MiB | `dataset9-more-complete/EFTA00051963.pdf` |
| 123.2 MiB | `dataset9-more-complete/EFTA00253281.pdf` |
| 123.1 MiB | `dataset9-more-complete/EFTA00044963.pdf` |
| 122.7 MiB | `VOL00010/IMAGES/0011/EFTA01335806.pdf` |
| 118.4 MiB | `VOL00010/NATIVES/0001/EFTA01648625.mp4` |
| 116.0 MiB | `dataset9-more-complete/EFTA00047963.pdf` |
| 113.4 MiB | `dataset9-more-complete/EFTA00050963.pdf` |
| 111.1 MiB | `VOL00010/NATIVES/0001/EFTA01648661.mp4` |
| 110.5 MiB | `dataset9-more-complete/EFTA00249786.pdf` |
| 109.0 MiB | `dataset9-more-complete/EFTA00045963.pdf` |
| 106.9 MiB | `VOL00010/IMAGES/0161/EFTA01607847.pdf` |
| 106.4 MiB | `dataset9-more-complete/EFTA00250229.pdf` |
| 106.2 MiB | `dataset9-more-complete/EFTA00255749.pdf` |
| 102.9 MiB | `dataset9-more-complete/EFTA00277580.pdf` |
| 102.8 MiB | `dataset9-more-complete/EFTA00251628.pdf` |
| 101.2 MiB | `dataset9-more-complete/EFTA00254244.pdf` |
| 101.0 MiB | `dataset9-more-complete/EFTA00252545.pdf` |
| 100.8 MiB | `VOL00010/IMAGES/0248/EFTA01846835.pdf` |

## Install

Use Python 3.12 for best compatibility with spaCy:

```bash
cd /Users/techmore/Documents/training_data
/opt/homebrew/bin/python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install pdfplumber PyPDF2 nltk spacy tqdm rich prompt_toolkit flask
python -m spacy download en_core_web_sm
```

## Run

Index PDFs:

```bash
python pdf_training_indexer_tui.py --root /path/to/training_data
```

Parallel indexing experiment:

```bash
python pdf_training_indexer_tui_parallel.py --root /path/to/training_data --workers 4
```

Query existing DB:

```bash
python pdf_training_query_tui.py --root /path/to/training_data
```

Run the web GUI:

```bash
cd GUI
python app.py --root /path/to/training_data
```
