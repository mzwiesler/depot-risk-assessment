# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Depot Risk Assessment harmonizes the underlying stocks across a portfolio (including ETFs and funds) to provide a consolidated risk overview, visualized via a Streamlit dashboard. The pipeline runs in two stages: (1) download ETF composition files from provider websites, then (2) process and merge them into a unified dataset.

## Commands

The project uses `uv` for dependency management and a `.venv` virtual environment.

```bash
# Install dependencies
uv sync

# Run the data pipeline (fetches live prices, processes ETFs, outputs depot_merged.csv)
.venv/bin/python -m depot_risk_assessment.main

# Download fresh ETF composition files into ./downloads/
.venv/bin/python -m depot_risk_assessment.download_main

# Launch the Streamlit dashboard (reads ./data/depot_merged.csv)
.venv/bin/streamlit run depot_risk_assessment/dashboard.py

# Lint and format
.venv/bin/ruff check depot_risk_assessment/
.venv/bin/ruff format depot_risk_assessment/
```

## Architecture

### Two-stage pipeline

**Stage 1 — Download** (`download_main.py`): Iterates `ticker_config` and dispatches to a provider-specific downloader. Each downloader uses Selenium (`download_amundi.py`, `download_invesco.py`, `download_hanetf.py`) or a direct HTTP GET (`iShares`). XLSX files are converted to CSV via `xslx_helper.py` and saved to `./downloads/`.

**Stage 2 — Process** (`main.py`): Loads `./data/depot.csv` (the user's holdings with WKNs, quantities, and date-keyed prices), fetches live prices/sector/country via `finance_data.py` (yfinance + yahooquery), then reads each ETF's composition CSV and normalizes it through `ETFHandler` → `ETFConfig`. Provider-specific transformations live in `etf_transformations.py`. The three provider branches (Amundi, Invesco, iShares) are merged into a single `depot_merged.csv` with columns `Emittententicker`, `Name`, `Sektor`, `Standort`, `Wert`, `Type`.

### Key data flow

```
depot.csv  ──→  load_and_enrich_depot()  ──→  depot (with Price, Wert, %)
                                                    │
ticker_config ──→  ETFHandler.from_dict()  ──→  etfs[]  (one ETFConfig per ETF)
                                                    │
                 process_amundi/invesco/ishares()   │
                                                    ↓
                              merge_etf_data() + prepare_single_type(aktie/krypto)
                                                    │
                                             depot_merged.csv
                                                    │
                                          dashboard.py (Streamlit)
```

### Supporting modules

- **`ticker_config.py`**: Dict mapping WKN → `{editor, url, file_name}`. Add new ETFs here.
- **`mapping.py`**: Country and sector normalization maps (iShares, Yahoo → canonical names).
- **`finance_data.py`**: Yahoo Finance wrappers; caches new ISIN lookups into `./data/isin_information.csv` to avoid repeated API calls.
- **`validation.py`**: `assert`-based sanity checks that ETF values balance across provider merges.
- **`ticker.py`**: Standalone utility to look up WKN/ISIN → Yahoo symbol (not part of the main pipeline).

### Adding a new ETF provider

1. Add a downloader module (`download_<provider>.py`) following the existing pattern.
2. Register it in `editor_download_functions` in `download_main.py`.
3. Add reader/prepare functions in `etf_transformations.py`.
4. Handle the new editor branch in `ETFHandler.from_dict()` (`etf_handling.py`).
5. Add a processing function in `main.py` alongside the existing `process_amundi_etfs` etc.

### Data files

- `./data/depot.csv`: Input — user holdings (WKN, ticker, type, date-keyed quantity columns, Standort, Sektor).
- `./data/isin_information.csv`: Cache of ISIN → Symbol/Sektor/Standort from Yahoo.
- `./data/depot_merged.csv`: Output — unified position list consumed by the dashboard.
- `./downloads/`: Transient — ETF composition CSVs downloaded by `download_main.py` (gitignored, recreated each run).
