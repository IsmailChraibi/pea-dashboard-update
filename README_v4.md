# PEA Performance Dashboard — v4

Built around **proven-working data sources** validated by the v2 diagnostic.

## Architecture

| What | Source | Why |
|---|---|---|
| Current prices | **Boursorama** (HTML scrape) | Confirmed working from Streamlit Cloud — returned €634.78 for CW8, matching broker exactly |
| Historical prices | **Yahoo Finance chart endpoint** with explicit `period1`/`period2` | Different rate-limit budget than realtime; works even when realtime returns 429 |
| Transaction storage | **Google Sheets** via service account | Same as v2/v3 |

## Deployment (fresh repo, ~10 min)

1. **Create new GitHub repo** named anything (e.g. `pea-dashboard-v4`). Public is fine if you set Streamlit visibility to "private" and require login.
2. **Upload these files at the root** (no nested folders):
   - `app.py`
   - `price_feed.py`
   - `storage.py`
   - `requirements.txt`
   - `.streamlit/config.toml` (optional, theme)
   - `.streamlit/secrets.toml.example` (template)
3. **Deploy on [share.streamlit.io](https://share.streamlit.io)**:
   - New app → pick the new repo, branch `main`, main file `app.py`
   - Click Deploy
4. **Add Google Sheets secrets** (App settings → Secrets, paste this template):
   ```toml
   [gcp_service_account]
   type = "service_account"
   project_id = "..."
   private_key_id = "..."
   private_key = "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"
   client_email = "...@....iam.gserviceaccount.com"
   client_id = "..."
   auth_uri = "https://accounts.google.com/o/oauth2/auth"
   token_uri = "https://oauth2.googleapis.com/token"
   auth_provider_x509_cert_url = "https://www.googleapis.com/oauth2/v1/certs"
   client_x509_cert_url = "..."
   universe_domain = "googleapis.com"

   [gsheets]
   sheet_url = "https://docs.google.com/spreadsheets/d/YOUR_SHEET_ID/edit"
   worksheet = "transactions"
   ```
5. The app auto-redeploys. First load takes ~30 seconds for fresh price fetches.

## Files

- `app.py` — main Streamlit application, 3 tabs (MASTER, Performance, Analysis) + Transactions
- `price_feed.py` — Boursorama current + Yahoo historical fetchers, fully error-instrumented
- `storage.py` — Google Sheets read/write
- `example_transactions.csv` — your 43 historical transactions for bulk import

## Features

**MASTER tab:**
- KPIs: Current Value, Net Invested, Total P&L, Since-Inception TWR (annualized when ≥6 months held), YTD, 30d, 1d, Annualized Volatility
- **Live Price Feed Status** — shows source + timestamp + freshness indicator (🟢/🟡/🔴) per ticker
- Market Update narrative
- **Per-Instrument Performance table** with Qty, Avg Cost, Cur Price, Market Value, % of Portfolio, P&L €, YTD €, 1M €, 1D €, Return SI, Annualized — plus a TOTAL row
- **Annual P&L by Instrument matrix** with one column per year (incl. current year YTD), Total column, TOTAL row

**Performance tab:**
- Portfolio Value vs Net Invested over time
- Cumulative TWR with MSCI World benchmark
- Annual P&L bar chart
- Stacked area chart of market value per instrument

**Analysis tab:**
- Risk metrics: Annualized Return, Volatility, Sharpe Ratio, Max Drawdown, Best/Worst Day, % Positive Days, Top Position Concentration
- Drawdown curve
- Geographic exposure (look-through pie chart for ETFs)

**Transactions tab:**
- Editable table with auto-save to Google Sheets
- Quick-add form
- CSV/XLSX bulk import
- CSV export

## Sidebar tools

- **🔄 Refresh prices** — clears the 30-min cache and refetches
- **🔁 Reload transactions** — refetch from Google Sheets
- **🩺 Test price feed** — single-source diagnostic for CW8, useful when prices look wrong

## What's different from v3

The v3 used `yfinance` for everything, and silently fell back to a hardcoded price dict when fetches failed — leading to wildly stale data being displayed as live. v4 fixes this completely:

1. **No hardcoded fallback prices.** If Boursorama can't fetch a ticker, that position is excluded with a clear error.
2. **Boursorama for current prices** (proven working from Streamlit Cloud network).
3. **Yahoo's historical endpoint** for charts — uses the explicit `period1`/`period2` query that's separately rate-limited from the realtime endpoint that v2 saw 429s on.
4. **Per-ticker error visibility** — every fetch failure shows the exact HTTP status or exception in an expandable "Diagnostic" section.

## Troubleshooting

**"Could not fetch current prices for: [...]"** with HTTP 429 → Boursorama throttling, very rare. Wait a minute, click Refresh prices.

**"Historical data missing for: [...]"** → Yahoo is throttling the historical endpoint. The realtime current price still works (from Boursorama), so the dashboard remains usable; charts will just be flat for the missing tickers until the next fetch (6-hour cache).

**"price pattern did not match"** → Boursorama changed their HTML structure. Open `price_feed.py` and update `RE_PRICE` regex; the v2 diagnostic with its 7-pattern test is helpful for finding the new pattern.
