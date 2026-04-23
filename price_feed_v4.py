"""
Price feed for Euronext Paris instruments.

CURRENT PRICES via Boursorama (proven working from Streamlit Cloud — confirmed
by v2 diagnostic, returns €634.78 for CW8 matching the user's broker).

HISTORICAL PRICES via Yahoo Finance's chart endpoint with explicit period1/period2
unix timestamps. This is what yfinance uses internally for the .history() method
and which works reliably even when the realtime quote endpoint returns 429 — they
are separately rate-limited.

NEVER returns hardcoded fallbacks. Failures bubble up to the UI as clear errors.
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import requests

HTTP_TIMEOUT = 15

BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Upgrade-Insecure-Requests": "1",
}

YAHOO_HEADERS = {
    "User-Agent": BROWSER_HEADERS["User-Agent"],
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://finance.yahoo.com",
    "Referer": "https://finance.yahoo.com/",
}

# Mapping per ticker: Boursorama URL slug + page kind, plus Yahoo symbol for history.
TICKER_INFO: dict[str, dict] = {
    "EPA:C40":    {"bourso_slug": "1rTC40",   "bourso_kind": "tracker", "yahoo": "C40.PA"},
    "EPA:ALO":    {"bourso_slug": "1rPALO",   "bourso_kind": "stock",   "yahoo": "ALO.PA"},
    "EPA:OBLI":   {"bourso_slug": "1rTOBLI",  "bourso_kind": "tracker", "yahoo": "OBLI.PA"},
    "EPA:P500H":  {"bourso_slug": "1rTP500H", "bourso_kind": "tracker", "yahoo": "P500H.PA"},
    "EPA:PE500":  {"bourso_slug": "1rTPE500", "bourso_kind": "tracker", "yahoo": "PE500.PA"},
    "EPA:PLEM":   {"bourso_slug": "1rTPLEM",  "bourso_kind": "tracker", "yahoo": "PLEM.PA"},
    "EPA:CW8":    {"bourso_slug": "1rTCW8",   "bourso_kind": "tracker", "yahoo": "CW8.PA"},
    "EPA:HLT":    {"bourso_slug": "1rTHLT",   "bourso_kind": "tracker", "yahoo": "HLT.PA"},
    "EPA:PAEEM":  {"bourso_slug": "1rTPAEEM", "bourso_kind": "tracker", "yahoo": "PAEEM.PA"},
}


def _parse_fr_number(s: str) -> float | None:
    """Parse '610,0279' or '610.0279' → 610.0279."""
    try:
        clean = s.strip().replace(" ", "").replace("\u00a0", "")
        return float(clean.replace(",", "."))
    except (ValueError, AttributeError):
        return None


# =====================================================================
# Quote dataclass
# =====================================================================

@dataclass
class Quote:
    price: float
    as_of: datetime
    ticker: str
    source: str

    @property
    def staleness_days(self) -> float:
        now = datetime.now(timezone.utc)
        aware = self.as_of if self.as_of.tzinfo else self.as_of.replace(tzinfo=timezone.utc)
        return max(0.0, (now - aware).total_seconds() / 86400.0)


# =====================================================================
# CURRENT PRICE — Boursorama
# Uses the c-instrument--last regex confirmed working in v2 diagnostic
# (returned €634.78 for CW8 matching user's broker).
# =====================================================================

RE_PRICE = re.compile(r'c-instrument--last[^>]*>\s*([0-9]+[.,][0-9]+)', re.IGNORECASE)
RE_AS_OF = re.compile(r"(\d{2}\.\d{2}\.\d{2,4})\s*/\s*(\d{1,2}:\d{2}(?::\d{2})?)")


def _bourso_url(ticker: str) -> str | None:
    info = TICKER_INFO.get(ticker)
    if not info:
        return None
    slug = info["bourso_slug"]
    if info["bourso_kind"] == "tracker":
        return f"https://www.boursorama.com/bourse/trackers/cours/{slug}/"
    else:
        return f"https://www.boursorama.com/cours/{slug}/"


def fetch_current_price(ticker: str) -> tuple[Quote | None, str | None]:
    """Fetch the live price from Boursorama. Returns (Quote, None) on success,
    (None, error_message) on failure."""
    url = _bourso_url(ticker)
    if not url:
        return None, f"no Boursorama URL for {ticker}"
    try:
        r = requests.get(url, timeout=HTTP_TIMEOUT, headers=BROWSER_HEADERS)
        if r.status_code != 200:
            return None, f"Boursorama HTTP {r.status_code}"
        html = r.text
        m = RE_PRICE.search(html)
        if not m:
            return None, "price pattern did not match (Boursorama changed layout?)"
        price = _parse_fr_number(m.group(1))
        if price is None or price <= 0:
            return None, f"could not parse price '{m.group(1)}'"

        # Extract "as of" timestamp if available
        ts = datetime.now(timezone.utc)
        m2 = RE_AS_OF.search(html)
        if m2:
            d_str, t_str = m2.group(1), m2.group(2)
            try:
                dd, mm, yy = d_str.split(".")
                year = int(yy) + 2000 if len(yy) == 2 else int(yy)
                tparts = t_str.split(":")
                hour, minute = int(tparts[0]), int(tparts[1])
                second = int(tparts[2]) if len(tparts) > 2 else 0
                ts = datetime(year, int(mm), int(dd), hour, minute, second,
                              tzinfo=timezone.utc)
            except (ValueError, IndexError):
                pass

        return Quote(price=price, as_of=ts, ticker=ticker, source="Boursorama"), None
    except requests.exceptions.Timeout:
        return None, "timeout (>15s)"
    except requests.exceptions.ConnectionError as e:
        return None, f"connection error: {type(e).__name__}"
    except Exception as e:
        return None, f"{type(e).__name__}: {str(e)[:120]}"


def fetch_all_current(tickers: list[str]) -> dict[str, tuple[Quote | None, str | None]]:
    """Fetch current prices for many tickers, with polite delays."""
    out: dict[str, tuple[Quote | None, str | None]] = {}
    for t in tickers:
        out[t] = fetch_current_price(t)
        time.sleep(0.25)
    return out


# =====================================================================
# HISTORICAL PRICES — Yahoo Finance chart endpoint with period1/period2
#
# This endpoint is what yfinance.history() uses. It's significantly less
# rate-limited than the realtime range=1d call. Even when the realtime
# endpoint returns 429, this one usually works.
# =====================================================================

def fetch_history_yahoo(ticker: str, start: date, end: date) -> tuple[pd.Series | None, str | None]:
    """Fetch daily closes from Yahoo's historical chart endpoint."""
    info = TICKER_INFO.get(ticker)
    if not info:
        return None, f"no Yahoo symbol for {ticker}"
    sym = info["yahoo"]

    # Convert dates to unix timestamps
    p1 = int(datetime.combine(start, datetime.min.time(), tzinfo=timezone.utc).timestamp())
    p2 = int(datetime.combine(end + timedelta(days=2), datetime.min.time(), tzinfo=timezone.utc).timestamp())

    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}"
    try:
        r = requests.get(
            url, timeout=HTTP_TIMEOUT, headers=YAHOO_HEADERS,
            params={"period1": p1, "period2": p2, "interval": "1d", "events": "history"},
        )
        if r.status_code == 429:
            return None, "Yahoo HTTP 429 (rate limited) — try again in a minute"
        if r.status_code != 200:
            return None, f"Yahoo HTTP {r.status_code}"

        data = r.json()
        result = (data.get("chart") or {}).get("result") or []
        if not result:
            err = (data.get("chart") or {}).get("error")
            return None, f"Yahoo: no result ({err})" if err else "Yahoo: empty result"

        res = result[0]
        timestamps = res.get("timestamp") or []
        quote_block = (res.get("indicators") or {}).get("quote") or [{}]
        closes = quote_block[0].get("close") or []
        if not timestamps or not closes:
            return None, "Yahoo: timestamps or closes missing"

        # Build series, drop NaN closes
        idx = pd.DatetimeIndex(
            [datetime.fromtimestamp(t, tz=timezone.utc).date() for t in timestamps]
        )
        s = pd.Series(closes, index=idx, name=ticker, dtype=float).dropna()
        if s.empty:
            return None, "Yahoo: all closes were NaN"
        return s, None
    except requests.exceptions.Timeout:
        return None, "timeout (>15s)"
    except requests.exceptions.ConnectionError as e:
        return None, f"connection error: {type(e).__name__}"
    except ValueError as e:
        return None, f"JSON decode error: {e}"
    except Exception as e:
        return None, f"{type(e).__name__}: {str(e)[:120]}"


def fetch_history_for_tickers(
    tickers: list[str], start: date, end: date,
) -> tuple[pd.DataFrame, dict[str, str]]:
    """Fetch daily history for all tickers. Returns (DataFrame, errors_by_ticker)
    where DataFrame is indexed by business day with one column per ticker.
    Tickers that fail are absent from the DataFrame."""
    series_dict: dict[str, pd.Series] = {}
    errors: dict[str, str] = {}
    for t in tickers:
        s, err = fetch_history_yahoo(t, start, end)
        if s is not None:
            series_dict[t] = s
        else:
            errors[t] = err or "unknown error"
        time.sleep(0.25)

    if not series_dict:
        return pd.DataFrame(index=pd.bdate_range(start=start, end=end)), errors

    df = pd.concat(series_dict.values(), axis=1)
    df.columns = list(series_dict.keys())
    idx = pd.bdate_range(start=start, end=end)
    df = df.reindex(idx).ffill()
    return df, errors
