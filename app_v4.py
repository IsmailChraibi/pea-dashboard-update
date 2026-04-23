"""
PEA Performance Dashboard — v4 (final)
=======================================
Architecture:
  • Current prices: Boursorama scrape (proven working from Streamlit Cloud)
  • Historical prices: Yahoo chart endpoint with explicit period1/period2
    (different rate limit budget than realtime — works even when realtime 429s)
  • Storage: Google Sheets via service account
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

import price_feed
import storage

# =====================================================================
# Config
# =====================================================================

st.set_page_config(
    page_title="PEA Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

INSTRUMENTS: dict[str, dict] = {
    "EPA:C40":   {"name": "Amundi CAC 40 ESG UCITS ETF",            "asset": "Equity ETF", "geo": "France"},
    "EPA:ALO":   {"name": "Alstom SA",                               "asset": "Equity",     "geo": "France"},
    "EPA:OBLI":  {"name": "Amundi Euro Government Bond ETF",         "asset": "Bond ETF",   "geo": "Eurozone"},
    "EPA:P500H": {"name": "Amundi PEA S&P 500 ESG UCITS ETF Hedged", "asset": "Equity ETF", "geo": "United States"},
    "EPA:PE500": {"name": "Amundi PEA S&P 500 ESG UCITS ETF",        "asset": "Equity ETF", "geo": "United States"},
    "EPA:PLEM":  {"name": "Amundi PEA MSCI EM EMEA ESG Leaders",     "asset": "Equity ETF", "geo": "Emerging EMEA"},
    "EPA:CW8":   {"name": "Amundi MSCI World Swap UCITS ETF",        "asset": "Equity ETF", "geo": "Developed Markets"},
    "EPA:HLT":   {"name": "Amundi STOXX Europe 600 Healthcare ETF",  "asset": "Equity ETF", "geo": "Europe"},
    "EPA:PAEEM": {"name": "Amundi PEA MSCI EM ESG ETF",              "asset": "Equity ETF", "geo": "Emerging Markets"},
}

GEO_EXPOSURE: dict[str, dict[str, float]] = {
    "EPA:C40":   {"France": 1.00},
    "EPA:ALO":   {"France": 1.00},
    "EPA:OBLI":  {"Eurozone": 1.00},
    "EPA:P500H": {"United States": 1.00},
    "EPA:PE500": {"United States": 1.00},
    "EPA:PLEM":  {"Emerging Europe": 0.45, "South Africa": 0.25, "Middle East": 0.30},
    "EPA:CW8":   {"United States": 0.72, "Japan": 0.06, "United Kingdom": 0.04,
                  "France": 0.03, "Canada": 0.03, "Germany": 0.025,
                  "Switzerland": 0.025, "Other Developed": 0.065},
    "EPA:HLT":   {"United Kingdom": 0.30, "Switzerland": 0.25, "Denmark": 0.20,
                  "France": 0.10, "Germany": 0.08, "Other Europe": 0.07},
    "EPA:PAEEM": {"China": 0.30, "India": 0.19, "Taiwan": 0.18, "South Korea": 0.13,
                  "Brazil": 0.06, "Saudi Arabia": 0.04, "Other EM": 0.10},
}

TRADING_DAYS = 252
MIN_ANNUALIZE_DAYS = 180
LOCAL_CACHE = Path(__file__).parent / ".local_transactions.csv"

# =====================================================================
# Session state
# =====================================================================

if "transactions" not in st.session_state:
    st.session_state.transactions = pd.DataFrame(columns=storage.REQUIRED_COLUMNS)
if "loaded_once" not in st.session_state:
    st.session_state.loaded_once = False
if "last_save_ok" not in st.session_state:
    st.session_state.last_save_ok = None


# =====================================================================
# Cached fetchers
# =====================================================================

@st.cache_data(ttl=1800, show_spinner=False)  # 30-min cache for current prices
def fetch_current_quotes(tickers: tuple[str, ...]) -> dict[str, dict]:
    """Returns {ticker: {"price": x, "as_of": iso, "source": s, "stale_days": f, "error": e}}.
    Either price+as_of+source are set OR error is set."""
    out: dict[str, dict] = {}
    for t in tickers:
        q, err = price_feed.fetch_current_price(t)
        if q is None:
            out[t] = {"failed": True, "error": err or "unknown error"}
        else:
            out[t] = {
                "price": q.price, "as_of": q.as_of.isoformat(),
                "source": q.source, "stale_days": q.staleness_days,
            }
    return out


@st.cache_data(ttl=21600, show_spinner=False)  # 6-hour cache for historical
def fetch_history(tickers: tuple[str, ...], start: date, end: date) -> tuple[pd.DataFrame, dict]:
    return price_feed.fetch_history_for_tickers(list(tickers), start, end)


# =====================================================================
# Portfolio analytics
# =====================================================================

def build_positions(transactions: pd.DataFrame, price_index: pd.DatetimeIndex) -> pd.DataFrame:
    tickers = sorted(transactions["Ticker"].unique())
    tx = transactions.copy()
    tx["Date"] = pd.to_datetime(tx["Date"])
    daily = tx.pivot_table(index="Date", columns="Ticker", values="Quantity",
                           aggfunc="sum", fill_value=0)
    daily = daily.reindex(price_index, fill_value=0)
    for t in tickers:
        if t not in daily.columns:
            daily[t] = 0
    return daily[tickers].cumsum()


def compute_portfolio_series(
    transactions: pd.DataFrame, positions: pd.DataFrame, prices: pd.DataFrame,
    current_prices: pd.Series,
) -> tuple[pd.Series, pd.Series, pd.Series, pd.Series]:
    """Daily portfolio market value, net invested, daily cashflows, and TWR.
    Last day's prices are overridden with live quotes from Boursorama."""
    common = [t for t in positions.columns if t in prices.columns]
    pos = positions[common]
    px = prices[common].copy()

    last_idx = px.index[-1]
    for t in common:
        if t in current_prices.index and pd.notna(current_prices[t]):
            px.loc[last_idx, t] = current_prices[t]

    total_value = (pos * px).sum(axis=1)

    tx = transactions.copy()
    tx["Date"] = pd.to_datetime(tx["Date"])
    tx["Cashflow"] = tx["Quantity"] * tx["Price"]
    daily_cf = (tx.groupby("Date")["Cashflow"].sum()
                  .reindex(total_value.index, fill_value=0.0))
    net_invested = daily_cf.cumsum()

    prev_v = total_value.shift(1)
    with np.errstate(divide="ignore", invalid="ignore"):
        r = (total_value - daily_cf) / prev_v - 1
    r = r.replace([np.inf, -np.inf], np.nan).fillna(0.0)
    return total_value, net_invested, daily_cf, r


def instrument_value_series(
    transactions: pd.DataFrame, positions: pd.DataFrame,
    prices: pd.DataFrame, current_prices: pd.Series,
) -> pd.DataFrame:
    common = [t for t in positions.columns if t in prices.columns]
    pos = positions[common]
    px = prices[common].copy()
    last_idx = px.index[-1]
    for t in common:
        if t in current_prices.index and pd.notna(current_prices[t]):
            px.loc[last_idx, t] = current_prices[t]
    return pos * px


def instrument_cashflow_series(
    transactions: pd.DataFrame, price_index: pd.DatetimeIndex,
) -> pd.DataFrame:
    tx = transactions.copy()
    tx["Date"] = pd.to_datetime(tx["Date"])
    tx["Cashflow"] = tx["Quantity"] * tx["Price"]
    daily = tx.pivot_table(index="Date", columns="Ticker", values="Cashflow",
                           aggfunc="sum", fill_value=0.0)
    return daily.reindex(price_index, fill_value=0.0)


def period_twr(daily_ret: pd.Series, start_ts: pd.Timestamp | None) -> float:
    s = daily_ret if start_ts is None else daily_ret.loc[daily_ret.index > start_ts]
    if s.empty:
        return 0.0
    return float((1 + s).prod() - 1)


def annualize(total_return: float, days: float) -> float | None:
    if days < MIN_ANNUALIZE_DAYS or total_return <= -1:
        return None
    years = days / 365.25
    return (1 + total_return) ** (1 / years) - 1


def max_drawdown(r: pd.Series) -> float:
    if r.empty:
        return 0.0
    cum = (1 + r).cumprod()
    return float(((cum - cum.cummax()) / cum.cummax()).min())


# =====================================================================
# Formatting helpers
# =====================================================================

def fmt_eur(x, d=0):
    if x is None or pd.isna(x):
        return "—"
    sign = "-" if x < 0 else ""
    return f"{sign}€{abs(x):,.{d}f}"


def fmt_pct(x, d=2):
    if x is None or pd.isna(x):
        return "—"
    return f"{x*100:+.{d}f}%"


def fmt_stale(days: float) -> str:
    if days < 1.5:
        return "🟢 fresh"
    if days < 4:
        return "🟡 " + (f"{int(round(days))}d" if days >= 1 else f"{int(round(days*24))}h")
    return f"🔴 {int(round(days))}d stale"


# =====================================================================
# Initial data loading
# =====================================================================

def load_initial_data():
    if st.session_state.loaded_once:
        return
    if storage.sheets_available():
        try:
            df = storage.load_from_sheets()
            st.session_state.transactions = df
            st.session_state.loaded_once = True
            st.session_state.storage_backend = "sheets"
            return
        except Exception as e:
            st.sidebar.error(f"Google Sheets error: {e}")
            st.session_state.storage_backend = "local"
    else:
        st.session_state.storage_backend = "local"
    df = storage.load_from_disk_cache(LOCAL_CACHE)
    st.session_state.transactions = df
    st.session_state.loaded_once = True


def persist(df: pd.DataFrame) -> tuple[bool, str]:
    df = storage.normalize_transactions(df)
    backend = st.session_state.get("storage_backend", "local")
    try:
        if backend == "sheets":
            storage.save_to_sheets(df)
            return True, "Saved to Google Sheet."
        else:
            storage.save_to_disk_cache(df, LOCAL_CACHE)
            return True, "Saved locally."
    except Exception as e:
        return False, f"Save failed: {e}"


load_initial_data()


# =====================================================================
# Sidebar
# =====================================================================

with st.sidebar:
    st.title("⚙️ Settings")

    backend = st.session_state.get("storage_backend", "local")
    if backend == "sheets":
        st.success("📗 Connected to Google Sheet")
    else:
        st.warning("💾 Local storage (not persistent on Streamlit Cloud)")

    st.caption(f"{len(st.session_state.transactions)} transactions loaded")

    rf = st.number_input("Risk-free rate (€STR proxy)", value=0.022,
                         min_value=0.0, max_value=0.10, step=0.001, format="%.3f")

    st.markdown("---")
    if st.button("🔄 Refresh prices"):
        fetch_current_quotes.clear()
        fetch_history.clear()
        st.rerun()

    if st.button("🔁 Reload transactions"):
        st.session_state.loaded_once = False
        load_initial_data()
        st.rerun()

    with st.expander("🩺 Test price feed"):
        if st.button("Test CW8 fetch"):
            with st.spinner("Testing…"):
                q, err = price_feed.fetch_current_price("EPA:CW8")
                if q is not None:
                    st.success(f"✅ €{q.price:.4f} from {q.source}\nas of {q.as_of}")
                else:
                    st.error(f"❌ {err}")

    with st.expander("ℹ️ About"):
        st.markdown(
            "- **Current prices**: Boursorama (always fresh from FR broker)\n"
            "- **Historical**: Yahoo Finance chart endpoint\n"
            "- **Storage**: Google Sheets via service account\n"
            "- **Returns**: time-weighted, cashflow-neutral\n"
            "- **Annualization**: only for holdings ≥6 months"
        )


# =====================================================================
# Header
# =====================================================================

st.title("📊 PEA Performance Dashboard")
today = date.today()
st.caption(
    f"As of {today.strftime('%A, %B %d, %Y')}  •  "
    f"{len(st.session_state.transactions)} transactions"
)

if st.session_state.transactions.empty:
    st.info("👋 Head to the **Transactions** tab to add your trades.")


# =====================================================================
# Compute analytics if data present
# =====================================================================

transactions = st.session_state.transactions.copy()
compute_ok = False

if not transactions.empty:
    tickers_used = sorted(transactions["Ticker"].unique().tolist())
    unknown = [t for t in tickers_used if t not in INSTRUMENTS]
    if unknown:
        st.warning(f"⚠️ Unknown tickers ignored: {unknown}")
        tickers_used = [t for t in tickers_used if t in INSTRUMENTS]
        transactions = transactions[transactions["Ticker"].isin(tickers_used)].reset_index(drop=True)

    start_date = transactions["Date"].min() - timedelta(days=10)

    # Current prices via Boursorama
    with st.spinner("Fetching live prices from Boursorama…"):
        quotes = fetch_current_quotes(tuple(tickers_used))

    # Historical via Yahoo
    with st.spinner("Fetching price history from Yahoo…"):
        history, history_errors = fetch_history(tuple(tickers_used), start_date, today)

    # Build current-price series from successful Boursorama fetches
    current_price_series = pd.Series({
        t: q["price"] for t, q in quotes.items()
        if not q.get("failed") and "price" in q
    }, dtype=float)

    # Identify failures
    missing_current = [t for t in tickers_used if quotes.get(t, {}).get("failed")]
    missing_history = [t for t in tickers_used if t not in history.columns]

    # If a ticker has current price but no history, seed history with current price
    for t in tickers_used:
        if t not in history.columns and t in current_price_series.index:
            history[t] = current_price_series[t]

    if missing_current:
        with st.container(border=True):
            st.error(
                f"❌ Could not fetch current prices for: **{', '.join(missing_current)}**. "
                "These positions are excluded from valuation."
            )
            with st.expander("🔍 Details"):
                for t in missing_current:
                    st.markdown(f"**{t}**: `{quotes[t].get('error', 'unknown')}`")
        for t in missing_current:
            if t in history.columns:
                history = history.drop(columns=[t])
            tickers_used = [x for x in tickers_used if x != t]
        transactions = transactions[transactions["Ticker"].isin(tickers_used)].reset_index(drop=True)

    if missing_history and history_errors:
        with st.container(border=True):
            st.warning(
                f"⚠️ Historical data missing for: **{', '.join(missing_history)}**. "
                "Charts will be flat for these instruments."
            )
            with st.expander("🔍 History errors"):
                for t, err in history_errors.items():
                    st.markdown(f"**{t}**: `{err}`")

    if not transactions.empty and len(history.columns) > 0:
        positions = build_positions(transactions, history.index)
        total_value, net_invested, daily_cf, daily_ret = compute_portfolio_series(
            transactions, positions, history, current_price_series
        )
        inst_values = instrument_value_series(
            transactions, positions, history, current_price_series
        )
        inst_cashflows = instrument_cashflow_series(transactions, history.index)
        compute_ok = True


# =====================================================================
# Tabs
# =====================================================================

tab_master, tab_perf, tab_analysis, tab_tx = st.tabs(
    ["🏠 MASTER", "📈 Performance", "🔍 Analysis", "✏️ Transactions"]
)

# =====================================================================
# MASTER tab
# =====================================================================
with tab_master:
    if not compute_ok:
        st.write("Add transactions first.")
    else:
        today_ts = total_value.index[-1]
        cur_value = float(total_value.iloc[-1])
        cur_invested = float(net_invested.iloc[-1])
        pnl = cur_value - cur_invested

        one_day_ago = total_value.index[-2] if len(total_value) >= 2 else total_value.index[-1]
        month_ago_ts = today_ts - pd.Timedelta(days=30)
        month_ago_ts = total_value.index[total_value.index <= month_ago_ts][-1] \
            if (total_value.index <= month_ago_ts).any() else total_value.index[0]
        ytd_start = pd.Timestamp(year=today_ts.year, month=1, day=1) - pd.Timedelta(days=1)

        si_ret = period_twr(daily_ret, None)
        ytd_ret = period_twr(daily_ret, ytd_start)
        mtd_ret = period_twr(daily_ret, month_ago_ts)
        last_day_ret = float(daily_ret.iloc[-1]) if len(daily_ret) else 0.0
        days_held = (total_value.index[-1] - total_value.index[0]).days
        ann_ret_si = annualize(si_ret, days_held)
        ann_vol = float(daily_ret.std() * np.sqrt(TRADING_DAYS))

        c1, c2, c3 = st.columns(3)
        c1.metric("Current Value", fmt_eur(cur_value), fmt_eur(pnl))
        c2.metric("Net Invested", fmt_eur(cur_invested))
        c3.metric("Total P&L", fmt_eur(pnl), fmt_pct(si_ret),
                  delta_color="normal" if pnl >= 0 else "inverse")

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Since Inception", fmt_pct(si_ret),
                  (f"ann. {fmt_pct(ann_ret_si)}" if ann_ret_si is not None else None))
        c2.metric("YTD", fmt_pct(ytd_ret))
        c3.metric("Last 30d", fmt_pct(mtd_ret))
        c4.metric("Last Day", fmt_pct(last_day_ret, 3))
        c5.metric("Ann. Vol.", fmt_pct(ann_vol))

        # Price feed status panel
        st.markdown("---")
        st.subheader("💹 Live Price Feed Status")
        feed_rows = []
        for t in tickers_used:
            q = quotes.get(t)
            if q is None or q.get("failed"):
                feed_rows.append({"Ticker": t, "Price": None, "Source": "—",
                                  "As of": "—", "Status": "❌ unavailable"})
            else:
                as_of_dt = datetime.fromisoformat(q["as_of"])
                feed_rows.append({
                    "Ticker": t, "Price": q["price"], "Source": q["source"],
                    "As of": as_of_dt.strftime("%Y-%m-%d %H:%M UTC"),
                    "Status": fmt_stale(q["stale_days"]),
                })
        st.dataframe(
            pd.DataFrame(feed_rows), hide_index=True, use_container_width=True,
            column_config={"Price": st.column_config.NumberColumn(format="€%.2f")},
        )

        # Market update
        st.markdown("---")
        st.subheader("📰 Market Update")
        per_inst = {}
        for t in tickers_used:
            qty = float(positions[t].iloc[-1]) if t in positions.columns else 0
            if qty == 0:
                continue
            try:
                ytd_row_idx = inst_values.index[inst_values.index >= ytd_start][0]
                ytd_val = float(inst_values[t].loc[ytd_row_idx])
            except (IndexError, KeyError):
                ytd_val = float(inst_values[t].iloc[0])
            cur_val = float(inst_values[t].iloc[-1])
            ytd_cf = float(inst_cashflows[t].loc[inst_cashflows.index > ytd_start].sum())
            ytd_pnl = cur_val - ytd_val - ytd_cf
            ytd_ret_inst = ytd_pnl / ytd_val if ytd_val else 0
            per_inst[t] = {"ytd_pnl": ytd_pnl, "ytd_ret": ytd_ret_inst,
                           "value": cur_val, "name": INSTRUMENTS[t]["name"]}

        if per_inst:
            best = max(per_inst.items(), key=lambda kv: kv[1]["ytd_ret"])
            worst = min(per_inst.items(), key=lambda kv: kv[1]["ytd_ret"])
            largest = max(per_inst.items(), key=lambda kv: kv[1]["value"])
            bt, bv = best; wt, wv = worst; lt, lv = largest
            ann_txt = (f" ({fmt_pct(ann_ret_si)} annualized over "
                       f"{days_held/365.25:.1f} years)") if ann_ret_si is not None else ""
            st.info(
                f"**Portfolio YTD:** {fmt_pct(ytd_ret)} ({fmt_eur(pnl)} total P&L{ann_txt}).  \n"
                f"**Best YTD:** {bt} — {bv['name']}: {fmt_pct(bv['ytd_ret'])} ({fmt_eur(bv['ytd_pnl'])}).  \n"
                f"**Worst YTD:** {wt} — {wv['name']}: {fmt_pct(wv['ytd_ret'])} ({fmt_eur(wv['ytd_pnl'])}).  \n"
                f"**Largest position:** {lt} at {fmt_eur(lv['value'])} "
                f"({lv['value']/cur_value*100:.1f}% of portfolio).  \n"
                f"**Last day:** {fmt_pct(last_day_ret, 3)}."
            )

        # Per-instrument table
        st.markdown("---")
        st.subheader("📋 Per-Instrument Performance")

        rows = []
        for t in tickers_used:
            qty = float(positions[t].iloc[-1]) if t in positions.columns else 0.0
            cur_px = float(current_price_series[t]) if t in current_price_series.index else np.nan
            buys = transactions[(transactions["Ticker"] == t) & (transactions["Quantity"] > 0)]
            wavg_cost = (buys["Quantity"] * buys["Price"]).sum() / buys["Quantity"].sum() \
                if buys["Quantity"].sum() > 0 else np.nan

            if qty == 0:
                sells = transactions[(transactions["Ticker"] == t) & (transactions["Quantity"] < 0)]
                total_sold = -sells["Quantity"].sum()
                if total_sold > 0 and not pd.isna(wavg_cost):
                    total_cost = total_sold * wavg_cost
                    total_proceeds = (-sells["Quantity"] * sells["Price"]).sum()
                    realized_pnl = total_proceeds - total_cost
                    ret_si = realized_pnl / total_cost if total_cost else 0
                else:
                    realized_pnl, ret_si = 0, 0
                mv, total_pnl_eur = 0, realized_pnl
            else:
                mv = qty * cur_px
                cost_basis = qty * wavg_cost if not pd.isna(wavg_cost) else 0
                total_pnl_eur = mv - cost_basis
                ret_si = (cur_px - wavg_cost) / wavg_cost if wavg_cost and wavg_cost > 0 else 0

            s_val = inst_values[t] if t in inst_values.columns else pd.Series(dtype=float)
            s_cf = inst_cashflows[t] if t in inst_cashflows.columns else pd.Series(dtype=float)

            def period_eur(start_ts):
                if s_val.empty:
                    return 0.0
                mask = s_val.index > start_ts
                if not mask.any():
                    return 0.0
                start_val = float(s_val.loc[s_val.index <= start_ts].iloc[-1]) \
                    if (s_val.index <= start_ts).any() else 0.0
                end_val = float(s_val.iloc[-1])
                cf_in_period = float(s_cf.loc[mask].sum()) if not s_cf.empty else 0.0
                return end_val - start_val - cf_in_period

            ytd_eur = period_eur(ytd_start)
            m1_eur = period_eur(month_ago_ts)
            d1_eur = period_eur(one_day_ago) if one_day_ago != today_ts else 0.0

            if buys["Quantity"].sum() > 0:
                buy_dates = pd.to_datetime(buys["Date"])
                wavg_date = (pd.to_numeric(buy_dates) * buys["Quantity"]).sum() / buys["Quantity"].sum()
                days_open = (today_ts - pd.to_datetime(wavg_date)).days
            else:
                days_open = 0
            ann_ret_inst = annualize(ret_si, days_open)

            rows.append({
                "Ticker": t, "Instrument": INSTRUMENTS[t]["name"],
                "Qty": qty, "Avg Cost": wavg_cost, "Cur Price": cur_px,
                "Market Value": mv, "% of Port.": None,
                "P&L (€)": total_pnl_eur, "YTD (€)": ytd_eur,
                "1M (€)": m1_eur, "1D (€)": d1_eur,
                "Return (SI)": ret_si * 100,
                "Annualized": (ann_ret_inst * 100) if ann_ret_inst is not None else None,
            })

        total_mv_all = sum(r["Market Value"] for r in rows if r["Market Value"])
        for r in rows:
            r["% of Port."] = (r["Market Value"] / total_mv_all * 100) \
                if (total_mv_all > 0 and r["Market Value"]) else 0.0

        df_inst = pd.DataFrame(rows)

        # TOTAL row
        tot_mv = df_inst["Market Value"].sum()
        tot_pnl = df_inst["P&L (€)"].sum()
        tot_cost = sum(float(r["Market Value"]) - float(r["P&L (€)"])
                       for r in rows if r.get("Market Value") and r["Qty"] != 0)
        tot_ret = (tot_mv - tot_cost) / tot_cost if tot_cost > 0 else 0
        tot_ann = annualize(tot_ret, days_held) if days_held else None

        total_row = pd.DataFrame([{
            "Ticker": "TOTAL", "Instrument": "— Portfolio total —",
            "Qty": None, "Avg Cost": None, "Cur Price": None,
            "Market Value": tot_mv, "% of Port.": 100.0 if tot_mv > 0 else 0.0,
            "P&L (€)": tot_pnl, "YTD (€)": df_inst["YTD (€)"].sum(),
            "1M (€)": df_inst["1M (€)"].sum(), "1D (€)": df_inst["1D (€)"].sum(),
            "Return (SI)": tot_ret * 100,
            "Annualized": (tot_ann * 100) if tot_ann is not None else None,
        }])
        df_inst_display = pd.concat([df_inst, total_row], ignore_index=True)

        st.dataframe(
            df_inst_display, hide_index=True, use_container_width=True,
            column_config={
                "Qty": st.column_config.NumberColumn(format="%.0f"),
                "Avg Cost": st.column_config.NumberColumn(format="€%.2f"),
                "Cur Price": st.column_config.NumberColumn(format="€%.2f"),
                "Market Value": st.column_config.NumberColumn(format="€%.0f"),
                "% of Port.": st.column_config.NumberColumn(format="%.1f%%"),
                "P&L (€)": st.column_config.NumberColumn(format="€%.0f"),
                "YTD (€)": st.column_config.NumberColumn(format="€%.0f"),
                "1M (€)": st.column_config.NumberColumn(format="€%.0f"),
                "1D (€)": st.column_config.NumberColumn(format="€%.0f"),
                "Return (SI)": st.column_config.NumberColumn(format="%.2f%%"),
                "Annualized": st.column_config.NumberColumn(
                    format="%.2f%%",
                    help=f"Blank when held <{MIN_ANNUALIZE_DAYS} days.",
                ),
            },
        )

        # Annual P&L matrix
        st.markdown("---")
        st.subheader("📅 Annual P&L by Instrument (€)")
        st.caption("Each cell = price-based return on start-of-year position, "
                   "plus new cashflows during the year. Current year shows YTD.")

        years = sorted({d.year for d in inst_values.index})
        cols_year = [str(y) if y < today_ts.year else f"{y} YTD" for y in years]

        annual_rows = []
        for t in tickers_used:
            row = {"Ticker": t, "Instrument": INSTRUMENTS[t]["name"]}
            s_val = inst_values[t] if t in inst_values.columns else pd.Series(dtype=float)
            s_cf = inst_cashflows[t] if t in inst_cashflows.columns else pd.Series(dtype=float)
            year_total = 0.0
            for y, col_name in zip(years, cols_year):
                y_start = pd.Timestamp(year=y, month=1, day=1) - pd.Timedelta(days=1)
                y_end = pd.Timestamp(year=y, month=12, day=31, hour=23, minute=59)
                mask_start = s_val.index <= y_start
                start_val = float(s_val.loc[mask_start].iloc[-1]) if mask_start.any() else 0.0
                mask_end = (s_val.index >= y_start) & (s_val.index <= y_end)
                if not mask_end.any():
                    row[col_name] = 0.0; continue
                end_val = float(s_val.loc[mask_end].iloc[-1])
                cf_in_year = float(s_cf.loc[(s_cf.index > y_start) & (s_cf.index <= y_end)].sum()) \
                    if not s_cf.empty else 0.0
                yr_pnl = end_val - start_val - cf_in_year
                row[col_name] = yr_pnl
                year_total += yr_pnl
            row["Total"] = year_total
            annual_rows.append(row)

        df_annual = pd.DataFrame(annual_rows)
        col_order = ["Ticker", "Instrument"] + cols_year + ["Total"]
        df_annual = df_annual[col_order]
        total_annual_row = {"Ticker": "TOTAL", "Instrument": "— Portfolio —"}
        for col in cols_year:
            total_annual_row[col] = df_annual[col].sum()
        total_annual_row["Total"] = df_annual["Total"].sum()
        df_annual_display = pd.concat([df_annual, pd.DataFrame([total_annual_row])],
                                       ignore_index=True)
        num_cols = cols_year + ["Total"]
        st.dataframe(
            df_annual_display, hide_index=True, use_container_width=True,
            column_config={col: st.column_config.NumberColumn(format="€%.0f")
                           for col in num_cols},
        )


# =====================================================================
# Performance tab
# =====================================================================
with tab_perf:
    if not compute_ok:
        st.write("Add transactions first.")
    else:
        st.subheader("Portfolio Value vs. Net Invested")
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=total_value.index, y=total_value.values,
                                 mode="lines", name="Portfolio Value",
                                 line=dict(color="#2E5CB8", width=2.5)))
        fig.add_trace(go.Scatter(x=net_invested.index, y=net_invested.values,
                                 mode="lines", name="Net Invested",
                                 line=dict(color="#C0392B", width=2, dash="dash")))
        fig.update_layout(hovermode="x unified", height=420,
                          margin=dict(l=20, r=20, t=30, b=20),
                          xaxis_title=None, yaxis_title="EUR",
                          legend=dict(orientation="h", y=-0.15))
        st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")
        st.subheader("Cumulative Time-Weighted Return")
        cum_twr = (1 + daily_ret).cumprod() - 1
        fig_twr = go.Figure()
        fig_twr.add_trace(go.Scatter(x=cum_twr.index, y=cum_twr.values * 100,
                                     mode="lines", name="Portfolio TWR",
                                     line=dict(color="#1D7F3E", width=2.5)))
        if "EPA:CW8" in history.columns:
            bench = history["EPA:CW8"].dropna()
            if len(bench) > 1:
                bench_ret = bench / bench.iloc[0] - 1
                fig_twr.add_trace(go.Scatter(x=bench_ret.index, y=bench_ret.values * 100,
                                             mode="lines", name="MSCI World (CW8)",
                                             line=dict(color="#888", width=1.5, dash="dot")))
        fig_twr.update_layout(hovermode="x unified", height=380,
                              margin=dict(l=20, r=20, t=30, b=20),
                              yaxis_title="%", xaxis_title=None,
                              legend=dict(orientation="h", y=-0.15))
        st.plotly_chart(fig_twr, use_container_width=True)

        st.markdown("---")
        st.subheader("Annual P&L (Portfolio Total)")
        yearly_value = total_value.resample("YE").last()
        yearly_invested = net_invested.resample("YE").last()
        prev_v = yearly_value.shift(1).fillna(0)
        prev_i = yearly_invested.shift(1).fillna(0)
        annual_pnl = (yearly_value - prev_v) - (yearly_invested - prev_i)
        annual_pnl.index = annual_pnl.index.year
        colors = ["#1D7F3E" if v >= 0 else "#C0392B" for v in annual_pnl.values]
        fig_ann = go.Figure(go.Bar(x=annual_pnl.index.astype(str), y=annual_pnl.values,
                                    marker_color=colors,
                                    text=[fmt_eur(v) for v in annual_pnl.values],
                                    textposition="outside"))
        fig_ann.update_layout(height=360, margin=dict(l=20, r=20, t=30, b=20),
                              yaxis_title="€", xaxis_title="Year", showlegend=False)
        st.plotly_chart(fig_ann, use_container_width=True)

        st.markdown("---")
        st.subheader("Market Value per Instrument (Stacked)")
        vpi = inst_values.fillna(0)
        fig_inst = go.Figure()
        for t in vpi.columns:
            if vpi[t].abs().sum() > 0:
                fig_inst.add_trace(go.Scatter(x=vpi.index, y=vpi[t].values,
                                               mode="lines", name=t, stackgroup="one"))
        fig_inst.update_layout(hovermode="x unified", height=420,
                               margin=dict(l=20, r=20, t=30, b=20),
                               yaxis_title="EUR", xaxis_title=None,
                               legend=dict(orientation="h", y=-0.15))
        st.plotly_chart(fig_inst, use_container_width=True)


# =====================================================================
# Analysis tab
# =====================================================================
with tab_analysis:
    if not compute_ok:
        st.write("Add transactions first.")
    else:
        st.subheader("Risk & Return Metrics")
        sharpe = ((ann_ret_si - rf) / ann_vol) if (ann_ret_si is not None and ann_vol > 0) else None
        mdd = max_drawdown(daily_ret)
        best_day = daily_ret.max() if not daily_ret.empty else 0
        worst_day = daily_ret.min() if not daily_ret.empty else 0

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Annualized Return",
                  fmt_pct(ann_ret_si) if ann_ret_si is not None else "—")
        c2.metric("Annualized Volatility", fmt_pct(ann_vol))
        c3.metric("Sharpe Ratio",
                  f"{sharpe:.2f}" if sharpe is not None else "—",
                  f"rf = {fmt_pct(rf)}")
        c4.metric("Max Drawdown", fmt_pct(mdd))

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Best Day", fmt_pct(best_day))
        c2.metric("Worst Day", fmt_pct(worst_day))
        nz = daily_ret[daily_ret != 0]
        pos_days = (daily_ret > 0).sum() / max(len(nz), 1)
        c3.metric("% Positive Days", f"{pos_days*100:.1f}%")
        mv_by = {t: float(inst_values[t].iloc[-1]) for t in tickers_used
                 if t in inst_values.columns}
        total_mv = sum(mv_by.values())
        concentration = max(mv_by.values()) / total_mv if total_mv > 0 else 0
        c4.metric("Top Position Share", f"{concentration*100:.1f}%")

        st.markdown("---")
        st.subheader("Drawdown Curve")
        cum = (1 + daily_ret).cumprod()
        dd = (cum - cum.cummax()) / cum.cummax()
        fig_dd = go.Figure(go.Scatter(x=dd.index, y=dd.values * 100, mode="lines",
                                       fill="tozeroy",
                                       line=dict(color="#C0392B", width=1.5),
                                       fillcolor="rgba(192,57,43,0.2)"))
        fig_dd.update_layout(hovermode="x unified", height=280,
                             margin=dict(l=20, r=20, t=30, b=20),
                             yaxis_title="%", showlegend=False)
        st.plotly_chart(fig_dd, use_container_width=True)

        st.markdown("---")
        st.subheader("Geographic Exposure (Look-Through)")
        geo_agg = {}
        tot_mv_nz = total_mv if total_mv > 0 else 1
        for t in tickers_used:
            mv = mv_by.get(t, 0)
            w = mv / tot_mv_nz
            for region, wr in GEO_EXPOSURE.get(t, {}).items():
                geo_agg[region] = geo_agg.get(region, 0) + w * wr

        if geo_agg:
            geo_df = pd.DataFrame(sorted(geo_agg.items(), key=lambda kv: -kv[1]),
                                  columns=["Region", "Weight"])
            geo_df["Weight %"] = geo_df["Weight"] * 100
            col1, col2 = st.columns([2, 1])
            with col1:
                fig_geo = px.pie(geo_df, values="Weight", names="Region",
                                 color_discrete_sequence=px.colors.qualitative.Set2)
                fig_geo.update_traces(textposition="inside", textinfo="percent+label")
                fig_geo.update_layout(height=400, margin=dict(l=0, r=0, t=20, b=20),
                                      showlegend=False)
                st.plotly_chart(fig_geo, use_container_width=True)
            with col2:
                st.dataframe(
                    geo_df[["Region", "Weight %"]], hide_index=True,
                    use_container_width=True,
                    column_config={"Weight %": st.column_config.NumberColumn(format="%.1f%%")},
                )


# =====================================================================
# Transactions tab
# =====================================================================
with tab_tx:
    st.subheader("✏️ Transactions")
    st.caption("Edit the table directly or use the quick-add form below.")

    with st.expander("➕ Add a new transaction",
                     expanded=st.session_state.transactions.empty):
        with st.form("add_tx_form", clear_on_submit=True):
            c1, c2, c3, c4 = st.columns([1.2, 1.5, 1, 1.2])
            new_date = c1.date_input("Date", value=date.today(), format="YYYY-MM-DD")
            new_ticker = c2.selectbox(
                "Ticker", options=list(INSTRUMENTS.keys()),
                format_func=lambda t: f"{t} — {INSTRUMENTS[t]['name'][:40]}",
            )
            new_qty = c3.number_input("Quantity (negative = sell)",
                                       value=0.0, step=1.0, format="%.4f")
            new_price = c4.number_input("Unit price (€)", value=0.0, step=0.01,
                                         format="%.4f", min_value=0.0)
            if st.form_submit_button("Add transaction", type="primary"):
                if new_qty == 0:
                    st.warning("Quantity cannot be zero.")
                elif new_price <= 0:
                    st.warning("Unit price must be > 0.")
                else:
                    new_row = pd.DataFrame([{
                        "Date": new_date, "Ticker": new_ticker,
                        "Quantity": new_qty, "Price": new_price,
                    }])
                    st.session_state.transactions = pd.concat(
                        [st.session_state.transactions, new_row], ignore_index=True
                    )
                    st.session_state.transactions = storage.normalize_transactions(
                        st.session_state.transactions
                    )
                    ok, msg = persist(st.session_state.transactions)
                    st.session_state.last_save_ok = (ok, msg)
                    st.rerun()

    st.markdown("**All transactions** (editable)")
    edited = st.data_editor(
        st.session_state.transactions,
        num_rows="dynamic", use_container_width=True, key="tx_editor",
        column_config={
            "Date": st.column_config.DateColumn("Date", format="YYYY-MM-DD", required=True),
            "Ticker": st.column_config.SelectboxColumn(
                "Ticker", options=list(INSTRUMENTS.keys()), required=True
            ),
            "Quantity": st.column_config.NumberColumn("Quantity", format="%.4f", required=True),
            "Price": st.column_config.NumberColumn("Unit Price (€)", format="%.4f",
                                                    min_value=0.0, required=True),
        },
        hide_index=True,
    )

    c1, c2, _ = st.columns([1, 1, 3])
    if c1.button("💾 Save changes", type="primary"):
        try:
            cleaned = storage.normalize_transactions(edited)
            st.session_state.transactions = cleaned
            ok, msg = persist(cleaned)
            st.session_state.last_save_ok = (ok, msg)
            (st.success if ok else st.error)(msg)
            st.rerun()
        except Exception as e:
            st.error(f"Could not save: {e}")
    if c2.button("↶ Discard changes"):
        st.session_state.loaded_once = False
        load_initial_data()
        st.rerun()

    if st.session_state.last_save_ok:
        ok, msg = st.session_state.last_save_ok
        (st.success if ok else st.error)(msg)

    st.markdown("---")
    st.markdown("**Bulk import (replaces all transactions)**")
    uploaded = st.file_uploader("Upload CSV / XLSX", type=["csv", "xlsx", "xls"])
    if uploaded is not None:
        try:
            imported = storage.load_from_csv_bytes(uploaded)
            st.write(f"Parsed **{len(imported)}** transactions:")
            st.dataframe(imported.head(10), use_container_width=True, hide_index=True)
            if st.button("Replace all with this file", type="primary"):
                st.session_state.transactions = imported
                ok, msg = persist(imported)
                st.session_state.last_save_ok = (ok, msg)
                st.rerun()
        except Exception as e:
            st.error(f"Could not parse: {e}")

    st.markdown("---")
    if not st.session_state.transactions.empty:
        csv_bytes = st.session_state.transactions.to_csv(index=False).encode()
        st.download_button("⬇️ Download as CSV", csv_bytes,
                           file_name=f"transactions_{date.today().isoformat()}.csv",
                           mime="text/csv")
