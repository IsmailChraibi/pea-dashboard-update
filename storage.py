"""
Storage backends for PEA transactions.
Primary: Google Sheets (via gspread + service account).
Fallback for local dev: a CSV file on disk.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

REQUIRED_COLUMNS = ["Date", "Ticker", "Quantity", "Price"]


def normalize_transactions(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame(index=df.index)
    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            out[col] = pd.NA
        else:
            out[col] = df[col].values

    out["Date"] = pd.to_datetime(out["Date"], errors="coerce").dt.date
    out["Ticker"] = out["Ticker"].astype(str).str.strip()
    out["Quantity"] = pd.to_numeric(out["Quantity"], errors="coerce")
    out["Price"] = pd.to_numeric(out["Price"], errors="coerce")

    out = out.dropna(subset=["Date", "Ticker"], how="any")
    out = out[out["Ticker"] != ""]
    out = out[out["Ticker"].str.lower() != "nan"]
    out = out.sort_values("Date").reset_index(drop=True)
    return out


def _gspread_client():
    import gspread
    from google.oauth2.service_account import Credentials

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    info = dict(st.secrets["gcp_service_account"])
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)


def _get_worksheet():
    import gspread

    client = _gspread_client()
    sheet_url = st.secrets["gsheets"]["sheet_url"]
    worksheet_name = st.secrets["gsheets"].get("worksheet", "transactions")

    sh = client.open_by_url(sheet_url)
    try:
        ws = sh.worksheet(worksheet_name)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=worksheet_name, rows=200, cols=10)
        ws.update(values=[REQUIRED_COLUMNS], range_name="A1")
    return ws


def sheets_available() -> bool:
    return "gcp_service_account" in st.secrets and "gsheets" in st.secrets


def load_from_sheets() -> pd.DataFrame:
    ws = _get_worksheet()
    records = ws.get_all_records()
    if not records:
        return pd.DataFrame(columns=REQUIRED_COLUMNS)
    return normalize_transactions(pd.DataFrame(records))


def save_to_sheets(df: pd.DataFrame) -> None:
    ws = _get_worksheet()
    df = normalize_transactions(df)

    rows = []
    for _, r in df.iterrows():
        rows.append([
            r["Date"].isoformat() if pd.notna(r["Date"]) else "",
            str(r["Ticker"]),
            float(r["Quantity"]) if pd.notna(r["Quantity"]) else 0,
            float(r["Price"]) if pd.notna(r["Price"]) else 0,
        ])
    ws.clear()
    ws.update(values=[REQUIRED_COLUMNS] + rows, range_name="A1")


def load_from_csv_bytes(uploaded) -> pd.DataFrame:
    name = uploaded.name.lower()
    if name.endswith((".xlsx", ".xls")):
        try:
            df = pd.read_excel(uploaded, header=1)
            df = df.dropna(axis=1, how="all")
            if df.shape[1] >= 3:
                df = df.iloc[:, :4] if df.shape[1] >= 4 else df.iloc[:, :3]
                cols = ["Date", "Ticker", "Quantity"]
                if df.shape[1] == 4:
                    cols.append("Price")
                df.columns = cols
        except Exception:
            uploaded.seek(0)
            df = pd.read_excel(uploaded)
    else:
        df = pd.read_csv(uploaded)
    return normalize_transactions(df)


def save_to_disk_cache(df: pd.DataFrame, path: Path) -> None:
    df.to_csv(path, index=False)


def load_from_disk_cache(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=REQUIRED_COLUMNS)
    return normalize_transactions(pd.read_csv(path))
