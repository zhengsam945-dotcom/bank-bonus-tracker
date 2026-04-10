import os
import json
import calendar
import traceback
from typing import Any, Optional
from datetime import datetime, date

import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "1VzVnbtZ2PioqFOZexVq-maNHrRVmRmCc5DQk_1eWaX8")


def get_client():
    service_account_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")

    if service_account_json:
        creds_dict = json.loads(service_account_json)
        creds = Credentials.from_service_account_info(
            creds_dict,
            scopes=SCOPES
        )
    else:
        creds = Credentials.from_service_account_file(
            "google_service_account.json",
            scopes=SCOPES
        )

    return gspread.authorize(creds)

# ---------- SHEET SCHEMA ----------
OFFERS_COLUMNS = [
    "offer_id",
    "bank_name",
    "account_type",
    "bonus_name",
    "bonus_type",
    "bonus_amount",
    "currency",
    "source_link",
    "offer_note",
    "monthly_fee",
    "waiver_condition",
    "requirement_summary",
    "requirement_tags",
    "required_amount",
    "dd_required",
    "dd_method",
    "dd_method_note",
    "need_keep_balance_until",
    "must_keep_account_open_until",
    "early_close_fee",
    "account_open_date",
    "dd_posted_date",
    "bonus_posted_date",
    "notes",
    "status",
]

TIMELINE_COLUMNS = [
    "offer_id",
    "stage_name",
    "stage_label",
    "stage_type",
    "start_date",
    "end_date",
    "status",
    "note",
]

DATE_COLUMNS_OFFERS = [
    "need_keep_balance_until",
    "must_keep_account_open_until",
    "account_open_date",
    "dd_posted_date",
    "bonus_posted_date",
]

DATE_COLUMNS_TIMELINE = ["start_date", "end_date"]
STATUS_OPTIONS = [
    "researching",
    "planned",
    "opened",
    "qualifying",
    "holding",
    "waiting_bonus",
    "bonus_posted",
    "completed",
    "failed",
    "expired",
    "closed",
]
STAGE_STATUS_OPTIONS = ["upcoming", "ongoing", "completed", "missed", "not_applicable"]
ACCOUNT_TYPES = ["checking", "saving", "checking+saving", "brokerage", "other"]


# ---------- AUTH ----------
def get_credentials() -> Credentials:
    # Render path 1: secret file path in env/secrets
    import os
    secret_path = os.environ.get("GOOGLE_SERVICE_ACCOUNT_FILE")
    if not secret_path:
        try:
            secret_path = st.secrets.get("GOOGLE_SERVICE_ACCOUNT_FILE", "")
        except Exception:
            secret_path = ""
    if secret_path:
        return Credentials.from_service_account_file(secret_path, scopes=SCOPES)

    # Render path 2: whole JSON stored in env/secrets
    raw_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not raw_json:
        try:
            raw_json = st.secrets.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")
        except Exception:
            raw_json = ""
    if raw_json:
        creds_dict = json.loads(raw_json)
        return Credentials.from_service_account_info(creds_dict, scopes=SCOPES)

    # Local fallback
    return Credentials.from_service_account_file("google_service_account.json", scopes=SCOPES)


@st.cache_resource
def get_sheet():
    creds = get_credentials()
    client = gspread.authorize(creds)
    return client.open_by_key(SPREADSHEET_ID)


def get_worksheets():
    sheet = get_sheet()
    return sheet.worksheet("offers"), sheet.worksheet("timeline_stages")


# ---------- HELPERS ----------
def ensure_spreadsheet_id() -> None:
    if not SPREADSHEET_ID:
        st.error("Missing SPREADSHEET_ID. Add it to Render environment variables or st.secrets.")
        st.stop()


def clean_value(value: Any) -> str:
    if value is None:
        return ""
    if pd.isna(value):
        return ""
    return str(value)

def parse_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    if pd.isna(value):
        return None

    text = str(value).strip()
    if text == "":
        return None

    dt = pd.to_datetime(text, errors="coerce")
    if pd.isna(dt):
        return None

    return dt.date()


def safe_to_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0)


def normalize_df(df: pd.DataFrame, expected_cols: List[str]) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=expected_cols)
    for col in expected_cols:
        if col not in df.columns:
            df[col] = ""
    return df[expected_cols]


def load_sheet_as_df(ws, expected_cols: List[str]) -> pd.DataFrame:
    values = ws.get_all_values()
    if not values:
        return pd.DataFrame(columns=expected_cols)

    headers = values[0]
    rows = values[1:]
    if not rows:
        return pd.DataFrame(columns=headers)

    padded_rows = []
    for idx, row in enumerate(rows, start=2):
        if len(row) < len(headers):
            row = row + [""] * (len(headers) - len(row))
        elif len(row) > len(headers):
            row = row[: len(headers)]
        record = dict(zip(headers, row))
        record["_row_number"] = idx
        padded_rows.append(record)

    df = pd.DataFrame(padded_rows)
    for col in expected_cols:
        if col not in df.columns:
            df[col] = ""
    front = ["_row_number"] + expected_cols
    return df[front]


def load_offers_df() -> pd.DataFrame:
    offers_ws, _ = get_worksheets()
    return load_sheet_as_df(offers_ws, OFFERS_COLUMNS)


def load_timeline_df() -> pd.DataFrame:
    _, timeline_ws = get_worksheets()
    return load_sheet_as_df(timeline_ws, TIMELINE_COLUMNS)


def append_offer_row(row_dict: Dict[str, Any]) -> None:
    offers_ws, _ = get_worksheets()
    row = [clean_value(row_dict.get(col, "")) for col in OFFERS_COLUMNS]
    offers_ws.append_row(row, value_input_option="USER_ENTERED")


def append_timeline_row(row_dict: Dict[str, Any]) -> None:
    _, timeline_ws = get_worksheets()
    row = [clean_value(row_dict.get(col, "")) for col in TIMELINE_COLUMNS]
    timeline_ws.append_row(row, value_input_option="USER_ENTERED")


def update_sheet_row(ws, row_number: int, row_dict: Dict[str, Any], columns: List[str]) -> None:
    row = [clean_value(row_dict.get(col, "")) for col in columns]
    ws.update(f"A{row_number}:{gspread.utils.rowcol_to_a1(row_number, len(columns)).split(str(row_number))[0]}{row_number}", [row], value_input_option="USER_ENTERED")


def update_offer_row(row_number: int, row_dict: Dict[str, Any]) -> None:
    offers_ws, _ = get_worksheets()
    update_sheet_row(offers_ws, row_number, row_dict, OFFERS_COLUMNS)


def update_timeline_row(row_number: int, row_dict: Dict[str, Any]) -> None:
    _, timeline_ws = get_worksheets()
    update_sheet_row(timeline_ws, row_number, row_dict, TIMELINE_COLUMNS)


def delete_offer_row(row_number: int) -> None:
    offers_ws, _ = get_worksheets()
    offers_ws.delete_rows(row_number)


def delete_timeline_row(row_number: int) -> None:
    _, timeline_ws = get_worksheets()
    timeline_ws.delete_rows(row_number)


def make_timeline_event_df(timeline_df: pd.DataFrame, offers_df: pd.DataFrame) -> pd.DataFrame:
    events = []

    # ---------- 1) timeline_stages -> start/end events ----------
    if timeline_df is not None and not timeline_df.empty:
        tmp = timeline_df.copy()

        if "start_date" in tmp.columns:
            tmp["start_date_dt"] = tmp["start_date"].apply(parse_date)
        else:
            tmp["start_date_dt"] = None

        if "end_date" in tmp.columns:
            tmp["end_date_dt"] = tmp["end_date"].apply(parse_date)
        else:
            tmp["end_date_dt"] = None

        for _, row in tmp.iterrows():
            offer_id = clean_value(row.get("offer_id"))
            stage_label = clean_value(row.get("stage_label"))
            stage_type = clean_value(row.get("stage_type"))
            status = clean_value(row.get("status"))
            note = clean_value(row.get("note"))

            start_dt = row.get("start_date_dt")
            end_dt = row.get("end_date_dt")

            # start event
            if start_dt is not None:
                events.append({
                    "event_date": start_dt,
                    "offer_id": offer_id,
                    "event_label": f"{stage_label} (Start)" if stage_label else "Stage Start",
                    "event_type": stage_type if stage_type else "timeline_stage_start",
                    "status": status,
                    "note": note,
                    "source": "timeline_stages",
                })

            # end event
            if end_dt is not None:
                events.append({
                    "event_date": end_dt,
                    "offer_id": offer_id,
                    "event_label": f"{stage_label} (End)" if stage_label else "Stage End",
                    "event_type": stage_type if stage_type else "timeline_stage_end",
                    "status": status,
                    "note": note,
                    "source": "timeline_stages",
                })

    # ---------- 2) offers -> key date events ----------
    if offers_df is not None and not offers_df.empty:
        offer_date_fields = [
            ("need_keep_balance_until", "Keep Balance Until", "offer_keep_balance_until"),
            ("must_keep_account_open_until", "Safe to Close After", "offer_safe_close_after"),
            ("dd_posted_date", "DD Posted", "offer_dd_posted"),
            ("bonus_posted_date", "Bonus Posted", "offer_bonus_posted"),
            ("account_open_date", "Account Opened", "offer_account_opened"),
        ]

        tmp = offers_df.copy()

        for _, row in tmp.iterrows():
            offer_id = clean_value(row.get("offer_id"))
            bank_name = clean_value(row.get("bank_name"))
            bonus_name = clean_value(row.get("bonus_name"))
            status = clean_value(row.get("status"))

            prefix = ""
            if bank_name or bonus_name:
                prefix = f"{bank_name} - {bonus_name}".strip(" -")

            for col_name, label, event_type in offer_date_fields:
                dt = parse_date(row.get(col_name))
                if dt is not None:
                    events.append({
                        "event_date": dt,
                        "offer_id": offer_id,
                        "event_label": label,
                        "event_type": event_type,
                        "status": status,
                        "note": prefix,
                        "source": "offers",
                    })

    if not events:
        return pd.DataFrame(columns=[
            "event_date", "offer_id", "event_label", "event_type", "status", "note", "source"
        ])

    event_df = pd.DataFrame(events)
    event_df = event_df.sort_values(["event_date", "offer_id", "event_label"], na_position="last")
    return event_df


def month_events_map(event_df: pd.DataFrame, year: int, month: int) -> Dict[int, List[str]]:
    out: Dict[int, List[str]] = {}
    if event_df.empty:
        return out
    for _, row in event_df.iterrows():
        start = row.get("start_date_dt")
        end = row.get("end_date_dt") or start
        if not start:
            continue
        if end and end < start:
            end = start
        cur = start
        while cur and cur <= end:
            if cur.year == year and cur.month == month:
                label = row.get("stage_label", "") or row.get("stage_name", "") or "Stage"
                offer = row.get("offer_id", "")
                status = row.get("status", "")
                text = f"{offer} · {label} · {status}" if offer else f"{label} · {status}"
                out.setdefault(cur.day, []).append(text)
            cur = cur.fromordinal(cur.toordinal() + 1)
    return out


def render_month_calendar(event_df: pd.DataFrame, year: int, month: int) -> None:
    cal = calendar.Calendar(firstweekday=0)
    weeks = cal.monthdayscalendar(year, month)
    events_by_day = month_events_map(event_df, year, month)

    st.subheader(f"{calendar.month_name[month]} {year}")
    headers = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    header_cols = st.columns(7)
    for c, h in zip(header_cols, headers):
        c.markdown(f"**{h}**")

    for week in weeks:
        cols = st.columns(7)
        for col, day in zip(cols, week):
            if day == 0:
                col.markdown(" ")
            else:
                lines = events_by_day.get(day, [])[:3]
                more = len(events_by_day.get(day, [])) - len(lines)
                body = "<br>".join([f"• {x}" for x in lines])
                if more > 0:
                    body += f"<br>+ {more} more"
                card = f"<div style='border:1px solid #ddd; border-radius:8px; padding:8px; min-height:120px;'>"
                card += f"<div style='font-weight:700; margin-bottom:6px;'>{day}</div>"
                card += f"<div style='font-size:0.82rem; line-height:1.25;'>{body}</div></div>"
                col.markdown(card, unsafe_allow_html=True)


# ---------- FORMS ----------
def offer_form(defaults: Optional[Dict[str, Any]] = None, key_prefix: str = "offer") -> Dict[str, Any]:
    defaults = defaults or {}
    c1, c2, c3 = st.columns(3)
    offer_id = c1.text_input("offer_id", value=clean_value(defaults.get("offer_id", "")), key=f"{key_prefix}_offer_id")
    bank_name = c2.text_input("bank_name", value=clean_value(defaults.get("bank_name", "")), key=f"{key_prefix}_bank_name")
    account_type_default = clean_value(defaults.get("account_type", ACCOUNT_TYPES[0]))
    account_type_index = ACCOUNT_TYPES.index(account_type_default) if account_type_default in ACCOUNT_TYPES else 0
    account_type = c3.selectbox("account_type", ACCOUNT_TYPES, index=account_type_index, key=f"{key_prefix}_account_type")

    c4, c5, c6 = st.columns(3)
    bonus_name = c4.text_input("bonus_name", value=clean_value(defaults.get("bonus_name", "")), key=f"{key_prefix}_bonus_name")
    bonus_type = c5.text_input("bonus_type", value=clean_value(defaults.get("bonus_type", "")), key=f"{key_prefix}_bonus_type")
    bonus_amount = c6.text_input("bonus_amount", value=clean_value(defaults.get("bonus_amount", "")), key=f"{key_prefix}_bonus_amount")

    c7, c8 = st.columns(2)
    currency = c7.text_input("currency", value=clean_value(defaults.get("currency", "USD")), key=f"{key_prefix}_currency")
    status_default = clean_value(defaults.get("status", STATUS_OPTIONS[1]))
    status_index = STATUS_OPTIONS.index(status_default) if status_default in STATUS_OPTIONS else 1
    status = c8.selectbox("status", STATUS_OPTIONS, index=status_index, key=f"{key_prefix}_status")

    source_link = st.text_input("source_link", value=clean_value(defaults.get("source_link", "")), key=f"{key_prefix}_source_link")
    offer_note = st.text_input("offer_note", value=clean_value(defaults.get("offer_note", "")), key=f"{key_prefix}_offer_note")

    c9, c10 = st.columns(2)
    monthly_fee = c9.text_input("monthly_fee", value=clean_value(defaults.get("monthly_fee", "")), key=f"{key_prefix}_monthly_fee")
    waiver_condition = c10.text_input("waiver_condition", value=clean_value(defaults.get("waiver_condition", "")), key=f"{key_prefix}_waiver_condition")

    requirement_summary = st.text_area("requirement_summary", value=clean_value(defaults.get("requirement_summary", "")), key=f"{key_prefix}_requirement_summary")
    requirement_tags = st.text_input("requirement_tags", value=clean_value(defaults.get("requirement_tags", "")), key=f"{key_prefix}_requirement_tags")

    c11, c12, c13 = st.columns(3)
    required_amount = c11.text_input("required_amount", value=clean_value(defaults.get("required_amount", "")), key=f"{key_prefix}_required_amount")
    dd_required = c12.selectbox("dd_required", ["TRUE", "FALSE", ""], index=["TRUE", "FALSE", ""].index(clean_value(defaults.get("dd_required", ""))) if clean_value(defaults.get("dd_required", "")) in ["TRUE", "FALSE", ""] else 2, key=f"{key_prefix}_dd_required")
    dd_method = c13.text_input("dd_method", value=clean_value(defaults.get("dd_method", "")), key=f"{key_prefix}_dd_method")

    dd_method_note = st.text_input("dd_method_note", value=clean_value(defaults.get("dd_method_note", "")), key=f"{key_prefix}_dd_method_note")

    c14, c15, c16 = st.columns(3)
    need_keep_balance_until = c14.text_input("need_keep_balance_until (YYYY-MM-DD)", value=clean_value(defaults.get("need_keep_balance_until", "")), key=f"{key_prefix}_need_keep_balance_until")
    must_keep_account_open_until = c15.text_input("must_keep_account_open_until (YYYY-MM-DD)", value=clean_value(defaults.get("must_keep_account_open_until", "")), key=f"{key_prefix}_must_keep_account_open_until")
    early_close_fee = c16.text_input("early_close_fee", value=clean_value(defaults.get("early_close_fee", "")), key=f"{key_prefix}_early_close_fee")

    c17, c18, c19 = st.columns(3)
    account_open_date = c17.text_input("account_open_date (YYYY-MM-DD)", value=clean_value(defaults.get("account_open_date", "")), key=f"{key_prefix}_account_open_date")
    dd_posted_date = c18.text_input("dd_posted_date (YYYY-MM-DD)", value=clean_value(defaults.get("dd_posted_date", "")), key=f"{key_prefix}_dd_posted_date")
    bonus_posted_date = c19.text_input("bonus_posted_date (YYYY-MM-DD)", value=clean_value(defaults.get("bonus_posted_date", "")), key=f"{key_prefix}_bonus_posted_date")

    notes = st.text_area("notes", value=clean_value(defaults.get("notes", "")), key=f"{key_prefix}_notes")

    return {
        "offer_id": offer_id.strip(),
        "bank_name": bank_name.strip(),
        "account_type": account_type,
        "bonus_name": bonus_name.strip(),
        "bonus_type": bonus_type.strip(),
        "bonus_amount": bonus_amount.strip(),
        "currency": currency.strip(),
        "source_link": source_link.strip(),
        "offer_note": offer_note.strip(),
        "monthly_fee": monthly_fee.strip(),
        "waiver_condition": waiver_condition.strip(),
        "requirement_summary": requirement_summary.strip(),
        "requirement_tags": requirement_tags.strip(),
        "required_amount": required_amount.strip(),
        "dd_required": dd_required,
        "dd_method": dd_method.strip(),
        "dd_method_note": dd_method_note.strip(),
        "need_keep_balance_until": need_keep_balance_until.strip(),
        "must_keep_account_open_until": must_keep_account_open_until.strip(),
        "early_close_fee": early_close_fee.strip(),
        "account_open_date": account_open_date.strip(),
        "dd_posted_date": dd_posted_date.strip(),
        "bonus_posted_date": bonus_posted_date.strip(),
        "notes": notes.strip(),
        "status": status,
    }


def timeline_form(offer_options: List[str], defaults: Optional[Dict[str, Any]] = None, key_prefix: str = "tl") -> Dict[str, Any]:
    defaults = defaults or {}
    default_offer = clean_value(defaults.get("offer_id", ""))
    if offer_options:
        default_index = offer_options.index(default_offer) if default_offer in offer_options else 0
        offer_id = st.selectbox("offer_id", offer_options, index=default_index, key=f"{key_prefix}_offer_id")
    else:
        offer_id = st.text_input("offer_id", value=default_offer, key=f"{key_prefix}_offer_id")

    c1, c2, c3 = st.columns(3)
    stage_name = c1.text_input("stage_name", value=clean_value(defaults.get("stage_name", "")), key=f"{key_prefix}_stage_name")
    stage_label = c2.text_input("stage_label", value=clean_value(defaults.get("stage_label", "")), key=f"{key_prefix}_stage_label")
    stage_type = c3.text_input("stage_type", value=clean_value(defaults.get("stage_type", "")), key=f"{key_prefix}_stage_type")

    c4, c5, c6 = st.columns(3)
    start_date = c4.text_input("start_date (YYYY-MM-DD)", value=clean_value(defaults.get("start_date", "")), key=f"{key_prefix}_start_date")
    end_date = c5.text_input("end_date (YYYY-MM-DD)", value=clean_value(defaults.get("end_date", "")), key=f"{key_prefix}_end_date")
    status_default = clean_value(defaults.get("status", STAGE_STATUS_OPTIONS[0]))
    status_index = STAGE_STATUS_OPTIONS.index(status_default) if status_default in STAGE_STATUS_OPTIONS else 0
    status = c6.selectbox("status", STAGE_STATUS_OPTIONS, index=status_index, key=f"{key_prefix}_status")

    note = st.text_area("note", value=clean_value(defaults.get("note", "")), key=f"{key_prefix}_note")

    return {
        "offer_id": offer_id.strip(),
        "stage_name": stage_name.strip(),
        "stage_label": stage_label.strip(),
        "stage_type": stage_type.strip(),
        "start_date": start_date.strip(),
        "end_date": end_date.strip(),
        "status": status,
        "note": note.strip(),
    }


# ---------- APP ----------
ensure_spreadsheet_id()
st.set_page_config(page_title="Bank Bonus Tracker", layout="wide")
st.title("🏦 Bank Bonus Tracker")

try:
    offers_df = load_offers_df()
except Exception as e:
    st.error(f"Failed to load offers sheet: {e}")
    st.stop()

try:
    timeline_df = load_timeline_df()
except Exception as e:
    st.error(f"Failed to load timeline_stages sheet: {e}")
    st.stop()

offer_options = []
if not offers_df.empty and "offer_id" in offers_df.columns:
    offer_options = [str(x) for x in offers_df["offer_id"].fillna("").tolist() if str(x).strip()]

menu = st.sidebar.radio(
    "Navigation",
    [
        "Dashboard",
        "Offers",
        "Timeline Stages",
        "Master Timeline",
        "Month Calendar",
        "Deploy Notes",
    ],
)

if menu == "Dashboard":
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Offers", len(offers_df))
    if not offers_df.empty and "bonus_amount" in offers_df.columns:
        total_bonus = safe_to_numeric(offers_df["bonus_amount"]).sum()
    else:
        total_bonus = 0
    c2.metric("Total Bonus Amount", f"${total_bonus:,.0f}")
    c3.metric("Timeline Stages", len(timeline_df))
    c4.metric("Unique Banks", offers_df["bank_name"].nunique() if not offers_df.empty and "bank_name" in offers_df.columns else 0)

    st.subheader("Offers")
    show_df = offers_df.drop(columns=["_row_number"], errors="ignore")
    st.dataframe(show_df, use_container_width=True)

    st.subheader("Timeline Stages")
    tl_show = timeline_df.drop(columns=["_row_number"], errors="ignore")
    if not tl_show.empty:
        tl_show = tl_show.sort_values(by=["start_date", "offer_id"], na_position="last")
    st.dataframe(tl_show, use_container_width=True)

elif menu == "Offers":
    tab1, tab2, tab3 = st.tabs(["View", "Add", "Edit / Delete"])

    with tab1:
        st.subheader("Offers Table")
        st.dataframe(offers_df.drop(columns=["_row_number"], errors="ignore"), use_container_width=True)

    with tab2:
        st.subheader("Add Offer")
        with st.form("add_offer_form"):
            new_offer = offer_form(key_prefix="add_offer")
            submitted = st.form_submit_button("Add Offer")
            if submitted:
                if not new_offer["offer_id"]:
                    st.error("offer_id is required.")
                else:
                    append_offer_row(new_offer)
                    st.success("Offer added.")
                    st.rerun()

    with tab3:
        st.subheader("Edit or Delete Offer")
        if offers_df.empty:
            st.info("No offers yet.")
        else:
            options = {
                f"{row['offer_id']} | {row.get('bank_name','')} | {row.get('bonus_name','')}": int(row["_row_number"])
                for _, row in offers_df.iterrows()
            }
            selected_label = st.selectbox("Choose one offer", list(options.keys()))
            row_number = options[selected_label]
            current = offers_df.loc[offers_df["_row_number"] == row_number].iloc[0].to_dict()

            with st.form("edit_offer_form"):
                edited = offer_form(defaults=current, key_prefix="edit_offer")
                c1, c2 = st.columns(2)
                update_btn = c1.form_submit_button("Update Offer")
                delete_btn = c2.form_submit_button("Delete Offer")

                if update_btn:
                    update_offer_row(row_number, edited)
                    st.success("Offer updated.")
                    st.rerun()
                if delete_btn:
                    delete_offer_row(row_number)
                    st.success("Offer deleted.")
                    st.rerun()

elif menu == "Timeline Stages":
    tab1, tab2, tab3 = st.tabs(["View", "Add", "Edit / Delete"])

    with tab1:
        st.subheader("Timeline Table")
        temp = timeline_df.drop(columns=["_row_number"], errors="ignore")
        if not temp.empty:
            temp = temp.sort_values(by=["start_date", "offer_id"], na_position="last")
        st.dataframe(temp, use_container_width=True)

    with tab2:
        st.subheader("Add Timeline Stage")
        with st.form("add_timeline_form"):
            new_stage = timeline_form(offer_options, key_prefix="add_timeline")
            submitted = st.form_submit_button("Add Timeline Stage")
            if submitted:
                if not new_stage["offer_id"]:
                    st.error("offer_id is required.")
                else:
                    append_timeline_row(new_stage)
                    st.success("Timeline stage added.")
                    st.rerun()

    with tab3:
        st.subheader("Edit or Delete Timeline Stage")
        if timeline_df.empty:
            st.info("No timeline stages yet.")
        else:
            options = {
                f"row {row['_row_number']} | {row.get('offer_id','')} | {row.get('stage_label','')} | {row.get('start_date','')}": int(row["_row_number"])
                for _, row in timeline_df.iterrows()
            }
            selected_label = st.selectbox("Choose one timeline row", list(options.keys()))
            row_number = options[selected_label]
            current = timeline_df.loc[timeline_df["_row_number"] == row_number].iloc[0].to_dict()

            with st.form("edit_timeline_form"):
                edited = timeline_form(offer_options, defaults=current, key_prefix="edit_timeline")
                c1, c2 = st.columns(2)
                update_btn = c1.form_submit_button("Update Timeline Stage")
                delete_btn = c2.form_submit_button("Delete Timeline Stage")

                if update_btn:
                    update_timeline_row(row_number, edited)
                    st.success("Timeline stage updated.")
                    st.rerun()
                if delete_btn:
                    delete_timeline_row(row_number)
                    st.success("Timeline stage deleted.")
                    st.rerun()

elif menu == "Master Timeline":
    st.subheader("Master Timeline")
    event_df = make_timeline_event_df(timeline_df)
    if event_df.empty:
        st.info("No timeline stages yet.")
    else:
        filter_offer = st.selectbox("Filter by offer_id", ["All"] + offer_options)
        if filter_offer != "All":
            event_df = event_df[event_df["offer_id"].astype(str) == filter_offer]
        event_df = event_df.sort_values(by=["start_date_dt", "offer_id"], na_position="last")
        for _, row in event_df.iterrows():
            st.markdown(
                f"**{row.get('start_date','')} → {row.get('end_date','')}**  \\  \n"
                f"Offer: `{row.get('offer_id','')}`  \\  \n"
                f"Stage: {row.get('stage_label','')} ({row.get('stage_type','')})  \\  \n"
                f"Status: {row.get('status','')}  \\  \n"
                f"Note: {row.get('note','')}"
            )
            st.divider()

elif menu == "Month Calendar":
    st.subheader("Month Calendar")
    event_df = make_timeline_event_df(timeline_df, offers_df)
    if event_df.empty:
        st.info("No timeline stages yet.")
    else:
        today = date.today()
        c1, c2 = st.columns(2)
        year = c1.number_input("Year", min_value=2020, max_value=2100, value=today.year, step=1)
        month = c2.selectbox("Month", list(range(1, 13)), index=today.month - 1, format_func=lambda x: calendar.month_name[x])
        filter_offer = st.selectbox("Filter offer_id", ["All"] + offer_options, key="calendar_offer_filter")
        if filter_offer != "All":
            event_df = event_df[event_df["offer_id"].astype(str) == filter_offer]
        render_month_calendar(event_df, int(year), int(month))

elif menu == "Deploy Notes":
    st.subheader("Render Deploy Checklist")
    st.markdown(
        """
1. Push this project to GitHub.
2. Create a **Web Service** on Render connected to that repo.
3. Build Command:
   ```bash
   pip install -r requirements.txt
   ```
4. Start Command:
   ```bash
   python -m streamlit run app.py --server.port $PORT --server.address 0.0.0.0
   ```
5. In Render environment variables, add:
   - `SPREADSHEET_ID`
   - either `GOOGLE_SERVICE_ACCOUNT_JSON` **or** `GOOGLE_SERVICE_ACCOUNT_FILE`
6. Do not commit your Google service account key into GitHub.
        """
    )
