"""Meetup Telegram Blaster — Streamlit UI.

Run with:  streamlit run app.py

The app is a step-by-step wizard. State lives in:
  * SQLite (data/state.db) — contacts, campaigns, send log
  * Telethon session file (sessions/*.session) — TG auth
  * Streamlit session state — only transient UI bits
"""
from __future__ import annotations

import asyncio
import io
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import streamlit as st

from core import auth, countries, csv_io, redis_client, template as template_mod
from core.md import telegram_md_to_html
from core.campaign_runner import CampaignRunner
from core.config import (
    UPLOADS_DIR,
    ensure_default_account_seeded,
    load_account_settings,
    load_settings,
    save_credentials,
)
from core.database import Database
from core.jobs import Dispatcher
from core.rate_limiter import PacingConfig
from core.resolver import resolve_one_phone, resolve_one_username


# --------------------------------------------------------------------------
# Page config & style
# --------------------------------------------------------------------------

st.set_page_config(
    page_title="Meetup TG Blaster",
    page_icon="📣",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
        /* Tighter header */
        .block-container { padding-top: 2rem; padding-bottom: 2rem; }

        /* Hero card */
        .hero {
            background: linear-gradient(135deg, #2AABEE 0%, #229ED9 100%);
            color: white; padding: 1.25rem 1.5rem; border-radius: 14px;
            margin-bottom: 1.25rem;
        }
        .hero h1 { color: white; margin: 0 0 .25rem 0; font-size: 1.5rem; }
        .hero p  { color: rgba(255,255,255,.9); margin: 0; }

        /* Step chip */
        .step-chip {
            display: inline-block; padding: .15rem .6rem; border-radius: 999px;
            background: #EAF6FE; color: #1D6FA5; font-size: .75rem; font-weight: 600;
            margin-bottom: .5rem; letter-spacing: .02em;
        }

        /* KPI cards */
        .kpi-card {
            background: #F5F7FA; padding: 1rem; border-radius: 12px;
            border: 1px solid #E1E8F0;
        }
        .kpi-value { font-size: 1.75rem; font-weight: 700; color: #1A2238; }
        .kpi-label { font-size: .8rem; color: #5A6375; margin-top: .15rem; }

        /* Disclaimer */
        .danger {
            border-left: 4px solid #E53935; padding: .75rem 1rem;
            background: #FFF4F4; border-radius: 6px; color: #8B1E1E;
            font-size: .9rem;
        }

        /* Make the sidebar radio look like steps */
        [data-testid="stSidebar"] .stRadio > label { font-size: .85rem; }
    </style>
    """,
    unsafe_allow_html=True,
)


# --------------------------------------------------------------------------
# Cached resources
# --------------------------------------------------------------------------

@st.cache_resource
def get_db() -> Database:
    db = Database(load_settings().db_path)
    # v1.5 bootstrap: if someone had a working .env setup pre-upgrade,
    # seed one "default" accounts row so the rest of the app sees an
    # account to pin to.
    try:
        ensure_default_account_seeded(db)
    except Exception:
        pass
    return db


def get_dispatcher() -> Dispatcher:
    """Dispatcher is re-instantiated each render so it reflects the
    currently-selected account from session_state. This is cheap —
    Dispatcher just holds a DB ref and an int."""
    aid = st.session_state.get("active_account_id")
    return Dispatcher(get_db(), account_id=aid)


def get_settings():
    """Return settings for the currently-selected account. Falls back to
    `.env` if no accounts row exists yet (fresh install before first login).
    """
    db = get_db()
    aid = st.session_state.get("active_account_id")
    s = load_account_settings(db, aid) if aid else load_account_settings(db)
    if s is None:
        return load_settings()
    if st.session_state.get("active_account_id") != s.id:
        st.session_state.active_account_id = s.id
    return s


def _backend_badge() -> str:
    if redis_client.is_redis_available():
        return "🟢 Redis/arq"
    return "🟡 In-process threads"


def is_authorized_cached(settings, ttl_sec: float = 8.0) -> bool:
    """UI-level cache for ``auth.is_authorized``.

    Nav clicks re-render the whole script; each render was calling
    ``auth.run_async(auth.is_authorized(...))`` which opens/locks the
    Telethon SQLiteSession. During worker contention that single call
    can block ~3 s per render — bad UX. Cache the result per
    (session_path, api_id) for a few seconds so back-to-back reruns
    don't re-hit the session file.
    """
    if not getattr(settings, "has_credentials", False):
        return False
    key = f"_auth_ok::{settings.session_path}::{settings.api_id}"
    now = time.time()
    cached = st.session_state.get(key)
    if cached and (now - cached[1]) < ttl_sec:
        return bool(cached[0])
    try:
        ok = bool(auth.run_async(auth.is_authorized(
            settings.session_path, settings.api_id, settings.api_hash,
        )))
    except Exception:
        ok = False
    st.session_state[key] = (ok, now)
    return ok


def _campaign_id_of_job(job) -> int | None:
    """Pluck the campaign_id out of a `jobs.payload_json` blob safely."""
    try:
        payload = json.loads(job["payload_json"] or "{}")
    except Exception:
        return None
    cid = payload.get("campaign_id")
    return int(cid) if cid is not None else None


def _render_job_panel(db, recent_jobs, running_jobs, label: str) -> None:
    """Render a live progress bar + event-log for the given job type.

    Picking which job to surface (v1.5 follow-up: stop showing ancient
    error rows forever):

      * A running/queued job wins.
      * Otherwise, the most recent job — *but* if that most recent one
        is `error` and a newer `done` job of the same type exists
        elsewhere in the history, prefer the done one.
      * For `error` jobs older than 12h with no newer history, skip
        entirely. A week-old "database is locked" shouldn't dominate
        the log page; anything actionable will be the latest run.
    """
    import datetime as _dt

    job_to_show = None
    if running_jobs:
        job_to_show = running_jobs[0]
    elif recent_jobs:
        first = recent_jobs[0]
        # recent_jobs is already ordered most-recent-first by list_jobs.
        if first["status"] == "error":
            # If a done/error run happened after this one, `first` would
            # already reflect it. So this branch just checks age.
            started = first["started_at"] or first["queued_at"]
            try:
                started_dt = _dt.datetime.fromisoformat(started.replace("Z", "+00:00"))
                if started_dt.tzinfo is None:
                    started_dt = started_dt.replace(tzinfo=_dt.timezone.utc)
            except Exception:
                started_dt = None
            if started_dt is not None:
                age_hours = (_dt.datetime.now(_dt.timezone.utc) - started_dt).total_seconds() / 3600
                if age_hours > 12:
                    # Too stale — hide unless user explicitly visits Jobs.
                    return
        job_to_show = first

    if job_to_show is None:
        return

    is_live = job_to_show["status"] in ("queued", "running")
    refresh = "1.5s" if is_live else None
    job_id = int(job_to_show["id"])

    @st.fragment(run_every=refresh)
    def _panel():
        fresh = db.get_job(job_id) or job_to_show
        try:
            payload = json.loads(fresh["progress_json"] or "{}")
        except Exception:
            payload = {}

        done = int(payload.get("done", 0))
        total = int(payload.get("total", 0) or 1)
        pct = min(1.0, done / total) if total else 0.0

        st.markdown(
            f"**{label} job #{fresh['id']}** · статус: `{fresh['status']}`"
            + (f" · бэкенд: `{fresh['backend']}`" if fresh["backend"] else "")
        )
        st.progress(
            pct,
            text=(
                f"Обработано {done}/{payload.get('total', 0)}  ·  "
                f"✓ {payload.get('resolved', 0)}   "
                f"✗ {payload.get('not_on_telegram', 0)}   "
                f"! {payload.get('errors', 0)}"
            ),
        )
        if payload.get("current"):
            st.caption(f"Последний: `{payload['current']}`")
        if fresh["status"] == "error" and fresh["error"]:
            st.error(f"Задача упала: {fresh['error']}")

        events = payload.get("events") or []
        with st.expander(
            f"🪵 Лог ({len(events)} строк)",
            expanded=(fresh["status"] in ("running", "queued", "error")),
        ):
            if events:
                st.code("\n".join(events[-80:]), language="text")
            else:
                st.caption("Событий пока нет.")

    _panel()


def _render_sheet_source_panel(db) -> None:
    """Compact 'Подключённый источник' panel at top of Contacts page.

    Singleton Google Sheet feeds the DB. Cadence-based auto-sync (Phase 6
    cron). Auto-resolve toggle chains the resolver on new contacts so the
    pipeline ends at 'validated, ready for any campaign'.
    """
    import requests

    st.markdown('<div class="step-chip">📡 ИСТОЧНИК · GOOGLE SHEETS</div>',
                unsafe_allow_html=True)
    current = db.get_sheet_source()
    redis_ok = redis_client.is_redis_available()

    head = st.columns([3, 1, 1, 1])
    if current is None:
        head[0].info("Источник ещё не подключён. Настрой ниже — мапинг → сохранить → автосинк.")
    else:
        head[0].success(
            f"**Подключён**: `{current['label'] or 'primary'}` · "
            f"строк в базе: **{current['rows_seen']}** · "
            f"последняя синхронизация: `{current['last_synced_at'] or '—'}` · "
            f"кадenция: "
            + (f"**{current['cadence_min']} мин**" if current["cadence_min"] else "**вручную**")
        )
    head[1].markdown(
        "🟢 Redis/arq" if redis_ok else "🟡 standalone (cron off)",
        help="Периодический автосинк работает только при живом arq-воркере.",
    )
    if current is not None:
        with head[2]:
            if st.button("🔄 Sync now", key="sheet_sync_now",
                         disabled=not current["column_map_json"]):
                _run_sheet_source_sync_now(db)
                st.rerun()
        with head[3]:
            if st.button("🗑 Удалить", key="sheet_source_del",
                         type="secondary"):
                db.delete_sheet_source()
                for k in list(st.session_state.keys()):
                    if k.startswith("sheet_src_") or k == "sheet_source_headers":
                        st.session_state.pop(k, None)
                st.toast("Источник удалён.", icon="🗑")
                st.rerun()

    with st.expander("⚙ Настроить / обновить источник", expanded=current is None):
        default_url = current["url"] if current else ""
        new_url = st.text_input(
            "Google Sheet URL (доступ «Anyone with the link · Viewer»)",
            value=default_url,
            key="sheet_src_url",
            placeholder="https://docs.google.com/spreadsheets/d/…",
        )
        cc_cadence, cc_auto, cc_read = st.columns([1, 1, 1])
        with cc_cadence:
            default_cadence = int(current["cadence_min"] or 0) if current else 0
            cadence = st.number_input(
                "Автосинк каждые, мин (0 — вручную)",
                min_value=0, max_value=1440, step=5,
                value=default_cadence,
                key="sheet_src_cadence",
            )
        with cc_auto:
            default_auto = bool(current["auto_resolve"]) if current else True
            auto_resolve = st.checkbox(
                "Авто-resolve новых контактов",
                value=default_auto,
                key="sheet_src_auto_resolve",
                help="После синка автоматически ставит resolve_contacts_job только для новых id.",
            )
        with cc_read:
            if st.button("📥 Прочитать заголовки", key="sheet_src_read",
                         disabled=not (new_url and new_url.strip())):
                export_url = _gsheet_export_url(new_url.strip())
                if not export_url:
                    st.error("Это не похоже на ссылку Google Sheets.")
                else:
                    try:
                        resp = requests.get(
                            export_url, timeout=30, allow_redirects=True,
                        )
                    except requests.RequestException as e:
                        st.error(f"Сеть не отдала ответ: {e}")
                    else:
                        if resp.status_code != 200 or not resp.content:
                            st.error(
                                f"HTTP {resp.status_code} — проверь доступ «Anyone with the link · Viewer»."
                            )
                        elif b"<!DOCTYPE html" in resp.content[:500]:
                            st.error(
                                "Google вернул login-страницу. Открой таблице доступ по ссылке."
                            )
                        else:
                            headers = csv_io.read_headers(io.BytesIO(resp.content))
                            guess = csv_io.guess_column_map(headers)
                            st.session_state["sheet_source_headers"] = {
                                "url": new_url.strip(),
                                "headers": headers,
                                "guess": guess,
                            }
                            st.toast(f"Прочитал {len(headers)} колонок.", icon="📥")

        # Mapping editor — either the just-read headers, or the saved map.
        hs = st.session_state.get("sheet_source_headers")
        map_source_headers: list[str] | None = None
        saved_map: dict = {}
        if hs:
            map_source_headers = hs["headers"]
            seed = hs["guess"]
            saved_map = {
                "phone": seed.get("phone"),
                "name": seed.get("name"),
                "username": seed.get("username"),
                "extra": seed.get("extra") or [],
            }
        elif current and current["column_map_json"]:
            try:
                saved_map = json.loads(current["column_map_json"])
            except Exception:
                saved_map = {}
            # We don't re-fetch headers for the saved view — use the columns
            # stored in the saved map (good enough to show the current mapping).
            map_source_headers = [c for c in (
                [saved_map.get("phone"), saved_map.get("name"),
                 saved_map.get("username")]
                + list(saved_map.get("extra") or [])
            ) if c]

        if map_source_headers:
            # `phone_or_username` = one column that holds EITHER a phone
            # OR a TG nick (Google-Form classic: "укажи телефон если нет
            # ника"). Parser already handles this when phone_col == tg_col.
            roles = ["skip", "phone", "name", "username",
                     "phone_or_username", "extra"]
            saved_phone = saved_map.get("phone")
            saved_uname = saved_map.get("username")
            saved_both = saved_phone and saved_phone == saved_uname
            def _role_for(h: str) -> str:
                if saved_both and h == saved_phone:
                    return "phone_or_username"
                if h == saved_phone:
                    return "phone"
                if h == saved_map.get("name"):
                    return "name"
                if h == saved_uname:
                    return "username"
                if h in (saved_map.get("extra") or []):
                    return "extra"
                return "skip"
            map_df = pd.DataFrame(
                [{"column": h, "role": _role_for(h)} for h in map_source_headers]
            )
            st.caption(
                "**Мэппинг колонок** — выбери роль. `phone_or_username` "
                "для колонок, где пользователь мог указать либо телефон, "
                "либо `@username` (Google Form «укажи номер если нет ника»)."
            )
            edited_map = st.data_editor(
                map_df,
                use_container_width=True,
                hide_index=True,
                num_rows="fixed",
                key="sheet_src_map_editor",
                column_config={
                    "column": st.column_config.TextColumn("column", disabled=True),
                    "role": st.column_config.SelectboxColumn(
                        "role", options=roles, required=True,
                    ),
                },
            )
            if st.button("💾 Сохранить источник",
                         type="primary", key="sheet_src_save"):
                role_to_cols: dict[str, list[str]] = {r: [] for r in roles}
                for _, r in edited_map.iterrows():
                    role_to_cols[r["role"]].append(r["column"])
                errors = []
                for single in ("phone", "name", "username", "phone_or_username"):
                    if len(role_to_cols[single]) > 1:
                        errors.append(
                            f"Колонок с ролью «{single}» несколько: "
                            f"{role_to_cols[single]} — оставь одну."
                        )
                # phone_or_username is mutually exclusive with phone/username.
                if role_to_cols["phone_or_username"] and (
                    role_to_cols["phone"] or role_to_cols["username"]
                ):
                    errors.append(
                        "«phone_or_username» нельзя комбинировать с отдельными "
                        "ролями «phone» или «username» — оставь одно из двух."
                    )
                if not (role_to_cols["phone"] or role_to_cols["username"]
                        or role_to_cols["phone_or_username"]):
                    errors.append(
                        "Нужна хотя бы одна колонка «phone», «username» "
                        "или «phone_or_username»."
                    )
                if errors:
                    for e in errors:
                        st.error(e)
                else:
                    combined = (role_to_cols["phone_or_username"] or [None])[0]
                    column_map = {
                        "phone": (role_to_cols["phone"] or [None])[0] or combined,
                        "name": (role_to_cols["name"] or [None])[0],
                        "username": (role_to_cols["username"] or [None])[0] or combined,
                        "extra": role_to_cols["extra"],
                    }
                    sid = db.upsert_sheet_source(
                        url=(hs["url"] if hs else current["url"] if current else new_url.strip()),
                        column_map_json=json.dumps(column_map, ensure_ascii=False),
                        cadence_min=int(cadence) if int(cadence) >= 5 else None,
                        auto_resolve=bool(auto_resolve),
                    )
                    st.session_state.pop("sheet_source_headers", None)
                    st.toast(f"Источник сохранён (id={sid}).", icon="💾")
                    st.rerun()

    st.divider()


def _run_sheet_source_sync_now(db) -> None:
    """Manual 'Sync now' from Contacts page. Same logic as the cron but
    surfaces progress inline and yields toast on success."""
    import datetime as _dt
    import hashlib
    import requests

    s = db.get_sheet_source()
    if s is None:
        st.error("Источник не настроен.")
        return
    try:
        column_map = json.loads(s["column_map_json"] or "{}")
    except Exception:
        st.error("Мэппинг колонок повреждён — пересохрани источник.")
        return
    export_url = _gsheet_export_url(s["url"] or "")
    if not export_url:
        st.error("Сохранённый URL не похож на Google Sheets.")
        return

    prog = st.progress(0.0, text="Скачиваю CSV…")
    try:
        resp = requests.get(export_url, timeout=30, allow_redirects=True)
    except requests.RequestException as e:
        prog.empty()
        st.error(f"Сеть не отдала ответ: {e}")
        return
    if (
        resp.status_code != 200
        or not resp.content
        or b"<!DOCTYPE html" in resp.content[:500]
    ):
        prog.empty()
        st.error(
            f"Не удалось скачать (HTTP {resp.status_code}). "
            "Проверь доступ «Anyone with the link»."
        )
        return

    prog.progress(0.4, text="Парсю по сохранённому мэппингу…")
    try:
        valid, invalid, duplicates = csv_io.parse_with_mapping(
            io.BytesIO(resp.content),
            column_map=column_map,
            default_region="UZ",
        )
    except Exception as e:
        prog.empty()
        st.error(f"Парсер упал: {e}")
        return

    prog.progress(0.7, text=f"Сохраняю {len(valid)} контактов…")
    before_ids = {int(i) for i in db.all_contacts_df()["id"].tolist()}
    db.upsert_contacts(csv_io.contacts_to_db_rows(valid))
    after_ids = {int(i) for i in db.all_contacts_df()["id"].tolist()}
    new_ids = sorted(after_ids - before_ids)

    phones_signature = "\n".join(sorted(c.phone for c in valid if c.phone)).encode("utf-8")
    db.update_sheet_source(
        last_synced_at=_dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds"),
        last_seen_phone_set_hash=hashlib.sha1(phones_signature).hexdigest(),
        rows_seen=len(valid),
    )

    # If auto-resolve is on and we have new contacts, chain the resolver.
    resolve_job_id = None
    if new_ids and bool(s["auto_resolve"]):
        try:
            disp = get_dispatcher()
            resolve_job_id = disp.enqueue_resolve(
                cleanup_imported=True, ids=new_ids,
            )
        except RuntimeError:
            # Another resolve already running — that's OK, these ids are
            # in DB and will be picked up on the next manual/automatic resolve.
            pass

    prog.progress(1.0, text="Готово.")
    msg = (
        f"строк: {len(valid)} · новых: {len(new_ids)} · "
        f"дубликатов: {len(duplicates)} · невалид: {len(invalid)}"
    )
    if resolve_job_id:
        msg += f" · resolve job #{resolve_job_id}"
    st.toast("✅ Синк готов: " + msg, icon="📡")

def _gsheet_export_url(url: str) -> str | None:
    """Convert a pasted Google Sheets URL into its CSV-export URL.

    If the pasted URL has `#gid=N` or `?gid=N`, we preserve that tab.
    Otherwise we omit `gid` entirely and let Google serve the first tab —
    defaulting to `gid=0` is wrong whenever the first tab has a different
    id (e.g. the original sheet was deleted and recreated).
    Returns None if the URL doesn't look like Sheets.
    """
    m = re.search(r"/spreadsheets/d/([A-Za-z0-9_\-]+)", url)
    if not m:
        return None
    sheet_id = m.group(1)
    gid_match = re.search(r"[?&#]gid=(\d+)", url)
    base = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"
    if gid_match:
        return f"{base}&gid={gid_match.group(1)}"
    return base


# --------------------------------------------------------------------------
# Sidebar — navigation
# --------------------------------------------------------------------------

STEPS = [
    ("1. 🔐 Login & setup", "setup"),
    ("2. 📋 Contacts", "contacts"),
    ("3. ✍️ Compose message", "compose"),
    ("4. 🧪 Dry-run test", "dryrun"),
    ("5. 🚀 Campaign", "campaign"),
    ("6. 📊 Log", "log"),
    ("7. ⚙️ Jobs", "jobs"),
]

with st.sidebar:
    st.markdown("### 📣 Meetup TG Blaster")
    st.caption("Локальный сервис массовой рассылки через твой TG-аккаунт")

    # Account picker. Sits above the step nav so it's always visible and
    # drives every page below via st.session_state.active_account_id.
    db = get_db()
    accounts = db.list_accounts()
    if accounts:
        account_ids = [int(a["id"]) for a in accounts]
        account_labels = {int(a["id"]): a["label"] for a in accounts}
        # Keep session_state in sync with DB's active row on first render.
        current_id = st.session_state.get("active_account_id")
        if current_id not in account_ids:
            active_row = db.get_active_account() or accounts[0]
            current_id = int(active_row["id"])
            st.session_state.active_account_id = current_id
        picked = st.selectbox(
            "Аккаунт",
            options=account_ids,
            index=account_ids.index(current_id),
            format_func=lambda aid: account_labels.get(aid, f"#{aid}"),
            key="account_selector",
        )
        if picked != st.session_state.get("active_account_id"):
            db.set_active_account(int(picked))
            st.session_state.active_account_id = int(picked)
            st.rerun()
    else:
        st.caption("Нет аккаунтов — залогинься в шаге 1.")

    # Consume any pending nav-redirect BEFORE the radio is instantiated.
    # Streamlit forbids mutating a widget's session_state value after the
    # widget renders, so any code that wants to switch pages mid-run writes
    # into _pending_nav_step and we pick it up here on the next rerun.
    _pending = st.session_state.pop("_pending_nav_step", None)
    if _pending:
        st.session_state["nav_step"] = _pending

    step_label = st.radio(
        "Шаги",
        [s[0] for s in STEPS],
        key="nav_step",
        label_visibility="collapsed",
    )
    step_id = next(k for label, k in STEPS if label == step_label)

    st.divider()
    settings = get_settings()

    # Connection status mini-badge — cached so nav clicks don't re-hammer
    # the Telethon session file.
    connected = is_authorized_cached(settings)
    if connected:
        st.success("✅ TG-аккаунт подключён")
    else:
        st.warning("⚠️ TG-аккаунт не подключён")

    # Cheap COUNT queries — no DataFrame construction, no row
    # materialization. Was burning visible time on every tab switch.
    st.metric("Контактов в базе", db.count_contacts())
    st.metric("С Telegram", db.count_resolved())

    st.divider()
    st.caption(f"Бэкенд задач: {_backend_badge()}")
    running_count = db.count_running_jobs()
    if running_count:
        st.caption(f"🔄 Активных задач: **{running_count}** — см. шаг 7")


# --------------------------------------------------------------------------
# STEP 1 — Setup / Login
# --------------------------------------------------------------------------

def render_setup():
    st.markdown(
        '<div class="hero"><h1>Подключи свой Telegram-аккаунт</h1>'
        '<p>Несколько аккаунтов — несколько строк. Сессии живут в '
        '<code>sessions/</code>, учётки — в таблице <code>accounts</code>.</p></div>',
        unsafe_allow_html=True,
    )

    db = get_db()
    accounts = db.list_accounts()

    # ---- Accounts list + management ---------------------------------
    st.markdown('<div class="step-chip">ШАГ 1 · АККАУНТЫ</div>', unsafe_allow_html=True)
    if accounts:
        rows = []
        for a in accounts:
            session_file = (Path(__file__).parent / "sessions" / f"{a['session_name']}.session")
            rows.append({
                "id": int(a["id"]),
                "метка": a["label"],
                "активен": "✓" if a["is_active"] else "",
                "session file": a["session_name"] + (
                    "  (есть)" if session_file.exists() else "  (нет)"
                ),
                "api_id": int(a["api_id"]),
                "notes": a["notes"] or "",
            })
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

        rm_col1, rm_col2 = st.columns([2, 1])
        with rm_col1:
            to_remove = st.selectbox(
                "Удалить аккаунт",
                options=[None] + [int(a["id"]) for a in accounts],
                format_func=lambda aid: "—" if aid is None else
                    next((a["label"] for a in accounts if int(a["id"]) == aid), f"#{aid}"),
                key="setup_rm_account",
            )
        with rm_col2:
            if st.button("🗑 Удалить", disabled=to_remove is None):
                db.delete_account(int(to_remove))
                if st.session_state.get("active_account_id") == to_remove:
                    st.session_state.pop("active_account_id", None)
                st.success("Аккаунт удалён.")
                st.rerun()
    else:
        st.info(
            "Ещё ни одного аккаунта. Заполни поля ниже и нажми «Сохранить», "
            "затем залогинься через код SMS."
        )

    # ---- Add / edit the active account ------------------------------
    with st.expander("➕ Добавить / обновить аккаунт", expanded=not accounts):
        st.markdown(
            """
            1. Открой [my.telegram.org](https://my.telegram.org) → **API development tools**.
            2. Создай приложение, скопируй **api_id** и **api_hash**.
            3. Придумай короткую метку (`meetup`, `community-b`, …) и имя файла сессии.
            """
        )
        new_col1, new_col2 = st.columns([1, 1])
        with new_col1:
            new_label = st.text_input("Метка (короткое имя)", key="new_acc_label")
            new_api_id = st.number_input(
                "API_ID", value=0, step=1, format="%d", key="new_acc_api_id",
            )
        with new_col2:
            new_api_hash = st.text_input(
                "API_HASH", type="password", key="new_acc_api_hash",
            )
            new_session = st.text_input(
                "Имя сессии (файл .session)", key="new_acc_session",
                placeholder="например, tg_session или community_b",
            )
        if st.button("💾 Сохранить аккаунт", type="primary"):
            if not new_label or not new_api_id or not new_api_hash or not new_session:
                st.error("Заполни все поля.")
            elif any(a["label"] == new_label.strip() for a in accounts):
                st.error(f"Метка «{new_label.strip()}» уже занята.")
            else:
                aid = db.upsert_account(
                    label=new_label.strip(),
                    api_id=int(new_api_id),
                    api_hash=new_api_hash.strip(),
                    session_name=new_session.strip(),
                    is_active=not accounts,  # first one becomes active
                )
                if not accounts:
                    st.session_state.active_account_id = aid
                st.success(f"Аккаунт #{aid} сохранён. Теперь залогинься ниже.")
                st.rerun()

    if not accounts:
        return  # nothing more to do until the first account exists

    st.divider()

    # ---- Login flow for the *currently-selected* account ------------
    settings_current = get_settings()
    st.markdown(
        f'<div class="step-chip">ШАГ 2 · АВТОРИЗАЦИЯ · '
        f'<code>{settings_current.label}</code></div>',
        unsafe_allow_html=True,
    )
    if not settings_current.has_credentials:
        st.info("У выбранного аккаунта нет API_ID/API_HASH — заполни их выше.")
        return
    current = settings_current

    authorized = False
    try:
        authorized = auth.run_async(auth.is_authorized(
            current.session_path, current.api_id, current.api_hash,
        ))
    except Exception as e:
        st.error(f"Ошибка подключения: {e}")

    if authorized:
        st.success(f"✅ Аккаунт `{current.label}` подключён.")
        if st.button("Выйти из аккаунта"):
            auth.run_async(auth.logout(
                current.session_path, current.api_id, current.api_hash,
            ))
            st.rerun()
        return

    if "login_stage" not in st.session_state:
        st.session_state.login_stage = "phone"

    if st.session_state.login_stage == "phone":
        phone = st.text_input("Твой номер телефона (в формате +12025550101)", key="login_phone")
        if st.button("📨 Прислать код", type="primary"):
            if not phone:
                st.error("Введи номер.")
            else:
                with st.spinner("Отправляю код в Telegram…"):
                    state = auth.run_async(auth.start_login(
                        current.session_path, current.api_id, current.api_hash, phone,
                    ))
                if state.authorized:
                    st.success("Уже залогинен!")
                    st.rerun()
                elif state.error:
                    st.error(f"Ошибка: {state.error}")
                elif state.needs_code:
                    st.session_state.login_stage = "code"
                    st.rerun()

    elif st.session_state.login_stage == "code":
        st.info("Код пришёл тебе в Telegram. Введи его ниже.")
        code = st.text_input("Код из Telegram", key="login_code", max_chars=10)
        password = st.text_input("2FA пароль (если включен)", type="password", key="login_pwd")
        colA, colB = st.columns(2)
        with colA:
            if st.button("✅ Войти", type="primary"):
                with st.spinner("Проверяю код…"):
                    state = auth.run_async(auth.complete_login(
                        current.session_path, current.api_id, current.api_hash,
                        code, password or None,
                    ))
                if state.authorized:
                    st.session_state.login_stage = "phone"
                    st.success("Вошёл 🎉")
                    st.rerun()
                elif state.needs_password:
                    st.warning("Нужен 2FA пароль — введи его и нажми Войти.")
                else:
                    st.error(f"Ошибка: {state.error}")
        with colB:
            if st.button("↩️ Начать заново"):
                st.session_state.login_stage = "phone"
                st.rerun()


# --------------------------------------------------------------------------
# STEP 2 — Contacts
# --------------------------------------------------------------------------

def render_contacts():
    st.markdown(
        '<div class="hero"><h1>База контактов</h1>'
        '<p>Один подключённый Google Sheet → автосинк → resolve/validate → готово к кампаниям. '
        'CSV и ручное добавление — на случай разовых пополнений.</p></div>',
        unsafe_allow_html=True,
    )

    # ------- 📡 Подключённый источник (singleton Google Sheet) ----------
    _render_sheet_source_panel(db)

    st.markdown('<div class="step-chip">CSV</div>', unsafe_allow_html=True)

    uploaded = st.file_uploader("Выбери CSV", type=["csv"], accept_multiple_files=False)
    region = st.selectbox(
        "Страна по умолчанию для номеров без +",
        countries.ISO_CODES,
        index=countries.ISO_CODES.index("UZ"),
        format_func=countries.label_for,
        help="Используется только если номер без +7/+1/+… в начале.",
    )
    col1, col2 = st.columns(2)
    with col1:
        if uploaded is not None:
            if st.button("📥 Импортировать", type="primary"):
                prog = st.progress(0.0, text="Читаю файл…")
                try:
                    prog.progress(0.25, text="Парсю CSV и нормализую номера…")
                    valid, invalid, duplicates = csv_io.parse_csv(uploaded, default_region=region)
                    prog.progress(0.7, text=f"Сохраняю {len(valid)} контактов…")
                    inserted = db.upsert_contacts(csv_io.contacts_to_db_rows(valid))
                    prog.progress(1.0, text="Готово.")
                    st.toast(
                        f"CSV: +{inserted} новых / {len(valid)} уник / "
                        f"{len(invalid)} без номера/username / "
                        f"{len(duplicates)} дубликатов",
                        icon="✅",
                    )
                    if invalid or duplicates:
                        with st.expander(f"Детали импорта · отбросили {len(invalid)} + {len(duplicates)} дубликатов"):
                            if invalid:
                                st.caption("Строки без валидного phone/username:")
                                st.dataframe(pd.DataFrame(invalid), use_container_width=True)
                            if duplicates:
                                st.caption("Дубликаты (один и тот же phone):")
                                st.dataframe(pd.DataFrame(duplicates), use_container_width=True)
                    st.rerun()
                except Exception as e:
                    prog.empty()
                    st.error(f"Не смог прочитать CSV: {e}")
    with col2:
        example_path = Path(__file__).parent / "data" / "example_contacts.csv"
        if example_path.exists():
            # Prefix UTF-8 BOM so Excel reads Cyrillic correctly.
            raw = example_path.read_bytes()
            if not raw.startswith(b"\xef\xbb\xbf"):
                raw = b"\xef\xbb\xbf" + raw
            st.download_button(
                "📄 Скачать пример CSV",
                data=raw,
                file_name="example_contacts.csv",
                mime="text/csv",
            )

    st.divider()
    st.markdown("### ➕ Добавить контакт вручную")
    st.caption(
        "Введи **телефон** (+7…) или **@username** / ссылку t.me/… — сам разберусь. "
        "Имя возьму из профиля Telegram, если в поле ниже пусто."
    )
    current_settings = load_settings()
    try:
        authorized = auth.run_async(auth.is_authorized(
            current_settings.session_path,
            current_settings.api_id,
            current_settings.api_hash,
        )) if current_settings.has_credentials else False
    except Exception:
        authorized = False

    m1, m2, m3 = st.columns([2, 2, 1])
    with m1:
        manual_target = st.text_input(
            "Телефон или @username",
            key="manual_phone",
            placeholder="+12025550101  или  @alice_example",
        )
    with m2:
        manual_name = st.text_input(
            "Имя (пусто — возьмём из TG)", key="manual_name",
        )
    with m3:
        manual_region = st.selectbox(
            "Регион (для номера без +)",
            countries.ISO_CODES,
            index=countries.ISO_CODES.index("UZ"),
            format_func=countries.label_for,
            key="manual_region",
        )

    add_clicked = st.button(
        "➕ Добавить и найти в Telegram",
        type="primary",
        disabled=not authorized,
    )
    if not authorized:
        st.caption("⚠️ Требуется залогиненный TG-аккаунт (шаг 1).")

    if add_clicked:
        raw = (manual_target or "").strip()
        if not raw:
            st.error("Поле пустое — введи телефон или @username.")
        else:
            # Try to classify. Phone wins if digits+ look valid; otherwise
            # treat as a username (with normalization for URL forms).
            hinted_username = csv_io._normalize_username(raw)
            e164 = csv_io.normalize_phone(raw, default_region=manual_region)

            client = auth.get_client(
                current_settings.session_path,
                current_settings.api_id,
                current_settings.api_hash,
            )

            try:
                if e164:
                    # Insert as pending phone, then resolve by ImportContacts.
                    inserted = db.upsert_contacts([{
                        "phone": e164,
                        "name": manual_name.strip(),
                        "raw_phone": raw,
                        "extra_json": None,
                    }])
                    match = db.all_contacts_df().query("phone == @e164")
                    cid = int(match.iloc[0]["id"])
                    with st.spinner("Ищу номер в Telegram…"):
                        result = auth.run_async(resolve_one_phone(
                            client, db,
                            contact_id=cid,
                            phone=e164,
                            name=manual_name.strip(),
                            cleanup_imported=True,
                        ))
                elif hinted_username:
                    with st.spinner(f"Ищу @{hinted_username} в Telegram…"):
                        result = auth.run_async(resolve_one_username(
                            client, db, hinted_username,
                        ))
                    inserted = 1 if result["status"] == "resolved" else 0
                else:
                    st.error(
                        "Не смог распознать ни номер, ни @username. "
                        "Примеры: `+12025550101`, `@some_user`, "
                        "`https://t.me/some_user`."
                    )
                    result = None
            except Exception as e:
                st.error(f"Ошибка: {e}")
                result = None

            if result is not None:
                if result["status"] == "resolved":
                    # If user didn't supply a name, fill it from TG profile.
                    display_name = manual_name.strip()
                    tg_fn = (result.get("tg_first_name") or "").strip()
                    tg_ln = (result.get("tg_last_name") or "").strip()
                    full_name = (tg_fn + " " + tg_ln).strip()
                    if not display_name and full_name:
                        db.update_contact(
                            int(result.get("contact_id") or cid),
                            {"name": full_name},
                        )
                        display_name = full_name
                    uname = f"@{result['username']}" if result["username"] else "—"
                    st.toast(
                        f"Нашёл: id={result['tg_user_id']} · {uname}"
                        + (f" · {full_name}" if full_name and not manual_name.strip() else ""),
                        icon="✅",
                    )
                elif result["status"] == "not_on_telegram":
                    st.warning(
                        "Этого адресата нет в Telegram "
                        "(номер/username не найден)."
                    )
                else:
                    st.error(f"Ошибка поиска: {result['error']}")
                st.rerun()

    st.divider()
    st.markdown("### 🔍 Проверить @usernames")
    pending_hints = db.pending_username_validations()
    st.caption(
        "Если в CSV/Sheets у контакта уже указан @username — можно сразу "
        "проверить его в Telegram и получить tg_user_id без импорта номеров. "
        "Выполняется в фоне воркером, прогресс обновляется ниже."
    )

    validate_dispatcher = get_dispatcher()
    recent_validate = [j for j in db.list_jobs(limit=20)
                       if j["type"] == "validate_usernames"]
    running_validate = [j for j in recent_validate
                        if j["status"] in ("queued", "running")]

    vc1, vc2 = st.columns([1, 3])
    with vc1:
        validate_clicked = st.button(
            f"🔍 Проверить ({len(pending_hints)})",
            type="secondary",
            disabled=not pending_hints or not authorized or bool(running_validate),
            key="validate_usernames_btn",
        )
    with vc2:
        if not authorized:
            st.caption("⚠️ Требуется залогиненный TG-аккаунт (шаг 1).")
        elif not pending_hints:
            st.caption("Нечего проверять — нет контактов с @username в статусе pending/error.")
        else:
            st.caption(
                f"Будут проверены {len(pending_hints)} контактов. "
                "Между запросами — пауза ~0.4 сек."
            )

    if validate_clicked:
        try:
            job_id = validate_dispatcher.enqueue_validate_usernames()
            st.toast(f"Задача #{job_id} в очереди.", icon="✅")
            time.sleep(0.4)
            st.rerun()
        except RuntimeError as e:
            st.warning(str(e))
        except Exception as e:
            st.error(f"Упало: {e} — см. шаг 6 (Log).")

    if running_validate:
        st.caption(
            f"🔁 Валидация #{running_validate[0]['id']} в работе — "
            "прогресс и лог на шаге **6 · Log**."
        )

    st.divider()
    st.markdown("### 📋 В базе сейчас")
    df = db.contacts_df_with_campaign_status()
    if df.empty:
        st.info("Контактов пока нет — загрузи CSV или добавь вручную.")
    else:
        # ---- Prep header (metrics + combined button) -------------------
        # Lives in a fragment so the four KPI tiles tick in real time
        # while a Resolve/Validate job is burning through the backlog;
        # idle → no refresh, no CPU. The data editor below stays out of
        # the fragment so its local selection state isn't reset.
        def _any_prep_running() -> bool:
            jobs = db.list_jobs(limit=20)
            prep_types = ("resolve_contacts", "validate_usernames")
            return any(
                j["status"] in ("queued", "running") and j["type"] in prep_types
                for j in jobs
            )
        _refresh = "2s" if _any_prep_running() else None

        @st.fragment(run_every=_refresh)
        def _prep_header():
            counts = db.count_by_resolve_status()
            pending_n = counts.get("pending", 0) + counts.get("error", 0)
            resolved_n = counts.get("resolved", 0)
            not_on_tg = counts.get("not_on_telegram", 0)
            total_n = db.count_contacts()
            workload = db.count_prepare_workload()
            # Upper bound on work the combined button will do. Phone
            # and @handle paths are additive here even though one
            # contact may appear in both — Validate only runs on rows
            # still pending after Resolve, so the real count lands ≤
            # this number. Shown on the button as the "estimated work"
            # figure; not a commitment.
            prepare_n = (
                workload["phone_resolvable"]
                + workload["username_validatable"]
            )

            jobs = db.list_jobs(limit=20)
            running_resolve = [j for j in jobs
                               if j["type"] == "resolve_contacts"
                               and j["status"] in ("queued", "running")]
            running_validate = [j for j in jobs
                                if j["type"] == "validate_usernames"
                                and j["status"] in ("queued", "running")]
            prep_busy = bool(running_resolve or running_validate)

            m1, m2, m3, m4, m5 = st.columns([1, 1, 1, 1, 2])
            m1.metric("Всего", total_n)
            m2.metric("С Telegram", resolved_n)
            m3.metric("Без Telegram", not_on_tg)
            m4.metric("Ждут resolve", pending_n)
            with m5:
                st.caption(" ")
                if st.button(
                    f"🚀 Подготовить к кампании ({prepare_n})",
                    type="primary",
                    disabled=prepare_n == 0 or prep_busy or not authorized,
                    key="contacts_prepare_all",
                    help="Один клик: Resolve по номерам + Validate по "
                         "@username. Контакты без Telegram и с неверным "
                         "@handle НЕ блокируют — они помечаются и "
                         "пропускаются; кампания пойдёт по всем, кто "
                         "резолвнулся. Прогресс — на шаге 6 · Log.",
                ):
                    try:
                        res = get_dispatcher().enqueue_prepare(
                            cleanup_imported=True,
                        )
                        if res.get("skipped"):
                            st.info("Нечего готовить — все контакты уже прошли resolve.")
                        else:
                            bits = []
                            if res.get("resolve_job_id"):
                                bits.append(f"Resolve #{res['resolve_job_id']}")
                            if res.get("validate_job_id"):
                                bits.append(f"Validate #{res['validate_job_id']}")
                            st.toast(" + ".join(bits) + " в очереди.",
                                     icon="🚀")
                        time.sleep(0.4)
                        st.rerun()
                    except RuntimeError as e:
                        st.warning(str(e))
                    except Exception as e:
                        st.error(f"Упало: {e} — см. шаг 6 (Log).")

            # Reachability breakdown — tells the user what WON'T
            # receive a message and why, so the combined button's "we
            # don't block on edge cases" promise is visible, not implicit.
            hints: list[str] = []
            if not_on_tg > 0:
                hints.append(
                    f"📵 Без Telegram: **{not_on_tg}** — номер не привязан к TG, "
                    f"пропустим автоматически."
                )
            err_n = counts.get("error", 0)
            if err_n > 0:
                hints.append(
                    f"⚠️ С ошибкой резолва: **{err_n}** — часто это опечатка "
                    f"в @username. Поправь hint в таблице и жми «Подготовить» "
                    f"ещё раз. Кампанию можно запускать и сейчас — эти "
                    f"будут пропущены."
                )
            if hints:
                st.caption(" · ".join(hints))

            if running_resolve or running_validate:
                live_bits = []
                if running_resolve:
                    live_bits.append(f"🔎 Resolve #{running_resolve[0]['id']}")
                if running_validate:
                    live_bits.append(f"✓ Validate #{running_validate[0]['id']}")
                st.caption(
                    "🔁 " + " · ".join(live_bits)
                    + " в работе — прогресс на шаге **6 · Log**."
                )

        _prep_header()

        st.caption(
            "Редактируемые поля: **name**, **phone**, **tg_user_id**, "
            "**tg_username**, **tg_username_hint** — изменения применяются "
            "по «💾 Сохранить». `campaigns_status` показывает `#id:last_status` "
            "по каждой кампании. Отметь чекбоксы «✅» → Find / Validate / Delete. "
            "Чтобы **добавить новый контакт**, используй секцию «➕ Добавить "
            "контакт вручную» выше."
        )

        # ---- Filter bar (tags + status) — applied before rendering ----
        known_tags_all = db.all_known_tags()
        STATUS_CHOICES = ["(any)", "sent", "error", "skipped", "opted_out", "none"]
        fc1, fc2, fc3 = st.columns([2, 1, 1])
        with fc1:
            filter_tags = st.multiselect(
                "Фильтр по тегам кампании",
                options=known_tags_all,
                default=[],
                key="contacts_filter_tags",
                help="Показать только контакты, которым писали из кампаний с этими тегами.",
            )
        with fc2:
            filter_status = st.selectbox(
                "Статус в send_log",
                options=STATUS_CHOICES,
                index=0,
                key="contacts_filter_status",
            )
        with fc3:
            resolve_choice = st.selectbox(
                "Resolve status",
                options=["(any)", "pending", "resolved", "not_on_telegram", "error"],
                index=0,
                key="contacts_filter_resolve",
            )

        if filter_tags:
            status_arg = None if filter_status == "(any)" else filter_status
            if filter_status == "none":
                # "never touched by these tags" — invert the set
                scope = db.contact_ids_for_campaign_tags(filter_tags)
                df = df[~df["id"].isin(scope)]
            else:
                scope = db.contact_ids_for_campaign_tags(filter_tags, status=status_arg)
                df = df[df["id"].isin(scope)]
        if resolve_choice != "(any)":
            df = df[df["resolve_status"] == resolve_choice]

        st.caption(f"Показано: **{len(df)}** строк.")

        # ---- Saved filters — apply / save --------------------------------
        with st.expander("💾 Сохранённые фильтры / когорты"):
            stored = db.list_saved_filters()
            sf_col1, sf_col2, sf_col3 = st.columns([2, 2, 1])
            with sf_col1:
                chosen = st.selectbox(
                    "Применить",
                    options=[None] + [int(s["id"]) for s in stored],
                    format_func=lambda sid: "— (ничего не выбрано) —" if sid is None
                        else next((f"{s['name']} (#{s['id']})"
                                   for s in stored if int(s["id"]) == sid), f"#{sid}"),
                    key="contacts_saved_filter_pick",
                )
            with sf_col2:
                save_name = st.text_input(
                    "Имя для сохранения текущего",
                    key="contacts_saved_filter_name",
                    placeholder="например, fresh-cold-list",
                )
            with sf_col3:
                if st.button("🗑 Удалить", disabled=chosen is None,
                             key="contacts_saved_filter_del"):
                    db.delete_saved_filter(int(chosen))
                    st.toast(f"Фильтр #{chosen} удалён.", icon="🗑")
                    st.rerun()

            # Build the "current filter" spec from the toolbar above.
            current_spec: dict = {
                "tag_any": filter_tags or [],
                "send_status": None if filter_status == "(any)" else filter_status,
                "resolve_status": None if resolve_choice == "(any)"
                                  else resolve_choice,
                "resolved_only": resolve_choice == "resolved",
                "has_username": False,
                "not_messaged_days": None,
                "exclude_tg_user_ids": [],
            }
            if st.button("💾 Сохранить текущий фильтр",
                         type="primary",
                         disabled=not save_name.strip(),
                         key="contacts_saved_filter_save"):
                sid = db.upsert_saved_filter(save_name.strip(),
                                             json.dumps(current_spec, ensure_ascii=False))
                st.toast(f"Фильтр «{save_name.strip()}» (#{sid}) сохранён.", icon="💾")
                st.rerun()

            if chosen is not None:
                # Preview how many contact ids the saved filter materializes.
                row = db.get_saved_filter(int(chosen))
                if row is not None:
                    try:
                        spec = json.loads(row["filter_json"] or "{}")
                    except Exception:
                        spec = {}
                    preview_ids = db.resolve_filter_to_contact_ids(spec)
                    st.caption(
                        f"Фильтр «{row['name']}» материализует "
                        f"**{len(preview_ids)}** контактов прямо сейчас."
                    )
                    st.code(json.dumps(spec, indent=2, ensure_ascii=False),
                             language="json")

        # Select-all toggling changes the default for the checkbox column
        # and forces the editor to remount via a version key — Streamlit's
        # data_editor otherwise remembers its in-memory state.
        st.session_state.setdefault("_contacts_editor_ver", 0)
        st.session_state.setdefault("_contacts_all_selected", False)

        tcol1, tcol2, _spacer = st.columns([1, 1, 4])
        with tcol1:
            if st.button("☑ Выбрать все", key="contacts_select_all"):
                st.session_state._contacts_all_selected = True
                st.session_state._contacts_editor_ver += 1
                st.rerun()
        with tcol2:
            if st.button("☐ Снять выделение", key="contacts_clear_all"):
                st.session_state._contacts_all_selected = False
                st.session_state._contacts_editor_ver += 1
                st.rerun()

        editable = df.copy()
        editable.insert(0, "✅", st.session_state._contacts_all_selected)
        editor_key = f"contacts_editor_v{st.session_state._contacts_editor_ver}"

        editable_field_cols = {"name", "phone", "tg_user_id",
                               "tg_username", "tg_username_hint"}
        disabled_cols = [
            c for c in editable.columns
            if c != "✅" and c not in editable_field_cols
        ]

        edited = st.data_editor(
            editable,
            use_container_width=True,
            hide_index=True,
            key=editor_key,
            num_rows="fixed",
            column_config={
                "✅": st.column_config.CheckboxColumn(
                    "✅", help="Отметь строки и нажми «Удалить отмеченные»",
                    default=bool(st.session_state._contacts_all_selected),
                ),
                "id": st.column_config.NumberColumn("id", disabled=True),
                "name": st.column_config.TextColumn("name"),
                "phone": st.column_config.TextColumn(
                    "phone", help="E.164 (+…) или tg:<username>",
                ),
                "tg_user_id": st.column_config.NumberColumn(
                    "tg_user_id", format="%d",
                ),
                "tg_username": st.column_config.TextColumn("tg_username"),
                "tg_username_hint": st.column_config.TextColumn("tg_username_hint"),
                "resolve_status": st.column_config.TextColumn(
                    "resolve_status", disabled=True,
                ),
                "resolve_error": st.column_config.TextColumn(
                    "resolve_error", disabled=True,
                ),
                "campaigns_status": st.column_config.TextColumn(
                    "campaigns_status", disabled=True,
                    help="#id:last_status per campaign, joined by ';'",
                ),
            },
            disabled=disabled_cols,
        )
        # Guarantee id column is NOT null so downstream filters are stable.
        # (num_rows="dynamic" mode would have yielded NaN ids for user-added
        # rows — we instead gate new-contact creation through the dedicated
        # "➕ Добавить вручную" section above so the checkbox/save paths
        # stay unambiguous.)

        # Split edited frame into:
        #   - existing rows (id preserved) → diff for updates
        #   - new rows (id blank/NaN from user-added row via data_editor) →
        #     upsert after normalizing phone / username
        diff_changes: list[tuple[int, dict]] = []
        new_rows_spec: list[dict] = []
        for _, row_after in edited.iterrows():
            raw_id = row_after.get("id")
            if pd.isna(raw_id) or str(raw_id).strip() in ("", "None"):
                # New row: require either phone or username
                phone_raw = "" if pd.isna(row_after.get("phone")) else str(row_after.get("phone")).strip()
                uname_raw = "" if pd.isna(row_after.get("tg_username_hint")) else str(row_after.get("tg_username_hint")).strip()
                name_raw = "" if pd.isna(row_after.get("name")) else str(row_after.get("name")).strip()
                tgu_raw = "" if pd.isna(row_after.get("tg_username")) else str(row_after.get("tg_username")).strip()
                if not phone_raw and not uname_raw and not tgu_raw:
                    continue
                new_rows_spec.append({
                    "phone": phone_raw,
                    "name": name_raw,
                    "tg_username_hint": uname_raw or tgu_raw,
                })
                continue
            cid = int(raw_id)
            row_before_match = editable.loc[editable["id"] == cid]
            if row_before_match.empty:
                continue
            row_before = row_before_match.iloc[0]
            changed = {}
            for col in editable_field_cols:
                if col not in row_after.index:
                    continue
                bv, av = row_before[col], row_after[col]
                # Treat NaN / None / "" as equal to avoid false positives.
                bv_norm = "" if pd.isna(bv) else str(bv)
                av_norm = "" if pd.isna(av) else str(av)
                if bv_norm != av_norm:
                    if col == "tg_user_id":
                        changed[col] = int(av) if av_norm and av_norm != "None" else None
                    else:
                        changed[col] = av_norm
            if changed:
                diff_changes.append((cid, changed))

        # Existing-row ids only for selection / action-eligibility.
        existing_rows = edited[edited["id"].apply(lambda v: not pd.isna(v))]
        selected_ids = [int(x) for x in
                        existing_rows.loc[existing_rows["✅"], "id"].tolist()]

        # Which of the selected rows are eligible for each action?
        # - phone-resolve: pending contacts with a real phone (not 'tg:…')
        # - username-validate: pending contacts that have a hint set
        edited_by_id = {int(r["id"]): r for _, r in existing_rows.iterrows()}
        # Both pending AND error rows are retryable — a previous failure
        # might have been FloodWait, a typo in the hint, or a stale number.
        _retryable_states = {"pending", "error"}
        resolve_eligible = [
            cid for cid in selected_ids
            if (row := edited_by_id.get(cid)) is not None
            and str(row.get("resolve_status")) in _retryable_states
            and not str(row.get("phone", "")).startswith("tg:")
        ]
        validate_eligible = [
            cid for cid in selected_ids
            if (row := edited_by_id.get(cid)) is not None
            and str(row.get("resolve_status")) in _retryable_states
            and str(row.get("tg_username_hint") or "").strip() not in ("", "None")
        ]

        contacts_dispatcher = get_dispatcher()
        recent_resolve = [j for j in db.list_jobs(limit=20)
                          if j["type"] == "resolve_contacts"]
        running_resolve = [j for j in recent_resolve
                           if j["status"] in ("queued", "running")]
        recent_validate = [j for j in db.list_jobs(limit=20)
                           if j["type"] == "validate_usernames"]
        running_validate = [j for j in recent_validate
                            if j["status"] in ("queued", "running")]

        bcol1, bcol2, bcol3, bcol4 = st.columns([1, 1, 1, 1])
        total_save = len(diff_changes) + len(new_rows_spec)
        with bcol1:
            if st.button(
                f"💾 Сохранить ({total_save})",
                type="primary",
                disabled=total_save == 0,
                key="contacts_save",
                help="Обновит отредактированные строки и вставит новые "
                     "(если дописать пустую строку внизу таблицы).",
            ):
                ok_upd, ok_new, failed = 0, 0, []
                # 1) Updates
                for cid, fields in diff_changes:
                    try:
                        if db.update_contact(cid, fields):
                            ok_upd += 1
                    except Exception as e:
                        failed.append((f"update id={cid}", str(e)))
                # 2) Inserts — normalize via the same pipeline as manual-add
                for spec in new_rows_spec:
                    raw_phone = spec["phone"]
                    hint = csv_io._normalize_username(spec.get("tg_username_hint"))
                    e164 = csv_io.normalize_phone(raw_phone, default_region="UZ") if raw_phone else None
                    if e164:
                        rec = {
                            "phone": e164, "name": spec["name"],
                            "raw_phone": raw_phone,
                            "tg_username_hint": hint,
                        }
                    elif hint:
                        rec = {
                            "phone": f"tg:{hint.lower()}",
                            "name": spec["name"] or hint,
                            "raw_phone": raw_phone or f"@{hint}",
                            "tg_username_hint": hint,
                        }
                    else:
                        failed.append(
                            (f"new row {raw_phone or '?'}",
                             "ни валидный phone, ни username не распознаны"),
                        )
                        continue
                    try:
                        inserted = db.upsert_contacts([rec])
                        ok_new += inserted
                        if inserted == 0:
                            # phone existed already — treat as update
                            pass
                    except Exception as e:
                        failed.append((f"new {rec['phone']}", str(e)))

                if ok_upd or ok_new:
                    st.toast(
                        f"Сохранено: обновлено {ok_upd}, добавлено {ok_new}.",
                        icon="💾",
                    )
                if failed:
                    st.error(
                        f"Часть операций не прошла ({len(failed)}). "
                        "Обычная причина — дубликат phone или пустое поле."
                    )
                    with st.expander("Детали ошибок сохранения"):
                        st.code("\n".join(f"{k}: {v}" for k, v in failed),
                                language="text")
                st.session_state._contacts_editor_ver += 1
                st.rerun()
        with bcol2:
            if st.button(
                f"🔍 Найти в TG ({len(resolve_eligible)})",
                type="secondary",
                disabled=not resolve_eligible or bool(running_resolve) or not authorized,
                key="contacts_resolve_selected",
                help="Запускает ImportContacts по отмеченным pending-номерам.",
            ):
                try:
                    jid = contacts_dispatcher.enqueue_resolve(
                        cleanup_imported=True, ids=resolve_eligible,
                    )
                    st.toast(f"Resolve #{jid}: {len(resolve_eligible)} id в очереди.",
                             icon="🔍")
                    time.sleep(0.4)
                    st.rerun()
                except RuntimeError as e:
                    st.warning(str(e))
                except Exception as e:
                    st.error(f"Упало: {e} — см. лог задачи ниже.")
        with bcol3:
            if st.button(
                f"✓ Валидировать @usernames ({len(validate_eligible)})",
                type="secondary",
                disabled=not validate_eligible or bool(running_validate) or not authorized,
                key="contacts_validate_selected",
                help="Проверяет @username-подсказки отмеченных строк через get_entity.",
            ):
                try:
                    jid = contacts_dispatcher.enqueue_validate_usernames(
                        ids=validate_eligible,
                    )
                    st.toast(
                        f"Validate #{jid}: {len(validate_eligible)} id в очереди.",
                        icon="✅",
                    )
                    time.sleep(0.4)
                    st.rerun()
                except RuntimeError as e:
                    st.warning(str(e))
                except Exception as e:
                    st.error(f"Упало: {e} — см. лог задачи ниже.")
        with bcol4:
            if st.button(
                f"🗑 Удалить отмеченные ({len(selected_ids)})",
                type="secondary",
                disabled=not selected_ids,
                key="delete_checked",
            ):
                n = db.delete_contacts(selected_ids)
                st.toast(
                    f"Удалено {n} контактов (journal rows тоже).",
                    icon="🗑",
                )
                st.session_state._contacts_all_selected = False
                st.session_state._contacts_editor_ver += 1
                st.rerun()

        if diff_changes:
            st.caption(f"Изменения в id: {', '.join(str(c) for c, _ in diff_changes)}")

        # Phase R — manual read-receipt check, adaptive scope.
        #   no selection  → every sent row across every campaign
        #   selection     → only the selected contacts, all their campaigns
        recent_readcheck = [j for j in db.list_jobs(limit=20)
                            if j["type"] == "check_read_receipts"]
        running_readcheck = [j for j in recent_readcheck
                             if j["status"] in ("queued", "running")]
        rc_col1, rc_col2 = st.columns([1, 3])
        with rc_col1:
            if selected_ids:
                rc_label = f"🔍 Прочтения у выбранных ({len(selected_ids)})"
                rc_scope_contacts = selected_ids
            else:
                rc_label = "🔍 Прочтения по всем кампаниям"
                rc_scope_contacts = None
            if st.button(
                rc_label,
                type="secondary",
                disabled=bool(running_readcheck) or not authorized,
                key="contacts_read_check",
                help="Опрашивает Telegram о статусе прочтения и проставляет "
                     "«Прочитано» в журнале (шаг 6 · Log). Опрос — readonly, "
                     "без расхода лимита рассылки.",
            ):
                try:
                    jid = contacts_dispatcher.enqueue_read_check(
                        campaign_ids=None, contact_ids=rc_scope_contacts,
                    )
                    st.toast(f"Read-check #{jid} в очереди.", icon="🔍")
                    time.sleep(0.4)
                    st.rerun()
                except RuntimeError as e:
                    st.warning(str(e))
                except Exception as e:
                    st.error(f"Упало: {e} — см. шаг 6 (Log).")
        with rc_col2:
            if running_readcheck:
                st.caption(
                    f"🔁 Read-check #{running_readcheck[0]['id']} в работе — "
                    "прогресс и логи на шаге **6 · Log**."
                )

        # Slim status hint — полная картинка с событиями живёт в шаге 6 · Log.
        live_msgs = []
        if running_resolve:
            live_msgs.append(
                f"🔎 Resolve #{running_resolve[0]['id']} в работе"
            )
        if running_validate:
            live_msgs.append(
                f"✓ Validate #{running_validate[0]['id']} в работе"
            )
        if live_msgs:
            st.caption(" · ".join(live_msgs) + " · прогресс и логи — на шаге **6 · Log**.")

        # Purge section — guarded with an explicit confirm checkbox.
        with st.expander("⚠️ Опасная зона"):
            confirm = st.checkbox(
                "Я понимаю — удалить ВСЕ контакты и все связанные записи "
                "в журнале отправок.",
                key="contacts_purge_confirm",
            )
            if st.button(
                "🗑💥 Очистить всю базу контактов",
                type="secondary",
                disabled=not confirm,
                key="contacts_purge_btn",
            ):
                n = db.purge_contacts()
                st.toast(f"База очищена: {n} контактов удалено.", icon="💥")
                st.session_state._contacts_purge_confirm = False
                st.session_state._contacts_all_selected = False
                st.session_state._contacts_editor_ver += 1
                st.rerun()




# --------------------------------------------------------------------------
# STEP 4 — Compose
# --------------------------------------------------------------------------

# --- Device preview (iPhone / iPad / Android / Desktop / Web) -------------
#
# Each target client renders Telegram bubbles differently in ways that
# matter for a composer: font stack (SF Pro on iOS, Roboto on Android,
# Segoe UI on Windows desktop, system on web), bubble radius, link
# color. The preview wraps the same HTML in five per-device containers
# so the user can tab-flip between them. System font stacks only — no
# webfont download, no license headache, still visually accurate.

_PREVIEW_CSS = """
<style>
.tg-preview {
  padding: 6px 6px 12px 6px; margin: 4px 0;
  box-shadow: 0 1px 2px rgba(0,0,0,.06);
  max-width: 520px;
  line-height: 1.45;
  word-wrap: break-word; overflow-wrap: break-word;
  overflow: hidden;   /* clip the image to the bubble's rounded corners */
}
.tg-preview .tg-body { padding: 10px 12px 4px 12px; }
.tg-preview .tg-img {
  display: block; width: 100%;
  max-height: 320px; object-fit: cover;
  border-radius: 10px 10px 4px 10px;   /* match bubble tail */
}
.tg-preview .tg-time {
  display: block; text-align: right; font-size: 11px; opacity: .55;
  margin-top: 6px; padding: 0 12px;
}
.tg-preview a { text-decoration: none; }
.tg-preview a:hover { text-decoration: underline; }
.tg-preview code {
  background: rgba(0,0,0,.05); padding: 1px 5px; border-radius: 4px;
  font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
  font-size: 0.95em;
}
.tg-preview pre {
  background: rgba(0,0,0,.05); padding: 10px 12px; border-radius: 8px;
  overflow-x: auto; font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
  font-size: 0.9em;
}
.tg-preview blockquote {
  margin: 6px 0; padding: 4px 10px;
  border-left: 3px solid #3390EC; background: rgba(51,144,236,.08);
  border-radius: 4px;
}

/* iPhone: SF Pro, rounded bubble, outgoing-green tint, iOS link blue. */
.tg-preview.ios {
  font-family: -apple-system, BlinkMacSystemFont, "SF Pro Text", system-ui, sans-serif;
  font-size: 17px;
  background: #E1FFC7; /* iOS Telegram outgoing message green */
  color: #000;
  border-radius: 18px 18px 4px 18px;
}
.tg-preview.ios a { color: #3390EC; }

/* iPad: same as iOS but wider and slightly larger type; Telegram's
   iPad build otherwise matches the iPhone bubble language exactly. */
.tg-preview.ipad {
  font-family: -apple-system, BlinkMacSystemFont, "SF Pro Text", system-ui, sans-serif;
  font-size: 18px;
  background: #E1FFC7;
  color: #000;
  border-radius: 20px 20px 4px 20px;
  max-width: 640px;
}
.tg-preview.ipad a { color: #3390EC; }

/* Android: Material-ish bubble, Roboto fallback, slightly squarer
   corner on the tail side, brighter link blue. */
.tg-preview.android {
  font-family: Roboto, "Helvetica Neue", Arial, sans-serif;
  font-size: 16px;
  background: #EFFDDE; /* Android TG outgoing tint */
  color: #000;
  border-radius: 16px 16px 4px 16px;
}
.tg-preview.android a { color: #3E88F7; }

/* Desktop (Qt, Windows/macOS/Linux native build): squarer bubble,
   Segoe UI on Windows, system fallback on Mac/Linux. Link color
   is Telegram Desktop's distinctive deeper blue. */
.tg-preview.desktop {
  font-family: "Segoe UI", Tahoma, Geneva, Verdana, sans-serif;
  font-size: 15px;
  background: #EEFFDE;
  color: #0C0C0E;
  border-radius: 10px 10px 2px 10px;
}
.tg-preview.desktop a { color: #2481CC; }

/* Web (Telegram Web A / K): system-ui, slightly tighter bubble,
   link color #168ACD. */
.tg-preview.web {
  font-family: system-ui, -apple-system, Roboto, sans-serif;
  font-size: 15px;
  background: #EEFFDE;
  color: #000;
  border-radius: 12px 12px 2px 12px;
}
.tg-preview.web a { color: #168ACD; }
</style>
"""


@st.cache_data(show_spinner=False, max_entries=4)
def _image_to_data_url(path_str: str) -> str | None:
    """Read an uploaded image and return a `data:image/...;base64,...`
    URL suitable for embedding in the preview HTML.

    Cached by path — re-encoding on every fragment rerun would be
    wasteful for marketing images (200–500 KB is typical). Cache holds
    at most 4 images so an iterating user doesn't balloon memory.
    """
    from pathlib import Path as _Path
    import base64 as _b64
    p = _Path(path_str)
    if not p.exists():
        return None
    ext = p.suffix.lower().lstrip(".")
    if ext in ("jpg", "jpe"):
        ext = "jpeg"
    mime = f"image/{ext}"
    try:
        data = _b64.b64encode(p.read_bytes()).decode("ascii")
    except OSError:
        return None
    return f"data:{mime};base64,{data}"


def _render_device_preview_tabs(html: str, image_path: str | None = None) -> None:
    """Render `html` (already-converted Telegram MD) inside five
    per-device styled bubbles, switchable via `st.tabs`. Optional
    `image_path` is embedded as a base64 data URL inside each bubble
    above the text — matching how Telegram clients render a photo
    message with a caption.

    All five tab DOMs render simultaneously (Streamlit's tabs hide the
    inactive ones via CSS). Embedding the same data URL five times
    duplicates the string but browsers deduplicate the decode.
    """
    st.markdown(_PREVIEW_CSS, unsafe_allow_html=True)
    tabs = st.tabs(["📱 iPhone", "📱 iPad", "🤖 Android",
                    "💻 Desktop", "🌐 Web"])
    classes = ["ios", "ipad", "android", "desktop", "web"]
    img_data_url = _image_to_data_url(image_path) if image_path else None
    img_html = (
        f'<img class="tg-img" src="{img_data_url}" alt="">'
        if img_data_url else ""
    )
    time_chip = '<span class="tg-time">now ✓✓</span>'
    for tab, cls in zip(tabs, classes):
        with tab:
            st.markdown(
                f'<div class="tg-preview {cls}">'
                f'{img_html}'
                f'<div class="tg-body">{html}</div>'
                f'{time_chip}'
                f'</div>',
                unsafe_allow_html=True,
            )


def _compose_load_campaign_into_state(db, cid: int) -> None:
    """Load a campaign's editable fields into the Compose widget-state
    keys. Called before the widgets render so they pick up the values
    on this rerun.

    Sets the widget session keys directly (Streamlit's documented
    programmatic-default pattern). Leaves staging keys in place so the
    editor fragment's text_area picks them up on its first render."""
    row = db.get_campaign(int(cid))
    if row is None:
        return
    st.session_state["compose_template_area"] = row["template"] or ""
    st.session_state["compose_name_input"] = row["name"] or ""
    st.session_state["compose_group_link"] = row["group_link"] or ""
    # Tags — widen the multiselect's option set so stored tags that
    # aren't in `all_known_tags()` anymore still appear selected.
    st.session_state["_compose_loaded_tags"] = list(db.get_campaign_tags(int(cid)))
    st.session_state["compose_tags"] = list(st.session_state["_compose_loaded_tags"])
    st.session_state["_compose_loaded_image_path"] = row["image_path"] or ""
    # Clear any in-session upload so the loaded campaign's image wins
    # until the user explicitly uploads a new one.
    st.session_state.pop("_compose_current_image_path", None)


def _compose_reset_state() -> None:
    """Wipe Compose's session keys so the next render starts fresh.
    Called after Save/Clone/Delete so the user sees an empty form for
    their next action instead of stale values."""
    for k in (
        "compose_template_area", "compose_name_input", "compose_group_link",
        "compose_tags", "compose_new_tag",
        "_compose_loaded_tags", "_compose_loaded_image_path",
        "_compose_current_image_path",
        "compose_vars_samples", "compose_last_loaded_cid",
        "compose_selected_cid",
    ):
        st.session_state.pop(k, None)


def _compose_insert_snippet(snippet: str) -> None:
    """on_click handler for toolbar buttons. Writes to a pending key so
    the editor fragment copies it into the text_area BEFORE rendering.
    This removes the triple-rerun pattern the old three-button toolbar
    had (each button used to call st.rerun() explicitly)."""
    current = st.session_state.get("compose_template_area", "")
    st.session_state["_compose_template_pending"] = current + snippet


# Curated set of Unicode emoji for the picker. Intentionally NOT using
# Telegram Premium custom emoji (those require a Premium subscription on
# the recipient's side and render as the fallback emoji anyway for most
# recipients). Grouped by theme; each group is a tab in the picker.
_EMOJI_GROUPS: dict[str, list[str]] = {
    "Лица": [
        "😀", "😁", "😂", "🤣", "😊", "😉", "😍", "😘", "😎", "🤩",
        "🤔", "😏", "😐", "😴", "🤗", "🤫", "🤭", "🫠", "🙃", "😇",
        "😌", "😢", "😭", "😤", "😡", "🥺", "😱", "🤯", "🥳", "😬",
    ],
    "Жесты": [
        "👍", "👎", "👏", "🙌", "🙏", "👋", "🤝", "✌️", "🤞", "🤟",
        "🤘", "🫶", "🫰", "🫡", "💪", "🫵", "👆", "👇", "👈", "👉",
    ],
    "Символы": [
        "❤️", "🧡", "💛", "💚", "💙", "💜", "🖤", "🤍", "💔", "❣️",
        "💯", "🔥", "✨", "⭐", "🌟", "⚡", "💥", "💫", "🎉", "🎊",
        "✅", "❌", "⚠️", "🆕", "🆓", "🚀", "🔔", "🔕", "📣", "📢",
    ],
    "Объекты": [
        "📩", "✉️", "📨", "📬", "📮", "📝", "📋", "📎", "🔗", "📅",
        "🗓", "⏰", "⏳", "🎁", "🎈", "🏆", "🥇", "💼", "💡", "🔑",
    ],
    "Еда/Напитки": [
        "☕", "🍵", "🍺", "🍷", "🥂", "🍰", "🍕", "🍔", "🍣", "🍎",
        "🍓", "🥐", "🍪", "🍫", "🧁", "🍩", "🍦", "🍿", "🍞", "🥗",
    ],
}


def _insert_emoji(emoji: str) -> None:
    """Callback — emoji insert uses the same staging key as the
    toolbar so typing + emoji-clicks don't race each other."""
    current = st.session_state.get("compose_template_area", "")
    st.session_state["_compose_template_pending"] = current + emoji


def _render_emoji_picker(disabled: bool) -> None:
    """Compact Unicode emoji grid, grouped into themed tabs.

    No Premium — every emoji here is standard Unicode, renders for
    every recipient regardless of their Telegram tier.
    """
    with st.expander("😊 Эмодзи", expanded=False):
        if disabled:
            st.caption("🚫 Заблокировано — кампания running.")
            return
        tab_labels = list(_EMOJI_GROUPS.keys())
        tabs = st.tabs(tab_labels)
        for tab, label in zip(tabs, tab_labels):
            with tab:
                emojis = _EMOJI_GROUPS[label]
                # 10 per row for a compact grid.
                per_row = 10
                for row_start in range(0, len(emojis), per_row):
                    cols = st.columns(per_row)
                    for ci, ch in enumerate(emojis[row_start:row_start + per_row]):
                        with cols[ci]:
                            st.button(
                                ch,
                                key=f"emoji_{label}_{row_start + ci}",
                                on_click=_insert_emoji,
                                args=(ch,),
                                help=f"Вставить {ch}",
                                use_container_width=True,
                            )


@st.cache_data(ttl=30, show_spinner=False)
def _cached_known_tags(_db_path: str) -> list[str]:
    """Cached wrapper around db.all_known_tags(). Compose page called
    this on every keystroke before the fragment refactor — cache + 30 s
    TTL means ≤ 1 query per half-minute no matter how fast the user types."""
    return list(get_db().all_known_tags())


@st.cache_data(ttl=30, show_spinner=False)
def _cached_preview_sample(_db_path: str) -> dict:
    """Cached first-resolved-contact for the live preview. Rotating it
    every 30 s is fine; the preview is illustrative, not transactional."""
    rows = get_db().resolved_contacts()
    return dict(rows[0]) if rows else {}


def render_compose():
    st.markdown(
        '<div class="hero"><h1>Составь сообщение</h1>'
        '<p>Markdown поддерживается. Используй <code>{name}</code>, '
        '<code>{first_name}</code>, <code>{group_link}</code> и любые '
        'колонки из CSV как плейсхолдеры.</p></div>',
        unsafe_allow_html=True,
    )
    st.markdown('<div class="step-chip">ШАГ 4 · MESSAGE</div>', unsafe_allow_html=True)

    default_tmpl = (
        "Привет, {first_name}! 👋\n\n"
        "Ты зарегистрировался на наш митап — спасибо!\n\n"
        "Скидываю ссылку на закрытую группу для координации: {group_link}\n\n"
        "До встречи!"
    )

    # -------- Mode selector + campaign picker -------------------------
    campaigns_all = list(db.list_campaigns())
    mode_labels = {
        "new": "🆕 Новая",
        "edit": "✏️ Редактировать",
        "clone": "📋 Клонировать",
    }
    if not campaigns_all:
        # Nothing to edit/clone yet; force new-mode without showing the selector.
        st.session_state["compose_mode"] = "new"
    mode = st.segmented_control(
        "Режим",
        options=list(mode_labels.keys()),
        format_func=lambda k: mode_labels[k],
        default=st.session_state.get("compose_mode", "new"),
        key="compose_mode",
        label_visibility="collapsed",
    )
    if mode is None:
        mode = "new"

    selected_cid: int | None = None
    selected_status: str = "draft"
    if mode in ("edit", "clone") and campaigns_all:
        def _label(c) -> str:
            return f"#{c['id']} · {c['name']} · {c['status']}"
        selected_cid = st.selectbox(
            "Кампания",
            options=[c["id"] for c in campaigns_all],
            format_func=lambda i: _label(
                next(c for c in campaigns_all if c["id"] == i)
            ),
            key="compose_selected_cid",
        )
        selected_row = next(c for c in campaigns_all if c["id"] == selected_cid)
        selected_status = selected_row["status"] or "draft"
        # Auto-load: if the chosen campaign changed since last render,
        # refresh the widget-state keys so the editor/name/link reflect it.
        if st.session_state.get("compose_last_loaded_cid") != (selected_cid, mode):
            _compose_load_campaign_into_state(db, int(selected_cid))
            st.session_state["compose_last_loaded_cid"] = (selected_cid, mode)
    else:
        # Leaving edit/clone mode → drop the per-campaign staging.
        st.session_state.pop("compose_last_loaded_cid", None)
        # Seed the default template for a truly fresh Compose session.
        st.session_state.setdefault("compose_template_area", default_tmpl)
        st.session_state.setdefault("compose_name_input", "Meetup invite")
        st.session_state.setdefault("compose_group_link", "")

    # -------- Running-state edit lock ---------------------------------
    is_running = (mode == "edit" and selected_status == "running")
    body_locked = is_running
    if is_running:
        st.warning(
            f"🚨 Кампания #{selected_cid} сейчас **running** — тело, "
            "ссылка и картинка заблокированы. Можно менять имя и теги, "
            "или нажать «📋 Клонировать» чтобы сделать новый черновик."
        )

    # -------- Editor + preview (fragment — isolates typing reruns) ----
    @st.fragment
    def _editor_and_preview():
        # Staging handshake: toolbar callbacks write to
        # `_compose_template_pending`; copy it into the widget key
        # BEFORE the text_area declares itself.
        if "_compose_template_pending" in st.session_state:
            st.session_state["compose_template_area"] = (
                st.session_state.pop("_compose_template_pending")
            )

        st.caption(
            "**Форматирование** (Telethon Markdown V1): "
            "`**жирный**`, `__курсив__`, `` `код` ``, `[текст](url)`. "
            "Кнопки добавляют сниппет в конец — поправь текст вручную."
        )

        tb = st.columns([1, 1, 1, 1, 6])
        with tb[0]:
            st.button("**B**", key="fmt_bold",
                      on_click=_compose_insert_snippet,
                      args=("**жирный_текст**",),
                      disabled=body_locked,
                      help="Вставить **жирный**")
        with tb[1]:
            st.button("*I*", key="fmt_italic",
                      on_click=_compose_insert_snippet,
                      args=("__курсивный_текст__",),
                      disabled=body_locked,
                      help="Вставить __курсивный__")
        with tb[2]:
            st.button("` `", key="fmt_code",
                      on_click=_compose_insert_snippet,
                      args=("`код`",),
                      disabled=body_locked,
                      help="Вставить `код`")
        with tb[3]:
            st.button("🔗", key="fmt_link",
                      on_click=_compose_insert_snippet,
                      args=("[название](https://example.com)",),
                      disabled=body_locked,
                      help="Вставить ссылку")

        template_text = st.text_area(
            "Шаблон сообщения (Markdown)",
            height=220,
            key="compose_template_area",
            disabled=body_locked,
        )

        # Char counter — Telegram's DM text cap is 4096.
        n = len(template_text or "")
        tone = "🔴" if n > 4096 else ("🟡" if n > 3500 else "🟢")
        st.caption(f"{tone} **{n}** / 4096 символов")

        _render_emoji_picker(disabled=body_locked)

        # Image upload lives inside the fragment so an upload only
        # re-renders the preview (not the whole page). Any upload is
        # persisted to `uploads/` under its original name — same
        # destination as the previous outside-fragment flow. The
        # current image path is cached in session_state so the outer
        # save/clone handlers can read it at submit time.
        image_file = st.file_uploader(
            "🖼 Картинка (появится в пузыре сверху, как в Telegram)",
            type=["jpg", "jpeg", "png", "webp"],
            key="compose_image_uploader",
            disabled=body_locked,
        )
        if image_file is not None:
            _img_path = UPLOADS_DIR / image_file.name
            _img_path.write_bytes(image_file.getbuffer())
            st.session_state["_compose_current_image_path"] = str(_img_path)
        current_image_path: str | None = (
            st.session_state.get("_compose_current_image_path")
            or (st.session_state.get("_compose_loaded_image_path")
                if mode in ("edit", "clone") else None)
            or None
        )
        if current_image_path and Path(current_image_path).exists():
            clear_col, _ = st.columns([1, 4])
            with clear_col:
                if st.button("🗑 Убрать картинку", key="compose_img_clear"):
                    st.session_state["_compose_current_image_path"] = ""
                    st.session_state["_compose_loaded_image_path"] = ""
                    st.rerun()

        # Live preview. Telethon MD → HTML, rendered once then re-used
        # in 5 device tabs (iPhone / iPad / Android / Desktop / Web) so
        # the user sees how Telegram's actual clients differ in font,
        # bubble shape, link color. All tabs' DOM exists simultaneously
        # — switching is pure CSS, no re-rerun.
        st.markdown("### 👁 Предпросмотр")
        used_phs = template_mod.placeholders(template_text or "")
        sample_row = _cached_preview_sample(str(db.path))
        preview_row = dict(sample_row)
        preview_row.update({
            k: v for k, v in
            st.session_state.get("compose_vars_samples", {}).items() if v
        })
        rendered = template_mod.render(template_text or "", preview_row)
        rendered = rendered.replace(
            "{group_link}",
            st.session_state.get("compose_group_link", "")
            or preview_row.get("group_link", "")
            or "[ссылка будет здесь]",
        )
        rendered_html = telegram_md_to_html(rendered)
        _render_device_preview_tabs(
            rendered_html,
            image_path=current_image_path if current_image_path else None,
        )
        if used_phs:
            st.caption(
                "Плейсхолдеры в шаблоне: "
                + ", ".join("{" + u + "}" for u in used_phs)
            )

    _editor_and_preview()

    # -------- Metadata (outside fragment — edited less often) ---------
    colL, colR = st.columns([2, 1])
    with colL:
        campaign_name = st.text_input(
            "Название кампании",
            key="compose_name_input",
        )
        group_link = st.text_input(
            "Ссылка на закрытую группу (t.me/...)",
            placeholder="https://t.me/+abcdef123456",
            key="compose_group_link",
            disabled=body_locked,
        )
        known_tags = _cached_known_tags(str(db.path))
        # Widen options to include any tags already attached to the
        # loaded campaign that aren't in the global tag set anymore.
        loaded_tags = st.session_state.get("_compose_loaded_tags", [])
        tag_options = list(dict.fromkeys([*known_tags, *loaded_tags]))
        tag_col, new_col = st.columns([3, 1])
        with tag_col:
            selected_tags = st.multiselect(
                "Теги (категории кампании)",
                options=tag_options,
                key="compose_tags",
                help="Выбери из существующих. Чтобы создать новый — "
                     "впиши его в поле справа и жми Save (или Enter).",
            )
        with new_col:
            new_tag_text = st.text_input(
                "Новый тег",
                key="compose_new_tag",
                placeholder="workshop-may",
                help="Создастся при Save-е и добавится к кампании.",
            )
        if not tag_options and not (new_tag_text or "").strip():
            st.caption(
                "Тегов ещё нет — впиши название справа. Первый Save создаст его."
            )
    with colR:
        # Image UI moved INSIDE the editor fragment above — it shows
        # inside the device-preview bubble where Telegram would put it.
        # This column is now just a visual filler so the tag section
        # doesn't stretch to full width.
        st.empty()

    # Image path feeds the save/clone handlers. Priority:
    # 1. New upload in this session → `_compose_current_image_path`.
    # 2. Pre-existing path from the loaded campaign (edit/clone mode).
    # Explicitly cleared via the "Убрать картинку" button → empty str.
    _new_img = st.session_state.get("_compose_current_image_path")
    if _new_img == "":
        image_path: Path | None = None
    elif _new_img:
        image_path = Path(_new_img)
    elif mode in ("edit", "clone") and st.session_state.get("_compose_loaded_image_path"):
        image_path = Path(st.session_state["_compose_loaded_image_path"])
    else:
        image_path = None

    # -------- Advanced (variables editor) ------------------------------
    with st.expander("🧩 Переменные (для предпросмотра)", expanded=False):
        template_text = st.session_state.get("compose_template_area", "") or ""
        used = template_mod.placeholders(template_text)
        sample_row = _cached_preview_sample(str(db.path))
        vars_state = st.session_state.setdefault("compose_vars_samples", {})
        if used:
            new_state = {}
            for ph in used:
                new_state[ph] = (
                    vars_state.get(ph)
                    or ("" if ph == "group_link" else str(sample_row.get(ph, "")))
                )
            st.session_state.compose_vars_samples = new_state
            vars_df = pd.DataFrame(
                [{"placeholder": ph, "sample": v}
                 for ph, v in new_state.items()]
            )
            edited_vars = st.data_editor(
                vars_df,
                use_container_width=True,
                hide_index=True,
                num_rows="fixed",
                key="compose_vars_editor",
                column_config={
                    "placeholder": st.column_config.TextColumn("placeholder", disabled=True),
                    "sample": st.column_config.TextColumn(
                        "sample value",
                        help="Подставится в предпросмотр. Не влияет на реальную отправку.",
                    ),
                },
            )
            st.session_state.compose_vars_samples = {
                str(r["placeholder"]): str(r["sample"])
                for _, r in edited_vars.iterrows()
            }
        else:
            st.caption(
                "В шаблоне нет `{placeholder}`-подстановок — переменные не нужны."
            )

    # -------- Action row (save / clone / delete) ----------------------
    st.divider()
    template_text = st.session_state.get("compose_template_area", "") or ""

    def _sanitized_tags() -> list[str]:
        """Merge the multiselect's chosen tags with the inline 'new tag'
        input. Case-insensitive dedup so 'Workshop' and 'workshop'
        don't split into two entries."""
        merged: list[str] = []
        seen: set[str] = set()
        inline_new = (st.session_state.get("compose_new_tag") or "").strip()
        for t in list(selected_tags or []) + ([inline_new] if inline_new else []):
            t = (t or "").strip()
            if not t or t.lower() in seen:
                continue
            seen.add(t.lower())
            merged.append(t)
        return merged

    def _current_image_path() -> str | None:
        if image_path is not None:
            return str(image_path)
        if mode in ("edit", "clone"):
            return st.session_state.get("_compose_loaded_image_path") or None
        return None

    can_delete = (
        mode == "edit" and selected_cid is not None
        and selected_status in ("draft", "stopped", "done", "error")
    )

    ac1, ac2, ac3, _spacer = st.columns([2, 2, 2, 3])

    with ac1:
        if mode == "new":
            save_clicked = st.button("💾 Сохранить как новую",
                                     type="primary",
                                     key="compose_save_new")
        else:
            save_clicked = st.button("💾 Сохранить изменения",
                                     type="primary",
                                     key="compose_save_edit",
                                     disabled=(selected_cid is None))
        if save_clicked:
            if not template_text.strip():
                st.error("Шаблон пустой.")
            elif mode == "new":
                cid = db.create_campaign(
                    (campaign_name or "Untitled"),
                    template_text,
                    _current_image_path(),
                    (group_link or None),
                    account_id=(get_settings().id if hasattr(get_settings(), "id") else None),
                    tags=_sanitized_tags(),
                )
                st.session_state.campaign_id = cid
                _cached_known_tags.clear()
                st.toast(f"Кампания #{cid} создана.", icon="💾")
                _compose_reset_state()
                st.rerun()
            else:
                assert selected_cid is not None
                fields: dict = {"name": campaign_name or "Untitled"}
                if not body_locked:
                    fields["template"] = template_text
                    fields["group_link"] = (group_link or None)
                    fields["image_path"] = _current_image_path()
                db.update_campaign(int(selected_cid), **fields)
                db.set_campaign_tags(int(selected_cid), _sanitized_tags())
                # Bump the caches the sidebar / list reads from.
                _cached_known_tags.clear()
                st.toast(f"Кампания #{selected_cid} обновлена.", icon="💾")
                st.rerun()

    with ac2:
        if st.button(
            "📋 Клонировать",
            key="compose_clone",
            disabled=(mode == "new" and selected_cid is None),
            help="Создаёт новый draft с этим же содержимым — полезно "
                 "чтобы не ломать уже отправленную кампанию.",
        ):
            if not template_text.strip():
                st.error("Шаблон пустой — клонировать нечего.")
            else:
                cid = db.create_campaign(
                    (campaign_name or "Untitled") + " (копия)",
                    template_text,
                    _current_image_path(),
                    (group_link or None),
                    account_id=(get_settings().id if hasattr(get_settings(), "id") else None),
                    tags=_sanitized_tags(),
                )
                st.session_state.campaign_id = cid
                _cached_known_tags.clear()
                st.toast(f"Клон создан как draft #{cid}.", icon="📋")
                _compose_reset_state()
                st.rerun()

    with ac3:
        if can_delete:
            confirm = st.checkbox(
                "Да, удалить",
                key=f"compose_delete_confirm_{selected_cid}",
                help="Удалит кампанию и её send_log. Необратимо.",
            )
            if st.button(
                "🗑 Удалить",
                key="compose_delete_btn",
                disabled=not confirm,
                type="secondary",
            ):
                ok = db.delete_campaign(int(selected_cid))
                if ok:
                    st.toast(f"Кампания #{selected_cid} удалена.", icon="🗑")
                    _compose_reset_state()
                    st.rerun()
                else:
                    st.error(
                        "Не могу удалить — проверь, что кампания не running / paused."
                    )


# --------------------------------------------------------------------------
# STEP 5 — Dry Run
# --------------------------------------------------------------------------

def render_dryrun():
    st.markdown(
        '<div class="hero"><h1>Тест «на себе»</h1>'
        '<p>Отправит первое сообщение тебе в «Избранное», чтобы ты '
        'убедился, что шаблон и картинка выглядят как надо.</p></div>',
        unsafe_allow_html=True,
    )
    st.markdown('<div class="step-chip">ШАГ 5 · DRY-RUN</div>', unsafe_allow_html=True)

    campaigns = db.list_campaigns()
    if not campaigns:
        st.info("Сначала сохрани кампанию на шаге 4.")
        return
    cid_default = st.session_state.get("campaign_id", campaigns[0]["id"])
    cid = st.selectbox(
        "Кампания",
        options=[c["id"] for c in campaigns],
        index=next((i for i, c in enumerate(campaigns) if c["id"] == cid_default), 0),
        format_func=lambda i: next(c["name"] for c in campaigns if c["id"] == i) + f" (#{i})",
    )

    if st.button("🧪 Отправить тест себе в Избранное", type="primary"):
        settings = load_settings()
        client = auth.get_client(settings.session_path, settings.api_id, settings.api_hash)
        runner = CampaignRunner(
            client=client, db=db, campaign_id=cid,
            pacing=PacingConfig(daily_cap=9999, min_delay_sec=0, max_delay_sec=0),
            dry_run_to_self=True,
        )
        runner.start()
        # Poll up to ~45s — `connect_client` can itself burn ~30s while the
        # session is contended by the worker. We surface clear outcomes at
        # the end so the spinner doesn't silently hide failures.
        with st.spinner("Отправляю в Saved Messages…"):
            for _ in range(90):
                if runner.is_done():
                    break
                time.sleep(0.5)

        if not runner.is_done():
            st.warning(
                "Тест всё ещё в работе — возможно, сессия временно занята. "
                "Обнови страницу через пару секунд, чтобы увидеть результат."
            )
        elif runner.error:
            st.error(f"Не удалось отправить тест: {runner.error}")
        elif runner.outcome is not None and getattr(runner.outcome, "sent", 0) == 0:
            st.warning(
                "Сессия завершилась без отправки — возможно, в кампании нет "
                "resolved-контактов. Добавь хотя бы одного (Contacts → Resolve) "
                "и повтори."
            )
        else:
            st.toast("Тест ушёл в Saved Messages.", icon="✅")

    st.divider()
    st.markdown("### 🎯 Свободный тест (test fire)")
    st.caption(
        "Отправить произвольное сообщение на конкретный адрес — "
        "не привязано к кампании. Для быстрой проверки шаблона или картинки."
    )

    settings = load_settings()
    try:
        tf_authorized = auth.run_async(auth.is_authorized(
            settings.session_path, settings.api_id, settings.api_hash,
        )) if settings.has_credentials else False
    except Exception:
        tf_authorized = False
    if not tf_authorized:
        st.warning("Нужен залогиненный TG-аккаунт (шаг 1).")
        return

    tf_target = st.text_input(
        "Адресат",
        placeholder="+12025550101  или  @username  или  me",
        key="tf_target",
        help="«me» отправит в твои Saved Messages.",
    )
    tf_text = st.text_area(
        "Сообщение",
        value="Тестовое сообщение с TG Blaster",
        height=120,
        key="tf_text",
    )
    tf_image = st.file_uploader(
        "Картинка (опционально)", type=["jpg", "jpeg", "png", "webp"], key="tf_image",
    )

    if st.button("🚀 Отправить сейчас", key="tf_send"):
        target_raw = (tf_target or "").strip()
        if not target_raw:
            st.error("Укажи адресата.")
        elif not tf_text.strip():
            st.error("Сообщение пустое.")
        else:
            # Resolve the target into something Telethon can send to.
            if target_raw.lower() == "me":
                target = "me"
            elif target_raw.startswith("@"):
                target = target_raw
            elif target_raw.startswith("+") or target_raw[0:1].isdigit():
                normalized = csv_io.normalize_phone(target_raw, default_region="UZ")
                target = normalized or target_raw
            else:
                target = target_raw

            tf_image_bytes = tf_image.getvalue() if tf_image is not None else None
            tf_image_name = tf_image.name if tf_image is not None else None

            async def _send_test():
                client = auth.get_client(
                    settings.session_path, settings.api_id, settings.api_hash,
                )
                if not client.is_connected():
                    await client.connect()
                if tf_image_bytes:
                    img_path = UPLOADS_DIR / f"_testfire_{tf_image_name}"
                    img_path.write_bytes(tf_image_bytes)
                    await client.send_file(
                        target, file=str(img_path),
                        caption=tf_text, parse_mode="md",
                    )
                else:
                    await client.send_message(
                        target, tf_text, parse_mode="md", link_preview=True,
                    )

            try:
                with st.spinner(f"Отправляю на {target_raw}…"):
                    auth.run_async(_send_test())
                st.success(f"✅ Отправлено на {target_raw}.")
            except Exception as e:
                st.error(f"Не ушло: {e}")


# --------------------------------------------------------------------------
# STEP 6 — Campaign
# --------------------------------------------------------------------------

def _pacing_from_settings(s) -> PacingConfig:
    return PacingConfig(
        min_delay_sec=s.min_delay_sec,
        max_delay_sec=s.max_delay_sec,
        long_pause_every=s.long_pause_every,
        long_pause_min_sec=s.long_pause_min_sec,
        long_pause_max_sec=s.long_pause_max_sec,
        daily_cap=s.daily_cap,
    )


def render_campaign():
    st.markdown(
        '<div class="hero"><h1>Боевая рассылка</h1>'
        '<p>Безопасный темп, пауза каждые 40 сообщений, лимит в сутки. '
        'Можно в любой момент нажать «Остановить» — прогресс сохранится.</p></div>',
        unsafe_allow_html=True,
    )
    st.markdown('<div class="step-chip">ШАГ 6 · SEND</div>', unsafe_allow_html=True)

    st.markdown(
        '<div class="danger">⚠️ <b>Правила безопасности</b><br>'
        '• Opt-in аудитория (люди сами зарегистрировались) — важно.<br>'
        '• При PeerFloodError рассылка остановится сама, не пытайся её сразу запускать.<br>'
        '• Не разгоняй темп: 30–90 сек между отправками — уже быстро для TG.</div>',
        unsafe_allow_html=True,
    )

    campaigns = db.list_campaigns()
    if not campaigns:
        st.info("Сначала сохрани кампанию на шаге 4.")
        return

    # Optional filter by tag, applied BEFORE the campaign selector so the
    # dropdown only offers campaigns matching the filter.
    known_tags = db.all_known_tags()
    filter_tags: list[str] = []
    if known_tags:
        filter_tags = st.multiselect(
            "Фильтр по тегам (опционально)",
            options=known_tags,
            default=[],
            key="campaign_tag_filter",
            help="Показать только кампании, у которых есть один из выбранных тегов.",
        )
    if filter_tags:
        wanted = {t.lower() for t in filter_tags}
        campaigns = [
            c for c in campaigns
            if wanted & {t.lower() for t in db.get_campaign_tags(int(c["id"]))}
        ]
        if not campaigns:
            st.info("По этому фильтру кампаний нет.")
            return

    cid_default = st.session_state.get("campaign_id", campaigns[0]["id"])
    cid = st.selectbox(
        "Кампания",
        options=[c["id"] for c in campaigns],
        index=next(
            (i for i, c in enumerate(campaigns) if c["id"] == cid_default), 0,
        ),
        format_func=lambda i: (
            next(c["name"] for c in campaigns if c["id"] == i)
            + f" (#{i})"
            + (
                " · " + ", ".join(db.get_campaign_tags(int(i)))
                if db.get_campaign_tags(int(i)) else ""
            )
        ),
        key="camp_sel",
    )

    # Inline tag editor for the selected campaign.
    current_tags = db.get_campaign_tags(int(cid))
    edit_col, add_col = st.columns([3, 1])
    with edit_col:
        edited_tags = st.multiselect(
            f"Теги кампании #{cid}",
            options=sorted(set(known_tags + current_tags),
                           key=str.lower),
            default=current_tags,
            key=f"edit_tags_{cid}",
        )
    with add_col:
        new_tag_here = st.text_input(
            "Новый тег", key=f"new_tag_inline_{cid}",
            placeholder="workshop-may",
        )
    if st.button("💾 Сохранить теги", key=f"save_tags_{cid}"):
        merged = []
        seen = set()
        for t in list(edited_tags) + [new_tag_here.strip()]:
            t = (t or "").strip()
            if not t or t.lower() in seen:
                continue
            seen.add(t.lower())
            merged.append(t)
        db.set_campaign_tags(int(cid), merged)
        st.success("Теги сохранены.")
        st.rerun()

    st.caption(
        "Кампания отправит сообщения **только тем контактам, которые уже "
        "`resolved`** в базе. Новые данные подтягивай через «Источник» на "
        "странице Contacts, затем пройди resolve/validate — после этого они "
        "автоматически попадут в целевую аудиторию."
    )

    # --- Saved-filter scope — optional narrowing of the audience ---
    saved = db.list_saved_filters()
    if saved:
        with st.expander("🎯 Аудитория из сохранённого фильтра"):
            choice = st.selectbox(
                "Сохранённый фильтр",
                options=[None] + [int(s["id"]) for s in saved],
                format_func=lambda sid: "— не использовать —" if sid is None
                    else next((f"{s['name']} (#{s['id']})"
                               for s in saved if int(s["id"]) == sid), f"#{sid}"),
                key=f"camp_sf_pick_{cid}",
            )
            if choice is not None:
                row = db.get_saved_filter(int(choice))
                try:
                    spec = json.loads(row["filter_json"] or "{}") if row else {}
                except Exception:
                    spec = {}
                materialized = db.resolve_filter_to_contact_ids(spec)
                st.caption(
                    f"Фильтр «{row['name'] if row else '?'}» → "
                    f"**{len(materialized)}** контактов."
                )
                if st.button(
                    f"📌 Использовать как аудиторию ({len(materialized)})",
                    key=f"camp_sf_apply_{cid}",
                    disabled=not materialized,
                ):
                    st.session_state[f"pending_delta_send_{cid}"] = materialized
                    st.toast("✅ Аудитория применена — ниже жми «Поставить в очередь».", icon="🎯")

    settings = load_settings()
    col1, col2, col3 = st.columns(3)
    with col1:
        min_d = st.number_input("Мин задержка, сек", 0, 3600, settings.min_delay_sec)
        max_d = st.number_input("Макс задержка, сек", 0, 3600, settings.max_delay_sec)
    with col2:
        every = st.number_input("Длинная пауза каждые N сообщений", 1, 500, settings.long_pause_every)
        min_lp = st.number_input("Длин пауза мин, сек", 0, 7200, settings.long_pause_min_sec)
        max_lp = st.number_input("Длин пауза макс, сек", 0, 7200, settings.long_pause_max_sec)
    with col3:
        daily = st.number_input("Лимит в сутки", 1, 5000, settings.daily_cap)

    pacing = PacingConfig(
        min_delay_sec=int(min_d),
        max_delay_sec=int(max_d),
        long_pause_every=int(every),
        long_pause_min_sec=int(min_lp),
        long_pause_max_sec=int(max_lp),
        daily_cap=int(daily),
    )

    stats = db.campaign_stats(cid)
    # Cheap counts — don't load every resolved contact row just to len it.
    resolved_total = db.count_resolved()
    already = len(db.already_sent_ids(cid))
    remaining = max(0, resolved_total - already)

    kpi_cols = st.columns(4)
    kpi_cols[0].metric("Осталось отправить", remaining,
                       help="Resolved-контакты минус те, кому уже отправили в этой кампании.")
    kpi_cols[1].metric("Отправлено", stats["sent"])
    kpi_cols[2].metric("Ошибок", stats["errors"])
    kpi_cols[3].metric("Пропущено", stats["skipped"])

    if already > 0 and remaining > 0:
        st.info(
            f"🔁 В этой кампании уже отправлено **{already}** из "
            f"**{resolved_total}**. При следующем старте уйдёт сообщение "
            f"только тем **{remaining}**, кто ещё не получил "
            "(дельта-отправка по умолчанию)."
        )

    # Advanced options (precedence across campaigns)
    with st.expander("🧩 Precedence / дубликаты"):
        skip_days = st.number_input(
            "Пропускать контакты, которым писали за последние N дней (0 — не пропускать)",
            min_value=0, max_value=365, value=0,
            help="Защита от повторных сообщений тому же человеку из разных кампаний.",
        )
        st.caption(
            "Дедуп по `tg_user_id` применяется автоматически — "
            "если два телефона у одного пользователя, один будет помечен как skipped."
        )

    dispatcher = get_dispatcher()

    # Find live campaign job, if any.
    def _live_job_for_campaign(cid: int):
        for j in db.list_jobs(limit=50):
            if j["type"] != "run_campaign":
                continue
            if j["status"] not in ("queued", "running"):
                continue
            try:
                payload = json.loads(j["payload_json"] or "{}")
            except Exception:
                payload = {}
            if payload.get("campaign_id") == cid:
                return j
        return None

    live_job = _live_job_for_campaign(cid)

    # --- Heartbeat diagnostic chip (H3c) ------------------------------
    # When a job is live, show age of started_at and last_heartbeat so the
    # user can tell alive-but-slow from dead. The manual 🧹 unstick button
    # appears when the heartbeat is clearly stale.
    if live_job is not None:
        from datetime import datetime as _dt, timezone as _tz
        fresh = db.get_job(int(live_job["id"])) or live_job

        def _age_sec(ts: str | None) -> int | None:
            if not ts:
                return None
            try:
                parsed = _dt.fromisoformat(ts.replace("Z", "+00:00"))
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=_tz.utc)
                return int((_dt.now(_tz.utc) - parsed).total_seconds())
            except Exception:
                return None

        started_age = _age_sec(fresh["started_at"]) or _age_sec(fresh["queued_at"])
        hb_age = _age_sec(fresh["last_heartbeat"])
        # Traffic-light color for the heartbeat.
        if hb_age is None:
            hb_color = "gray"
            hb_text = f"heartbeat: — (job {fresh['status']})"
        elif hb_age < 60:
            hb_color = "green"
            hb_text = f"heartbeat: {hb_age}s"
        elif hb_age < 180:
            hb_color = "orange"
            hb_text = f"heartbeat: {hb_age}s ⚠"
        else:
            hb_color = "red"
            hb_text = f"heartbeat: {hb_age}s 🔴"

        d1, d2, d3 = st.columns([1.2, 1.2, 3])
        d1.markdown(
            f"**Job #{fresh['id']}** · `{fresh['status']}`"
            + (f" · started {started_age}s ago" if started_age is not None else "")
        )
        d2.markdown(f":{hb_color}[**{hb_text}**]")
        with d3:
            should_offer_unstick = (
                hb_age is None and started_age is not None and started_age > 120
            ) or (hb_age is not None and hb_age > 180)
            if should_offer_unstick:
                if st.button(
                    "🧹 Освободить застрявшую задачу",
                    key=f"unstick_{fresh['id']}",
                    help="Пометить задачу как error и освободить блокировку — "
                         "следующий Start сможет подняться с последнего отправленного.",
                ):
                    from core.jobs import release_stuck_job
                    if release_stuck_job(db, int(fresh["id"]),
                                         "manual unstick from Campaign page"):
                        st.toast(f"Задача #{fresh['id']} освобождена.", icon="🧹")
                    else:
                        st.toast("Не удалось — возможно, уже не в running.",
                                 icon="ℹ️")
                    time.sleep(0.3)
                    st.rerun()

    # Auto-jump preference — persisted across reruns, default ON.
    auto_jump = st.checkbox(
        "После старта кампании — сразу открыть Log",
        value=st.session_state.get("auto_jump_to_log", True),
        key="auto_jump_to_log",
        help="Сразу после постановки задачи в очередь переключит навигацию на шаг 6 (Log).",
    )

    # Optional scoped audience from a saved filter (Phase 5). Absent that,
    # the sender naturally sends only to resolved contacts that weren't
    # already marked sent in this campaign — that's the per-campaign delta.
    pending_delta_ids = st.session_state.get(f"pending_delta_send_{cid}") or []
    scope_ids: list[int] | None = list(pending_delta_ids) if pending_delta_ids else None

    # Phase H6 — state-machine buttons. Legal transitions are enforced
    # server-side by `db.transition_campaign_state` too.
    camp_row = db.get_campaign(cid)
    camp_state = (camp_row["status"] if camp_row else "draft") or "draft"
    state_badge = {
        "draft":   "⚪ draft",
        "running": "🟢 running",
        "paused":  "🟡 paused",
        "stopped": "⚫ stopped",
        "done":    "✅ done",
        "error":   "🔴 error",
    }.get(camp_state, f"? {camp_state}")
    st.markdown(f"**Статус кампании:** {state_badge}")

    is_live = live_job is not None
    has_progress = already > 0

    # Phase H0 — if Redis is unavailable, campaigns would run in thread
    # mode (same process as the Streamlit UI), which means a page crash
    # or refresh would kill them mid-send. Warn loudly before Start.
    if not redis_client.is_redis_available():
        st.warning(
            "⚠️ **Thread-режим** — Redis недоступен. Если запустить кампанию "
            "сейчас, отправка будет жить в процессе Streamlit и прервётся при "
            "падении/перезапуске UI. Подними Redis перед боевой рассылкой — "
            "тогда задача уйдёт в отдельный arq-воркер."
        )

    bc1, bc2, bc3, bc4, bc5 = st.columns([1.1, 1, 1.2, 1, 1.1])

    with bc1:
        # Start / Resume / Send-to-rest / Send-to-audience, depending on state.
        if scope_ids is not None:
            start_label = f"🎯 Отправить выбранной аудитории ({len(scope_ids)})"
        elif camp_state == "paused":
            start_label = f"▶ Продолжить с последнего ({remaining})"
        elif has_progress:
            start_label = f"🔁 Отправить остальным ({remaining})"
        else:
            start_label = f"🚀 Запустить кампанию ({remaining})"
        start_disabled = is_live or (scope_ids is None and remaining == 0)
        if st.button(start_label, type="primary", disabled=start_disabled,
                     key=f"camp_start_{cid}"):
            try:
                # Clear any stale pause/stop keys from a previous run.
                dispatcher.clear_campaign_signals(cid)
                job_id = dispatcher.enqueue_campaign(
                    campaign_id=cid,
                    pacing=pacing,
                    dry_run=False,
                    skip_recently_messaged_days=int(skip_days),
                    ids=scope_ids,
                )
                db.transition_campaign_state(cid, "running")
                st.toast(
                    f"Кампания #{cid} → задача #{job_id}"
                    + (f" (scope: {len(scope_ids)})" if scope_ids else ""),
                    icon="🚀",
                )
                st.session_state.pop(f"pending_delta_send_{cid}", None)
                if auto_jump:
                    log_label = next(label for label, key in STEPS if key == "log")
                    st.session_state["_pending_nav_step"] = log_label
                time.sleep(0.4)
                st.rerun()
            except RuntimeError as e:
                st.warning(str(e))
            except Exception as e:
                st.error(f"Упало: {e}")

    with bc2:
        if st.button("⏸ Пауза", disabled=not is_live,
                     key=f"camp_pause_{cid}",
                     help="Сохранит прогресс; «▶ Продолжить» поднимет отправку "
                          "с того же места."):
            dispatcher.request_pause(cid)
            st.toast("Пауза запрошена — завершу текущий адресат и встану.",
                     icon="⏸")

    with bc3:
        if st.button("⏹ Остановить",
                     disabled=not is_live and camp_state != "paused",
                     key=f"camp_stop_{cid}",
                     help="То же, что пауза, но помечает кампанию как "
                          "«stopped» — явное «хватит на сегодня»."):
            if is_live:
                dispatcher.request_stop(cid)
                st.toast("Просьба остановки отправлена.", icon="⏹")
            else:
                # paused → stopped transition when no job is live
                db.transition_campaign_state(cid, "stopped")
                st.toast("Статус кампании: stopped.", icon="⏹")
                time.sleep(0.3)
                st.rerun()

    with bc4:
        # Reset guarded by a confirm checkbox in a small expander.
        # Streamlit forbids mutating a widget's session_state key AFTER
        # the widget renders this run — so we stage a clear via a
        # separate flag and consume it BEFORE the checkbox renders.
        _reset_confirm_key = f"camp_reset_confirm_{cid}"
        _reset_clear_flag = f"_clear_reset_confirm_{cid}"
        if st.session_state.pop(_reset_clear_flag, False):
            st.session_state.pop(_reset_confirm_key, None)

        with st.popover("🔄 Сбросить", disabled=is_live or camp_state == "running"):
            st.caption(
                "Сброс УДАЛИТ все записи `send_log` по этой кампании "
                "(sent / pending / error / skipped) и вернёт статус в `draft`. "
                "Глобальные opt-outs сохранятся."
            )
            confirm_reset = st.checkbox(
                "Я понимаю — очистить журнал этой кампании",
                key=_reset_confirm_key,
            )
            if st.button("🗑💥 Подтвердить сброс",
                         type="secondary",
                         disabled=not confirm_reset,
                         key=f"camp_reset_go_{cid}"):
                n = db.reset_campaign_progress(cid)
                st.toast(f"Сброшено: удалено {n} записей.", icon="🔄")
                # Ask the next run to clear the checkbox before it renders.
                st.session_state[_reset_clear_flag] = True
                time.sleep(0.3)
                st.rerun()

    with bc5:
        if is_live:
            st.info(f"job #{live_job['id']} · {live_job['status']}")
        else:
            last_done = None
            for j in db.list_jobs(limit=50):
                if j["type"] != "run_campaign":
                    continue
                try:
                    payload = json.loads(j["payload_json"] or "{}")
                except Exception:
                    payload = {}
                if payload.get("campaign_id") == cid and j["status"] in ("done", "error", "cancelled"):
                    last_done = j
                    break
            if last_done is not None:
                # Speak in CAMPAIGN-state terms, not arq-job-state. A job
                # marked `done` may mean the campaign itself is `done`,
                # `paused`, or `stopped` — distinguish so the user isn't
                # mislead to re-click Pause/Stop.
                job_ref = f"job #{last_done['id']}"
                if last_done["status"] == "error":
                    st.error(
                        f"Последний запуск упал ({job_ref}): "
                        f"{last_done['error']}"
                    )
                elif camp_state == "paused":
                    st.info(
                        f"⏸ Кампания на паузе — {remaining} ещё не отправлены. "
                        f"Жми «▶ Продолжить с последнего» чтобы возобновить. "
                        f"({job_ref})"
                    )
                elif camp_state == "stopped":
                    st.warning(
                        f"⏹ Кампания остановлена — {remaining} не отправлены. "
                        f"Жми «▶ Продолжить» или «🔄 Сбросить». ({job_ref})"
                    )
                elif camp_state == "done":
                    st.success(
                        f"✅ Кампания завершена полностью ({job_ref})."
                    )
                elif last_done["status"] == "cancelled":
                    st.info(f"Последний запуск отменён ({job_ref}).")
                else:
                    st.success(f"Последний запуск завершён ({job_ref}).")

    # Live progress panel — fragment so the rest of the page stays
    # interactive (Stop button, selectbox, sidebar). Runs every 1 sec
    # while the job is live, 5 sec while it's finishing/idle.
    if live_job is not None:
        live_job_id = int(live_job["id"])

        @st.fragment(run_every="1s")
        def _campaign_panel():
            fresh = db.get_job(live_job_id)
            try:
                last = json.loads(fresh["progress_json"] or "{}") if fresh else {}
            except Exception:
                last = {}
            if not last:
                st.caption("Ожидаю первого события от воркера…")
                return

            sent = last.get("sent", 0)
            total = last.get("total", 0) or 1
            st.progress(
                min(1.0, sent / total),
                text=(
                    f"Отправлено {sent} / {last.get('total', 0)}  ·  "
                    f"✓ {sent}   ! {last.get('errors', 0)}   "
                    f"⊘ {last.get('skipped', 0)}   🚫 {last.get('opted_out', 0)}"
                ),
            )

            status = last.get("status", "")
            cur = last.get("current", "")
            wait_until = last.get("wait_until")
            wait_sec = last.get("wait_sec")

            if status in ("waiting", "flood_wait") and wait_until:
                remaining = max(0, int(wait_until - time.time()))
                total_wait = int(wait_sec or remaining or 1)
                pct = max(0.0, min(1.0, 1 - remaining / total_wait)) if total_wait else 1.0
                label = (
                    f"⏱ Пауза до следующей отправки: **{remaining} сек** "
                    f"(из {wait_sec} сек)"
                    if status == "waiting"
                    else f"🚨 FloodWait: ещё **{remaining} сек** (Telegram попросил {wait_sec})"
                )
                st.progress(pct, text=label)
                if cur:
                    st.caption(f"Последний адресат: `{cur}`")
            elif status == "daily_cap":
                st.warning("Дневной лимит достигнут — продолжим завтра")
            elif status == "peer_flood":
                st.error("PeerFlood — кампания остановлена для защиты аккаунта")
            elif status == "sent":
                st.caption(f"✅ Отправлено: `{cur}`")
            elif status == "error":
                st.error(f"Ошибка при отправке `{cur}`: {last.get('error','?')}")
            elif cur:
                st.caption(f"Последний адресат: `{cur}`")

            if fresh and fresh["status"] not in ("queued", "running"):
                # Refresh the campaign-state badge — the worker may have
                # transitioned the campaign to paused/stopped/done while
                # this fragment was between polls.
                row_now = db.get_campaign(cid) or {}
                state_now = (row_now["status"] if row_now else "draft") or "draft"
                if state_now == "paused":
                    st.info(
                        f"⏸ Кампания встала на паузу (job #{fresh['id']}). "
                        f"Нажми «▶ Продолжить с последнего» чтобы доотправить."
                    )
                elif state_now == "stopped":
                    st.warning(
                        f"⏹ Кампания остановлена (job #{fresh['id']})."
                    )
                elif state_now == "done":
                    st.success(
                        f"✅ Кампания завершена полностью (job #{fresh['id']})."
                    )
                else:
                    st.info(
                        f"Задача #{fresh['id']} завершена · состояние "
                        f"кампании: `{state_now}`."
                    )

        _campaign_panel()


# --------------------------------------------------------------------------
# STEP 7 — Log
# --------------------------------------------------------------------------

def render_log():
    st.markdown(
        '<div class="hero"><h1>Журнал</h1>'
        '<p>Лайв-прогресс resolve / validate / campaign + история отправок.</p></div>',
        unsafe_allow_html=True,
    )

    # ---- Live job panels — moved here from Contacts/Resolve/Campaign ----
    st.markdown('<div class="step-chip">🪵 АКТИВНЫЕ ЗАДАЧИ</div>',
                unsafe_allow_html=True)
    recent_resolve = [j for j in db.list_jobs(limit=20)
                      if j["type"] == "resolve_contacts"]
    running_resolve = [j for j in recent_resolve
                       if j["status"] in ("queued", "running")]
    recent_validate = [j for j in db.list_jobs(limit=20)
                       if j["type"] == "validate_usernames"]
    running_validate = [j for j in recent_validate
                        if j["status"] in ("queued", "running")]
    recent_campaign = [j for j in db.list_jobs(limit=20)
                       if j["type"] == "run_campaign"]
    running_campaign = [j for j in recent_campaign
                        if j["status"] in ("queued", "running")]
    recent_readcheck = [j for j in db.list_jobs(limit=20)
                        if j["type"] == "check_read_receipts"]
    running_readcheck = [j for j in recent_readcheck
                         if j["status"] in ("queued", "running")]

    any_live = any([running_resolve, running_validate, running_campaign,
                    running_readcheck])
    if not any_live and not (recent_resolve or recent_validate
                              or recent_campaign or recent_readcheck):
        st.caption("Пока никаких задач не было — запусти Resolve / Validate / Campaign.")
    else:
        _render_job_panel(db, recent_resolve, running_resolve, label="Resolve")
        _render_job_panel(db, recent_validate, running_validate, label="Validate")
        _render_job_panel(db, recent_campaign, running_campaign, label="Campaign")
        _render_job_panel(db, recent_readcheck, running_readcheck, label="Read-check")

    st.divider()
    st.markdown('<div class="step-chip">📊 ЖУРНАЛ ОТПРАВОК ПО КАМПАНИИ</div>',
                unsafe_allow_html=True)
    campaigns = db.list_campaigns()
    if not campaigns:
        st.info("Нет кампаний.")
        return
    cid = st.selectbox(
        "Кампания",
        options=[c["id"] for c in campaigns],
        format_func=lambda i: next(c["name"] for c in campaigns if c["id"] == i) + f" (#{i})",
        key="log_cid",
    )

    # Is a campaign for this cid currently running? If yes, auto-refresh
    # the KPI metrics + send-log dataframe so the user sees rows appear as
    # they're sent without having to F5. Idle → no refresh, no CPU cost.
    is_live = any(
        j["status"] in ("queued", "running")
        for j in recent_campaign
        if _campaign_id_of_job(j) == int(cid)
    )
    refresh = "2s" if is_live else None

    # Phase R — manual read-check trigger. Scoped to this campaign; lives
    # outside the live fragment so its click handler is stable under
    # auto-refresh.
    read_col, _ = st.columns([1, 3])
    with read_col:
        if st.button("🔍 Проверить прочтения", key=f"log_read_check_{cid}",
                     help="Опрашивает Telegram о статусе прочтения отправленных"
                          " сообщений этой кампании и обновляет колонку «Прочитано»."):
            log_dispatcher = get_dispatcher()
            try:
                rjob = log_dispatcher.enqueue_read_check(
                    campaign_ids=[int(cid)], contact_ids=None,
                )
                st.toast(f"Read-check #{rjob} поставлен в очередь",
                         icon="🔍")
            except RuntimeError as e:
                st.warning(str(e))
            except Exception as e:  # noqa: BLE001
                st.error(f"Упало: {e}")

    @st.fragment(run_every=refresh)
    def _live_log_table():
        fresh_df = db.send_log_df(int(cid))
        fresh_stats = db.campaign_stats(int(cid))

        k = st.columns(5)
        k[0].metric("Отправлено", fresh_stats["sent"])
        k[1].metric("Прочитано",
                    f"{fresh_stats.get('read', 0)} / {fresh_stats['sent']}")
        k[2].metric("Ошибок", fresh_stats["errors"])
        k[3].metric("Пропущено", fresh_stats["skipped"])
        k[4].metric("Отписок", fresh_stats["opted_out"])

        if is_live:
            st.caption(
                f"🔁 Авто-обновление каждые 2 сек — кампания #{cid} в работе."
            )

        display_df = fresh_df.copy()
        if "read_at" in display_df.columns:
            # Only precise matches survive — we only flag rows whose
            # captured message_id is ≤ the peer's read_outbox cursor.
            # Legacy rows without a message_id stay blank (honest
            # "unknown" vs. the old coarse fallback's false-positives).
            display_df = display_df.rename(columns={"read_at": "Прочитано"})
            display_df = display_df.drop(columns=["message_id"],
                                         errors="ignore")

        st.dataframe(display_df, use_container_width=True, hide_index=True)

        if not fresh_df.empty:
            st.download_button(
                "⬇️ Скачать CSV",
                # utf-8-sig adds a BOM so Excel reads Cyrillic correctly.
                data=fresh_df.to_csv(index=False).encode("utf-8-sig"),
                file_name=f"campaign_{cid}_log.csv",
                mime="text/csv",
                key=f"log_csv_{cid}",
            )

    _live_log_table()


# --------------------------------------------------------------------------
# STEP 8 — Jobs (queue / concurrency / status)
# --------------------------------------------------------------------------

def _pretty_progress(row) -> str:
    try:
        p = json.loads(row["progress_json"] or "{}")
    except Exception:
        return ""
    if not p:
        return ""
    if "sent" in p and "total" in p:
        return f"{p.get('sent', 0)}/{p.get('total', 0)}  {p.get('status', '')}"
    if "done" in p and "total" in p:
        return f"{p.get('done', 0)}/{p.get('total', 0)}"
    return str(p)[:80]


def render_jobs():
    st.markdown(
        '<div class="hero"><h1>Очередь задач</h1>'
        '<p>Resolve, кампании и зависимости между ними. '
        'Параллельные запуски одной и той же кампании автоматически '
        'блокируются.</p></div>',
        unsafe_allow_html=True,
    )
    st.markdown('<div class="step-chip">ШАГ 8 · JOBS</div>', unsafe_allow_html=True)

    badge_cols = st.columns([1, 1, 1, 1])
    badge_cols[0].caption(f"Бэкенд: **{_backend_badge()}**")

    df = db.jobs_df()
    if df.empty:
        st.info("Задач ещё не было. Поставь resolve или кампанию в очередь.")
        return

    # Decorate progress
    df = df.copy()
    df["progress"] = df.apply(
        lambda r: _pretty_progress(r) if r["progress_json"] else "", axis=1,
    )
    display_df = df[[
        "id", "type", "status", "backend", "depends_on",
        "queued_at", "started_at", "finished_at", "progress", "error",
    ]].rename(columns={
        "id": "#",
        "type": "тип",
        "status": "статус",
        "backend": "движок",
        "depends_on": "зависит от",
        "queued_at": "в очереди с",
        "started_at": "старт",
        "finished_at": "финиш",
        "progress": "прогресс",
        "error": "ошибка",
    })

    # KPIs
    k = st.columns(4)
    k[0].metric("В очереди", int((df["status"] == "queued").sum()))
    k[1].metric("Бегут", int((df["status"] == "running").sum()))
    k[2].metric("Готово", int((df["status"] == "done").sum()))
    k[3].metric("Ошибок", int((df["status"] == "error").sum()))

    auto_refresh = st.toggle("Авто-обновление (каждые 3 сек)", value=True)

    st.dataframe(display_df, use_container_width=True, hide_index=True)

    # Job details drawer
    st.divider()
    st.markdown("### 🔬 Детали задачи")
    job_ids = df["id"].tolist()
    if job_ids:
        selected = st.selectbox(
            "Выбери задачу",
            options=job_ids,
            format_func=lambda i: f"#{i} · {df.loc[df['id']==i, 'type'].iloc[0]}"
                                  f" · {df.loc[df['id']==i, 'status'].iloc[0]}",
        )
        job = db.get_job(int(selected))
        if job:
            colL, colR = st.columns(2)
            with colL:
                st.markdown("**Payload**")
                try:
                    st.json(json.loads(job["payload_json"] or "{}"))
                except Exception:
                    st.code(job["payload_json"] or "")
            with colR:
                st.markdown("**Progress**")
                try:
                    st.json(json.loads(job["progress_json"] or "{}"))
                except Exception:
                    st.code(job["progress_json"] or "")
            if job["error"]:
                st.error(job["error"])

    if auto_refresh and ((df["status"] == "running").any() or (df["status"] == "queued").any()):
        time.sleep(3)
        st.rerun()


# --------------------------------------------------------------------------
# Router
# --------------------------------------------------------------------------

router = {
    "setup": render_setup,
    "contacts": render_contacts,
    "compose": render_compose,
    "dryrun": render_dryrun,
    "campaign": render_campaign,
    "log": render_log,
    "jobs": render_jobs,
}
router[step_id]()
