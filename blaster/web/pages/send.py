import datetime as dt
import time

import pandas as pd
import streamlit as st

from blaster.normalize import optout_key
from blaster.report import csv_text
from blaster.sender import check_campaign
from blaster.web.common import STATE_LABEL, blaster_argv, ctx, is_tested, show_log

c = ctx()
st.title("🚀 Рассылка")

camps = c.db.campaigns()
if not camps:
    st.info("Сначала создай сообщение на странице «Сообщение».")
    st.stop()

stt = c.jobs.status()
names = [x["name"] for x in camps]
active = stt.meta.get("campaign") if stt.job and stt.job.kind == "send" else None
name = st.selectbox("Кампания", names, index=names.index(active) if active in names else len(names) - 1)
camp = c.db.get_campaign(name)
mine = active == name
p = c.settings.pacing

st.caption(f"Темп: {p.min_delay}–{p.max_delay} с между сообщениями, пауза {p.long_pause_min // 60}–"
           f"{p.long_pause_max // 60} мин каждые {p.long_pause_every}, не больше {p.daily_cap} за 24 ч, "
           f"окно {p.window_start:%H:%M}–{p.window_end:%H:%M} ({p.timezone}). Меняется в «Настройках».")


# ---- live status --------------------------------------------------------------
@st.fragment(run_every=3)
def live() -> None:
    cc = ctx()
    s = cc.jobs.status()
    if s.busy != stt.busy:
        st.rerun()
    cs = cc.db.campaign_stats(camp["id"])
    done = cs["sent"] + cs["skipped"] + cs["error"] + cs["uncertain"]
    total = done + cs["remaining"] or 1
    st.progress(done / total, text=f"Обработано {done} из {total}")
    m = st.columns(6)
    m[0].metric("Отправлено", cs["sent"])
    m[1].metric("Осталось", cs["remaining"])
    m[2].metric("Пропущено", cs["skipped"], help="закрытый профиль, бот, удалён и т.п.")
    m[3].metric("Ошибки", cs["error"])
    m[4].metric("Неясно", cs["uncertain"], help="связь оборвалась в момент отправки — не переотправляю сам")
    m[5].metric("За 24 ч (все)", f"{cc.db.sent_since(time.time() - 86400)} / {p.daily_cap}")
    if s.job and s.job.kind == "send" and s.meta.get("campaign") == name:
        st.caption(f"Рассылка: **{STATE_LABEL[s.state]}**")
        show_log(cc, 30)


live()

# ---- controls -------------------------------------------------------------------
problems = check_campaign(c.db, camp)
tested = is_tested(c, camp)
stats = c.db.campaign_stats(camp["id"])
remaining = stats["remaining"]          # never tried yet; error/uncertain come back only via retry flags
argv = blaster_argv("send", name, "--until-done", "--yes")

if stt.busy and mine:
    a, b = st.columns(2)
    if a.button("⏸ Пауза", help="Доотправит текущее сообщение и остановится; продолжить можно в любой момент"):
        c.jobs.pause()
        st.rerun()
    if b.button("⏹ Стоп"):
        c.jobs.stop()
        st.rerun()
elif stt.busy:
    st.info("Сейчас идёт другая операция — дождись её окончания.")
else:
    for x in problems:
        st.error(x)
    if not tested:
        st.warning("Сначала отправь себе тест этой версии сообщения (страница «Сообщение»).")
    blocked = bool(problems) or not tested
    if mine and stt.state in ("paused", "interrupted") and remaining:
        if st.button("▶ Продолжить", type="primary", disabled=blocked):
            c.jobs.resume()
            st.rerun()
    else:
        failed = [r for r in c.db.report(camp["id"]) if r["send_status"] in ("error", "uncertain")]
        with st.expander(f"Повторы ({len(failed)})", expanded=bool(failed)):
            for r in failed:
                st.caption(f"{r['name'] or r['phone'] or r['username']}: {r['send_status']} — {r['detail']}")
            if any("PeerFlood" in (r["detail"] or "") for r in failed):
                st.caption("PeerFlood — Telegram ограничил сообщения незнакомым. Если повтор снова упадёт, "
                           "подожди сутки и проверь аккаунт в @SpamBot.")
            r_unc = st.checkbox(f"Переотправить «неясно» ({stats['uncertain']}) — может дать дубль, "
                                f"если сообщение всё же дошло", disabled=not stats["uncertain"])
            r_err = st.checkbox(f"Повторить ошибки ({stats['error']})", disabled=not stats["error"])
        to_send = remaining + (stats["error"] if r_err else 0) + (stats["uncertain"] if r_unc else 0)
        blocked = blocked or to_send == 0
        ok = st.checkbox(f"Я посмотрел превью и тест. Отправить **{to_send}** людям по-настоящему.",
                         disabled=blocked, key=f"ok:{camp['id']}")
        if st.button("▶ Запустить рассылку", type="primary", disabled=blocked or not ok):
            extra = (["--retry-uncertain"] if r_unc else []) + (["--retry-errors"] if r_err else [])
            c.jobs.start("send", argv + extra, meta={"campaign": name}, resume_argv=argv)
            st.rerun()

# ---- report & opt-outs ----------------------------------------------------------------
st.divider()
rows = c.db.report(camp["id"])
for r in rows:
    if r.get("updated_at"):
        r["updated_at"] = dt.datetime.fromtimestamp(r["updated_at"]).isoformat(timespec="seconds")
st.download_button("⬇ CSV-отчёт по каждому контакту", csv_text(rows).encode("utf-8-sig"),
                   file_name=f"report-{name}.csv", mime="text/csv")

with st.expander("Отписки — эти люди не получат ни одной рассылки"):
    with st.form("optout", clear_on_submit=True):
        vals = st.text_area("Телефоны, @username или id:123 — по одному в строке")
        reason = st.text_input("Причина (необязательно)")
        if st.form_submit_button("Добавить"):
            for v in [x for x in vals.splitlines() if x.strip()]:
                key = optout_key(v, c.settings.region)
                if key:
                    c.db.add_optout(key, reason)
                else:
                    st.error(f"Не понял «{v}»")
    oo = c.db.optouts()
    if oo:
        st.dataframe(pd.DataFrame([{"кто": o["value"], "причина": o["reason"],
                                    "когда": dt.datetime.fromtimestamp(o["created_at"]).strftime("%d.%m %H:%M")}
                                   for o in oo]), hide_index=True)
