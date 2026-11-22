import pandas as pd
import streamlit as st

from blaster.web.common import STATE_LABEL, blaster_argv, ctx, show_log

c = ctx()
st.title("🔎 Поиск в Telegram")
st.caption("Ищу каждого: сначала в твоей адресной книге, потом по номеру, потом по @username. "
           "Временно добавленные контакты удаляю, твои существующие не трогаю.")

lists = [r["list_name"] for r in c.db.lists()]
if not lists:
    st.info("Сначала импортируй контакты на странице «Контакты».")
    st.stop()

stt = c.jobs.status()
running_list = stt.meta.get("list") if stt.job and stt.job.kind == "resolve" else None
default = lists.index(running_list) if running_list in lists else 0
list_name = st.selectbox("Список", lists, index=default)

retry = st.checkbox("Повторить и те, где была ошибка")
a, b = st.columns(2)
if a.button("Найти в Telegram", type="primary", disabled=stt.busy):
    args = ["resolve", "--list", list_name] + (["--retry-errors"] if retry else [])
    c.jobs.start("resolve", blaster_argv(*args), meta={"list": list_name},
                 resume_argv=blaster_argv("resolve", "--list", list_name))
    st.rerun()
if stt.busy and stt.job.kind == "resolve" and b.button("Остановить"):
    c.jobs.pause()
    st.rerun()


@st.fragment(run_every=3)
def live() -> None:
    cc = ctx()
    s = cc.jobs.status()
    if s.busy != stt.busy:
        st.rerun()                      # job started/ended: refresh buttons too
    ls = cc.db.list_stats(list_name)
    total = ls["total"] or 1
    st.progress((total - ls["pending"]) / total,
                text=f"Проверено {ls['total'] - ls['pending']} из {ls['total']}")
    m = st.columns(4)
    m[0].metric("Найдено", ls["resolved"])
    m[1].metric("Нет в Telegram", ls["not_found"])
    m[2].metric("Ошибки", ls["errors"])
    m[3].metric("Ждут", ls["pending"])
    if s.job and s.job.kind == "resolve":
        st.caption(f"Операция: {STATE_LABEL[s.state]}")
        show_log(cc, 25)


live()

with st.expander("Кого не нашли / ошибки"):
    rows = [r for r in c.db.contacts(list_name) if r["status"] in ("not_found", "error")]
    if rows:
        st.dataframe(pd.DataFrame([{"стр.": r["row_no"], "ФИО": r["name"], "телефон": r["phone"],
                                    "telegram": r["username"], "статус": r["status"],
                                    "причина": r["resolve_error"]} for r in rows]), hide_index=True)
    else:
        st.caption("Пока нет.")
