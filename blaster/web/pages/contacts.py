import datetime as dt
import re
from dataclasses import replace

import pandas as pd
import streamlit as st

from blaster.importer import Overrides, build_plan, parse_rows, pick_sheet
from blaster.report import write_csv
from blaster.tables import load
from blaster.web.common import ctx

c = ctx()
ss = st.session_state
st.title("📇 Контакты")

# ---- 1. source ---------------------------------------------------------------
tab_file, tab_link = st.tabs(["Файл", "Ссылка (Google Sheets / URL)"])
with tab_file:
    up = st.file_uploader("Таблица участников", type=["xlsx", "xlsm", "xls", "csv", "tsv", "txt", "json"])
    if up is not None and ss.get("src_name") != up.name:
        c.inbox.mkdir(parents=True, exist_ok=True)
        path = c.inbox / re.sub(r"[^\w.\-]+", "_", up.name)
        path.write_bytes(up.getvalue())
        try:
            ss.update(sheets=load(path), src_name=up.name)
        except ValueError as e:
            st.error(str(e))
with tab_link:
    with st.form("link"):
        url = st.text_input("Ссылка", placeholder="https://docs.google.com/spreadsheets/d/…")
        if st.form_submit_button("Загрузить") and url.strip():
            try:
                with st.spinner("Скачиваю…"):
                    ss.update(sheets=load(url.strip()), src_name=url.strip())
            except Exception as e:  # noqa: BLE001 — network/format errors go to the user
                st.error(f"Не удалось загрузить: {e}. Для Google Sheets включи доступ «всем, у кого есть ссылка».")

sheets = ss.get("sheets")
if not sheets:
    st.info("Загрузи файл или вставь ссылку — покажу, как распознались колонки, до импорта.")
else:
    st.caption(f"Источник: {ss['src_name']}")
    region = c.settings.region
    names = [s.name for s in sheets]
    auto = pick_sheet(sheets, region).name
    sheet_name = st.selectbox("Лист", names, index=names.index(auto)) if len(sheets) > 1 else names[0]
    sheet = next(s for s in sheets if s.name == sheet_name)

    auto_plan = build_plan(sheet, region)
    hdr_auto = 0 if auto_plan.header_row is None else auto_plan.header_row + 1
    header_row = st.number_input("Строка заголовка (0 = заголовка нет)", 0, min(50, len(sheet.rows)),
                                 hdr_auto, key=f"hdr:{ss['src_name']}:{sheet_name}")
    plan = build_plan(sheet, region, Overrides(header_row=int(header_row)))

    with st.expander("Как выглядят первые строки файла", expanded=False):
        st.dataframe(pd.DataFrame(sheet.rows[:10]), hide_index=False)

    # ---- 2. roles -------------------------------------------------------------
    st.subheader("Колонки")
    opts = list(range(len(plan.headers)))
    label = lambda i: f"{plan.headers[i]} (#{i + 1})"  # noqa: E731
    k = f"{ss['src_name']}:{sheet_name}:{header_row}"

    def pick(title: str, current: int | None, key: str) -> int | None:
        choices = [None] + opts
        return st.selectbox(title, choices, index=choices.index(current), key=f"{key}:{k}",
                            format_func=lambda i: "—" if i is None else label(i))

    a, b = st.columns(2)
    with a:
        phones = st.multiselect("Телефон", opts, plan.phone_cols, format_func=label, key=f"ph:{k}")
        users = st.multiselect("Telegram (@username / ссылка)", opts, plan.username_cols,
                               format_func=label, key=f"un:{k}")
        consent = pick("Согласие на рассылку («нет» → исключить)", plan.consent_col, "cons")
    with b:
        full = pick("ФИО целиком", plan.full_name_col, "full")
        first = pick("Имя", plan.first_name_col, "first")
        last = pick("Фамилия", plan.last_name_col, "last")
        greet = pick("Обращение (как называть)", plan.greeting_col, "greet")
        orders = ["auto", "surname_first", "given_first"]
        order = st.selectbox("Порядок в ФИО", orders, orders.index(plan.name_order), key=f"ord:{k}",
                             format_func={"auto": "определить сам", "surname_first": "Фамилия Имя",
                                          "given_first": "Имя Фамилия"}.get)
    plan = replace(plan, phone_cols=phones, username_cols=users, full_name_col=full,
                   first_name_col=first, last_name_col=last, greeting_col=greet,
                   consent_col=consent, name_order=order)

    # ---- 3. result --------------------------------------------------------------
    try:
        res = parse_rows(sheet, plan, region)
    except ValueError as e:
        st.error(str(e))
        st.stop()
    with_phone = sum(1 for x in res.contacts if x.phones)
    m = st.columns(5)
    m[0].metric("Строк с данными", res.total_rows)
    m[1].metric("К рассылке", len(res.contacts))
    m[2].metric("С телефоном", with_phone)
    m[3].metric("С именем", sum(1 for x in res.contacts if x.first_name))
    m[4].metric("Дубли / отброшено", f"{len(res.duplicates)} / {len(res.rejected)}")

    st.write("**Как будут прочитаны контакты** (первые 30):")
    st.dataframe(pd.DataFrame([{
        "стр.": x.row_no, "обращение": x.first_name, "ФИО": x.name, "телефоны": ", ".join(x.phones),
        "telegram": f"@{x.username}" if x.username else "", **dict(list(x.extra.items())[:4]),
    } for x in res.contacts[:30]]), hide_index=True)
    if res.rejected:
        with st.expander(f"Отброшено: {len(res.rejected)}"):
            st.dataframe(pd.DataFrame(res.rejected), hide_index=True)
    if res.duplicates:
        with st.expander(f"Дубликаты: {len(res.duplicates)}"):
            st.dataframe(pd.DataFrame(res.duplicates), hide_index=True)

    # ---- 4. import --------------------------------------------------------------
    st.subheader("Импорт")
    default = "list-" + dt.date.today().strftime("%Y-%m")
    list_name = st.text_input("Имя списка", default, key="list_name", help="Повторный импорт в тот же список обновит данные, "
                              "найденные в Telegram контакты сохранятся.").strip()
    if st.button(f"Импортировать {len(res.contacts)} контактов в «{list_name}»", type="primary",
                 disabled=not res.contacts or not list_name):
        ins, upd = c.db.upsert_contacts(list_name, res.contacts)
        stamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
        write_csv(c.settings.reports_dir / f"import-{list_name}-{stamp}-rejected.csv",
                  res.rejected + [dict(d, reason="дубликат") for d in res.duplicates])
        st.success(f"Список «{list_name}»: новых {ins}, обновлено {upd}. Дальше — «Поиск в Telegram».")

# ---- lists -------------------------------------------------------------------
st.subheader("Списки")
lists = c.db.lists()
if lists:
    st.dataframe(pd.DataFrame(lists).rename(columns={
        "list_name": "список", "total": "всего", "resolved": "найдено в TG", "not_found": "нет в TG",
        "errors": "ошибки", "pending": "ждут поиска"}), hide_index=True)
else:
    st.caption("Пока пусто.")
