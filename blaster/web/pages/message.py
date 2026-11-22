import re
from pathlib import Path

import streamlit as st

from blaster import telegram as tg
from blaster.sender import check_campaign, rendered_too_long
from blaster.template import render
from blaster.web.common import ctx, home, is_tested, mark_tested, run_tg, save_image

c = ctx()
ss = st.session_state
st.title("✉️ Сообщение")

lists = [r["list_name"] for r in c.db.lists()]
if not lists:
    st.info("Сначала импортируй контакты на странице «Контакты».")
    st.stop()

NEW = "➕ Новая кампания"
camps = c.db.campaigns()
names = [x["name"] for x in camps]
choice = st.selectbox("Кампания", [NEW] + names, index=len(names) if names else 0)
camp = c.db.get_campaign(choice) if choice != NEW else None

if camp is None:
    a, b = st.columns(2)
    name = a.text_input("Название кампании", placeholder="meetup-dec").strip()
    list_name = b.selectbox("Список получателей", lists)
    example = home() / "campaigns" / "examples" / "invite.md"
    start_text = example.read_text(encoding="utf-8").strip() if example.is_file() else ""
    key = "new"
else:
    name, list_name, start_text, key = camp["name"], camp["list_name"], camp["template"], str(camp["id"])
    st.caption(f"Список получателей: **{list_name}**")

left, right = st.columns([3, 2])
with left:
    text = st.text_area("Текст", start_text, height=320, key=f"tpl:{key}",
                        help="{first_name|друзья} — имя или «друзья», если имени нет. "
                             "Любая колонка таблицы — тоже плейсхолдер: {company}. "
                             "**жирный**, __курсив__, [текст](https://ссылка).")
    modes = ["md", "html"]
    mode = st.radio("Разметка", modes, horizontal=True, key=f"mode:{key}",
                    index=modes.index(camp["parse_mode"]) if camp else 0,
                    format_func={"md": "Markdown", "html": "HTML"}.get)
    preview_links = st.toggle("Превью ссылок", bool(camp["link_preview"]) if camp else True, key=f"lp:{key}")
    img_now = camp["image_path"] if camp else None
    if img_now:
        st.image(img_now, width=240, caption=Path(img_now).name)
        drop_img = st.checkbox("Убрать картинку", key=f"drop:{key}")
    else:
        drop_img = False
    upload = st.file_uploader("Картинка (баннер)", type=["jpg", "jpeg", "png", "webp"], key=f"img:{key}")

    if st.button("Сохранить", type="primary", disabled=not name):
        image = None if drop_img else img_now
        if upload is not None:
            try:
                image = save_image(c.campaigns_dir / re.sub(r"[^\w\-]+", "_", name), upload.name,
                                   upload.getvalue())
            except ValueError as e:
                st.error(str(e))
                st.stop()
        if camp is None:
            if c.db.get_campaign(name):
                st.error(f"Кампания «{name}» уже есть — выбери её в списке.")
                st.stop()
            c.db.create_campaign(name, list_name, text.strip(), image, mode, link_preview=preview_links)
        else:
            c.db.update_campaign(camp["id"], template=text.strip(), image_path=image, parse_mode=mode,
                                 link_preview=int(preview_links))
        st.rerun()

with right:
    st.subheader("Превью")
    contacts = c.db.contacts(list_name)
    pool = [x for x in contacts if x["status"] == "resolved"] or contacts
    who = st.selectbox("На ком посмотреть", range(len(pool)), key=f"who:{key}",
                       format_func=lambda i: f"{pool[i]['name'] or pool[i]['username'] or '—'} "
                                             f"(стр. {pool[i]['row_no']})")
    sample = pool[who] if pool else {}
    rendered = render(text, sample)
    with st.container(border=True):
        if img_now and not drop_img:
            st.image(img_now)
        if mode == "html":
            st.html(rendered.replace("\n", "<br>"))
        else:
            st.markdown(rendered.replace("\n", "  \n"))
    st.caption(f"{len(rendered)} символов. Точный вид — в тестовом сообщении.")

if camp is None:
    st.info("Сохрани кампанию — появятся проверка и тест себе.")
    st.stop()

dirty = text.strip() != camp["template"] or mode != camp["parse_mode"] or \
    int(preview_links) != camp["link_preview"] or drop_img or upload is not None
problems = check_campaign(c.db, camp)
too_long = sum(1 for x in contacts if rendered_too_long(render(camp["template"], x), bool(camp["image_path"])))
if too_long:
    problems.append(f"{too_long} сообщений длиннее лимита Telegram")
no_name = sum(1 for x in pool if not (x.get("first_name") or x.get("tg_first_name")))

st.divider()
if dirty:
    st.warning("Есть несохранённые изменения — тест и рассылка используют сохранённую версию.")
for p in problems:
    st.error(p)
if not problems:
    st.success(f"Шаблон в порядке. Без имени: {no_name} — им уйдёт вариант после «|».")

tested = is_tested(c, camp)
label = "Отправить тест себе в Избранное" + (" ещё раз" if tested else "")
if st.button(label, disabled=bool(problems) or dirty):
    async def send(cl):
        await tg.ensure_authorized(cl)
        return await cl.send_message("me", render(camp["template"], sample), parse_mode=camp["parse_mode"],
                                     link_preview=bool(camp["link_preview"]), file=camp["image_path"] or None)
    with st.spinner("Отправляю…"):
        _, err = run_tg(c, send)
    if err:
        st.error(err)
    else:
        mark_tested(c, camp)
        st.success("Тест в Избранном (Saved Messages). Проверь вид — потом страница «Рассылка».")
        tested = True
if not tested:
    st.caption("Рассылка откроется после теста этой версии сообщения.")
