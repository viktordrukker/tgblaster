"""TGBlaster web UI — `python -m blaster.web` (or `docker compose up -d`)."""
import hmac

import streamlit as st

from blaster import telegram as tg
from blaster.web.common import account_line, ctx, env_values, job_caption

st.set_page_config(page_title="TGBlaster", page_icon="📨", layout="wide")


def password_gate() -> None:
    pw = env_values().get("UI_PASSWORD", "")
    if not pw or st.session_state.get("authed"):
        return
    st.title("TGBlaster")
    entered = st.text_input("Пароль", type="password")
    if entered and hmac.compare_digest(entered.encode(), pw.encode()):
        st.session_state["authed"] = True
        st.rerun()
    if entered:
        st.error("Неверный пароль")
    st.stop()


password_gate()
c = ctx()
with st.sidebar:
    st.caption(f"Telegram: {account_line(tg.load_account(c.settings))}")
    st.caption(job_caption(c))

nav = st.navigation([
    st.Page("pages/telegram.py", title="Подключение Telegram", icon="🔌"),
    st.Page("pages/contacts.py", title="Контакты", icon="📇"),
    st.Page("pages/resolve.py", title="Поиск в Telegram", icon="🔎"),
    st.Page("pages/message.py", title="Сообщение", icon="✉️"),
    st.Page("pages/send.py", title="Рассылка", icon="🚀"),
    st.Page("pages/settings.py", title="Настройки", icon="⚙️"),
])
nav.run()
