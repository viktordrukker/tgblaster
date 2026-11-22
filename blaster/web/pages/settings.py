import datetime as dt

import streamlit as st

from blaster import envfile
from blaster.config import Pacing
from blaster.web.common import ctx, env_path, env_values

c = ctx()
p = c.settings.pacing
st.title("⚙️ Настройки")
st.caption("Сохраняются в .env. Идущая рассылка подхватит новый темп после паузы и продолжения.")

with st.form("pacing"):
    st.subheader("Темп")
    a, b, d = st.columns(3)
    cap = a.number_input("Не больше сообщений за 24 ч", 1, 1000, p.daily_cap,
                         help="Скользящие 24 часа, по всем кампаниям. Для нового аккаунта безопасно 100–150.")
    dmin = b.number_input("Пауза между сообщениями: от, с", 0, 3600, p.min_delay)
    dmax = d.number_input("до, с", 0, 3600, p.max_delay)
    a, b, d = st.columns(3)
    every = a.number_input("Длинная пауза каждые N сообщений", 1, 1000, p.long_pause_every)
    lmin = b.number_input("Длинная пауза: от, мин", 0, 240, p.long_pause_min // 60)
    lmax = d.number_input("до, мин", 0, 240, p.long_pause_max // 60)

    st.subheader("Окно отправки")
    a, b, d = st.columns(3)
    ws = a.time_input("С", p.window_start, step=dt.timedelta(minutes=30))
    we = b.time_input("До", p.window_end, step=dt.timedelta(minutes=30))
    tz = d.text_input("Часовой пояс получателей", p.timezone)

    st.subheader("Прочее")
    a, b = st.columns(2)
    regions = ["UZ", "RU", "KZ", "KG", "TJ"]
    region = a.selectbox("Страна для номеров без кода", regions,
                         regions.index(c.settings.region) if c.settings.region in regions else 0)
    pw = b.text_input("Пароль на вход в интерфейс (пусто — без пароля)", env_values().get("UI_PASSWORD", ""),
                      type="password")

    if st.form_submit_button("Сохранить", type="primary"):
        try:
            Pacing(min_delay=dmin, max_delay=dmax, long_pause_every=every, long_pause_min=lmin * 60,
                   long_pause_max=lmax * 60, daily_cap=cap, window_start=ws, window_end=we, timezone=tz)
        except Exception as e:  # noqa: BLE001 — invalid combination → tell the user, write nothing
            st.error(f"Не сохранено: {e}")
        else:
            envfile.update(env_path(), {
                "DAILY_CAP": cap, "MIN_DELAY_SEC": dmin, "MAX_DELAY_SEC": dmax,
                "LONG_PAUSE_EVERY": every, "LONG_PAUSE_MIN_SEC": lmin * 60, "LONG_PAUSE_MAX_SEC": lmax * 60,
                "SEND_WINDOW": f"{ws:%H:%M}-{we:%H:%M}", "TZ_SEND": tz, "DEFAULT_REGION": region,
                "UI_PASSWORD": pw,
            })
            st.success("Сохранено.")
