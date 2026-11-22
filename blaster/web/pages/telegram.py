import streamlit as st

from blaster import telegram as tg
from blaster.web.common import account_line, ctx, run_tg

c = ctx()
ss = st.session_state
st.title("🔌 Подключение Telegram")

if not c.settings.api_id or not c.settings.api_hash:
    st.error("В .env нет TG_API_ID / TG_API_HASH. Их выдаёт https://my.telegram.org → API development tools.")
    st.stop()

st.caption(f"Сессия: `{c.settings.session_path.name}`")


def check() -> None:
    async def who(client):
        return await client.get_me() if await client.is_user_authorized() else None
    me, err = run_tg(c, who)
    if err:
        ss["tg_err"] = err
        return
    ss.pop("tg_err", None)
    tg.save_account(c.settings, me)
    ss["tg_checked"] = True


if c.jobs.status().busy:
    st.info("Идёт фоновая операция — Telegram занят ею. Показываю последний известный вход.")
elif not ss.get("tg_checked"):
    with st.spinner("Проверяю сессию…"):
        check()

acc = tg.load_account(c.settings)
if ss.get("tg_err"):
    st.warning(ss["tg_err"])
if acc:
    st.success(f"Вошли как **{account_line(acc)}**, id {acc['id']}")
else:
    st.warning("Вход не выполнен.")
if st.button("Проверить ещё раз"):
    check()
    st.rerun()

if acc:
    with st.expander("Сменить аккаунт"):
        st.write("Сначала выйти из текущего — потом войти новым номером.")
        if st.button("Выйти из аккаунта"):
            _, err = run_tg(c, lambda cl: cl.log_out())
            if err:
                st.error(err)
            else:
                tg.save_account(c.settings, None)
                st.rerun()
    st.stop()

# ---- login: phone → code → (2FA password) ----------------------------------
def logged_in(me) -> None:
    tg.save_account(c.settings, me)
    for k in ("login_step", "login_phone", "login_hash"):
        ss.pop(k, None)
    st.rerun()


st.subheader("Вход")
step = ss.get("login_step", "phone")

if step == "phone":
    with st.form("phone"):
        phone = st.text_input("Телефон аккаунта-отправителя", placeholder="+998 90 123 45 67")
        if st.form_submit_button("Получить код", type="primary") and phone.strip():
            h, err = run_tg(c, lambda cl: tg.send_code(cl, phone))
            if err:
                st.error(err)
            else:
                ss.update(login_step="code", login_phone=phone, login_hash=h)
                st.rerun()

elif step == "code":
    st.write(f"Код отправлен в Telegram на {ss['login_phone']} (сообщение от «Telegram»).")
    with st.form("code"):
        code = st.text_input("Код из Telegram")
        if st.form_submit_button("Войти", type="primary") and code.strip():
            me, err = run_tg(c, lambda cl: tg.sign_in_code(cl, ss["login_phone"], code, ss["login_hash"]))
            if err:
                st.error(err)
            elif me is None:
                ss["login_step"] = "password"
                st.rerun()
            else:
                logged_in(me)
    if st.button("Другой номер / новый код"):
        ss["login_step"] = "phone"
        st.rerun()

elif step == "password":
    with st.form("password"):
        pw = st.text_input("Пароль двухэтапной проверки (2FA)", type="password")
        if st.form_submit_button("Войти", type="primary") and pw:
            me, err = run_tg(c, lambda cl: tg.sign_in_password(cl, pw))
            if err:
                st.error(err)
            else:
                logged_in(me)
