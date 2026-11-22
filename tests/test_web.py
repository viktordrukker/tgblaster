"""Web UI pages driven like a user would, Telegram and job spawning faked."""
from pathlib import Path
from types import SimpleNamespace

import pytest
from streamlit.testing.v1 import AppTest
from telethon import errors

from blaster import envfile
from blaster import telegram as tg
from blaster.db import Database
from blaster.importer import parse_table
from blaster.jobs import Jobs
from blaster.tables import Sheet

PAGES = Path(__file__).resolve().parents[1] / "blaster" / "web" / "pages"
ME = SimpleNamespace(id=7, first_name="Виктор", last_name="", username="vd", phone="998901112233")
ROWS = [["ФИО", "Телефон", "Telegram", "Компания"],
        ["Азиз Каримов", "+998 90 123 45 67", "@aziz_k", "EPAM"],
        ["Иванов Иван", "93 555 12 34", "", "Uzum"],
        ["Dilnoza", "", "@dilnoza_r", ""]]


class FakeTG:
    def __init__(self, authorized=False, need_password=False):
        self.authorized, self.need_password = authorized, need_password
        self.sent = []

    async def connect(self):
        pass

    def is_connected(self):
        return True

    async def disconnect(self):
        pass

    async def is_user_authorized(self):
        return self.authorized

    async def get_me(self):
        return ME if self.authorized else None

    async def send_code_request(self, phone):
        return SimpleNamespace(phone_code_hash="H")

    async def sign_in(self, phone=None, code=None, *, password=None, phone_code_hash=None):
        if code and self.need_password:
            raise errors.SessionPasswordNeededError(None)
        self.authorized = True
        return ME

    async def send_message(self, peer, text, **kw):
        self.sent.append((peer, text, kw))
        return SimpleNamespace(id=1)


@pytest.fixture
def home(tmp_path, monkeypatch):
    monkeypatch.setenv("BLASTER_HOME", str(tmp_path))
    for k in ("TG_API_ID", "TG_API_HASH", "UI_PASSWORD", "DAILY_CAP"):
        monkeypatch.delenv(k, raising=False)
    envfile.update(tmp_path / ".env", {"TG_API_ID": "1", "TG_API_HASH": "h", "DAILY_CAP": "150"})
    (tmp_path / "campaigns" / "examples").mkdir(parents=True)
    (tmp_path / "campaigns" / "examples" / "invite.md").write_text("Привет, {first_name|друзья}!", encoding="utf-8")
    return tmp_path


@pytest.fixture
def fake_tg(monkeypatch):
    client = FakeTG()
    monkeypatch.setattr(tg, "make_client", lambda _s: client)
    return client


@pytest.fixture
def started(monkeypatch):
    calls = []

    def fake_start(self, kind, argv, meta=None, resume_argv=None):
        calls.append((kind, argv, meta, resume_argv))
    monkeypatch.setattr(Jobs, "start", fake_start)
    return calls


def png(path, color, size=(10, 10)):
    from PIL import Image
    Image.new("RGB", size, color).save(path)


def db_of(home) -> Database:
    return Database(home / "data" / "blaster.db")


def seed(home, resolve=True, campaign=True) -> Database:
    db = db_of(home)
    db.upsert_contacts("meetup", parse_table([Sheet("s", ROWS)]).contacts)
    if resolve:
        for r in db.contacts("meetup"):
            db.mark_resolved(r["id"], 100 + r["id"], 1, None, r["first_name"], "test")
    if campaign:
        db.create_campaign("dec", "meetup", "Привет, {first_name|друзья}!", None)
    return db


def page(name: str) -> AppTest:
    at = AppTest.from_file(str(PAGES / f"{name}.py"), default_timeout=30)
    at.run()
    assert not at.exception, at.exception
    return at


def button(at, label):
    return next(b for b in at.button if b.label.startswith(label))


@pytest.mark.parametrize("name", ["telegram", "contacts", "resolve", "message", "send", "settings"])
def test_every_page_opens_on_an_empty_install(home, fake_tg, name):
    page(name)


def test_login_phone_code_password(home, fake_tg):
    fake_tg.need_password = True
    at = page("telegram")
    assert any("Вход не выполнен" in w.value for w in at.warning)
    at.text_input[0].input("+998 90 111 22 33")
    button(at, "Получить код").click().run()
    at.text_input[0].input("12345")
    button(at, "Войти").click().run()
    assert at.session_state["login_step"] == "password"
    at.text_input[0].input("secret")
    button(at, "Войти").click().run()
    assert not at.exception
    assert any("Виктор" in s.value for s in at.success)
    assert tg.load_account(SimpleNamespace(db_path=home / "data" / "blaster.db"))["username"] == "vd"


def test_contacts_shows_detection_and_imports_after_ok(home, fake_tg):
    at = AppTest.from_file(str(PAGES / "contacts.py"), default_timeout=30)
    at.session_state["sheets"] = [Sheet("Лист1", ROWS)]
    at.session_state["src_name"] = "meetup.xlsx"
    at.run()
    assert not at.exception
    assert at.multiselect[0].value == [1]                 # Телефон column detected
    assert {m.label: m.value for m in at.metric}["К рассылке"] == "3"
    assert db_of(home).lists() == []                       # nothing imported before the click
    at.text_input(key="list_name").set_value("meetup").run()
    button(at, "Импортировать 3").click().run()
    assert db_of(home).list_stats("meetup")["total"] == 3


def test_contacts_role_override_changes_result(home, fake_tg):
    at = AppTest.from_file(str(PAGES / "contacts.py"), default_timeout=30)
    at.session_state["sheets"] = [Sheet("Лист1", ROWS)]
    at.session_state["src_name"] = "x.csv"
    at.run()
    at.multiselect[1].set_value([]).run()                  # drop the Telegram column
    assert {m.label: m.value for m in at.metric}["К рассылке"] == "2"   # Dilnoza had only @username


def test_message_create_then_test_to_saved_messages(home, fake_tg):
    seed(home, campaign=False)
    fake_tg.authorized = True
    at = page("message")
    at.text_input[0].input("dec").run()
    button(at, "Сохранить").click().run()
    camp = db_of(home).get_campaign("dec")
    assert camp["template"] == "Привет, {first_name|друзья}!"
    at.selectbox[0].set_value("dec").run()
    button(at, "Отправить тест себе").click().run()
    assert not at.exception
    peer, text, _ = fake_tg.sent[-1]
    assert peer == "me" and text.startswith("Привет, ")
    at.run()
    button(at, "Отправить тест себе в Избранное ещё раз").click().run()
    assert not at.exception and len(fake_tg.sent) == 2


def test_send_is_locked_until_tested_and_confirmed(home, fake_tg, started):
    seed(home)
    fake_tg.authorized = True
    at = page("send")
    assert button(at, "▶ Запустить").disabled
    msg = page("message")
    msg.selectbox[0].set_value("dec").run()
    button(msg, "Отправить тест себе").click().run()

    at = page("send")
    assert button(at, "▶ Запустить").disabled               # still needs the explicit checkbox
    next(x for x in at.checkbox if x.label.startswith("Я посмотрел")).check().run()
    button(at, "▶ Запустить").click().run()
    kind, argv, meta, resume_argv = started[-1]
    assert kind == "send" and meta == {"campaign": "dec"}
    assert argv[-4:] == ["send", "dec", "--until-done", "--yes"]


def test_editing_after_test_locks_sending_again(home, fake_tg):
    db = seed(home)
    fake_tg.authorized = True
    msg = page("message")
    msg.selectbox[0].set_value("dec").run()
    button(msg, "Отправить тест себе").click().run()
    db.update_campaign(db.get_campaign("dec")["id"], template="Другой текст")
    at = page("send")
    assert any("тест этой версии" in w.value for w in at.warning)


def test_settings_writes_env(home, fake_tg):
    at = page("settings")
    at.number_input[0].set_value(120)
    button(at, "Сохранить").click().run()
    env = envfile.read(home / ".env")
    assert env["DAILY_CAP"] == "120" and env["MIN_DELAY_SEC"] == "40" and env["TG_API_ID"] == "1"


def test_settings_rejects_min_above_max(home, fake_tg):
    at = page("settings")
    at.number_input[1].set_value(500)
    button(at, "Сохранить").click().run()
    assert at.error and envfile.read(home / ".env").get("MIN_DELAY_SEC") is None


def test_optout_added_from_send_page(home, fake_tg):
    seed(home)
    at = page("send")
    at.text_area[0].input("90 123 45 67\n???")
    button(at, "Добавить").click().run()
    assert [o["value"] for o in db_of(home).optouts()] == ["+998901234567"]
    assert any("???" in e.value for e in at.error)


APP = PAGES.parent / "app.py"


def test_app_opens_with_navigation(home, fake_tg):
    at = AppTest.from_file(str(APP), default_timeout=30)
    at.run()
    assert not at.exception
    assert any("Telegram" in t.value for t in at.title)    # first page is the login page


def test_app_password_gate(home, fake_tg):
    envfile.update(home / ".env", {"UI_PASSWORD": "s3cret"})
    at = AppTest.from_file(str(APP), default_timeout=30)
    at.run()
    assert [t.value for t in at.title] == ["TGBlaster"]     # nothing else is rendered
    at.text_input[0].input("wrong").run()
    assert at.error
    at.text_input[0].input("s3cret").run()
    assert any("Telegram" in t.value for t in at.title)


def test_resolve_page_shows_progress_and_starts_lookup(home, fake_tg, started):
    seed(home, resolve=False, campaign=False)
    at = page("resolve")
    assert any("Проверено 0 из 3" in (p.proto.text or "") for p in at.get("progress"))
    button(at, "Найти в Telegram").click().run()
    kind, argv, meta, _ = started[-1]
    assert kind == "resolve" and meta == {"list": "meetup"} and argv[-3:] == ["resolve", "--list", "meetup"]


def test_sidebar_shows_how_the_last_job_ended(home, fake_tg):
    import sys
    import time
    jobs = Jobs(home / "data" / "jobs")
    jobs.start("send", [sys.executable, "-c", "raise SystemExit(2)"], meta={"campaign": "dec"})
    for _ in range(100):
        if not jobs.status().busy:
            break
        time.sleep(0.1)
    at = AppTest.from_file(str(APP), default_timeout=30)
    at.run()
    captions = [x.value for x in at.sidebar.caption]
    assert "Рассылка «dec»: завершена, код 2" in captions
    assert "Telegram: не выполнен" in captions


def test_new_banner_in_the_same_file_locks_sending_again(home, fake_tg):
    db = seed(home)
    banner = home / "campaigns" / "banner.png"
    png(banner, "red")
    db.update_campaign(db.get_campaign("dec")["id"], image_path=str(banner))
    fake_tg.authorized = True
    msg = page("message")
    msg.selectbox[0].set_value("dec").run()
    button(msg, "Отправить тест себе").click().run()
    assert not any("тест этой версии" in w.value for w in page("send").warning)
    png(banner, "blue", size=(20, 20))
    assert any("тест этой версии" in w.value for w in page("send").warning)


def test_upload_that_is_not_an_image_is_refused(home, fake_tg):
    from blaster.web.common import save_image
    with pytest.raises(ValueError, match="не картинка"):
        save_image(home / "campaigns" / "dec", "banner.png", b"not an image")
    assert not (home / "campaigns" / "dec" / "banner.png").exists()
    p = save_image(home / "campaigns" / "dec", "my banner.png", _png_bytes())
    assert Path(p).name == "my_banner.png" and Path(p).is_file()


def _png_bytes() -> bytes:
    import io
    from PIL import Image
    buf = io.BytesIO()
    Image.new("RGB", (4, 4), "red").save(buf, "PNG")
    return buf.getvalue()
