from blaster import envfile


def test_update_replaces_keeps_comments_and_appends(tmp_path):
    p = tmp_path / ".env"
    p.write_text("# api\nTG_API_ID=1\nDAILY_CAP=300\n\n# pace\nMIN_DELAY_SEC=30\n", encoding="utf-8")
    envfile.update(p, {"DAILY_CAP": 150, "MIN_DELAY_SEC": "40", "SEND_WINDOW": "09:00-21:00"})
    assert p.read_text(encoding="utf-8") == (
        "# api\nTG_API_ID=1\nDAILY_CAP=150\n\n# pace\nMIN_DELAY_SEC=40\nSEND_WINDOW=09:00-21:00\n")


def test_update_creates_missing_file_and_read_roundtrip(tmp_path):
    p = tmp_path / ".env"
    envfile.update(p, {"A": "1"})
    envfile.update(p, {"B": "x y"})
    assert envfile.read(p) == {"A": "1", "B": "x y"}


def test_read_missing_file_is_empty(tmp_path):
    assert envfile.read(tmp_path / "nope") == {}
