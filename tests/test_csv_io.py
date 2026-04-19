"""Tests for the CSV import + phone normalization pipeline."""
import io
import pytest

from core.csv_io import normalize_phone, parse_csv, contacts_to_db_rows


class TestNormalizePhone:
    def test_international_plus(self):
        assert normalize_phone("+79001112233") == "+79001112233"

    def test_ru_8_prefix(self):
        # 8 → +7 conversion only happens with RU region hint
        assert normalize_phone("89001112233", default_region="RU") == "+79001112233"

    def test_spaces_and_dashes(self):
        assert normalize_phone("+7 (900) 111-22-33") == "+79001112233"

    def test_unicode_spaces(self):
        assert normalize_phone("+7\u00a0900\u200b1112233") == "+79001112233"

    def test_invalid(self):
        assert normalize_phone("not a phone") is None
        assert normalize_phone("") is None
        assert normalize_phone(None) is None

    def test_us_number(self):
        assert normalize_phone("+14155552671") == "+14155552671"


class TestParseCSV:
    def _csv(self, text: str):
        return io.StringIO(text)

    def test_basic_columns(self):
        data = "name,phone\nИван,+79001112233\nПётр,89005554433\n"
        valid, invalid, dups = parse_csv(self._csv(data))
        assert len(valid) == 2
        assert invalid == []
        assert dups == []
        assert valid[0].phone == "+79001112233"
        assert valid[1].phone == "+79005554433"

    def test_russian_columns(self):
        data = "имя,телефон\nВася,+79001112233\n"
        valid, _invalid, _dups = parse_csv(self._csv(data))
        assert len(valid) == 1
        assert valid[0].name == "Вася"

    def test_no_phone_column_raises(self):
        data = "name,email\nA,a@b.c\n"
        with pytest.raises(ValueError):
            parse_csv(self._csv(data))

    def test_invalid_phones_collected(self):
        # When phone-column value is neither a valid phone NOR a valid TG
        # username (too short + contains whitespace), the row is rejected.
        data = "name,phone\nOK,+79001112233\nBad,oh no\n"
        valid, invalid, _dups = parse_csv(self._csv(data))
        assert len(valid) == 1
        assert len(invalid) == 1
        assert invalid[0]["name"] == "Bad"

    def test_phone_column_value_rescued_as_username(self):
        # The column is labelled as phone but a row holds a TG handle —
        # parser rescues it via the username fallback (no separate tg_col).
        data = "name,phone\nOK,+79001112233\nAnna,@anna_ivanova\n"
        valid, invalid, _dups = parse_csv(self._csv(data))
        assert len(valid) == 2
        assert len(invalid) == 0
        synthetic = next(c for c in valid if c.phone.startswith("tg:"))
        assert synthetic.telegram_hint == "anna_ivanova"

    def test_tg_hint_column(self):
        # Hints are normalized: leading '@' and wrapping t.me URLs stripped.
        data = "name,phone,telegram\nA,+79001112233,@anna_b\n"
        valid, _invalid, _dups = parse_csv(self._csv(data))
        assert valid[0].telegram_hint == "anna_b"

    def test_extra_columns_preserved(self):
        data = "name,phone,company,city\nA,+79001112233,Acme,Moscow\n"
        valid, _, _ = parse_csv(self._csv(data))
        rows = contacts_to_db_rows(valid)
        assert "Acme" in rows[0]["extra_json"]
        assert "Moscow" in rows[0]["extra_json"]

    def test_deduplicates_same_phone(self):
        # Two rows with the same phone — parser dedups (first wins).
        data = "name,phone\nA,+79001112233\nB,+79001112233\n"
        valid, _, dups = parse_csv(self._csv(data))
        assert len(valid) == 1
        assert valid[0].name == "A"
        assert len(dups) == 1
        assert dups[0]["phone"] == "+79001112233"

    def test_dedup_normalizes_before_comparing(self):
        # Two different formattings of the same phone should dedup.
        data = "name,phone\nA,+79001112233\nB,8 900 111 22 33\n"
        valid, _, dups = parse_csv(self._csv(data))
        assert len(valid) == 1
        assert len(dups) == 1
