import csv
import io

from blaster.report import csv_text


def test_csv_neutralises_formulas_and_unions_columns():
    text = csv_text([{"name": "=HYPERLINK(1)", "phone": "+998"}, {"name": "Азиз", "extra": None}])
    rows = list(csv.DictReader(io.StringIO(text)))
    assert rows[0]["name"] == "'=HYPERLINK(1)"
    assert rows[1] == {"name": "Азиз", "phone": "", "extra": ""}


def test_empty_report_is_empty():
    assert csv_text([]) == ""


def test_write_csv_creates_folders_and_excel_bom(tmp_path):
    from blaster.report import write_csv
    out = tmp_path / "a" / "b" / "r.csv"
    write_csv(out, [{"x": 1}])
    write_csv(out, [{"x": 2}])                       # existing folder is fine
    assert out.read_bytes().startswith(b"\xef\xbb\xbfx")
