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
