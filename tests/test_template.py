"""Tests for template rendering."""
import json
from core.template import render, placeholders


class TestPlaceholders:
    def test_find_all(self):
        t = "Привет, {name}! Ссылка: {group_link}. Ник: @{tg_username}."
        assert placeholders(t) == ["name", "group_link", "tg_username"]

    def test_deduplicates(self):
        t = "{name} {name} {other}"
        assert placeholders(t) == ["name", "other"]

    def test_empty(self):
        assert placeholders("plain text") == []


class TestRender:
    def test_basic(self):
        t = "Привет, {first_name}!"
        out = render(t, {"name": "Иван Петров"})
        assert out == "Привет, Иван!"

    def test_full_name(self):
        t = "Для {name}"
        out = render(t, {"name": "Иван Петров"})
        assert out == "Для Иван Петров"

    def test_missing_placeholder_survives(self):
        # Missing placeholder should not crash; it stays as {something}.
        t = "Ссылка: {group_link}"
        out = render(t, {"name": "A"})
        assert out == "Ссылка: {group_link}"

    def test_extras_from_json(self):
        t = "Компания: {company}, Город: {city}"
        out = render(t, {
            "name": "A",
            "extra_json": json.dumps({"company": "Acme", "city": "Msk"}),
        })
        assert "Acme" in out and "Msk" in out

    def test_extras_tolerate_bad_json(self):
        t = "Компания: {company}"
        out = render(t, {"name": "A", "extra_json": "not json"})
        assert out == "Компания: {company}"

    def test_empty_name(self):
        t = "Привет{first_name}!"
        out = render(t, {"name": ""})
        assert out == "Привет!"
