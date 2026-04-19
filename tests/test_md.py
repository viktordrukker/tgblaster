"""Tests for the Telethon MD V1 → HTML preview converter."""
from __future__ import annotations

import pytest

from core.md import telegram_md_to_html


class TestBasicRules:
    def test_bold(self):
        assert telegram_md_to_html("**hi**") == "<b>hi</b>"

    def test_italic_is_double_underscore(self):
        """Telethon MD V1: __x__ is italic (CommonMark: bold).
        This mismatch is exactly the reason we wrote this converter."""
        assert telegram_md_to_html("__hi__") == "<i>hi</i>"

    def test_inline_code(self):
        assert telegram_md_to_html("`x`") == "<code>x</code>"

    def test_plain_link(self):
        html = telegram_md_to_html("[go](https://ex.com)")
        assert '<a href="https://ex.com"' in html
        assert ">go</a>" in html
        assert 'rel="noopener noreferrer"' in html

    def test_newlines_become_br(self):
        assert telegram_md_to_html("a\nb") == "a<br>b"


class TestPrecedence:
    def test_bold_inside_code_is_literal(self):
        """Fenced / inline code must swallow ** and __ so users can
        show raw markdown syntax in a code span."""
        out = telegram_md_to_html("`**not bold**`")
        assert "<b>" not in out
        assert "<code>**not bold**</code>" == out

    def test_fenced_block_preserves_content(self):
        out = telegram_md_to_html("```py\nprint('hi')\n```")
        assert '<pre><code class="language-py">' in out
        assert "print(&#x27;hi&#x27;)" in out
        # Line break inside <pre> must NOT become <br> (pre preserves \n).
        assert "<br>" not in out

    def test_link_inside_bold(self):
        out = telegram_md_to_html("**[ok](https://ex.com)**")
        assert "<b>" in out
        assert '<a href="https://ex.com"' in out


class TestXSSHardening:
    def test_script_tag_escaped(self):
        out = telegram_md_to_html("<script>alert(1)</script>")
        assert "<script>" not in out
        assert "&lt;script&gt;" in out

    def test_javascript_link_stripped(self):
        out = telegram_md_to_html("[bad](javascript:alert(1))")
        assert "<a " not in out
        # We keep the visible text so the user sees why it didn't render.
        assert "bad" in out

    def test_data_link_stripped(self):
        out = telegram_md_to_html("[bad](data:text/html,hi)")
        assert "<a " not in out
        assert "bad" in out

    def test_http_ok(self):
        out = telegram_md_to_html("[ok](http://ex.com)")
        assert '<a href="http://ex.com"' in out

    def test_tg_scheme_ok(self):
        out = telegram_md_to_html("[chat](tg://openmessage?user_id=1)")
        assert '<a href="tg://openmessage?user_id=1"' in out

    def test_schemeless_url_auto_https(self):
        out = telegram_md_to_html("[click](t.me/foo)")
        assert '<a href="https://t.me/foo"' in out


class TestBareUrlAutolink:
    """Telegram auto-linkifies bare URLs in all clients. Our preview
    mirrors that so `Скидываю {group_link}` in a template (where
    group_link is substituted to a plain URL) renders as a clickable
    link, not grey text."""

    def test_https_bare_url_linkified(self):
        out = telegram_md_to_html("Go to https://ex.com now")
        assert '<a href="https://ex.com"' in out

    def test_t_me_bare_url_linkified(self):
        out = telegram_md_to_html("Join t.me/+otmxZbndC3hhZTMy")
        assert '<a href="https://t.me/+otmxZbndC3hhZTMy"' in out

    def test_explicit_markdown_link_not_double_wrapped(self):
        # [text](url) path must still win over bare-URL; the test
        # guards against linkifying the URL inside the markdown source.
        out = telegram_md_to_html("[click](https://ex.com)")
        assert out.count("<a ") == 1
        assert ">click</a>" in out

    def test_trailing_period_excluded(self):
        out = telegram_md_to_html("See https://ex.com. Thanks!")
        # The period must land OUTSIDE the anchor so the link destination
        # stays intact.
        assert '<a href="https://ex.com"' in out
        assert ">https://ex.com</a>." in out

    def test_url_inside_parens_not_swallowed(self):
        out = telegram_md_to_html("(see http://ex.com)")
        assert '<a href="http://ex.com"' in out
        assert ">http://ex.com</a>)" in out

    def test_url_inside_inline_code_kept_literal(self):
        # `code` stash runs BEFORE the bare-URL pass, so the URL stays
        # as literal inside the <code> span.
        out = telegram_md_to_html("`curl https://ex.com`")
        assert "<a " not in out
        assert "<code>curl https://ex.com</code>" == out


class TestNullAndEmpty:
    def test_none_returns_empty(self):
        assert telegram_md_to_html(None) == ""

    def test_empty_returns_empty(self):
        assert telegram_md_to_html("") == ""

    def test_plain_text_is_just_escaped(self):
        assert telegram_md_to_html("a & b") == "a &amp; b"


class TestRealisticTemplate:
    def test_full_message(self):
        tpl = (
            "**Привет, {name}!**\n\n"
            "Это __итальянский__ стиль, а вот `код`.\n"
            "Подробнее: [наш канал](https://t.me/example)"
        )
        out = telegram_md_to_html(tpl)
        assert "<b>Привет, {name}!</b>" in out
        assert "<i>итальянский</i>" in out
        assert "<code>код</code>" in out
        assert '<a href="https://t.me/example"' in out
        assert "<br>" in out
