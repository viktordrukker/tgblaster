"""Telethon Markdown V1 → HTML converter for the Compose preview.

Telethon's `parse_mode='md'` uses a specific markdown flavor that differs
from CommonMark in three important ways:

* `__text__` is *italic* (CommonMark: bold)
* no strikethrough / no tables / no lists (just inline formatting)
* URL allow-list is strict on the send side — we mirror it on the
  preview side so the user can't trick themselves into thinking a
  `javascript:` link will land

Why not reuse `telethon.extensions.markdown.parse()`? It returns the
text plus MTProto entities (offset+length pairs) — perfect for sending,
but building back to HTML from offsets is more code than the five
regex passes below, and it drags a Telethon import into the UI layer.

Precedence (first-wins, non-overlapping):
  1. fenced code blocks   ```lang\ncode\n```   → <pre><code>
  2. inline code          `text`                → <code>
  3. links                [text](url)           → <a href="url">text</a>
  4. bold                 **text**              → <b>text</b>
  5. italic               __text__              → <i>text</i>
  6. newlines             \\n                    → <br>
"""
from __future__ import annotations

import html as _html
import re
from urllib.parse import urlparse


_SAFE_URL_SCHEMES = {"http", "https", "tg", "mailto"}


def _safe_url(raw: str) -> str | None:
    """Return the URL if it uses a whitelisted scheme, else None.

    Refuses `javascript:`, `data:`, and any empty/weird input so the
    preview can't render an XSS vector even if the user pastes one.
    Returns the URL unchanged on success (caller still HTML-escapes it
    as an attribute value).
    """
    if not raw:
        return None
    try:
        scheme = urlparse(raw).scheme.lower()
    except Exception:  # noqa: BLE001
        return None
    if scheme in _SAFE_URL_SCHEMES:
        return raw
    # If *any* scheme is present (even a rejected one like `data:` or
    # `javascript:`), don't try to "help" by prepending https://.
    # That's how we'd accidentally render `https://data:text/html,hi`.
    if scheme:
        return None
    # Scheme-less URL (e.g. `t.me/foo`, `example.com/page`) — prepend
    # https if it looks at all link-shaped.
    if "/" in raw or "." in raw:
        return "https://" + raw
    return None


# Token placeholders used while protecting already-converted segments
# from later passes. The sentinel char is a private-use Unicode point
# that won't appear in user text.
_SENTINEL = "\uE000"


def _stash(store: list[str], html: str) -> str:
    store.append(html)
    return f"{_SENTINEL}{len(store) - 1}{_SENTINEL}"


def _unstash(text: str, store: list[str]) -> str:
    """Iteratively replace placeholders until no sentinels remain.

    Nested stashes (e.g. a link inside a bold span — each pass stashed
    its own wrapper, so the bold wrapper contains a link-placeholder)
    require the un-stash to recurse through layers. A fixed number of
    passes bounded by `len(store)` guarantees termination even in the
    pathological case."""
    pattern = re.compile(rf"{_SENTINEL}(\d+){_SENTINEL}")

    def _r(m: re.Match) -> str:
        return store[int(m.group(1))]

    for _ in range(len(store) + 1):
        new_text = pattern.sub(_r, text)
        if new_text == text:
            return new_text
        text = new_text
    return text


_FENCED_RE = re.compile(r"```([a-zA-Z0-9_+-]*)\n?(.*?)```", re.DOTALL)
_INLINE_CODE_RE = re.compile(r"`([^`\n]+?)`")
_LINK_RE = re.compile(r"\[([^\]]+?)\]\(([^)\s]+)\)")
_BOLD_RE = re.compile(r"\*\*(.+?)\*\*", re.DOTALL)
_ITALIC_RE = re.compile(r"__(.+?)__", re.DOTALL)

# Bare URL auto-linkify — Telegram clients turn any plain http/https/
# tg/t.me URL into a tappable link automatically. We mirror that here
# so the preview matches what recipients will see. Runs AFTER the
# `[text](url)` pass (which stashes its output into a sentinel), so a
# link typed in explicit markdown form isn't double-wrapped.
#
# Trailing punctuation `.,;:!?` is excluded via a negative look-behind
# at the match tail (handled in the substitution — see _bare_url_sub).
_BARE_URL_RE = re.compile(
    r"(?<![\w@\-/])"                    # not glued to a word, @, - or /
    r"((?:https?://|tg://|t\.me/)"      # schemes we care about
    r"[^\s<>\"']+)"                     # URL body — anything up to whitespace
)

def telegram_md_to_html(text: str) -> str:
    """Render Telethon-Markdown-V1 into safe HTML for preview.

    Escape first, then run regex passes against the escaped text. This
    means a literal `<script>` typed by the user survives as
    `&lt;script&gt;` in the output — we never feed raw HTML into the
    page.
    """
    if text is None:
        return ""
    # 1) Escape all HTML specials so user text is inert.
    escaped = _html.escape(text, quote=True)

    stash: list[str] = []

    # 2) Fenced code blocks (run first — they eat stars/underscores
    #    inside; don't want later passes touching those).
    def _fenced(m: re.Match) -> str:
        lang = m.group(1) or ""
        body = m.group(2)
        cls = f' class="language-{_html.escape(lang, quote=True)}"' if lang else ""
        return _stash(stash, f"<pre><code{cls}>{body}</code></pre>")
    escaped = _FENCED_RE.sub(_fenced, escaped)

    # 3) Inline code.
    escaped = _INLINE_CODE_RE.sub(
        lambda m: _stash(stash, f"<code>{m.group(1)}</code>"),
        escaped,
    )

    # 4) Links.
    def _link(m: re.Match) -> str:
        label = m.group(1)
        # URL was HTML-escaped in step 1 — unescape ONLY for the safety
        # check (so we compare real scheme), then re-escape for the
        # attribute value.
        raw = _html.unescape(m.group(2))
        ok = _safe_url(raw)
        if ok is None:
            # Render the link text as plain text; strip the URL silently.
            # This keeps the preview honest about what will be sent.
            return label
        href = _html.escape(ok, quote=True)
        return _stash(stash, f'<a href="{href}" target="_blank" rel="noopener noreferrer">{label}</a>')
    escaped = _LINK_RE.sub(_link, escaped)

    # 4b) Bare URL auto-linkify — turn plain `https://...`, `t.me/...`,
    #     `tg://...` into anchors. Telegram does this natively in all
    #     clients; the preview matches that behavior so a template like
    #     `Скидываю ссылку: {group_link}` shows as a tappable link.
    def _bare_url(m: re.Match) -> str:
        raw = _html.unescape(m.group(1))
        # Trim trailing sentence-punctuation that the greedy match ate.
        trailing = ""
        while raw and raw[-1] in ".,;:!?)]»":
            trailing = raw[-1] + trailing
            raw = raw[:-1]
        ok = _safe_url(raw)
        if ok is None:
            return m.group(0)
        href = _html.escape(ok, quote=True)
        label = _html.escape(raw, quote=True)
        anchor = _stash(
            stash,
            f'<a href="{href}" target="_blank" rel="noopener noreferrer">{label}</a>',
        )
        return anchor + trailing
    escaped = _BARE_URL_RE.sub(_bare_url, escaped)

    # 5) Bold **…**
    escaped = _BOLD_RE.sub(
        lambda m: _stash(stash, f"<b>{m.group(1)}</b>"),
        escaped,
    )

    # 6) Italic __…__ (Telethon V1 — NOT bold like CommonMark).
    escaped = _ITALIC_RE.sub(
        lambda m: _stash(stash, f"<i>{m.group(1)}</i>"),
        escaped,
    )

    # 7) Line breaks — do this BEFORE un-stashing so the stashed
    #    content (fenced <pre>, inline <code>, <a>, <b>, <i>) keeps
    #    its literal \n. Only the surrounding prose gets <br>.
    escaped = escaped.replace("\n", "<br>")

    # 8) Put stashed HTML back in place.
    return _unstash(escaped, stash)
