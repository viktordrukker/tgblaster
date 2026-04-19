"""Read-receipt fetching via Telethon's `GetPeerDialogsRequest`.

Telegram/MTProto exposes per-dialog cursors but NOT per-message delivery.
For each peer we fetch `read_outbox_max_id` — the highest message id of
ours the peer has read. Mapping this back to per-row `send_log.read_at`
is done in `Database.apply_read_receipts`.

This is a READ-only protocol call — no ban-risk surface, no pacing
concerns, no random_id bookkeeping.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Iterable

from telethon import TelegramClient
from telethon.tl.functions.messages import GetPeerDialogsRequest
from telethon.tl.types import InputDialogPeer, InputPeerUser, PeerUser


log = logging.getLogger(__name__)


BATCH_SIZE = 100
"""MTProto soft-limit: GetPeerDialogsRequest accepts up to ~100 peers
per call before starting to truncate silently."""

BETWEEN_BATCH_SLEEP = 0.3
"""Short pause between batches — read-only, but mirrors the pacing
discipline of the send path."""

PER_BATCH_TIMEOUT = 30
"""Wall-clock cap per RPC call — Telegram answers in ≤ 1–2 s normally."""


def _peer_user_id(peer) -> int | None:
    """Extract the user_id from a Dialog.peer (which is a TypePeer like
    PeerUser / PeerChat / PeerChannel). We only care about PeerUser."""
    if isinstance(peer, PeerUser):
        return int(peer.user_id)
    return None


async def fetch_read_outbox_cursors(
    client: TelegramClient,
    peers: Iterable[tuple[int, int | None]],
) -> tuple[dict[int, int], int]:
    """Query `read_outbox_max_id` for a set of user peers.

    Args:
        client: Connected Telethon client.
        peers: Iterable of (tg_user_id, tg_access_hash). Entries with
               `access_hash is None` are skipped — we can't build an
               `InputPeerUser` without one, and re-resolving per peer
               would cost one extra RPC each. The caller sees the count
               in the returned `skipped` int and can nudge the user to
               re-run Resolve.

    Returns:
        (cursors, skipped_count) where `cursors[tg_user_id] = read_outbox_max_id`.
    """
    input_peers: list[tuple[int, InputDialogPeer]] = []
    skipped = 0
    for user_id, access_hash in peers:
        if access_hash is None:
            skipped += 1
            continue
        ip = InputPeerUser(user_id=int(user_id), access_hash=int(access_hash))
        input_peers.append((int(user_id), InputDialogPeer(peer=ip)))

    cursors: dict[int, int] = {}
    total = len(input_peers)
    for i in range(0, total, BATCH_SIZE):
        chunk = input_peers[i : i + BATCH_SIZE]
        req = GetPeerDialogsRequest(peers=[p for _, p in chunk])
        try:
            resp = await asyncio.wait_for(client(req), timeout=PER_BATCH_TIMEOUT)
        except asyncio.TimeoutError:
            log.warning(
                "GetPeerDialogsRequest timed out for chunk %d..%d (skipping chunk)",
                i, i + len(chunk),
            )
            continue
        except Exception:  # noqa: BLE001
            log.exception("GetPeerDialogsRequest failed for chunk %d", i)
            continue
        for d in getattr(resp, "dialogs", []) or []:
            uid = _peer_user_id(getattr(d, "peer", None))
            if uid is None:
                continue
            cursors[uid] = int(getattr(d, "read_outbox_max_id", 0) or 0)
        if i + BATCH_SIZE < total:
            await asyncio.sleep(BETWEEN_BATCH_SLEEP)

    return cursors, skipped
