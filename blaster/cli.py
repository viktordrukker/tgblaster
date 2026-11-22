"""Command line interface.

Typical flow:
    blaster inspect contacts.xlsx            # see how the file is understood
    blaster import contacts.xlsx --list meetup-dec
    blaster login                            # once, interactive
    blaster resolve --list meetup-dec
    blaster campaign create dec-invite --list meetup-dec --template campaigns/invite.md
    blaster preview dec-invite
    blaster test dec-invite                  # one message to Saved Messages
    blaster send dec-invite --until-done
"""
from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import logging
import signal
import sys
from pathlib import Path

from . import __version__
from .config import Settings
from .db import Database, LockHeld
from .importer import Overrides, ParseResult, parse_table
from .report import write_csv as _write_csv
from .tables import load

# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #


def _out(msg: str = "") -> None:
    stamp = dt.datetime.now().strftime("%H:%M:%S")
    print(f"{stamp}  {msg}" if msg else "", flush=True)


def _confirm(question: str, assume_yes: bool) -> bool:
    if assume_yes:
        return True
    if not sys.stdin.isatty():
        print(f"{question} — нужен флаг --yes (нет интерактивного терминала)")
        return False
    return input(f"{question} [y/N]: ").strip().lower() in ("y", "yes", "д", "да")


def _overrides(a) -> Overrides:
    return Overrides(header_row=a.header_row, phone=a.phone_col, username=a.username_col,
                     name=a.name_col, first_name=a.first_name_col, last_name=a.last_name_col,
                     consent=a.consent_col, name_order=a.name_order)


def _parse(a, settings: Settings) -> ParseResult:
    sheets = load(a.file)
    if len(sheets) > 1:
        _out(f"Листы в файле: {', '.join(s.name for s in sheets)}")
    return parse_table(sheets, region=a.region or settings.region, sheet=a.sheet,
                       overrides=_overrides(a))


def _print_parse(r: ParseResult, sample: int) -> None:
    print(f"\nЛист: «{r.sheet}». Колонки определены так:")
    print(r.plan.describe())
    print()
    print(r.summary())
    if r.contacts:
        print(f"\nПримеры ({min(sample, len(r.contacts))}):")
        for c in r.contacts[:sample]:
            extra = ", ".join(f"{k}={v}" for k, v in list(c.extra.items())[:3])
            print(f"  стр.{c.row_no:>4}  имя={c.first_name or '—':<12} ФИО={c.name or '—':<28} "
                  f"тел={','.join(c.phones) or '—':<16} tg={'@' + c.username if c.username else '—':<18} {extra[:60]}")
    if r.rejected:
        print(f"\nОтброшено (первые {min(10, len(r.rejected))}):")
        for x in r.rejected[:10]:
            print(f"  стр.{x['row']:>4}  {x['reason']}: {x['data'][:90]}")
    if r.duplicates:
        print(f"\nДубликаты (первые {min(5, len(r.duplicates))}):")
        for x in r.duplicates[:5]:
            print(f"  стр.{x['row']:>4} = стр.{x['duplicate_of_row']}: {x['data'][:90]}")


def _install_stop(stop: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()

    def _handler(*_):
        if not stop.is_set():
            print("\n⏹ Останавливаюсь после текущего шага… (ещё раз Ctrl+C — жёстко)", flush=True)
            stop.set()
        else:
            raise KeyboardInterrupt
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handler)
        except (NotImplementedError, RuntimeError, ValueError):
            signal.signal(sig, lambda *_: loop.call_soon_threadsafe(_handler))


async def _with_client(settings: Settings, db: Database, fn, *, login: bool = False):
    from . import telegram as tg

    try:
        async with tg.connected(settings, db) as client:
            if login:
                me = await tg.interactive_login(client)
            else:
                me = await tg.ensure_authorized(client)
            tg.save_account(settings, me)
            return await fn(client, me)
    except LockHeld as e:
        raise SystemExit(str(e)) from None


def _campaign(db: Database, ref: str) -> dict:
    camp = db.get_campaign(ref)
    if camp is None:
        raise SystemExit(f"Кампания «{ref}» не найдена. Есть: {[c['name'] for c in db.campaigns()]}")
    return camp


# --------------------------------------------------------------------------- #
# commands
# --------------------------------------------------------------------------- #

def cmd_inspect(a, s: Settings, db: Database) -> int:
    r = _parse(a, s)
    _print_parse(r, a.sample)
    return 0


def cmd_import(a, s: Settings, db: Database) -> int:
    r = _parse(a, s)
    _print_parse(r, a.sample)
    if not r.contacts:
        print("\nНечего импортировать.")
        return 1
    if not _confirm(f"\nИмпортировать {len(r.contacts)} контактов в список «{a.list}»?", a.yes):
        return 1
    ins, upd = db.upsert_contacts(a.list, r.contacts)
    stamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    rej_path = s.reports_dir / f"import-{a.list}-{stamp}-rejected.csv"
    _write_csv(rej_path, r.rejected + [dict(d, reason="дубликат") for d in r.duplicates])
    print(f"\n✓ Список «{a.list}»: новых {ins}, обновлено {upd}. Отброшенные/дубли → {rej_path}")
    st = db.list_stats(a.list)
    print(f"  всего в списке {st['total']}, ждут резолва {st['pending']}")
    return 0


def cmd_lists(a, s: Settings, db: Database) -> int:
    rows = db.lists()
    if not rows:
        print("Списков нет. Начни с: blaster import FILE --list NAME")
    for r in rows:
        print(f"{r['list_name']:<24} всего {r['total']:>5}  найдено в TG {r['resolved']:>5}  "
              f"нет в TG {r['not_found']:>4}  ошибок {r['errors']:>3}  ждут {r['pending']:>4}")
    return 0


def cmd_contacts(a, s: Settings, db: Database) -> int:
    rows = db.contacts(a.list, a.status)
    flat = [{"row": r["row_no"], "name": r["name"], "first_name": r["first_name"],
             "phones": " ".join(r["phones"]), "username": r["username"], "status": r["status"],
             "tg_user_id": r["tg_user_id"], "tg_username": r["tg_username"],
             "resolved_via": r["resolved_via"], "error": r["resolve_error"]} for r in rows]
    if a.out:
        _write_csv(Path(a.out), flat)
        print(f"✓ {len(flat)} строк → {a.out}")
    else:
        for r in flat:
            print(f"стр.{r['row'] or 0:>4} {r['status']:<10} {r['first_name'] or '—':<12} "
                  f"{r['phones'] or '—':<16} {('@' + r['username']) if r['username'] else '':<18} "
                  f"{r['error'] or ''}")
    return 0


def cmd_login(a, s: Settings, db: Database) -> int:
    async def go(client, me):
        print(f"✓ Вошли как {me.first_name or ''} {me.last_name or ''} "
              f"(@{me.username or '—'}, id {me.id}). Сессия: {s.session_path}")
    asyncio.run(_with_client(s, db, go, login=True))
    return 0


def cmd_whoami(a, s: Settings, db: Database) -> int:
    async def go(client, me):
        print(f"{me.first_name or ''} {me.last_name or ''} @{me.username or '—'} "
              f"id={me.id} phone=+{me.phone}")
    asyncio.run(_with_client(s, db, go))
    return 0


def cmd_resolve(a, s: Settings, db: Database) -> int:
    from . import telegram as tg

    if not db.list_stats(a.list)["total"]:
        raise SystemExit(f"Список «{a.list}» пуст или не существует.")

    async def go(client, me):
        stop = asyncio.Event()
        _install_stop(stop)
        _out(f"Резолв списка «{a.list}» от имени @{me.username or me.id}…")
        st = await tg.resolve_list(client, db, a.list, retry_errors=a.retry_errors,
                                   keep_contacts=a.keep_contacts,
                                   max_flood_wait=s.pacing.max_flood_wait, stop=stop, out=_out)
        _out(f"Итог: найдено {st['resolved']}, нет в TG {st['not_found']}, ошибок {st['errors']}"
             + (f" — прервано: {st['stopped']}" if st["stopped"] else ""))
        ls = db.list_stats(a.list)
        _out(f"Список: всего {ls['total']}, готовы к отправке {ls['resolved']}, "
             f"нет в TG {ls['not_found']}, ошибок {ls['errors']}, ждут {ls['pending']}")
        return 0 if not st["stopped"] else 2
    return asyncio.run(_with_client(s, db, go))


def _read_template(path: str) -> tuple[str, str]:
    p = Path(path)
    text = p.read_text(encoding="utf-8-sig").strip()
    mode = "html" if p.suffix.lower() in (".html", ".htm") else "md"
    return text, mode


def cmd_campaign_create(a, s: Settings, db: Database) -> int:
    from .sender import check_campaign

    if db.get_campaign(a.name):
        raise SystemExit(f"Кампания «{a.name}» уже есть — используй `campaign update`.")
    if not db.list_stats(a.list)["total"]:
        raise SystemExit(f"Список «{a.list}» пуст или не существует.")
    text, mode = _read_template(a.template)
    image = str(Path(a.image)) if a.image else None
    cid = db.create_campaign(a.name, a.list, text, image, mode, link_preview=not a.no_preview)
    problems = check_campaign(db, db.get_campaign(cid))
    print(f"✓ Кампания #{cid} «{a.name}» для списка «{a.list}» ({mode}).")
    for p in problems:
        print(f"  ⚠ {p}")
    return 0 if not problems else 1


def cmd_campaign_update(a, s: Settings, db: Database) -> int:
    from .sender import check_campaign

    camp = _campaign(db, a.name)
    fields = {}
    if a.template:
        fields["template"], fields["parse_mode"] = _read_template(a.template)
    if a.image is not None:
        fields["image_path"] = str(Path(a.image)) if a.image else None
    if a.no_preview is not None:
        fields["link_preview"] = 0 if a.no_preview else 1
    db.update_campaign(camp["id"], **fields)
    print(f"✓ обновлено: {', '.join(fields) or 'ничего'}")
    for p in check_campaign(db, db.get_campaign(camp["id"])):
        print(f"  ⚠ {p}")
    return 0


def cmd_campaign_list(a, s: Settings, db: Database) -> int:
    for c in db.campaigns():
        st = db.campaign_stats(c["id"])
        print(f"#{c['id']:<3} {c['name']:<24} список={c['list_name']:<18} {c['status']:<8} "
              f"sent={st['sent']} skipped={st['skipped']} err={st['error']} "
              f"uncertain={st['uncertain']} осталось={st['remaining']}")
    return 0


def cmd_preview(a, s: Settings, db: Database) -> int:
    from .sender import check_campaign, rendered_too_long
    from .template import render

    camp = _campaign(db, a.name)
    problems = check_campaign(db, camp)
    contacts = db.contacts(camp["list_name"])
    targets = db.targets(camp["id"])
    pool = targets or contacts
    print(f"Кампания «{camp['name']}» → список «{camp['list_name']}», parse_mode={camp['parse_mode']}, "
          f"картинка: {camp['image_path'] or 'нет'}")
    for c in pool[:a.n]:
        print("\n" + "─" * 60 + f"\n→ {c['name'] or '—'} ({c['phone'] or '@' + str(c['username'])})\n")
        print(render(camp["template"], c))
    too_long = [c for c in contacts if rendered_too_long(render(camp["template"], c),
                                                          bool(camp["image_path"]))]
    no_name = sum(1 for c in targets if not (c.get("first_name") or c.get("tg_first_name")))
    print("\n" + "─" * 60)
    print(f"Получателей сейчас: {len(targets)} (без имени: {no_name} — сработает fallback)")
    if too_long:
        problems.append(f"{len(too_long)} сообщений длиннее лимита Telegram")
    for p in problems:
        print(f"⚠ {p}")
    return 0 if not problems else 1


def cmd_test(a, s: Settings, db: Database) -> int:
    from .sender import check_campaign
    from .template import render

    camp = _campaign(db, a.name)
    problems = check_campaign(db, camp)
    if problems:
        raise SystemExit("Кампания не прошла проверку:\n  - " + "\n  - ".join(problems))
    contacts = db.contacts(camp["list_name"])
    sample = next((c for c in contacts if a.row and c["row_no"] == a.row), None) or \
        next((c for c in contacts if c["first_name"]), None) or (contacts[0] if contacts else {})
    text = render(camp["template"], sample)

    async def go(client, me):
        target = a.to if a.to and a.to != "me" else "me"
        msg = await client.send_message(target, text, parse_mode=camp["parse_mode"],
                                        link_preview=bool(camp["link_preview"]),
                                        file=camp["image_path"] or None)
        print(f"✓ Тестовое сообщение (данные строки {sample.get('row_no')}) отправлено в "
              f"{'Избранное' if target == 'me' else target}, id={msg.id}")
        return 0
    return asyncio.run(_with_client(s, db, go))


def cmd_send(a, s: Settings, db: Database) -> int:
    from .sender import Runner, check_campaign

    camp = _campaign(db, a.name)
    problems = check_campaign(db, camp)
    if problems:
        raise SystemExit("Кампания не прошла проверку:\n  - " + "\n  - ".join(problems))
    n = len(db.targets(camp["id"]))
    p = s.pacing
    print(f"Кампания «{camp['name']}»: получателей {n}. Темп {p.min_delay}–{p.max_delay}s, "
          f"пауза {p.long_pause_min // 60}–{p.long_pause_max // 60} мин каждые {p.long_pause_every}, "
          f"лимит {p.daily_cap}/24ч, окно {p.window_start:%H:%M}–{p.window_end:%H:%M} {p.timezone}.")
    if not _confirm("Запустить РЕАЛЬНУЮ рассылку?", a.yes):
        return 1

    log_path = s.reports_dir / f"send-{camp['name']}.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logf = log_path.open("a", encoding="utf-8")

    def out(msg: str) -> None:
        _out(msg)
        logf.write(f"{dt.datetime.now().isoformat(timespec='seconds')}  {msg}\n")
        logf.flush()

    async def go(client, me):
        stop = asyncio.Event()
        _install_stop(stop)
        out(f"Отправитель: @{me.username or '—'} id={me.id}")
        res = await Runner(client, db, camp["id"], s.pacing, stop=stop, out=out).run(
            limit=a.limit, until_done=a.until_done,
            retry_uncertain=a.retry_uncertain, retry_errors=a.retry_errors)
        out("■ " + res.line())
        # 0 = everything sent; 2 = paused (limit/window/cap/flood) — re-run to continue.
        return 0 if res.remaining == 0 else 2
    try:
        return asyncio.run(_with_client(s, db, go))
    finally:
        logf.close()


def cmd_status(a, s: Settings, db: Database) -> int:
    camp = _campaign(db, a.name)
    st = db.campaign_stats(camp["id"])
    ls = db.list_stats(camp["list_name"])
    print(f"«{camp['name']}» [{camp['status']}] список «{camp['list_name']}»: всего {ls['total']}, "
          f"в TG {ls['resolved']}, нет в TG {ls['not_found']}, ждут резолва {ls['pending']}")
    print(f"  отправлено {st['sent']}, пропущено {st['skipped']}, ошибок {st['error']}, "
          f"неясно {st['uncertain']}, осталось {st['remaining']}")
    print(f"  за последние 24ч (все кампании): {db.sent_since(__import__('time').time() - 86400)}"
          f" / {s.pacing.daily_cap}")
    return 0


def cmd_report(a, s: Settings, db: Database) -> int:
    camp = _campaign(db, a.name)
    rows = db.report(camp["id"])
    for r in rows:
        if r.get("updated_at"):
            r["updated_at"] = dt.datetime.fromtimestamp(r["updated_at"]).isoformat(timespec="seconds")
    out = Path(a.out) if a.out else s.reports_dir / f"report-{camp['name']}.csv"
    _write_csv(out, rows)
    print(f"✓ {len(rows)} строк → {out}")
    return 0


def cmd_optout(a, s: Settings, db: Database) -> int:
    from .normalize import optout_key

    for v in a.values:
        key = optout_key(v, s.region)
        if not key:
            print(f"✗ не понял «{v}» (телефон, @username или id:123)")
            continue
        db.add_optout(key, a.reason or "")
        print(f"✓ {key} больше не получит рассылки")
    return 0


# --------------------------------------------------------------------------- #
# parser
# --------------------------------------------------------------------------- #

def _add_parse_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("file", help="путь к файлу (.xlsx .xls .csv .tsv .json) или URL Google Sheets")
    p.add_argument("--sheet", help="имя или номер листа")
    p.add_argument("--region", help="регион по умолчанию для номеров без кода (UZ)")
    p.add_argument("--header-row", type=int, help="номер строки заголовка (1-based), 0 = без заголовка")
    p.add_argument("--phone-col", action="append", help="колонка с телефоном (имя или номер); можно несколько")
    p.add_argument("--username-col", action="append", help="колонка с Telegram; можно несколько")
    p.add_argument("--name-col", help="колонка с ФИО целиком")
    p.add_argument("--first-name-col", help="колонка с именем")
    p.add_argument("--last-name-col", help="колонка с фамилией")
    p.add_argument("--consent-col", help="колонка согласия на рассылку ('нет' → исключить)")
    p.add_argument("--name-order", choices=["auto", "surname_first", "given_first"])
    p.add_argument("--sample", type=int, default=10, help="сколько примеров показать")


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="blaster", description="TGBlaster — рассылка в Telegram по таблице")
    ap.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    ap.add_argument("-v", "--verbose", action="store_true")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("inspect", help="показать, как распознана таблица (ничего не сохраняет)")
    _add_parse_args(p)
    p.set_defaults(fn=cmd_inspect)

    p = sub.add_parser("import", help="импортировать таблицу в список")
    _add_parse_args(p)
    p.add_argument("--list", required=True, help="имя списка, напр. agile-uz-dec")
    p.add_argument("--yes", "-y", action="store_true")
    p.set_defaults(fn=cmd_import)

    p = sub.add_parser("lists", help="списки контактов")
    p.set_defaults(fn=cmd_lists)

    p = sub.add_parser("contacts", help="контакты списка")
    p.add_argument("--list", required=True)
    p.add_argument("--status", choices=["new", "resolved", "not_found", "error"])
    p.add_argument("--out", help="сохранить в CSV")
    p.set_defaults(fn=cmd_contacts)

    p = sub.add_parser("login", help="войти в Telegram (интерактивно)")
    p.set_defaults(fn=cmd_login)
    p = sub.add_parser("whoami", help="проверить сессию")
    p.set_defaults(fn=cmd_whoami)

    p = sub.add_parser("resolve", help="найти контакты списка в Telegram")
    p.add_argument("--list", required=True)
    p.add_argument("--retry-errors", action="store_true")
    p.add_argument("--keep-contacts", action="store_true",
                   help="не удалять временно импортированные контакты из адресной книги")
    p.set_defaults(fn=cmd_resolve)

    camp = sub.add_parser("campaign", help="кампании").add_subparsers(dest="sub", required=True)
    p = camp.add_parser("create")
    p.add_argument("name")
    p.add_argument("--list", required=True)
    p.add_argument("--template", required=True, help=".md (Telegram markdown) или .html")
    p.add_argument("--image")
    p.add_argument("--no-preview", action="store_true", help="без превью ссылок")
    p.set_defaults(fn=cmd_campaign_create)
    p = camp.add_parser("update")
    p.add_argument("name")
    p.add_argument("--template")
    p.add_argument("--image", help="путь; пустая строка — убрать картинку")
    p.add_argument("--no-preview", action=argparse.BooleanOptionalAction, default=None)
    p.set_defaults(fn=cmd_campaign_update)
    p = camp.add_parser("list")
    p.set_defaults(fn=cmd_campaign_list)

    p = sub.add_parser("preview", help="посмотреть сообщения и проверить шаблон")
    p.add_argument("name")
    p.add_argument("-n", type=int, default=3)
    p.set_defaults(fn=cmd_preview)

    p = sub.add_parser("test", help="одно тестовое сообщение себе (Избранное) или на @username")
    p.add_argument("name")
    p.add_argument("--to", default="me")
    p.add_argument("--row", type=int, help="взять данные этой строки таблицы")
    p.set_defaults(fn=cmd_test)

    p = sub.add_parser("send", help="РЕАЛЬНАЯ рассылка (возобновляемая)")
    p.add_argument("name")
    p.add_argument("--limit", type=int, help="отправить не больше N за этот запуск")
    p.add_argument("--until-done", action="store_true",
                   help="ждать окна/дневного лимита и продолжать до конца")
    p.add_argument("--retry-uncertain", action="store_true")
    p.add_argument("--retry-errors", action="store_true")
    p.add_argument("--yes", "-y", action="store_true")
    p.set_defaults(fn=cmd_send)

    p = sub.add_parser("status", help="статус кампании")
    p.add_argument("name")
    p.set_defaults(fn=cmd_status)

    p = sub.add_parser("report", help="CSV-отчёт по каждому контакту")
    p.add_argument("name")
    p.add_argument("--out")
    p.set_defaults(fn=cmd_report)

    p = sub.add_parser("optout", help="исключить людей из всех рассылок")
    p.add_argument("values", nargs="+", help="телефон, @username или id:123")
    p.add_argument("--reason")
    p.set_defaults(fn=cmd_optout)
    return ap


def main(argv: list[str] | None = None) -> int:
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8")
        except Exception:  # noqa: BLE001
            pass
    a = build_parser().parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if a.verbose else logging.WARNING,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    settings = Settings.load()
    db = Database(settings.db_path)
    try:
        return int(a.fn(a, settings, db) or 0)
    except ValueError as e:
        print(f"✗ {e}", file=sys.stderr)
        return 1
    finally:
        db.close()
