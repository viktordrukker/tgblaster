from types import SimpleNamespace

from telethon import errors
from telethon.tl import functions, types

from blaster.telegram import resolve_list
from conftest import contact


def user(uid, phone=None, username=None, bot=False):
    return types.User(id=uid, access_hash=uid * 7, first_name=f"U{uid}", username=username,
                      phone=phone, bot=bot)


class FakeTG:
    def __init__(self, phone_map=None, usernames=None, book=None, import_errors=None):
        self.phone_map = phone_map or {}          # "+998…" → User
        self.usernames = usernames or {}          # "name" → User
        self.book = book or []
        self.import_errors = list(import_errors or [])
        self.deleted = []
        self.imported_phones = []

    async def __call__(self, req):
        if isinstance(req, functions.contacts.GetContactsRequest):
            return SimpleNamespace(users=self.book)
        if isinstance(req, functions.contacts.ImportContactsRequest):
            if self.import_errors:
                raise self.import_errors.pop(0)
            imported, users = [], []
            for c in req.contacts:
                self.imported_phones.append(c.phone)
                u = self.phone_map.get(c.phone)
                if u:
                    imported.append(types.ImportedContact(user_id=u.id, client_id=c.client_id))
                    users.append(u)
            return SimpleNamespace(imported=imported, users=users, retry_contacts=[])
        if isinstance(req, functions.contacts.ResolveUsernameRequest):
            u = self.usernames.get(req.username)
            if not u:
                raise errors.UsernameNotOccupiedError(request=None)
            return SimpleNamespace(users=[u])
        if isinstance(req, functions.contacts.DeleteContactsRequest):
            self.deleted.extend(x.user_id for x in req.id)
            return None
        raise AssertionError(req)


NO_PAUSE = ((0, 0), (0, 0))


async def test_resolve_phones_usernames_alternates_and_cleanup(db):
    db.upsert_contacts("L", [
        contact(2, phones=["+998901111111"]),                         # found by phone
        contact(3, phones=["+998902222222", "+998903333333"]),         # found by 2nd phone
        contact(4, phones=["+998904444444"], username="hidden_user"),  # privacy → username
        contact(5, phones=[], username="ghost_user"),                  # doesn't exist
        contact(6, phones=["+998905555555"]),                          # in address book
        contact(7, phones=["+998906666666"]),                          # bot → not a person
    ])
    book_user = user(50, phone="998905555555")
    tg = FakeTG(
        phone_map={"+998901111111": user(10), "+998903333333": user(11),
                   "+998906666666": user(12, bot=True)},
        usernames={"hidden_user": user(13, username="hidden_user")},
        book=[book_user],
    )
    st = await resolve_list(tg, db, "L", out=lambda m: None, pauses=NO_PAUSE)
    by_row = {c["row_no"]: c for c in db.contacts("L")}
    assert by_row[2]["tg_user_id"] == 10 and by_row[2]["tg_access_hash"] == 70
    assert by_row[3]["tg_user_id"] == 11 and by_row[3]["resolved_via"] == "phone:+998903333333"
    assert by_row[4]["tg_user_id"] == 13 and by_row[4]["resolved_via"].startswith("username")
    assert by_row[5]["status"] == "not_found"
    assert by_row[6]["tg_user_id"] == 50 and by_row[6]["resolved_via"] == "address_book"
    assert by_row[7]["status"] == "not_found"
    # the organiser's own contact was neither re-imported nor deleted
    assert "+998905555555" not in tg.imported_phones
    assert 50 not in tg.deleted and set(tg.deleted) == {10, 11, 12}
    assert st["resolved"] == 4 and st["not_found"] == 2


async def test_keep_contacts_flag(db):
    db.upsert_contacts("L", [contact(2, phones=["+998901111111"])])
    tg = FakeTG(phone_map={"+998901111111": user(10)})
    await resolve_list(tg, db, "L", keep_contacts=True, out=lambda m: None, pauses=NO_PAUSE)
    assert tg.deleted == []


async def test_flood_wait_short_retries_long_stops(db):
    db.upsert_contacts("L", [contact(2, phones=["+998901111111"])])
    tg = FakeTG(phone_map={"+998901111111": user(10)},
                import_errors=[errors.FloodWaitError(request=None, capture=0)])
    st = await resolve_list(tg, db, "L", out=lambda m: None, pauses=NO_PAUSE)
    assert st["resolved"] == 1

    db.upsert_contacts("M", [contact(2, phones=["+998901111112"])])
    tg = FakeTG(import_errors=[errors.FloodWaitError(request=None, capture=99999)])
    st = await resolve_list(tg, db, "M", out=lambda m: None, pauses=NO_PAUSE)
    assert st["stopped"].startswith("flood_wait")
    assert db.contacts("M")[0]["status"] == "new"      # untouched, retry later


async def test_rerun_is_noop(db):
    db.upsert_contacts("L", [contact(2, phones=["+998901111111"])])
    tg = FakeTG(phone_map={"+998901111111": user(10)})
    await resolve_list(tg, db, "L", out=lambda m: None, pauses=NO_PAUSE)
    tg2 = FakeTG()
    st = await resolve_list(tg2, db, "L", out=lambda m: None, pauses=NO_PAUSE)
    assert st["total"] == 0 and tg2.imported_phones == []
