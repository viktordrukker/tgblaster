# TGBlaster

**Рассылка в Telegram по таблице участников: аккуратный темп, возобновление после сбоя, без дублей.**
Один контейнер, CLI, состояние в SQLite. Для сообществ и митапов, где люди сами оставили контакты.

> 🇬🇧 [English README](./README.md) · ⚠️ перед первой рассылкой прочитай [DISCLAIMER.md](./DISCLAIMER.md)

---

## Что умеет

- **Принимает почти любую таблицу**: `.xlsx`, `.xls`, `.csv`/`.tsv` (любая кодировка и разделитель), `.json`,
  ссылку на Google Sheets. Сам находит строку заголовка (даже под шапкой «Регистрация на митап»),
  колонки телефона, Telegram, ФИО/имени/фамилии/«как к вам обращаться», согласия на рассылку.
  Колонки определяются и по названию (рус/узб/англ), и по содержимому. Всё можно переопределить флагами.
- **Понимает грязные данные**: `998901234567.0` из Excel, `90 123 45 67`, `8 (90) 123-45-67`,
  несколько номеров в ячейке, `@ник`, `t.me/ник`, `t.me/+998…`, «нет», общая колонка «телефон или ник».
- **Имя для обращения**: `Иванов Иван Иванович` → `Иван`, `Karimov Aziz` → `Aziz`; если имени нет,
  срабатывает fallback из шаблона: `{first_name|друзья}`.
- **Резолв в Telegram**: сначала адресная книга, затем пачками по номерам (включая альтернативные номера),
  потом по `@username`. Временно импортированные контакты удаляются, **существующие контакты организатора
  не трогаются**.
- **Безопасная отправка**: случайная пауза 40–100 с, длинная пауза каждые 30, лимит за скользящие 24 ч,
  окно отправки (по умолчанию 09:00–21:00 Asia/Tashkent), FloodWait/PeerFlood, остановка после 5 ошибок подряд.
- **Без дублей**: журнал «зарезервировано → отправлено». Если процесс упал посреди отправки,
  контакт помечается `uncertain` и **не** переотправляется автоматически. Один человек с двумя номерами
  получит одно сообщение.
- **Отписки** (`optout`), CSV-отчёты по каждому контакту, тестовая отправка себе в «Избранное».

## Быстрый старт (Docker)

```bash
cp .env.example .env          # TG_API_ID / TG_API_HASH с https://my.telegram.org
docker compose build
alias tgb='docker compose run --rm blaster'

tgb login                                        # один раз: телефон → код → пароль 2FA
tgb whoami
```

Положи файл с контактами в `data/inbox/` (папка `data/` не попадает в git).

```bash
# 1. Посмотреть, как распознан файл (ничего не сохраняет)
tgb inspect data/inbox/contacts.xlsx
tgb inspect "https://docs.google.com/spreadsheets/d/…/edit"   # таблица по ссылке

# 2. Импорт в список (можно повторять с обновлённым файлом: добавятся только новые)
tgb import data/inbox/contacts.xlsx --list agile-dec --yes

# 3. Найти людей в Telegram
tgb resolve --list agile-dec
tgb contacts --list agile-dec --status not_found --out data/reports/not-found.csv

# 4. Кампания: текст в campaigns/*.md (Telegram markdown) или *.html
tgb campaign create agile-dec-invite --list agile-dec \
    --template campaigns/agile-dec.md --image campaigns/banner.png
tgb preview agile-dec-invite -n 5
tgb test agile-dec-invite                     # одно сообщение себе в «Избранное»

# 5. Рассылка: в фоне, до конца, с паузами на ночь и на дневной лимит
docker compose run -d --name tgb-send blaster send agile-dec-invite --until-done --yes
docker logs -f tgb-send
tgb status agile-dec-invite
tgb report agile-dec-invite                   # → data/reports/report-agile-dec-invite.csv
```

Остановить: `docker stop -t 90 tgb-send`. Продолжить: та же команда `send`. Уже отправленные не повторятся.

## Если колонки распознались не так

```bash
tgb inspect file.xlsx --sheet "Участники" --header-row 3 \
    --phone-col "Номер" --username-col "Ник" --name-col "ФИО" --name-order surname_first
```

Колонку можно указать по названию или по номеру (1, 2, 3…). Флаги `--phone-col` и `--username-col` можно
повторять. `--consent-col` задаёт колонку согласия: строки с ответом «нет» будут исключены.

## Шаблон сообщения

```markdown
Привет, {first_name|друзья}! 👋

Приглашаем на **Agile Uzbekistan Meetup** — 12 декабря, 19:00.
Регистрация: [по ссылке](https://example.com)
```

- `{first_name}`, `{name}`, `{phone}`, `{username}` и **любая колонка таблицы** по её названию:
  `{Компания}` или `{компания}`.
- `{ключ|запасной текст}` подставит запасной текст, если значение пустое. `{{` и `}}` дают литеральные скобки.
- Неизвестный плейсхолдер — ошибка до старта рассылки, а не «{compnay}» у 300 человек.
- Разметка Telegram `md`: `**жирный**`, `__курсив__`, `[текст](url)`. Для HTML используй файл `.html`.
- Лимиты: 4096 символов для текста, 1024 для подписи к картинке. `preview` проверяет их для каждого получателя.

## Статусы

| контакт      | значение                                         |
|--------------|--------------------------------------------------|
| `new`        | ещё не искали в Telegram                         |
| `resolved`   | найден, можно отправлять                         |
| `not_found`  | нет в Telegram или скрыт приватностью            |
| `error`      | ошибка поиска (`resolve --retry-errors`)         |

| отправка     | значение                                                                 |
|--------------|--------------------------------------------------------------------------|
| `sent`       | доставлено                                                               |
| `skipped`    | приватность, блокировка, удалённый аккаунт, платные сообщения             |
| `error`      | Telegram отказал (`send --retry-errors`)                                 |
| `uncertain`  | связь оборвалась во время отправки; проверь вручную, затем `--retry-uncertain` |

## Настройки (`.env`)

См. [.env.example](./.env.example): `DAILY_CAP`, `MIN_DELAY_SEC`/`MAX_DELAY_SEC`, `LONG_PAUSE_*`,
`SEND_WINDOW`, `TZ_SEND`, `DEFAULT_REGION`, `MAX_FLOOD_WAIT_SEC`, `PEER_FLOOD_STOP`.

## Без Docker

```bash
python -m venv .venv && .venv/bin/pip install -r requirements-dev.txt
.venv/bin/python -m blaster --help
.venv/bin/python -m pytest
```

## Устройство

```
blaster/
  tables.py     загрузка xlsx/xls/csv/json/URL → листы строк
  normalize.py  телефоны, @username, имена (чистые функции)
  importer.py   заголовок, роли колонок, разбор строк, дедупликация
  template.py   шаблоны, fallback, валидация
  db.py         SQLite: списки, кампании, журнал отправок, отписки, lock сессии
  telegram.py   клиент, логин, резолв телефонов/username
  sender.py     цикл отправки: темп, окна, лимиты, классификация ошибок
  cli.py        команды
```

Одновременно с сессией Telegram может работать только один процесс: lock в БД не даст запустить второй
`send`/`resolve`, потому что два клиента на одном ключе авторизации могут привести к отзыву сессии.
