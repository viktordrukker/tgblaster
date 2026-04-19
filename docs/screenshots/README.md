# Screenshots

The main README references six screenshots by these exact filenames:

| Filename | What it shows |
|---|---|
| `01-login.jpg` | Step 1 · Accounts page — logged-in state ("TG-аккаунт подключён"), default account row visible, `api_id` masked. |
| `02-contacts.jpg` | Contacts page — "В базе сейчас" metrics (Всего / С Telegram / Без Telegram / Ждут resolve) + the "🚀 Подготовить к кампании" button. |
| `03-dryrun.jpg` | Dry-run page — send a test to your own Saved Messages before firing a real campaign. |
| `04-campaign.jpg` | Campaign page — state badge (`done` in the screenshot), KPI tiles (Осталось / Отправлено / Ошибок / Пропущено), Pause / Stop / Reset buttons. |
| `05-log.jpg` | Log page — "Проверить прочтения" button + 5-column KPI row including "Прочитано", send-log table with `read_at` populated. |
| `06-jobs.jpg` | Jobs page — job table with payload and progress JSON visible, covering resolve / validate / run_campaign / check_read_receipts. |

## Taking new screenshots

1. Run a clean install against the synthetic `data/example_contacts.csv`.
2. Log in with a test Telegram account (not your primary).
3. Capture at 1280×800 or larger.
4. Redact API hash and any real handles before commit.
5. Optimize: `pngquant --quality=80-95 <file>.png` typically halves
   size with no visible quality loss.

Screenshots should never contain:
- Real API credentials (hash visible in "Step 1 · Accounts")
- Real phone numbers beyond the synthetic `+1-202-555-01XX` range
- Real recipient names or messages
