# Screenshots

The main README references six screenshots by these exact filenames.
After the first commit, drop the PNGs into this directory with the
following names:

| Filename | What it shows |
|---|---|
| `01-login.png` | Step 1 · Accounts page — logged-in state ("TG-аккаунт подключён"), default account row visible, `api_id` masked. |
| `02-contacts.png` | Contacts page — "В базе сейчас" metrics (Всего / С Telegram / Без Telegram / Ждут resolve) + the "🚀 Подготовить к кампании" button. |
| `03-compose.png` | Compose Message — editor toolbar, text area, 5-device preview tabs with image in bubble. |
| `04-campaign.png` | Campaign page — running state with KPI tiles (Осталось / Отправлено / Ошибок / Пропущено), state badge, Pause/Stop buttons. |
| `05-log.png` | Log page — Read-check + 5-column KPI row including "Прочитано", send-log table with `read_at` populated. |
| `06-jobs.png` | Jobs page — job table with payload and progress JSON expanded. |

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
