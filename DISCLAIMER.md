# ⚠️ Disclaimer — read before using TGBlaster

TGBlaster automates outgoing direct messages from a real Telegram user
account. **How you use it is entirely your responsibility.** The project
authors provide this software as a tool; they do not operate your
Telegram account, do not store your messages, and cannot intervene if
Telegram bans you or a recipient reports you.

## Telegram's Terms of Service

By using Telegram you agreed to <https://telegram.org/tos> and the
platform's [anti-spam policy](https://telegram.org/faq#q-what-am-i-allowed-to-do-on-telegram).
The key rule:

> Unsolicited bulk messaging is prohibited. Account bans for spam are
> issued automatically and are very hard to reverse.

Telegram does not publish an explicit allowed-volume threshold. In our
experience, accounts sending more than a few dozen cold DMs to
non-contacts per day will trigger `PeerFloodError` and, eventually, a
permanent ban. TGBlaster pauses on `PeerFloodError` and logs the event,
but **it cannot undo a send that has already left your account.**

## When TGBlaster is reasonable to use

- Inviting your own event's registrants who gave you their phone
  numbers with the explicit expectation of a Telegram follow-up.
- Reaching out to your own subscribers, students, or customers who
  provided their phone as part of a sign-up flow that disclosed
  Telegram as a communication channel.
- Drip-sending to a contact list you personally own, where each
  recipient has an existing relationship with the sender's account.

## When TGBlaster is NOT reasonable to use

- **Cold outreach / lead generation** to scraped or purchased contact
  lists. This is spam. Your account will be banned.
- Political messaging, affiliate marketing, or crypto promotion.
- Any message that would violate your recipient's reasonable
  expectation of how their phone number would be used.
- Jurisdictions with anti-spam statutes (EU ePrivacy, US TCPA, UK PECR,
  Canada CASL, many others) that require prior opt-in consent for
  automated contact.

## Data-protection responsibilities (GDPR, etc.)

If any recipient is in the EU, you are a data controller under GDPR.
Your obligations include:

- Maintaining a lawful basis for processing (consent is the only realistic
  basis for cold DMs — and cold DMs without opt-in are almost always
  non-compliant, see above).
- Honoring subject-access and erasure requests on the contacts you store.
- Retaining messages only as long as necessary.
- Disclosing Telegram as a sub-processor in your privacy policy.

TGBlaster stores contact phone numbers and resolved Telegram IDs in a
local SQLite database (`data/state.db`). No data ever leaves your
server except (a) the Telegram API calls you make and (b) whatever
logs your hosting provider captures.

## What TGBlaster does to protect you

- Pacing: 30–90 s between sends, a long pause every 40 messages,
  configurable daily cap.
- Stops the campaign automatically on `PeerFloodError`.
- Logs every send with its Telegram `message_id` for audit.
- Reserve-then-confirm idempotency — retrying a flaky send won't
  double-deliver.
- Opt-out table — once a user is marked opted-out, they are never
  contacted again.

These are mitigations, not guarantees. The volume, content, and
recipient list are yours to choose and yours to defend.

## Warranty

**The Software is provided "AS IS" with no warranty of any kind.** See
the [MIT LICENSE](./LICENSE) for the legally-binding text. The authors
disclaim liability for:

- Account suspensions or bans from Telegram
- Lost revenue, missed events, or reputation damage
- Regulatory fines or complaints brought against the operator
- Data loss from SQLite corruption, disk failure, or user error

If you cannot accept these terms, do not use this software.

## If you are unsure

Run a 5-contact dry-run to your own test accounts first. If you are
unsure whether your intended use is compliant with Telegram's rules or
your local law, consult a lawyer **before** your first real campaign.
The cost of a one-hour consultation is trivial compared to a banned
account or a regulatory fine.
