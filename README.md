# Echo backend — Express + Turso + Socket.IO (free, permanent storage)

Same deployment shape as the Orbit backend — one `server.js`, no build
step, GitHub -> Render — but built for Echo's chat features: real accounts,
conversations, groups/channels with roles, messages, reactions, pins,
notifications, and real-time updates over Socket.IO.

Unlike a one-JSON-blob-per-user design, this uses real relational tables
(`users`, `conversations`, `conversation_members`, `messages`,
`message_reactions`, `attachments`, `notifications`, etc.) so data is
actually queryable and indexed, instead of being nested inside one big
saved object.

---

## Step 1: Create your free Turso database

1. Go to **turso.tech** -> **Sign up** (GitHub sign-in works)
2. **Create Database** (or "+ New")
3. Name it anything, e.g. `echo-db`
4. Pick the closest region (or leave default) -> **Create**

## Step 2: Get your connection details

1. **Database URL** — on the database's page, find **Connect** / the "URL"
   field: `libsql://echo-db-yourusername.turso.io`
2. **Auth Token** — find **Create Token** (or "Generate Token"). Copy it
   now — you can't view it again later.

## Step 3: Deploy to Render

1. Push this folder to a **GitHub repository**
2. **render.com** -> **New** -> **Web Service** -> connect your repo
3. Settings:
   - **Build Command:** `npm install`
   - **Start Command:** `npm start`
4. **Environment** tab, add:

   | Key | Value |
   |---|---|
   | `TURSO_DATABASE_URL` | from Step 2 |
   | `TURSO_AUTH_TOKEN` | from Step 2 |
   | `JWT_SECRET` | long random string — `node -e "console.log(require('crypto').randomBytes(48).toString('hex'))"` |
   | `CORS_ORIGIN` | `*` for now, or your frontend's exact URL once you have one |
   | `NODE_ENV` | `production` |

5. **Create Web Service**. Watch the logs for:
   ```
   Connected to Turso database and verified tables.
   Echo backend listening on port ... (production)
   ```

Render gives you a URL like `https://echo-xxxx.onrender.com`.

## Step 4: Point the frontend at your backend

`index.html` already talks to the API at the same origin it's served from
(`window.ECHO_API_BASE = ""`).

**One deployment (simplest):** the backend serves `index.html` itself (see
`server.js`'s "Serve frontend" section) — just visit your Render URL.

**Frontend on GitHub Pages, backend on Render:** edit the line near the top
of `index.html`:
```html
<script>window.ECHO_API_BASE = "";</script>
```
to your Render URL, enable GitHub Pages for the repo, and set
`CORS_ORIGIN` in Render to your Pages URL instead of `*`.

## Step 5: Test it

1. Create an account, start a conversation with another test account, send
   a few messages, react, pin one
2. Wait for Render's free instance to spin down (or redeploy manually)
3. Log back in — everything should still be there

## Local development

```bash
npm install
cp .env.example .env
# fill in TURSO_DATABASE_URL / TURSO_AUTH_TOKEN / JWT_SECRET
npm run dev
```
Visit `http://localhost:3000`.

---

## What's included

| File | Purpose |
|---|---|
| `server.js` | The entire backend — auth, conversations, messages, reactions, notifications, Socket.IO, static file serving |
| `package.json` | Dependencies (`express`, `@libsql/client`, `bcryptjs`, `jsonwebtoken`, `cors`, `express-rate-limit`, `socket.io`) |
| `index.html` / `public/index.html` | The Echo frontend, wired to this backend |
| `.env.example` | Template for local environment variables |

## Data model

Real tables, not one JSON blob:

| Table | Purpose |
|---|---|
| `users` | Accounts |
| `user_preferences` | Theme/settings + palette |
| `conversations` | Direct chats, groups, channels (`type` column) |
| `conversation_members` | Membership, role (admin/moderator/member), pinned/muted/archived, last-read |
| `messages` | Text, reply-to, edited/deleted flags |
| `message_mentions` | @user and @everyone, validated against real membership |
| `message_reactions` | Emoji reactions |
| `attachments` | File/image/video/voice **metadata only** — see below |
| `pinned_messages` | Per-conversation pinned message ids |
| `notifications` | Mentions, messages, etc. |

Groups/channels are **not** a separate system — they're `conversations`
with `type = "group" | "channel"`, sharing the same membership/role logic
as direct chats.

**Attachments are metadata-only.** This backend doesn't store file bytes
or run an upload endpoint. The client should upload to storage you control
(S3, Cloudflare R2, Cloudinary, etc.) and send the resulting URL as the
message's `attachment` field.

## Authorization

Every conversation/message/group route re-checks the caller's **actual**
role from `conversation_members` in the database — never whatever the
client claims. Sending, adding members, editing group info, and pinning
are all gated by the conversation's real `who_can_send` /
`who_can_add_members` / `who_can_edit_info` settings.

## API reference

Granular REST endpoints (recommended for any future frontend work that
manages conversations/messages incrementally instead of syncing one big
blob):

| Method | Path | Purpose |
|---|---|---|
| POST | `/api/auth/register`, `/login`, `/logout` | Auth |
| GET/PATCH | `/api/me`, `/api/me/status` | Current user |
| GET | `/api/users/search?q=` | Find people to start a chat with |
| GET/PUT | `/api/preferences` | Settings + palette |
| GET/POST | `/api/conversations` | List / create conversations |
| GET/PATCH | `/api/conversations/:id` | Detail / update group info |
| POST/DELETE | `/api/conversations/:id/members`, `/members/:userId` | Membership |
| PATCH | `/api/conversations/:id/members/:userId/role` | Change role |
| PATCH | `/api/conversations/:id/state` | Pin/mute/archive (per-member) |
| POST | `/api/conversations/:id/read` | Mark read |
| POST/DELETE | `/api/conversations/:id/pins` | Pin/unpin a message |
| GET/POST | `/api/conversations/:id/messages` | List / send messages |
| PATCH/DELETE | `/api/conversations/:id/messages/:messageId` | Edit / delete |
| POST/DELETE | `.../messages/:messageId/reactions` | React / unreact |
| GET | `/api/notifications` | List notifications |
| GET | `/api/search?q=` | Search your conversations & messages |

**Compatibility layer** (what the bundled frontend actually uses today):
`GET /api/me` also returns a `data` object shaped like the old JSON-blob
contract (`conversations` with messages nested inline, `notifItems`,
`settings`, `palette`, `profile`, `otherUsersById`) — assembled live from
the relational tables above, not stored as a blob. `PUT /api/me/data`
persists the settings/profile portion of that payload (conversations and
messages are created through the granular endpoints as they happen, not
through this bulk endpoint).

## Real-time (Socket.IO)

Connect with `io(url, { auth: { token: accessToken } })`. On connect, the
socket auto-joins `user:<id>` and every `conversation:<id>` room the user
belongs to.

Events: `message:new`, `message:updated`, `message:deleted`,
`message:reaction_added`, `message:reaction_removed`, `message:pinned`,
`message:unpinned`, `message:read`, `conversation:created`,
`conversation:updated`, `conversation:members_added`,
`conversation:member_removed`, `conversation:member_role_changed`,
`conversation:removed`, `presence:update`, `typing:update`,
`notification:new`.

Client -> server: `typing:start` / `typing:stop` (`{ conversationId }`,
auto-clears after 5s), `conversation:join` (`{ conversationId }`, joins a
room mid-session after creating/being added to a conversation).

**Note:** the frontend currently syncs via the `GET /api/me` /
`PUT /api/me/data` compatibility layer and does not yet consume these
Socket.IO events or the granular REST endpoints for its own conversation
actions (sending a message, reacting, etc. still update local state and
rely on the periodic full-blob save). Wiring the frontend to call the
granular endpoints directly and listen for the Socket.IO events is the
natural next step for real multi-device real-time sync — the backend
contract is ready for it.

## Known simplifications

- **Per-recipient read/delivery status** isn't tracked individually
  (there's no `message_status` table in this version) — messages report a
  fixed `status: "sent"` rather than per-recipient delivered/read
  timestamps. `conversation.unread` counts are still accurate (computed
  from each member's `last_read_message_id`).
- **Per-conversation chat themes** (`chatThemes`) are accepted in the
  compatibility payload shape but not yet persisted to a table.
- No email sending — registration completes immediately, no verification
  step, no real password-reset flow.
- No file storage/upload endpoint — attachments are metadata pointing at
  storage you control.
- No fake/seed/demo data — every table starts empty.
