// Echo backend — Express + Turso (libSQL, a free cloud SQLite service) + Socket.IO
//
// Same deployment shape as the Orbit backend (one server.js, no build step,
// GitHub -> Render), but with real relational tables -- accounts,
// conversations, membership/roles, messages, reactions, attachments,
// notifications -- instead of one big JSON blob per user. Real-time updates
// (new messages, reactions, typing, presence) go out over Socket.IO.
//
// You need a free Turso database -- see README.md for setup.

require("dotenv").config();
const path = require("path");
const fs = require("fs");
const http = require("http");
const crypto = require("crypto");
const express = require("express");
const cors = require("cors");
const jwt = require("jsonwebtoken");
const bcrypt = require("bcryptjs");
const rateLimit = require("express-rate-limit");
const { createClient } = require("@libsql/client");
const { Server: SocketIOServer } = require("socket.io");

// ---------- Config ----------
const PORT = process.env.PORT || 3000;
const JWT_SECRET = process.env.JWT_SECRET;
const NODE_ENV = process.env.NODE_ENV || "development";
const TURSO_DATABASE_URL = process.env.TURSO_DATABASE_URL;
const TURSO_AUTH_TOKEN = process.env.TURSO_AUTH_TOKEN;
const ACCOUNT_RECOVERY_DAYS = 30;
const CORS_ORIGIN = process.env.CORS_ORIGIN || "*";

if (!JWT_SECRET) {
  if (NODE_ENV === "production") {
    console.error("FATAL: JWT_SECRET is not set.");
    process.exit(1);
  } else {
    console.warn("WARNING: JWT_SECRET is not set -- using an insecure development secret.");
  }
}
const EFFECTIVE_JWT_SECRET = JWT_SECRET || "dev-only-insecure-secret-change-me";

if (!TURSO_DATABASE_URL || !TURSO_AUTH_TOKEN) {
  console.error(
    "FATAL: TURSO_DATABASE_URL and TURSO_AUTH_TOKEN must both be set. " +
      "Create a free database at turso.tech and set these env vars -- see README.md."
  );
  process.exit(1);
}

// ---------- Database ----------
const db = createClient({ url: TURSO_DATABASE_URL, authToken: TURSO_AUTH_TOKEN });

async function query(sql, params = []) {
  try {
    const result = await db.execute({ sql, args: params });
    return result.rows;
  } catch (e) {
    console.error("Query error:", sql, e.message);
    return [];
  }
}

async function run(sql, params = []) {
  try {
    await db.execute({ sql, args: params });
    return true;
  } catch (e) {
    console.error("Run error:", sql, e.message);
    return false;
  }
}

function newId(prefix) {
  return `${prefix}_${crypto.randomBytes(9).toString("hex")}`;
}

async function initDatabase() {
  await db.execute(`
    CREATE TABLE IF NOT EXISTS users (
      id TEXT PRIMARY KEY,
      email TEXT UNIQUE NOT NULL,
      username TEXT UNIQUE NOT NULL,
      name TEXT NOT NULL,
      password_hash TEXT NOT NULL,
      avatar_color TEXT NOT NULL DEFAULT '#7C5CFC',
      status TEXT NOT NULL DEFAULT 'offline',
      status_message TEXT,
      bio TEXT,
      last_seen_at TEXT,
      deleted INTEGER NOT NULL DEFAULT 0,
      deleted_at TEXT,
      created_at TEXT NOT NULL
    )
  `);
  await db.execute(`
    CREATE TABLE IF NOT EXISTS user_preferences (
      user_id TEXT PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
      settings TEXT NOT NULL DEFAULT '{}',
      palette TEXT NOT NULL DEFAULT 'signal',
      updated_at TEXT NOT NULL
    )
  `);
  await db.execute(`
    CREATE TABLE IF NOT EXISTS conversations (
      id TEXT PRIMARY KEY,
      type TEXT NOT NULL,
      name TEXT,
      description TEXT,
      group_avatar_color TEXT,
      privacy TEXT NOT NULL DEFAULT 'private',
      who_can_send TEXT NOT NULL DEFAULT 'everyone',
      who_can_add_members TEXT NOT NULL DEFAULT 'everyone',
      who_can_edit_info TEXT NOT NULL DEFAULT 'everyone',
      created_by TEXT REFERENCES users(id) ON DELETE SET NULL,
      last_message_at TEXT,
      created_at TEXT NOT NULL
    )
  `);
  await db.execute(`
    CREATE TABLE IF NOT EXISTS conversation_members (
      id TEXT PRIMARY KEY,
      conversation_id TEXT NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
      user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      role TEXT NOT NULL DEFAULT 'member',
      pinned INTEGER NOT NULL DEFAULT 0,
      muted INTEGER NOT NULL DEFAULT 0,
      archived INTEGER NOT NULL DEFAULT 0,
      last_read_message_id TEXT,
      joined_at TEXT NOT NULL,
      left_at TEXT
    )
  `);
  await db.execute(`CREATE UNIQUE INDEX IF NOT EXISTS members_convo_user_idx ON conversation_members(conversation_id, user_id)`);
  await db.execute(`CREATE INDEX IF NOT EXISTS members_user_idx ON conversation_members(user_id)`);

  await db.execute(`
    CREATE TABLE IF NOT EXISTS messages (
      id TEXT PRIMARY KEY,
      conversation_id TEXT NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
      sender_id TEXT REFERENCES users(id) ON DELETE SET NULL,
      type TEXT NOT NULL DEFAULT 'message',
      text TEXT NOT NULL DEFAULT '',
      reply_to_message_id TEXT,
      edited INTEGER NOT NULL DEFAULT 0,
      edited_at TEXT,
      deleted INTEGER NOT NULL DEFAULT 0,
      deleted_at TEXT,
      created_at TEXT NOT NULL
    )
  `);
  await db.execute(`CREATE INDEX IF NOT EXISTS messages_convo_created_idx ON messages(conversation_id, created_at)`);

  await db.execute(`
    CREATE TABLE IF NOT EXISTS message_mentions (
      id TEXT PRIMARY KEY,
      message_id TEXT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
      mentioned_user_id TEXT NOT NULL
    )
  `);
  await db.execute(`CREATE INDEX IF NOT EXISTS mentions_message_idx ON message_mentions(message_id)`);

  await db.execute(`
    CREATE TABLE IF NOT EXISTS message_reactions (
      id TEXT PRIMARY KEY,
      message_id TEXT NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
      user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      emoji TEXT NOT NULL,
      created_at TEXT NOT NULL
    )
  `);
  await db.execute(`CREATE UNIQUE INDEX IF NOT EXISTS reactions_msg_user_emoji_idx ON message_reactions(message_id, user_id, emoji)`);

  await db.execute(`
    CREATE TABLE IF NOT EXISTS attachments (
      id TEXT PRIMARY KEY,
      message_id TEXT NOT NULL UNIQUE REFERENCES messages(id) ON DELETE CASCADE,
      kind TEXT NOT NULL,
      name TEXT NOT NULL,
      url TEXT NOT NULL,
      mime_type TEXT,
      size_bytes INTEGER,
      duration_seconds INTEGER,
      created_at TEXT NOT NULL
    )
  `);

  await db.execute(`
    CREATE TABLE IF NOT EXISTS pinned_messages (
      id TEXT PRIMARY KEY,
      conversation_id TEXT NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
      message_id TEXT NOT NULL,
      pinned_by TEXT REFERENCES users(id) ON DELETE SET NULL,
      pinned_at TEXT NOT NULL
    )
  `);
  await db.execute(`CREATE UNIQUE INDEX IF NOT EXISTS pinned_convo_msg_idx ON pinned_messages(conversation_id, message_id)`);

  await db.execute(`
    CREATE TABLE IF NOT EXISTS notifications (
      id TEXT PRIMARY KEY,
      user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      type TEXT NOT NULL,
      conversation_id TEXT,
      message_id TEXT,
      actor_id TEXT,
      preview TEXT NOT NULL,
      read INTEGER NOT NULL DEFAULT 0,
      created_at TEXT NOT NULL
    )
  `);
  await db.execute(`CREATE INDEX IF NOT EXISTS notifications_user_read_idx ON notifications(user_id, read)`);

  console.log("Connected to Turso database and verified tables.");
  await purgeExpiredDeletedAccounts();
}

// ---------- User helpers ----------
async function getUserById(userId) {
  const rows = await query("SELECT * FROM users WHERE id = ?", [userId]);
  return rows[0] || null;
}
async function getUserByEmail(email) {
  const rows = await query("SELECT * FROM users WHERE email = ?", [String(email).toLowerCase()]);
  return rows[0] || null;
}
async function getUserByUsername(username) {
  const rows = await query("SELECT * FROM users WHERE username = ?", [String(username).toLowerCase()]);
  return rows[0] || null;
}
async function getUserByIdentifier(identifier) {
  const value = String(identifier || "").trim().toLowerCase();
  if (!value) return null;
  return value.includes("@") ? getUserByEmail(value) : getUserByUsername(value);
}

function toPublicUser(row) {
  if (!row) return null;
  return {
    id: row.id,
    email: row.email,
    username: row.username,
    name: row.name,
    avatarColor: row.avatar_color,
    status: row.status,
    statusMessage: row.status_message,
    bio: row.bio,
    createdAt: row.created_at,
  };
}

async function purgeExpiredDeletedAccounts() {
  const rows = await query("SELECT id, deleted_at FROM users WHERE deleted = 1");
  const now = Date.now();
  for (const r of rows) {
    if (!r.deleted_at) continue;
    const days = (now - new Date(r.deleted_at).getTime()) / 86400000;
    if (days > ACCOUNT_RECOVERY_DAYS) await run("DELETE FROM users WHERE id = ?", [r.id]);
  }
}
setInterval(purgeExpiredDeletedAccounts, 6 * 60 * 60 * 1000);

// ---------- App ----------
const app = express();
app.set("trust proxy", 1);
app.use(express.json({ limit: "8mb" }));

const allowedOrigins = CORS_ORIGIN === "*" ? "*" : CORS_ORIGIN.split(",").map((s) => s.trim());
app.use(cors({ origin: allowedOrigins, credentials: false }));

const authLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 30,
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: "Too many attempts. Try again later." },
});
const writeLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: 60,
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: "You're sending requests too quickly." },
});

function signToken(userId) {
  return jwt.sign({ sub: userId }, EFFECTIVE_JWT_SECRET, { expiresIn: "30d" });
}

async function requireAuth(req, res, next) {
  const header = req.headers.authorization || "";
  const token = header.startsWith("Bearer ") ? header.slice(7) : req.query.token || null;
  if (!token) return res.status(401).json({ error: "Not authenticated" });
  try {
    const payload = jwt.verify(token, EFFECTIVE_JWT_SECRET);
    const user = await getUserById(payload.sub);
    if (!user || user.deleted) return res.status(401).json({ error: "Account not found" });
    req.user = user;
    next();
  } catch (e) {
    return res.status(401).json({ error: "Invalid or expired session" });
  }
}

function isValidEmail(email) {
  return typeof email === "string" && /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
}
function isValidUsername(username) {
  return typeof username === "string" && /^[a-zA-Z0-9_.]{3,20}$/.test(username);
}

// Wrap async route handlers so a thrown/rejected error returns 500 instead of hanging
const ah = (fn) => (req, res, next) => fn(req, res, next).catch(next);

// ---------- Membership / authorization helpers ----------
// Every conversation/message action re-checks the caller's ACTUAL role in
// conversation_members -- never whatever the client claims about itself.
async function requireMembership(conversationId, userId) {
  const rows = await query(
    "SELECT * FROM conversation_members WHERE conversation_id = ? AND user_id = ? AND left_at IS NULL",
    [conversationId, userId]
  );
  return rows[0] || null;
}
async function getConversation(conversationId) {
  const rows = await query("SELECT * FROM conversations WHERE id = ?", [conversationId]);
  return rows[0] || null;
}
function isPrivileged(role) {
  return role === "admin" || role === "moderator";
}
function checkGate(gate, role) {
  return gate === "everyone" || isPrivileged(role);
}

// ---------- Auth routes ----------
app.post("/api/auth/register", authLimiter, ah(async (req, res) => {
  const { name, password } = req.body || {};
  const email = String(req.body?.email || "").trim().toLowerCase();
  const username = String(req.body?.username || "").trim().toLowerCase();

  if (!email || !isValidEmail(email) || !name || !username || !password) {
    return res.status(400).json({ error: "Name, username, a valid email, and a password are required" });
  }
  if (!isValidUsername(username)) {
    return res.status(400).json({ error: "Username must be 3-20 characters: letters, numbers, underscores only" });
  }
  if (String(password).length < 8) {
    return res.status(400).json({ error: "Password must be at least 8 characters" });
  }

  const existingByEmail = await getUserByEmail(email);
  if (existingByEmail && !existingByEmail.deleted) {
    return res.status(409).json({ error: "An account with that email already exists" });
  }
  const existingByUsername = await getUserByUsername(username);
  if (existingByUsername && !existingByUsername.deleted) {
    return res.status(409).json({ error: "That username is already taken" });
  }
  if (existingByEmail && existingByEmail.deleted) await run("DELETE FROM users WHERE id = ?", [existingByEmail.id]);

  const userId = newId("u");
  const hash = bcrypt.hashSync(password, 10);
  const createdAt = new Date().toISOString();
  const displayName = String(name).trim();

  await run(
    "INSERT INTO users (id, email, username, name, password_hash, status, created_at) VALUES (?, ?, ?, ?, ?, 'online', ?)",
    [userId, email, username, displayName, hash, createdAt]
  );
  await run("INSERT INTO user_preferences (user_id, updated_at) VALUES (?, ?)", [userId, createdAt]);

  const user = await getUserById(userId);
  const token = signToken(userId);
  res.status(201).json({ token, user: toPublicUser(user) });
}));

app.post("/api/auth/login", authLimiter, ah(async (req, res) => {
  const { identifier, password } = req.body || {};
  if (!identifier || !password) {
    return res.status(400).json({ error: "Email/username and password are required" });
  }
  const user = await getUserByIdentifier(identifier);
  if (!user || !bcrypt.compareSync(String(password), user.password_hash)) {
    return res.status(401).json({ error: "That email/username or password looks incorrect" });
  }
  let restored = false;
  if (user.deleted) {
    await run("UPDATE users SET deleted = 0, deleted_at = NULL WHERE id = ?", [user.id]);
    restored = true;
  }
  await run("UPDATE users SET status = 'online', last_seen_at = ? WHERE id = ?", [new Date().toISOString(), user.id]);
  const token = signToken(user.id);
  res.json({ token, user: toPublicUser(await getUserById(user.id)), restored });
}));

app.post("/api/auth/logout", requireAuth, ah(async (req, res) => {
  await run("UPDATE users SET status = 'offline', last_seen_at = ? WHERE id = ?", [new Date().toISOString(), req.user.id]);
  res.status(204).send();
}));

// ---------- Current user ----------
// GET/PUT /api/me and /api/me/data below are a compatibility layer for the
// existing frontend, which currently syncs its whole app state (settings,
// profile, conversations, notifications) as one JSON-shaped read/write —
// same contract as the previous JSON-blob backend. Under the hood this now
// reads/writes the real relational tables (conversations, messages,
// notifications, user_preferences) instead of a single blob column, so the
// data is fully queryable — the granular /api/conversations,
// /api/conversations/:id/messages, etc. routes above operate on the exact
// same tables and are the recommended way to integrate a rewritten
// frontend that manages conversations/messages incrementally instead of
// one big synced blob.
app.get("/api/me", requireAuth, ah(async (req, res) => {
  const prefRows = await query("SELECT * FROM user_preferences WHERE user_id = ?", [req.user.id]);
  const settings = prefRows[0] ? JSON.parse(prefRows[0].settings || "{}") : {};
  const palette = prefRows[0]?.palette || "signal";

  const convoRows = await query(
    `SELECT c.*, cm.role as my_role, cm.pinned as my_pinned, cm.muted as my_muted, cm.archived as my_archived, cm.last_read_message_id
     FROM conversation_members cm JOIN conversations c ON c.id = cm.conversation_id
     WHERE cm.user_id = ? AND cm.left_at IS NULL ORDER BY c.last_message_at DESC`,
    [req.user.id]
  );
  const conversations = [];
  const chatThemes = {};
  const otherUsersById = {};
  for (const row of convoRows) {
    const detail = await hydrateConversation(row.id);
    const messageRows = await query("SELECT * FROM messages WHERE conversation_id = ? ORDER BY created_at ASC LIMIT 200", [row.id]);
    const messages = await hydrateMessages(messageRows);

    const participantIds = detail.members.map((m) => m.userId).filter((uid) => uid !== req.user.id);
    detail.members.forEach((m) => {
      if (m.userId === req.user.id) return;
      otherUsersById[m.userId] = { id: m.userId, name: m.name, handle: "@" + m.username, avatarColor: m.avatarColor, status: m.status };
    });

    let unread = 0;
    if (row.last_read_message_id) {
      const cutoffRows = await query("SELECT created_at FROM messages WHERE id = ?", [row.last_read_message_id]);
      const cutoff = cutoffRows[0] ? cutoffRows[0].created_at : "0";
      const unreadRows = await query("SELECT COUNT(*) as count FROM messages WHERE conversation_id = ? AND deleted = 0 AND created_at > ?", [row.id, cutoff]);
      unread = unreadRows[0]?.count || 0;
    } else {
      const unreadRows = await query("SELECT COUNT(*) as count FROM messages WHERE conversation_id = ? AND deleted = 0", [row.id]);
      unread = unreadRows[0]?.count || 0;
    }

    conversations.push({
      id: row.id, type: row.type, name: row.name, description: row.description,
      groupAvatarColor: row.group_avatar_color, privacy: row.privacy,
      whoCanSend: row.who_can_send, whoCanAddMembers: row.who_can_add_members, whoCanEditInfo: row.who_can_edit_info,
      createdAt: row.created_at, pinned: !!row.my_pinned, muted: !!row.my_muted, archived: !!row.my_archived,
      unread, pinnedMessageIds: detail.pinnedMessageIds, participantIds, members: detail.members, messages,
    });
  }

  const notifRows = await query("SELECT * FROM notifications WHERE user_id = ? ORDER BY created_at DESC LIMIT 100", [req.user.id]);
  const notifItems = notifRows.map((r) => ({
    id: r.id, type: r.type, conversationId: r.conversation_id, messageId: r.message_id,
    actorId: r.actor_id, preview: r.preview, read: !!r.read, createdAt: r.created_at,
  }));

  res.json({
    user: toPublicUser(req.user),
    data: {
      conversations, notifItems, settings, palette, chatThemes, otherUsersById,
      profile: {
        name: req.user.name, handle: "@" + req.user.username, bio: req.user.bio,
        statusMessage: req.user.status_message, avatarColor: req.user.avatar_color, status: req.user.status,
      },
    },
  });
}));

// Persists settings/profile/palette (the parts that don't already have
// their own dedicated endpoints above). Conversations/messages/notifications
// arriving in this payload are NOT written here — those are created via
// the granular /api/conversations and /api/conversations/:id/messages
// routes as they happen, so this endpoint only needs to keep the
// lightweight per-user fields in sync.
app.put("/api/me/data", requireAuth, ah(async (req, res) => {
  const { data } = req.body || {};
  if (!data || typeof data !== "object") return res.status(400).json({ error: "Missing data payload" });

  const now = new Date().toISOString();
  const existingPrefs = await query("SELECT user_id FROM user_preferences WHERE user_id = ?", [req.user.id]);
  if (existingPrefs.length > 0) {
    await run("UPDATE user_preferences SET settings = ?, palette = ?, updated_at = ? WHERE user_id = ?", [
      JSON.stringify(data.settings || {}), data.palette || "signal", now, req.user.id,
    ]);
  } else {
    await run("INSERT INTO user_preferences (user_id, settings, palette, updated_at) VALUES (?, ?, ?, ?)", [
      req.user.id, JSON.stringify(data.settings || {}), data.palette || "signal", now,
    ]);
  }

  if (data.profile) {
    const p = data.profile;
    const fields = [];
    const params = [];
    if (p.name !== undefined) { fields.push("name = ?"); params.push(String(p.name)); }
    if (p.bio !== undefined) { fields.push("bio = ?"); params.push(String(p.bio || "")); }
    if (p.statusMessage !== undefined) { fields.push("status_message = ?"); params.push(String(p.statusMessage || "")); }
    if (p.avatarColor !== undefined) { fields.push("avatar_color = ?"); params.push(String(p.avatarColor)); }
    if (fields.length > 0) {
      params.push(req.user.id);
      await run(`UPDATE users SET ${fields.join(", ")} WHERE id = ?`, params);
    }
  }

  res.json({ ok: true });
}));

app.post("/api/me/data/beacon", requireAuth, ah(async (req, res) => {
  // Same handler as PUT /api/me/data, reachable via navigator.sendBeacon()
  // on tab close (which can only POST and can't set an Authorization
  // header — requireAuth already accepts ?token= for that reason).
  const { data } = req.body || {};
  if (!data || typeof data !== "object") return res.status(400).json({ error: "Missing data payload" });
  const now = new Date().toISOString();
  const existingPrefs = await query("SELECT user_id FROM user_preferences WHERE user_id = ?", [req.user.id]);
  if (existingPrefs.length > 0) {
    await run("UPDATE user_preferences SET settings = ?, palette = ?, updated_at = ? WHERE user_id = ?", [
      JSON.stringify(data.settings || {}), data.palette || "signal", now, req.user.id,
    ]);
  } else {
    await run("INSERT INTO user_preferences (user_id, settings, palette, updated_at) VALUES (?, ?, ?, ?)", [
      req.user.id, JSON.stringify(data.settings || {}), data.palette || "signal", now,
    ]);
  }
  res.status(204).send();
}));

app.patch("/api/me", requireAuth, ah(async (req, res) => {
  const { name, bio, statusMessage, avatarColor } = req.body || {};
  const fields = [];
  const params = [];
  if (name !== undefined) { fields.push("name = ?"); params.push(String(name).trim()); }
  if (bio !== undefined) { fields.push("bio = ?"); params.push(String(bio)); }
  if (statusMessage !== undefined) { fields.push("status_message = ?"); params.push(String(statusMessage)); }
  if (avatarColor !== undefined) { fields.push("avatar_color = ?"); params.push(String(avatarColor)); }
  if (fields.length > 0) {
    params.push(req.user.id);
    await run(`UPDATE users SET ${fields.join(", ")} WHERE id = ?`, params);
  }
  res.json({ user: toPublicUser(await getUserById(req.user.id)) });
}));

app.patch("/api/me/status", requireAuth, ah(async (req, res) => {
  const status = req.body?.status;
  if (!["online", "away", "offline"].includes(status)) return res.status(400).json({ error: "Invalid status" });
  await run("UPDATE users SET status = ?, last_seen_at = ? WHERE id = ?", [status, new Date().toISOString(), req.user.id]);
  broadcastPresence(req.user.id, status);
  res.status(204).send();
}));

app.get("/api/users/search", requireAuth, ah(async (req, res) => {
  const q = String(req.query.q || "").trim().toLowerCase();
  if (!q) return res.json({ users: [] });
  const rows = await query(
    "SELECT * FROM users WHERE deleted = 0 AND id != ? AND (LOWER(username) LIKE ? OR LOWER(name) LIKE ?) LIMIT 20",
    [req.user.id, `%${q}%`, `%${q}%`]
  );
  res.json({ users: rows.map(toPublicUser) });
}));

// ---------- Preferences ----------
app.get("/api/preferences", requireAuth, ah(async (req, res) => {
  const rows = await query("SELECT * FROM user_preferences WHERE user_id = ?", [req.user.id]);
  const row = rows[0];
  res.json({
    preferences: row ? { settings: JSON.parse(row.settings || "{}"), palette: row.palette } : { settings: {}, palette: "signal" },
  });
}));

app.put("/api/preferences", requireAuth, ah(async (req, res) => {
  const { settings, palette } = req.body || {};
  const existing = await query("SELECT user_id FROM user_preferences WHERE user_id = ?", [req.user.id]);
  const now = new Date().toISOString();
  if (existing.length > 0) {
    await run("UPDATE user_preferences SET settings = ?, palette = ?, updated_at = ? WHERE user_id = ?", [
      JSON.stringify(settings || {}), palette || "signal", now, req.user.id,
    ]);
  } else {
    await run("INSERT INTO user_preferences (user_id, settings, palette, updated_at) VALUES (?, ?, ?, ?)", [
      req.user.id, JSON.stringify(settings || {}), palette || "signal", now,
    ]);
  }
  res.json({ ok: true });
}));

// ---------- Conversations ----------
async function hydrateConversation(conversationId) {
  const convo = await getConversation(conversationId);
  if (!convo) return null;
  const members = await query(
    `SELECT cm.user_id, cm.role, cm.joined_at, u.name, u.username, u.avatar_color, u.status
     FROM conversation_members cm JOIN users u ON u.id = cm.user_id
     WHERE cm.conversation_id = ? AND cm.left_at IS NULL`,
    [conversationId]
  );
  const pins = await query("SELECT message_id FROM pinned_messages WHERE conversation_id = ? ORDER BY pinned_at DESC", [conversationId]);
  return {
    conversation: convo,
    members: members.map((m) => ({
      userId: m.user_id, role: m.role, joinedAt: m.joined_at,
      name: m.name, username: m.username, avatarColor: m.avatar_color, status: m.status,
    })),
    pinnedMessageIds: pins.map((p) => p.message_id),
  };
}

app.get("/api/conversations", requireAuth, ah(async (req, res) => {
  const rows = await query(
    `SELECT c.*, cm.role as my_role, cm.pinned as my_pinned, cm.muted as my_muted,
            cm.archived as my_archived, cm.last_read_message_id
     FROM conversation_members cm JOIN conversations c ON c.id = cm.conversation_id
     WHERE cm.user_id = ? AND cm.left_at IS NULL ORDER BY c.last_message_at DESC`,
    [req.user.id]
  );
  const results = [];
  for (const row of rows) {
    let unread = 0;
    let cutoff = 0;
    if (row.last_read_message_id) {
      const cutoffRows = await query("SELECT created_at FROM messages WHERE id = ?", [row.last_read_message_id]);
      cutoff = cutoffRows[0] ? new Date(cutoffRows[0].created_at).getTime() : 0;
    }
    const unreadRows = await query(
      "SELECT COUNT(*) as count FROM messages WHERE conversation_id = ? AND deleted = 0 AND created_at > ?",
      [row.id, new Date(cutoff).toISOString()]
    );
    unread = unreadRows[0]?.count || 0;

    results.push({
      id: row.id, type: row.type, name: row.name, description: row.description,
      groupAvatarColor: row.group_avatar_color, privacy: row.privacy,
      whoCanSend: row.who_can_send, whoCanAddMembers: row.who_can_add_members, whoCanEditInfo: row.who_can_edit_info,
      lastMessageAt: row.last_message_at, createdAt: row.created_at,
      myRole: row.my_role, pinned: !!row.my_pinned, muted: !!row.my_muted, archived: !!row.my_archived,
      unreadCount: unread,
    });
  }
  res.json({ conversations: results });
}));

app.get("/api/conversations/:conversationId", requireAuth, ah(async (req, res) => {
  const membership = await requireMembership(req.params.conversationId, req.user.id);
  if (!membership) return res.status(403).json({ error: "You are not a member of this conversation" });
  const detail = await hydrateConversation(req.params.conversationId);
  if (!detail) return res.status(404).json({ error: "Conversation not found" });
  res.json(detail);
}));

app.post("/api/conversations", requireAuth, writeLimiter, ah(async (req, res) => {
  const { type } = req.body || {};
  const now = new Date().toISOString();

  if (type === "direct") {
    const otherUserId = req.body?.memberId;
    if (!otherUserId || otherUserId === req.user.id) return res.status(400).json({ error: "Invalid memberId" });
    const other = await getUserById(otherUserId);
    if (!other) return res.status(404).json({ error: "User not found" });

    const existing = await query(
      `SELECT c.id FROM conversations c
       JOIN conversation_members m1 ON m1.conversation_id = c.id AND m1.user_id = ? AND m1.left_at IS NULL
       JOIN conversation_members m2 ON m2.conversation_id = c.id AND m2.user_id = ? AND m2.left_at IS NULL
       WHERE c.type = 'direct' LIMIT 1`,
      [req.user.id, otherUserId]
    );
    if (existing[0]) {
      const detail = await hydrateConversation(existing[0].id);
      return res.status(200).json(detail);
    }

    const conversationId = newId("c");
    await run("INSERT INTO conversations (id, type, privacy, created_by, created_at) VALUES (?, 'direct', 'private', ?, ?)", [
      conversationId, req.user.id, now,
    ]);
    await run("INSERT INTO conversation_members (id, conversation_id, user_id, role, joined_at) VALUES (?, ?, ?, 'member', ?)", [
      newId("mem"), conversationId, req.user.id, now,
    ]);
    await run("INSERT INTO conversation_members (id, conversation_id, user_id, role, joined_at) VALUES (?, ?, ?, 'member', ?)", [
      newId("mem"), conversationId, otherUserId, now,
    ]);
    const detail = await hydrateConversation(conversationId);
    io.to(`user:${otherUserId}`).emit("conversation:created", detail);
    return res.status(201).json(detail);
  }

  if (type === "group" || type === "channel") {
    const name = String(req.body?.name || "").trim();
    if (!name) return res.status(400).json({ error: "Group/channel name is required" });
    const memberIds = Array.isArray(req.body?.memberIds) ? req.body.memberIds : [];

    const conversationId = newId("c");
    await run(
      `INSERT INTO conversations (id, type, name, description, privacy, who_can_send, who_can_add_members, who_can_edit_info, created_by, created_at)
       VALUES (?, ?, ?, ?, ?, ?, 'admins', 'admins', ?, ?)`,
      [
        conversationId, type, name, req.body?.description || null,
        req.body?.privacy || (type === "channel" ? "public" : "private"),
        type === "channel" ? "admins" : "everyone",
        req.user.id, now,
      ]
    );
    const uniqueIds = Array.from(new Set([req.user.id, ...memberIds]));
    for (const uid of uniqueIds) {
      const exists = await getUserById(uid);
      if (!exists) continue;
      await run("INSERT INTO conversation_members (id, conversation_id, user_id, role, joined_at) VALUES (?, ?, ?, ?, ?)", [
        newId("mem"), conversationId, uid, uid === req.user.id ? "admin" : "member", now,
      ]);
    }
    const detail = await hydrateConversation(conversationId);
    detail.members.forEach((m) => { if (m.userId !== req.user.id) io.to(`user:${m.userId}`).emit("conversation:created", detail); });
    return res.status(201).json(detail);
  }

  res.status(400).json({ error: "Invalid conversation type" });
}));

app.patch("/api/conversations/:conversationId", requireAuth, ah(async (req, res) => {
  const { conversationId } = req.params;
  const membership = await requireMembership(conversationId, req.user.id);
  if (!membership) return res.status(403).json({ error: "You are not a member of this conversation" });
  const convo = await getConversation(conversationId);
  if (!convo) return res.status(404).json({ error: "Conversation not found" });
  if (convo.type === "direct") return res.status(400).json({ error: "Direct conversations cannot be edited" });
  if (!checkGate(convo.who_can_edit_info, membership.role)) return res.status(403).json({ error: "Only admins can edit this conversation" });

  const allowed = ["name", "description", "group_avatar_color", "privacy", "who_can_send", "who_can_add_members", "who_can_edit_info"];
  const bodyKeyMap = { name: "name", description: "description", groupAvatarColor: "group_avatar_color", privacy: "privacy", whoCanSend: "who_can_send", whoCanAddMembers: "who_can_add_members", whoCanEditInfo: "who_can_edit_info" };
  const fields = [];
  const params = [];
  for (const [bodyKey, column] of Object.entries(bodyKeyMap)) {
    if (req.body?.[bodyKey] !== undefined && allowed.includes(column)) {
      fields.push(`${column} = ?`);
      params.push(req.body[bodyKey]);
    }
  }
  if (fields.length > 0) {
    params.push(conversationId);
    await run(`UPDATE conversations SET ${fields.join(", ")} WHERE id = ?`, params);
  }
  const detail = await hydrateConversation(conversationId);
  io.to(`conversation:${conversationId}`).emit("conversation:updated", detail);
  res.json(detail);
}));

app.post("/api/conversations/:conversationId/members", requireAuth, ah(async (req, res) => {
  const { conversationId } = req.params;
  const membership = await requireMembership(conversationId, req.user.id);
  if (!membership) return res.status(403).json({ error: "You are not a member of this conversation" });
  const convo = await getConversation(conversationId);
  if (!convo || convo.type === "direct") return res.status(400).json({ error: "Cannot add members here" });
  if (!checkGate(convo.who_can_add_members, membership.role)) return res.status(403).json({ error: "You don't have permission to add members" });

  const userIds = Array.isArray(req.body?.userIds) ? req.body.userIds : [];
  const now = new Date().toISOString();
  const added = [];
  for (const uid of userIds) {
    const exists = await getUserById(uid);
    if (!exists) continue;
    const already = await requireMembership(conversationId, uid);
    if (already) continue;
    await run("INSERT INTO conversation_members (id, conversation_id, user_id, role, joined_at) VALUES (?, ?, ?, 'member', ?)", [
      newId("mem"), conversationId, uid, now,
    ]);
    added.push(uid);
  }
  const detail = await hydrateConversation(conversationId);
  io.to(`conversation:${conversationId}`).emit("conversation:members_added", { conversationId, added });
  added.forEach((uid) => io.to(`user:${uid}`).emit("conversation:created", detail));
  res.status(201).json({ added });
}));

app.delete("/api/conversations/:conversationId/members/:userId", requireAuth, ah(async (req, res) => {
  const { conversationId, userId: targetUserId } = req.params;
  const convo = await getConversation(conversationId);
  if (!convo || convo.type === "direct") return res.status(400).json({ error: "Cannot remove members here" });

  if (req.user.id !== targetUserId) {
    const membership = await requireMembership(conversationId, req.user.id);
    if (!membership || !isPrivileged(membership.role)) return res.status(403).json({ error: "You don't have permission to remove members" });
  } else {
    const membership = await requireMembership(conversationId, req.user.id);
    if (!membership) return res.status(403).json({ error: "You are not a member of this conversation" });
  }

  await run("UPDATE conversation_members SET left_at = ? WHERE conversation_id = ? AND user_id = ?", [
    new Date().toISOString(), conversationId, targetUserId,
  ]);
  io.to(`conversation:${conversationId}`).emit("conversation:member_removed", { conversationId, userId: targetUserId });
  io.to(`user:${targetUserId}`).emit("conversation:removed", { conversationId });
  res.status(204).send();
}));

app.patch("/api/conversations/:conversationId/members/:userId/role", requireAuth, ah(async (req, res) => {
  const { conversationId, userId: targetUserId } = req.params;
  const role = req.body?.role;
  if (!["admin", "moderator", "member"].includes(role)) return res.status(400).json({ error: "Invalid role" });

  const membership = await requireMembership(conversationId, req.user.id);
  if (!membership || membership.role !== "admin") return res.status(403).json({ error: "Only admins can change roles" });
  const target = await requireMembership(conversationId, targetUserId);
  if (!target) return res.status(404).json({ error: "That user is not a member of this conversation" });

  await run("UPDATE conversation_members SET role = ? WHERE conversation_id = ? AND user_id = ?", [role, conversationId, targetUserId]);
  io.to(`conversation:${conversationId}`).emit("conversation:member_role_changed", { conversationId, userId: targetUserId, role });
  res.status(204).send();
}));

app.patch("/api/conversations/:conversationId/state", requireAuth, ah(async (req, res) => {
  const { conversationId } = req.params;
  const membership = await requireMembership(conversationId, req.user.id);
  if (!membership) return res.status(403).json({ error: "You are not a member of this conversation" });

  const fields = [];
  const params = [];
  if (req.body?.pinned !== undefined) { fields.push("pinned = ?"); params.push(req.body.pinned ? 1 : 0); }
  if (req.body?.muted !== undefined) { fields.push("muted = ?"); params.push(req.body.muted ? 1 : 0); }
  if (req.body?.archived !== undefined) { fields.push("archived = ?"); params.push(req.body.archived ? 1 : 0); }
  if (fields.length > 0) {
    params.push(conversationId, req.user.id);
    await run(`UPDATE conversation_members SET ${fields.join(", ")} WHERE conversation_id = ? AND user_id = ?`, params);
  }
  res.status(204).send();
}));

app.post("/api/conversations/:conversationId/read", requireAuth, ah(async (req, res) => {
  const { conversationId } = req.params;
  const { messageId } = req.body || {};
  const membership = await requireMembership(conversationId, req.user.id);
  if (!membership) return res.status(403).json({ error: "You are not a member of this conversation" });
  await run("UPDATE conversation_members SET last_read_message_id = ? WHERE conversation_id = ? AND user_id = ?", [
    messageId, conversationId, req.user.id,
  ]);
  io.to(`conversation:${conversationId}`).emit("message:read", { conversationId, userId: req.user.id, messageId });
  res.status(204).send();
}));

app.post("/api/conversations/:conversationId/pins", requireAuth, ah(async (req, res) => {
  const { conversationId } = req.params;
  const { messageId } = req.body || {};
  const membership = await requireMembership(conversationId, req.user.id);
  if (!membership) return res.status(403).json({ error: "You are not a member of this conversation" });
  const convo = await getConversation(conversationId);
  if (convo.type !== "direct" && !checkGate(convo.who_can_edit_info, membership.role)) {
    return res.status(403).json({ error: "You don't have permission to pin messages" });
  }
  const msgRows = await query("SELECT id FROM messages WHERE id = ? AND conversation_id = ?", [messageId, conversationId]);
  if (!msgRows[0]) return res.status(404).json({ error: "Message not found in this conversation" });

  await run("INSERT OR IGNORE INTO pinned_messages (id, conversation_id, message_id, pinned_by, pinned_at) VALUES (?, ?, ?, ?, ?)", [
    newId("pin"), conversationId, messageId, req.user.id, new Date().toISOString(),
  ]);
  io.to(`conversation:${conversationId}`).emit("message:pinned", { conversationId, messageId });
  res.status(204).send();
}));

app.delete("/api/conversations/:conversationId/pins/:messageId", requireAuth, ah(async (req, res) => {
  const { conversationId, messageId } = req.params;
  const membership = await requireMembership(conversationId, req.user.id);
  if (!membership) return res.status(403).json({ error: "You are not a member of this conversation" });
  await run("DELETE FROM pinned_messages WHERE conversation_id = ? AND message_id = ?", [conversationId, messageId]);
  io.to(`conversation:${conversationId}`).emit("message:unpinned", { conversationId, messageId });
  res.status(204).send();
}));

// ---------- Messages ----------
async function hydrateMessages(rows) {
  if (rows.length === 0) return [];
  const ids = rows.map((r) => r.id);
  const placeholders = ids.map(() => "?").join(",");

  const mentionRows = await query(`SELECT * FROM message_mentions WHERE message_id IN (${placeholders})`, ids);
  const reactionRows = await query(`SELECT * FROM message_reactions WHERE message_id IN (${placeholders})`, ids);
  const attachmentRows = await query(`SELECT * FROM attachments WHERE message_id IN (${placeholders})`, ids);

  const mentionsByMsg = {};
  mentionRows.forEach((m) => { (mentionsByMsg[m.message_id] ||= []).push(m.mentioned_user_id); });

  const reactionsByMsg = {};
  reactionRows.forEach((r) => {
    reactionsByMsg[r.message_id] ||= {};
    (reactionsByMsg[r.message_id][r.emoji] ||= []).push(r.user_id);
  });

  const attachmentByMsg = {};
  attachmentRows.forEach((a) => { attachmentByMsg[a.message_id] = a; });

  // Embed the replied-to message's { id, text, senderId } inline (rather
  // than just its id) since that's the shape the frontend's reply-preview
  // UI expects — see index.html's seed data (`replyTo: { id, text, senderId }`).
  const replyIds = Array.from(new Set(rows.map((r) => r.reply_to_message_id).filter(Boolean)));
  const replyById = {};
  if (replyIds.length > 0) {
    const replyPlaceholders = replyIds.map(() => "?").join(",");
    const replyRows = await query(`SELECT id, text, sender_id, deleted FROM messages WHERE id IN (${replyPlaceholders})`, replyIds);
    replyRows.forEach((r) => { replyById[r.id] = { id: r.id, text: r.deleted ? "" : r.text, senderId: r.sender_id }; });
  }

  return rows.map((row) => {
    const attachment = attachmentByMsg[row.id];
    const reactionMap = reactionsByMsg[row.id];
    return {
      id: row.id, conversationId: row.conversation_id, senderId: row.sender_id, type: row.type,
      text: row.deleted ? "" : row.text,
      replyTo: row.reply_to_message_id ? (replyById[row.reply_to_message_id] || null) : null,
      edited: !!row.edited, editedAt: row.edited_at ? new Date(row.edited_at).getTime() : null,
      deleted: !!row.deleted, ts: new Date(row.created_at).getTime(),
      status: row.deleted ? "sent" : "sent",
      mentions: mentionsByMsg[row.id] || [],
      reactions: reactionMap ? Object.entries(reactionMap).map(([emoji, userIds]) => ({ emoji, userIds })) : [],
      attachment: attachment
        ? { id: attachment.id, kind: attachment.kind, name: attachment.name, url: attachment.url, mimeType: attachment.mime_type, sizeBytes: attachment.size_bytes, durationSeconds: attachment.duration_seconds }
        : null,
    };
  });
}

app.get("/api/conversations/:conversationId/messages", requireAuth, ah(async (req, res) => {
  const { conversationId } = req.params;
  const membership = await requireMembership(conversationId, req.user.id);
  if (!membership) return res.status(403).json({ error: "You are not a member of this conversation" });

  const limit = Math.min(Math.max(parseInt(req.query.limit, 10) || 50, 1), 100);
  let cutoff = "9999-12-31T23:59:59.999Z";
  if (req.query.before) {
    const cutoffRows = await query("SELECT created_at FROM messages WHERE id = ?", [req.query.before]);
    if (cutoffRows[0]) cutoff = cutoffRows[0].created_at;
  }
  const rows = await query(
    "SELECT * FROM messages WHERE conversation_id = ? AND created_at < ? ORDER BY created_at DESC LIMIT ?",
    [conversationId, cutoff, limit]
  );
  const hydrated = await hydrateMessages(rows.reverse());
  res.json({ messages: hydrated });
}));

app.post("/api/conversations/:conversationId/messages", requireAuth, writeLimiter, ah(async (req, res) => {
  const { conversationId } = req.params;
  const membership = await requireMembership(conversationId, req.user.id);
  if (!membership) return res.status(403).json({ error: "You are not a member of this conversation" });
  const convo = await getConversation(conversationId);
  if (!checkGate(convo.who_can_send, membership.role)) return res.status(403).json({ error: "You don't have permission to send messages here" });

  const text = String(req.body?.text || "").trim();
  const attachment = req.body?.attachment;
  if (!text && !attachment) return res.status(400).json({ error: "Message must have text or an attachment" });

  if (req.body?.replyToMessageId) {
    const replyRows = await query("SELECT id FROM messages WHERE id = ? AND conversation_id = ?", [req.body.replyToMessageId, conversationId]);
    if (!replyRows[0]) return res.status(400).json({ error: "The message being replied to was not found in this conversation" });
  }

  const messageId = newId("m");
  const createdAt = new Date().toISOString();
  await run("INSERT INTO messages (id, conversation_id, sender_id, text, reply_to_message_id, created_at) VALUES (?, ?, ?, ?, ?, ?)", [
    messageId, conversationId, req.user.id, text, req.body?.replyToMessageId || null, createdAt,
  ]);

  if (attachment) {
    await run(
      "INSERT INTO attachments (id, message_id, kind, name, url, mime_type, size_bytes, duration_seconds, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
      [newId("att"), messageId, attachment.kind, attachment.name, attachment.url, attachment.mimeType || null, attachment.sizeBytes || null, attachment.durationSeconds || null, createdAt]
    );
  }

  const memberRows = await query("SELECT user_id FROM conversation_members WHERE conversation_id = ? AND left_at IS NULL", [conversationId]);
  const memberIds = memberRows.map((m) => m.user_id);
  const requestedMentions = Array.isArray(req.body?.mentions) ? req.body.mentions : [];
  const validMentions = requestedMentions.filter((m) => m === "everyone" || memberIds.includes(m));
  for (const mentionedUserId of validMentions) {
    await run("INSERT INTO message_mentions (id, message_id, mentioned_user_id) VALUES (?, ?, ?)", [newId("men"), messageId, mentionedUserId]);
  }

  await run("UPDATE conversations SET last_message_at = ? WHERE id = ?", [createdAt, conversationId]);

  const mentionedSet = new Set(validMentions);
  for (const memberId of memberIds) {
    if (memberId === req.user.id) continue;
    const isMentioned = mentionedSet.has(memberId) || mentionedSet.has("everyone");
    const preview = text.slice(0, 140) || (attachment ? `Sent ${attachment.kind === "image" ? "an image" : "a file"}` : "");
    const notifId = newId("notif");
    await run(
      "INSERT INTO notifications (id, user_id, type, conversation_id, message_id, actor_id, preview, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
      [notifId, memberId, isMentioned ? "mention" : "message", conversationId, messageId, req.user.id, preview, createdAt]
    );
    io.to(`user:${memberId}`).emit("notification:new", { id: notifId, type: isMentioned ? "mention" : "message", conversationId, messageId, actorId: req.user.id, preview, createdAt });
  }

  const [row] = await query("SELECT * FROM messages WHERE id = ?", [messageId]);
  const [hydrated] = await hydrateMessages([row]);
  io.to(`conversation:${conversationId}`).emit("message:new", hydrated);
  res.status(201).json({ message: hydrated });
}));

app.patch("/api/conversations/:conversationId/messages/:messageId", requireAuth, ah(async (req, res) => {
  const { conversationId, messageId } = req.params;
  const membership = await requireMembership(conversationId, req.user.id);
  if (!membership) return res.status(403).json({ error: "You are not a member of this conversation" });
  const rows = await query("SELECT * FROM messages WHERE id = ? AND conversation_id = ?", [messageId, conversationId]);
  const existing = rows[0];
  if (!existing || existing.deleted) return res.status(404).json({ error: "Message not found" });
  if (existing.sender_id !== req.user.id) return res.status(403).json({ error: "You can only edit your own messages" });

  const text = String(req.body?.text || "").trim();
  if (!text) return res.status(400).json({ error: "Message text cannot be empty" });
  await run("UPDATE messages SET text = ?, edited = 1, edited_at = ? WHERE id = ?", [text, new Date().toISOString(), messageId]);

  const [row] = await query("SELECT * FROM messages WHERE id = ?", [messageId]);
  const [hydrated] = await hydrateMessages([row]);
  io.to(`conversation:${conversationId}`).emit("message:updated", hydrated);
  res.json({ message: hydrated });
}));

app.delete("/api/conversations/:conversationId/messages/:messageId", requireAuth, ah(async (req, res) => {
  const { conversationId, messageId } = req.params;
  const membership = await requireMembership(conversationId, req.user.id);
  if (!membership) return res.status(403).json({ error: "You are not a member of this conversation" });
  const rows = await query("SELECT * FROM messages WHERE id = ? AND conversation_id = ?", [messageId, conversationId]);
  const existing = rows[0];
  if (!existing || existing.deleted) return res.status(404).json({ error: "Message not found" });

  const isOwner = existing.sender_id === req.user.id;
  if (!isOwner && !isPrivileged(membership.role)) return res.status(403).json({ error: "You don't have permission to delete this message" });

  await run("UPDATE messages SET deleted = 1, deleted_at = ?, text = '' WHERE id = ?", [new Date().toISOString(), messageId]);
  io.to(`conversation:${conversationId}`).emit("message:deleted", { conversationId, messageId });
  res.status(204).send();
}));

app.post("/api/conversations/:conversationId/messages/:messageId/reactions", requireAuth, ah(async (req, res) => {
  const { conversationId, messageId } = req.params;
  const membership = await requireMembership(conversationId, req.user.id);
  if (!membership) return res.status(403).json({ error: "You are not a member of this conversation" });
  const emoji = String(req.body?.emoji || "").trim();
  if (!emoji) return res.status(400).json({ error: "emoji is required" });
  const msgRows = await query("SELECT id FROM messages WHERE id = ? AND conversation_id = ?", [messageId, conversationId]);
  if (!msgRows[0]) return res.status(404).json({ error: "Message not found" });

  await run("INSERT OR IGNORE INTO message_reactions (id, message_id, user_id, emoji, created_at) VALUES (?, ?, ?, ?, ?)", [
    newId("rx"), messageId, req.user.id, emoji, new Date().toISOString(),
  ]);
  io.to(`conversation:${conversationId}`).emit("message:reaction_added", { conversationId, messageId, userId: req.user.id, emoji });
  res.status(204).send();
}));

app.delete("/api/conversations/:conversationId/messages/:messageId/reactions/:emoji", requireAuth, ah(async (req, res) => {
  const { conversationId, messageId, emoji } = req.params;
  const membership = await requireMembership(conversationId, req.user.id);
  if (!membership) return res.status(403).json({ error: "You are not a member of this conversation" });
  await run("DELETE FROM message_reactions WHERE message_id = ? AND user_id = ? AND emoji = ?", [messageId, req.user.id, emoji]);
  io.to(`conversation:${conversationId}`).emit("message:reaction_removed", { conversationId, messageId, userId: req.user.id, emoji });
  res.status(204).send();
}));

// ---------- Notifications ----------
app.get("/api/notifications", requireAuth, ah(async (req, res) => {
  const unreadOnly = req.query.unreadOnly === "true";
  const limit = Math.min(Math.max(parseInt(req.query.limit, 10) || 50, 1), 100);
  const rows = unreadOnly
    ? await query("SELECT * FROM notifications WHERE user_id = ? AND read = 0 ORDER BY created_at DESC LIMIT ?", [req.user.id, limit])
    : await query("SELECT * FROM notifications WHERE user_id = ? ORDER BY created_at DESC LIMIT ?", [req.user.id, limit]);
  res.json({
    notifications: rows.map((r) => ({
      id: r.id, type: r.type, conversationId: r.conversation_id, messageId: r.message_id,
      actorId: r.actor_id, preview: r.preview, read: !!r.read, createdAt: r.created_at,
    })),
  });
}));

app.post("/api/notifications/read-all", requireAuth, ah(async (req, res) => {
  await run("UPDATE notifications SET read = 1 WHERE user_id = ? AND read = 0", [req.user.id]);
  res.status(204).send();
}));

app.patch("/api/notifications/:notificationId/read", requireAuth, ah(async (req, res) => {
  const rows = await query("SELECT id FROM notifications WHERE id = ? AND user_id = ?", [req.params.notificationId, req.user.id]);
  if (!rows[0]) return res.status(404).json({ error: "Notification not found" });
  await run("UPDATE notifications SET read = 1 WHERE id = ?", [req.params.notificationId]);
  res.status(204).send();
}));

app.delete("/api/notifications/:notificationId", requireAuth, ah(async (req, res) => {
  const rows = await query("SELECT id FROM notifications WHERE id = ? AND user_id = ?", [req.params.notificationId, req.user.id]);
  if (!rows[0]) return res.status(404).json({ error: "Notification not found" });
  await run("DELETE FROM notifications WHERE id = ?", [req.params.notificationId]);
  res.status(204).send();
}));

// ---------- Search ----------
app.get("/api/search", requireAuth, ah(async (req, res) => {
  const q = String(req.query.q || "").trim();
  if (!q) return res.json({ conversations: [], messages: [] });

  const memberRows = await query("SELECT conversation_id FROM conversation_members WHERE user_id = ? AND left_at IS NULL", [req.user.id]);
  const convoIds = memberRows.map((r) => r.conversation_id);
  if (convoIds.length === 0) return res.json({ conversations: [], messages: [] });
  const placeholders = convoIds.map(() => "?").join(",");
  const term = `%${q.toLowerCase()}%`;

  const matchingConvos = await query(
    `SELECT * FROM conversations WHERE id IN (${placeholders}) AND LOWER(name) LIKE ? LIMIT 20`,
    [...convoIds, term]
  );
  const matchingMessages = await query(
    `SELECT * FROM messages WHERE conversation_id IN (${placeholders}) AND deleted = 0 AND LOWER(text) LIKE ? ORDER BY created_at DESC LIMIT 30`,
    [...convoIds, term]
  );
  const hydrated = await hydrateMessages(matchingMessages);
  res.json({ conversations: matchingConvos, messages: hydrated });
}));

app.get("/health", (req, res) => res.json({ ok: true }));

// ---------- Serve frontend ----------
const PUBLIC_DIR = path.join(__dirname, "public");
const ROOT_INDEX = path.join(__dirname, "index.html");
const FRONTEND_DIR = fs.existsSync(path.join(PUBLIC_DIR, "index.html"))
  ? PUBLIC_DIR
  : fs.existsSync(ROOT_INDEX)
  ? __dirname
  : null;

if (FRONTEND_DIR) {
  console.log(`Serving frontend from: ${FRONTEND_DIR}`);
  app.use(express.static(FRONTEND_DIR, { index: false }));
  app.get(/^(?!\/api\/).*/, (req, res) => {
    res.sendFile(path.join(FRONTEND_DIR, "index.html"));
  });
} else {
  console.warn("No index.html found in ./public or the repo root -- this deployment will only serve the API.");
}

app.use((req, res) => res.status(404).json({ error: "Not found" }));
app.use((err, req, res, next) => {
  console.error(err);
  res.status(500).json({ error: "Server error" });
});

// ---------- Socket.IO (real-time) ----------
const httpServer = http.createServer(app);
const io = new SocketIOServer(httpServer, {
  cors: { origin: allowedOrigins, credentials: false },
  pingInterval: 25000,
  pingTimeout: 20000,
});

const socketConnectionCounts = new Map(); // userId -> open socket count (multi-tab aware)
const typingTimers = new Map(); // "conversationId:userId" -> timeout handle

function broadcastPresence(userId, status) {
  query("SELECT conversation_id FROM conversation_members WHERE user_id = ? AND left_at IS NULL", [userId]).then((rows) => {
    rows.forEach((r) => io.to(`conversation:${r.conversation_id}`).emit("presence:update", { userId, status, lastSeenAt: new Date().toISOString() }));
  });
}

io.use((socket, next) => {
  try {
    const token = socket.handshake.auth?.token;
    if (!token) return next(new Error("Authentication required"));
    const payload = jwt.verify(token, EFFECTIVE_JWT_SECRET);
    socket.userId = payload.sub;
    next();
  } catch (e) {
    next(new Error("Invalid or expired token"));
  }
});

io.on("connection", (socket) => {
  const { userId } = socket;

  (async () => {
    socket.join(`user:${userId}`);
    const rows = await query("SELECT conversation_id FROM conversation_members WHERE user_id = ? AND left_at IS NULL", [userId]);
    rows.forEach((r) => socket.join(`conversation:${r.conversation_id}`));

    const prior = socketConnectionCounts.get(userId) || 0;
    socketConnectionCounts.set(userId, prior + 1);
    if (prior === 0) {
      await run("UPDATE users SET status = 'online', last_seen_at = ? WHERE id = ?", [new Date().toISOString(), userId]);
      broadcastPresence(userId, "online");
    }
  })().catch((e) => console.error("Socket connection setup error:", e.message));

  socket.on("typing:start", (payload) => {
    const conversationId = payload && payload.conversationId;
    if (!conversationId || !socket.rooms.has(`conversation:${conversationId}`)) return;
    const key = `${conversationId}:${userId}`;
    const existing = typingTimers.get(key);
    if (existing) clearTimeout(existing);
    socket.to(`conversation:${conversationId}`).emit("typing:update", { conversationId, userId, typing: true });
    typingTimers.set(key, setTimeout(() => {
      socket.to(`conversation:${conversationId}`).emit("typing:update", { conversationId, userId, typing: false });
      typingTimers.delete(key);
    }, 5000));
  });

  socket.on("typing:stop", (payload) => {
    const conversationId = payload && payload.conversationId;
    if (!conversationId) return;
    const key = `${conversationId}:${userId}`;
    const existing = typingTimers.get(key);
    if (existing) { clearTimeout(existing); typingTimers.delete(key); }
    socket.to(`conversation:${conversationId}`).emit("typing:update", { conversationId, userId, typing: false });
  });

  socket.on("conversation:join", (payload) => {
    const conversationId = payload && payload.conversationId;
    if (!conversationId) return;
    requireMembership(conversationId, userId).then((membership) => {
      if (membership) socket.join(`conversation:${conversationId}`);
    });
  });

  socket.on("disconnect", () => {
    (async () => {
      const remaining = Math.max(0, (socketConnectionCounts.get(userId) || 1) - 1);
      socketConnectionCounts.set(userId, remaining);
      if (remaining === 0) {
        socketConnectionCounts.delete(userId);
        await run("UPDATE users SET status = 'offline', last_seen_at = ? WHERE id = ?", [new Date().toISOString(), userId]);
        broadcastPresence(userId, "offline");
      }
    })().catch((e) => console.error("Socket disconnect error:", e.message));
  });
});

// ---------- Start server ----------
initDatabase()
  .then(() => {
    httpServer.listen(PORT, () => {
      console.log(`Echo backend listening on port ${PORT} (${NODE_ENV})`);
    });
  })
  .catch((e) => {
    console.error("FATAL: could not connect to Turso database:", e.message);
    process.exit(1);
  });
