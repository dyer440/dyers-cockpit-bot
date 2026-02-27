import "dotenv/config";
import { Client, GatewayIntentBits, Partials } from "discord.js";

function mustEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env var: ${name}`);
  return v;
}

function getEnv(name, def = "") {
  return process.env[name] || def;
}

function clampInt(n, def, min, max) {
  const x = Number(n);
  if (!Number.isFinite(x)) return def;
  return Math.max(min, Math.min(max, Math.floor(x)));
}

const CFG = {
  token: mustEnv("DISCORD_BOT_TOKEN"),
  apiBase: mustEnv("COCKPIT_API_BASE").replace(/\/$/, ""),
  ingestSecret: mustEnv("COCKPIT_INGEST_SECRET"),
  processUrl: getEnv("COCKPIT_PROCESS_URL", "").replace(/\/$/, ""),
  processSecret: getEnv("COCKPIT_PROCESS_SECRET", ""),
  channels: {
    reeRaw: mustEnv("REERAW_CHANNEL_ID"),
    coalRaw: mustEnv("COALRAW_CHANNEL_ID"),
    policyRaw: mustEnv("POLICYRAW_CHANNEL_ID"),
    botLogs: mustEnv("BOTLOGS_CHANNEL_ID"),
    reeBrief: getEnv("REEBRIEF_CHANNEL_ID", ""),
    coalBrief: getEnv("COALBRIEF_CHANNEL_ID", ""),
    triage: getEnv("TRIAGE_CHANNEL_ID", "1476283871208145087"),
  },
};

const intakeMap = new Map([
  [CFG.channels.reeRaw, { vertical: "ree" }],
  [CFG.channels.coalRaw, { vertical: "coal" }],
  [CFG.channels.policyRaw, { vertical: "policy" }],
]);

const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent,
  ],
  partials: [Partials.Channel],
});

// ---------- utils ----------
function extractUrls(text) {
  if (!text) return [];
  const matches = text.match(/https?:\/\/[^\s<>()]+/g);
  if (!matches) return [];
  return [...new Set(matches.map((u) => u.trim()))];
}

async function logToBotLogs(msg) {
  try {
    const ch = await client.channels.fetch(CFG.channels.botLogs);
    if (ch && ch.isTextBased()) await ch.send(msg);
  } catch {
    // ignore logging failures
  }
}

async function fetchTextChannel(channelId) {
  if (!channelId) return null;
  const ch = await client.channels.fetch(channelId);
  if (ch && ch.isTextBased()) return ch;
  return null;
}

function fmtTags(tags) {
  if (!Array.isArray(tags) || tags.length === 0) return "";
  const cleaned = tags
    .map((t) => String(t || "").trim())
    .filter(Boolean)
    .slice(0, 6);
  return cleaned.length ? `\n**Tags:** ${cleaned.join(", ")}` : "";
}

function fmtBullets(bullets) {
  if (!Array.isArray(bullets)) return "";
  const cleaned = bullets
    .map((b) => String(b || "").trim())
    .filter(Boolean)
    .slice(0, 5);
  if (!cleaned.length) return "";
  return cleaned.map((b) => `• ${b.replace(/^\s*[-•]\s*/, "").trim()}`).join("\n");
}

function buildBriefMessage(item, verticalLabel) {
  const score = Number(item?.relevance_score ?? 0);
  const title = String(item?.title ?? "").trim() || "(untitled)";
  const summary = String(item?.summary_1 ?? "").trim();
  const why = String(item?.why_it_matters ?? "").trim();
  const url = String(item?.url ?? "").trim();

  const bulletsBlock = fmtBullets(item?.bullets);
  const tagsBlock = fmtTags(item?.tags);

  const header = `🟣 **${verticalLabel} Brief** | Score: **${score}**`;
  const parts = [
    header,
    `\n**${title}**`,
    summary ? `\n${summary}` : "",
    bulletsBlock ? `\n${bulletsBlock}` : "",
    why ? `\n**Why it matters:** ${why}` : "",
    tagsBlock,
    url ? `\n**Source:** ${url}` : "",
  ].filter(Boolean);

  let msg = parts.join("\n");
  if (msg.length > 1900) {
    const shortBullets = bulletsBlock
      ? bulletsBlock.split("\n").slice(0, 3).join("\n")
      : "";
    const parts2 = [
      header,
      `\n**${title}**`,
      summary ? `\n${summary}` : "",
      shortBullets ? `\n${shortBullets}` : "",
      why ? `\n**Why it matters:** ${why}` : "",
      url ? `\n**Source:** ${url}` : "",
    ].filter(Boolean);
    msg = parts2.join("\n");
  }
  if (msg.length > 1900) msg = msg.slice(0, 1890) + "…";
  return msg;
}

// ---------- ingestion ----------
async function ingestOne({ url, vertical, message }) {
  const res = await fetch(`${CFG.apiBase}/api/ingest`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-cockpit-secret": CFG.ingestSecret,
    },
    body: JSON.stringify({
      url,
      vertical,
      source: "discord",
      source_channel_id: message.channelId,
      source_message_id: message.id,
      author_id: message.author?.id ?? null,
      author_username: message.author?.username ?? null,
      posted_at: new Date().toISOString(),
    }),
  });

  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`Ingest failed ${res.status}: ${body.slice(0, 300)}`);
  }

  return res.json().catch(() => ({}));
}

// ---------- processor loop ----------
async function runProcessorOnce() {
  try {
    const base = mustEnv("COCKPIT_PROCESS_URL").replace(/\/$/, "");
    const secret = mustEnv("COCKPIT_PROCESS_SECRET");
    const limit = clampInt(process.env.COCKPIT_PROCESS_LIMIT || 20, 20, 1, 50);

    const url = `${base}?secret=${encodeURIComponent(secret)}&limit=${encodeURIComponent(
      String(limit)
    )}`;

    const res = await fetch(url, { method: "GET" });
    const text = await res.text();

    if (!res.ok) {
      await logToBotLogs(`⚠️ Process run failed (${res.status}): ${text.slice(0, 400)}`);
      return;
    }

    let json;
    try {
      json = JSON.parse(text);
    } catch {
      await logToBotLogs(`⚠️ Process returned non-JSON: ${text.slice(0, 400)}`);
      return;
    }

    const processed = json?.processed ?? 0;
    const picked = json?.picked ?? 0;

    if (picked > 0 || processed > 0) {
      await logToBotLogs(`🧠 Process run: picked=${picked}, processed=${processed}`);
    }
  } catch (e) {
    await logToBotLogs(`🔥 Process runner crash: ${String(e?.message ?? e)}`);
  }
}

// ---------- publisher loop ----------
const publishLocks = { ree: false, coal: false };

async function fetchUnposted(vertical, limit) {
  const secret = mustEnv("COCKPIT_PROCESS_SECRET");
  const url = `${CFG.apiBase}/api/brief/unposted?vertical=${encodeURIComponent(
    vertical
  )}&limit=${encodeURIComponent(String(limit))}`;

  const res = await fetch(url, { method: "GET", headers: { "x-cockpit-secret": secret } });
  const txt = await res.text();

  if (!res.ok) throw new Error(`unposted fetch failed ${res.status}: ${txt.slice(0, 300)}`);

  let json;
  try {
    json = JSON.parse(txt);
  } catch {
    throw new Error(`unposted returned non-JSON: ${txt.slice(0, 300)}`);
  }

  return Array.isArray(json?.items) ? json.items : [];
}

async function markPosted(ids) {
  if (!ids || ids.length === 0) return 0;

  const secret = mustEnv("COCKPIT_PROCESS_SECRET");
  const res = await fetch(`${CFG.apiBase}/api/brief/mark-posted`, {
    method: "POST",
    headers: { "Content-Type": "application/json", "x-cockpit-secret": secret },
    body: JSON.stringify({ ids }),
  });

  const txt = await res.text();
  if (!res.ok) throw new Error(`mark-posted failed ${res.status}: ${txt.slice(0, 300)}`);

  try {
    const json = JSON.parse(txt);
    return Number(json?.updated ?? 0);
  } catch {
    return 0;
  }
}

async function publishVerticalOnce(vertical) {
  if (publishLocks[vertical]) return;
  publishLocks[vertical] = true;

  try {
    const limit = clampInt(process.env.COCKPIT_PUBLISH_LIMIT || 5, 5, 1, 10);

    let channelId = "";
    let label = "";

    if (vertical === "ree") {
      channelId = CFG.channels.reeBrief;
      label = "REE";
    } else if (vertical === "coal") {
      channelId = CFG.channels.coalBrief;
      label = "Coal";
    } else {
      return;
    }

    // Publishing not configured
    if (!channelId) return;

    const items = await fetchUnposted(vertical, limit);
    if (!items.length) return;

    const ch = await fetchTextChannel(channelId);
    if (!ch) throw new Error(`Brief channel not text-based: ${channelId}`);

    const triageCh = await fetchTextChannel(CFG.channels.triage);

    const TRIAGE_SCORE = clampInt(process.env.COCKPIT_TRIAGE_SCORE || 85, 85, 1, 100);

    for (const item of items) {
      const id = Number(item?.id);
      if (!Number.isFinite(id) || id <= 0) continue;

      const msg = buildBriefMessage(item, label);

      await ch.send(msg);

      const score = Number(item?.relevance_score ?? 0);
      if (triageCh && Number.isFinite(score) && score >= TRIAGE_SCORE) {
        await triageCh.send(`🚨 **High-signal ${label}** (Score: **${score}**)\n\n${msg}`);
        await logToBotLogs(`🚨 Triage posted ${vertical} processed_item_id=${id} score=${score}`);
      }

      await markPosted([id]);
      await logToBotLogs(`📣 Posted ${vertical} processed_item_id=${id}`);
    }
  } catch (e) {
    await logToBotLogs(`🔥 Publisher crash (${vertical}): ${String(e?.message ?? e)}`);
  } finally {
    publishLocks[vertical] = false;
  }
}

async function runPublisherOnce() {
  await publishVerticalOnce("ree");
  await publishVerticalOnce("coal");
}

// ---------- ready ----------
client.once("ready", async () => {
  console.log(`Logged in as ${client.user.tag}`);
  await logToBotLogs(`🟢 Online as ${client.user.tag}`);

  const procIntervalMin = clampInt(process.env.COCKPIT_PROCESS_INTERVAL_MIN || 10, 10, 1, 1440);
  const procIntervalMs = procIntervalMin * 60 * 1000;

  setTimeout(runProcessorOnce, 15_000);
  setInterval(runProcessorOnce, procIntervalMs);

  const pubIntervalMin = clampInt(process.env.COCKPIT_PUBLISH_INTERVAL_MIN || 15, 15, 1, 1440);
  const pubIntervalMs = pubIntervalMin * 60 * 1000;

  setTimeout(runPublisherOnce, 30_000);
  setInterval(runPublisherOnce, pubIntervalMs);
});

// ---------- message handler ----------
client.on("messageCreate", async (message) => {
  try {
    if (message.author?.bot) return;

    const route = intakeMap.get(message.channelId);
    if (!route) return;

    const urls = extractUrls(message.content);
    if (urls.length === 0) return;

    let insertedCount = 0;
    let dedupedCount = 0;
    const errs = [];

    for (const url of urls) {
      try {
        const out = await ingestOne({ url, vertical: route.vertical, message });
        if (out?.inserted) insertedCount += 1;
        else dedupedCount += 1;
      } catch (e) {
        errs.push({ url, err: String(e?.message ?? e) });
      }
    }

    if (errs.length === 0) {
      if (insertedCount > 0) await message.react("✅");
      if (dedupedCount > 0) await message.react("☑️");

      await logToBotLogs(
        `🧾 Ingest from <#${message.channelId}>: inserted=${insertedCount}, deduped=${dedupedCount}`
      );
    } else {
      await message.react("⚠️");
      await logToBotLogs(
        `⚠️ Ingest errors from <#${message.channelId}>. inserted=${insertedCount} deduped=${dedupedCount} err=${errs.length}\n` +
          errs.map((x) => `• ${x.url}\n  ↳ ${x.err}`).join("\n")
      );
    }
  } catch (e) {
    await logToBotLogs(`🔥 Handler crash: ${String(e?.message ?? e)}`);
  }
});

client.login(CFG.token);
