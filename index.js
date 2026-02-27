import "dotenv/config";
import { Client, GatewayIntentBits, Partials } from "discord.js";
import { XMLParser } from "fast-xml-parser";

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

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

    const TRIAGE_SCORE = clampInt(process.env.COCKPIT_TRIAGE_SCORE, 85, 1, 100);
    
    for (const item of items) {
      const id = Number(item?.id);
      if (!Number.isFinite(id) || id <= 0) continue;

      const msg = buildBriefMessage(item, label) + `\n<${item.url}>`;

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

const rssParser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: "@_",
});

function normLink(s) {
  const x = String(s || "").trim();
  if (!x) return "";
  // strip wrapping <> sometimes found
  return x.replace(/^<|>$/g, "");
}

function pickRssItems(parsed) {
  // RSS 2.0: rss.channel.item
  const channel = parsed?.rss?.channel;
  if (channel?.item) return Array.isArray(channel.item) ? channel.item : [channel.item];

  // Atom: feed.entry
  const feed = parsed?.feed;
  if (feed?.entry) return Array.isArray(feed.entry) ? feed.entry : [feed.entry];

  return [];
}

function getItemKey(item) {
  // Prefer guid/id, else link, else title hash-ish
  const guid = item?.guid?.["#text"] ?? item?.guid;
  const id = item?.id;
  const link =
    typeof item?.link === "string"
      ? item.link
      : item?.link?.["@_href"] ?? item?.link?.href ?? "";
  const title = item?.title?.["#text"] ?? item?.title ?? "";

  const key = String(guid || id || link || title).trim();
  return key ? key.slice(0, 500) : "";
}

function getItemLink(item) {
  // RSS: link can be string
  if (typeof item?.link === "string") return normLink(item.link);

  // Atom: link is object or array with href
  const l = item?.link;
  if (Array.isArray(l)) {
    const alt = l.find((x) => x?.["@_rel"] === "alternate") || l[0];
    return normLink(alt?.["@_href"] || alt?.href || "");
  }
  return normLink(l?.["@_href"] || l?.href || "");
}

async function cockpitGet(path) {
  const secret = mustEnv("COCKPIT_PROCESS_SECRET");
  const res = await fetch(`${CFG.apiBase}${path}`, {
    method: "GET",
    headers: { "x-cockpit-secret": secret },
  });
  const txt = await res.text();
  if (!res.ok) throw new Error(`GET ${path} failed ${res.status}: ${txt.slice(0, 300)}`);
  return JSON.parse(txt);
}

async function cockpitPost(path, body) {
  const secret = mustEnv("COCKPIT_PROCESS_SECRET");
  const res = await fetch(`${CFG.apiBase}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json", "x-cockpit-secret": secret },
    body: JSON.stringify(body),
  });
  const txt = await res.text();
  if (!res.ok) throw new Error(`POST ${path} failed ${res.status}: ${txt.slice(0, 300)}`);
  return JSON.parse(txt);
}

async function fetchSeenKeys(sourceId, keys) {
  if (!keys.length) return new Set();
  const out = await cockpitPost("/api/sources/rss/seen", { source_id: sourceId, keys });
  const seen = Array.isArray(out?.seen) ? out.seen : [];
  return new Set(seen);
}

async function ingestRssItem({ url, vertical, sourceId, sourceName, sourceType }) {
  const res = await fetch(`${CFG.apiBase}/api/ingest`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-cockpit-secret": CFG.ingestSecret,
    },
    body: JSON.stringify({
      url,
      vertical,
      source: "rss",
      metadata: { source_id: sourceId, source_name: sourceName, source_type: sourceType },      
      posted_at: new Date().toISOString(),
    }),
  });

  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`Ingest failed ${res.status}: ${body.slice(0, 300)}`);
  }

  return res.json().catch(() => ({}));
}

async function pollOneFeed(src) {
  const maxItems = clampInt(process.env.COCKPIT_RSS_MAX_ITEMS_PER_FEED || 10, 10, 1, 50);

  const defaultUa = getEnv(
    "COCKPIT_RSS_USER_AGENT",
    "DyersCockpitBot/1.0 (+https://dyerempire.com)"
  );

  const urlStr = String(src?.url || "");
  const domain = urlStr.replace(/^https?:\/\//i, "").split("/")[0].toLowerCase();
  const isSec = domain === "www.sec.gov" || domain === "sec.gov";

  // Politeness delay for SEC
  const secDelayMs = clampInt(process.env.COCKPIT_SEC_DELAY_MS || 1200, 1200, 0, 10000);
  if (isSec && secDelayMs > 0) {
    await sleep(secDelayMs);
  }

  // Optionally use a slightly more "browser-like" UA for SEC
  const ua = isSec
    ? getEnv(
        "COCKPIT_SEC_USER_AGENT",
        defaultUa
      )
    : defaultUa;

  let lastError = null;
  let etag = src?.etag || null;
  let lastModified = src?.last_modified || null;

  // Tag SEC as corporate
  const sourceType = isSec ? "corporate" : "rss";

  try {
    const headers = {
      "User-Agent": ua,
      Accept: "application/rss+xml, application/atom+xml, text/xml;q=0.9, */*;q=0.8",
    };
    if (etag) headers["If-None-Match"] = etag;
    if (lastModified) headers["If-Modified-Since"] = lastModified;

    const res = await fetch(src.url, { method: "GET", headers, redirect: "follow" });

    if (res.status === 304) {
      await cockpitPost("/api/sources/rss/report", {
        source_id: src.id,
        etag,
        last_modified: lastModified,
        last_error: null,
        seen_keys: [],
      });
      return { fetched: 0, ingested: 0, skipped: 0 };
    }

    const xml = await res.text();

    if (!res.ok) throw new Error(`RSS fetch failed ${res.status}: ${xml.slice(0, 200)}`);

    etag = res.headers.get("etag") || etag;
    lastModified = res.headers.get("last-modified") || lastModified;

    const parsed = rssParser.parse(xml);
    const items = pickRssItems(parsed).slice(0, maxItems);

    const keys = items.map(getItemKey).filter(Boolean);
    const seenSet = await fetchSeenKeys(src.id, keys);

    let ingested = 0;
    let skipped = 0;
    const newlySeen = [];

    for (const item of items) {
      const key = getItemKey(item);
      const link = getItemLink(item);
      if (!key || !link) continue;

      if (seenSet.has(key)) {
        skipped += 1;
        continue;
      }

      const out = await ingestRssItem({
        url: link,
        vertical: src.vertical,
        sourceId: src.id,
        sourceName: src.name,
        sourceType, // NEW
      });

      newlySeen.push(key);

      if (out?.inserted) ingested += 1;
      else skipped += 1;
    }

    await cockpitPost("/api/sources/rss/report", {
      source_id: src.id,
      etag,
      last_modified: lastModified,
      last_error: null,
      seen_keys: newlySeen,
    });

    return { fetched: items.length, ingested, skipped };
  } catch (e) {
    lastError = String(e?.message ?? e);
    await cockpitPost("/api/sources/rss/report", {
      source_id: src.id,
      etag,
      last_modified: lastModified,
      last_error: lastError,
      seen_keys: [],
    });
    throw e;
  }
}

let rssPolling = false;

async function runRssOnce() {
  if (rssPolling) return;
  rssPolling = true;

  try {
    const limitSources = clampInt(process.env.COCKPIT_RSS_LIMIT_SOURCES || 100, 100, 1, 500);
    const out = await cockpitGet(`/api/sources/rss?limit=${encodeURIComponent(String(limitSources))}`);
    const sources = Array.isArray(out?.sources) ? out.sources : [];

    if (!sources.length) return;

    let totalIngested = 0;
    let totalFetched = 0;
    let totalSkipped = 0;

    for (const src of sources) {
      try {
        const r = await pollOneFeed(src);
        totalFetched += r.fetched;
        totalIngested += r.ingested;
        totalSkipped += r.skipped;
      } catch (e) {
        await logToBotLogs(`⚠️ RSS poll error for "${src?.name}" (${src?.vertical}): ${String(e?.message ?? e)}`);
      }
    }

    if (totalIngested > 0) {
      await logToBotLogs(`📥 RSS inflow: fetched=${totalFetched} ingested=${totalIngested} skipped=${totalSkipped}`);
    }
  } catch (e) {
    await logToBotLogs(`🔥 RSS runner crash: ${String(e?.message ?? e)}`);
  } finally {
    rssPolling = false;
  }
}

// ---------- ready ----------
client.once("ready", async () => {
  console.log(`Logged in as ${client.user.tag}`);
  await logToBotLogs(`🟢 Online as ${client.user.tag}`);

  const procIntervalMin = clampInt(process.env.COCKPIT_PROCESS_INTERVAL_MIN || 10, 10, 1, 1440);
  const procIntervalMs = procIntervalMin * 60 * 1000;

  setTimeout(runProcessorOnce, 25_000);
  setInterval(runProcessorOnce, procIntervalMs);

  const pubIntervalMin = clampInt(process.env.COCKPIT_PUBLISH_INTERVAL_MIN || 15, 15, 1, 1440);
  const pubIntervalMs = pubIntervalMin * 60 * 1000;

  setTimeout(runPublisherOnce, 40_000);
  setInterval(runPublisherOnce, pubIntervalMs);

  const rssIntervalMin = clampInt(process.env.COCKPIT_RSS_INTERVAL_MIN || 10, 10, 1, 1440);
  const rssIntervalMs = rssIntervalMin * 60 * 1000;

  setTimeout(runRssOnce, 10_000);
  setInterval(runRssOnce, rssIntervalMs);
  
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
