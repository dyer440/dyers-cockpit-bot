import "dotenv/config";
import { Client, GatewayIntentBits, Partials } from "discord.js";

function mustEnv(name) {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env var: ${name}`);
  return v;
}

const CFG = {
  token: mustEnv("DISCORD_BOT_TOKEN"),
  apiBase: mustEnv("COCKPIT_API_BASE").replace(/\/$/, ""),
  secret: mustEnv("COCKPIT_INGEST_SECRET"),
  channels: {
    reeRaw: mustEnv("REERAW_CHANNEL_ID"),
    coalRaw: mustEnv("COALRAW_CHANNEL_ID"),
    policyRaw: mustEnv("POLICYRAW_CHANNEL_ID"),
    botLogs: mustEnv("BOTLOGS_CHANNEL_ID"),
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

function extractUrls(text) {
  if (!text) return [];
  const matches = text.match(/https?:\/\/[^\s<>()]+/g);
  if (!matches) return [];
  return [...new Set(matches.map((u) => u.trim()))];
}

async function runProcessorOnce() {
  try {
    const base = mustEnv("COCKPIT_PROCESS_URL").replace(/\/$/, "");
    const secret = mustEnv("COCKPIT_PROCESS_SECRET");
    const limit = Number(process.env.COCKPIT_PROCESS_LIMIT || 20);

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

    // Only log when something happened
    if (picked > 0 || processed > 0) {
      await logToBotLogs(`🧠 Process run: picked=${picked}, processed=${processed}`);
    }
  } catch (e) {
    await logToBotLogs(`🔥 Process runner crash: ${String(e?.message ?? e)}`);
  }
}



async function logToBotLogs(msg) {
  try {
    const ch = await client.channels.fetch(CFG.channels.botLogs);
    if (ch && ch.isTextBased()) await ch.send(msg);
  } catch {
    // ignore logging failures
  }
}

async function ingestOne({ url, vertical, message }) {
  const res = await fetch(`${CFG.apiBase}/api/ingest`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-cockpit-secret": CFG.secret,
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

client.once("ready", async () => {
  console.log(`Logged in as ${client.user.tag}`);
  await logToBotLogs(`🟢 Online as ${client.user.tag}`);
});

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
    
        if (out?.inserted) {
          insertedCount += 1;
        } else {
          dedupedCount += 1;
        }
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
  } // <-- you need this brace
  
  } catch (e) {
    await logToBotLogs(`🔥 Handler crash: ${String(e?.message ?? e)}`);
  }
});

client.login(CFG.token);
