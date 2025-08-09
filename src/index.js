import fs from 'fs';
import path from 'path';
import http from 'http';
import axios from 'axios';
import dotenv from 'dotenv';
import FormData from 'form-data';

dotenv.config();

// Configuration and constants
const ROOT_DIR = path.resolve(process.cwd());
const DATA_DIR = path.join(ROOT_DIR, 'data');
const STATE_FILE = path.join(DATA_DIR, 'state.json');
const CONFIG_FILE = path.join(ROOT_DIR, 'config.json');
const PUBLIC_PORT = process.env.PUBLIC_PORT ? Number(process.env.PUBLIC_PORT) : 8787;

// Simple structured logger
function log(level, message, meta = {}) { 
  const ts = new Date().toISOString();
  const flatMeta = Object.entries(meta)
    .map(([k, v]) => `${k}=${typeof v === 'object' ? JSON.stringify(v) : v}`)
    .join(' ');
  const line = flatMeta ? `[${ts}] [${level}] ${message} ${flatMeta}` : `[${ts}] [${level}] ${message}`;
  // eslint-disable-next-line no-console
  console.log(line);
}

// Utilities
function ensureDirectoryExists(directoryPath) {
  if (!fs.existsSync(directoryPath)) {
    fs.mkdirSync(directoryPath, { recursive: true });
  }
}

function readJsonFile(filePath, fallback) {
  try {
    const raw = fs.readFileSync(filePath, 'utf8');
    const text = raw.replace(/^\uFEFF/, '');
    return JSON.parse(text);
  } catch (err) {
    return fallback;
  }
}

function writeJsonFile(filePath, data) {
  fs.writeFileSync(filePath, JSON.stringify(data, null, 2), 'utf8');
}

function formatUtcDateToDisplay(date) {
  const yyyy = date.getUTCFullYear();
  const mm = String(date.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(date.getUTCDate()).padStart(2, '0');
  const hh = String(date.getUTCHours()).padStart(2, '0');
  const mi = String(date.getUTCMinutes()).padStart(2, '0');
  const ss = String(date.getUTCSeconds()).padStart(2, '0');
  return `${yyyy}-${mm}-${dd} ${hh}:${mi}:${ss}`;
}

function getConfigSignature(config) {
  const shallow = {
    startTimeUtc: config.startTimeUtc,
    endTimeUtc: config.endTimeUtc,
    targetTotalUsd: config.targetTotalUsd,
    minPerHour: config.minPerHour,
    maxPerHour: config.maxPerHour,
    amountPerMessageUsd: config.amountPerMessageUsd,
    tokensPerHundred: config.tokensPerHundred,
    enforceHardStopAtEnd: config.enforceHardStopAtEnd,
    specialPhase: config.specialPhase ? {
      enabled: !!config.specialPhase.enabled,
      countdownStartUtc: config.specialPhase.countdownStartUtc,
      presaleStartUtc: config.specialPhase.presaleStartUtc,
      initialBurstMinutes: config.specialPhase.initialBurstMinutes,
      initialBurstCount: config.specialPhase.initialBurstCount
    } : null
  };
  return JSON.stringify(shallow);
}

function toIsoHour(date) {
  const d = new Date(date);
  d.setUTCMinutes(0, 0, 0);
  return d.toISOString();
}

// Time management with one-time internet sync
let timeOffset = null; // Difference between internet UTC and system time

async function initializeTimeOffset() {
  if (timeOffset !== null) return; // Already initialized
  
  log('INFO', 'Initializing time offset with internet UTC');
  const systemTime = new Date();
  
  // Try first API: worldtimeapi.org
  try {
    log('INFO', 'Attempting to fetch time from worldtimeapi.org');
    const res = await axios.get('https://worldtimeapi.org/api/timezone/Etc/UTC', { timeout: 8000 });
    const internetTime = new Date(res.data.utc_datetime);
    timeOffset = internetTime.getTime() - systemTime.getTime();
    log('INFO', 'Successfully synchronized with worldtimeapi.org', { 
      systemTime: systemTime.toISOString(),
      internetTime: internetTime.toISOString(),
      offsetMs: timeOffset
    });
    return;
  } catch (e) {
    log('WARN', 'Failed to fetch time from worldtimeapi.org', { 
      error: e.message, 
      code: e.code
    });
  }
  
  // Try second API: timeapi.io
  try {
    log('INFO', 'Attempting to fetch time from timeapi.io');
    const res2 = await axios.get('https://timeapi.io/api/Time/current/zone?timeZone=UTC', { timeout: 8000 });
    const formattedDate = `${res2.data.year}-${String(res2.data.month).padStart(2, '0')}-${String(res2.data.day).padStart(2, '0')}T${String(res2.data.hour).padStart(2, '0')}:${String(res2.data.minute).padStart(2, '0')}:${String(res2.data.seconds).padStart(2, '0')}Z`;
    const internetTime = new Date(formattedDate);
    timeOffset = internetTime.getTime() - systemTime.getTime();
    log('INFO', 'Successfully synchronized with timeapi.io', { 
      systemTime: systemTime.toISOString(),
      internetTime: internetTime.toISOString(),
      offsetMs: timeOffset
    });
    return;
  } catch (e2) {
    log('WARN', 'Failed to fetch time from timeapi.io', { 
      error: e2.message, 
      code: e2.code
    });
  }
  
  // Try third API: worldclockapi.com
  try {
    log('INFO', 'Attempting to fetch time from worldclockapi.com');
    const res3 = await axios.get('http://worldclockapi.com/api/json/utc/now', { timeout: 8000 });
    const internetTime = new Date(res3.data.currentDateTime);
    timeOffset = internetTime.getTime() - systemTime.getTime();
    log('INFO', 'Successfully synchronized with worldclockapi.com', { 
      systemTime: systemTime.toISOString(),
      internetTime: internetTime.toISOString(),
      offsetMs: timeOffset
    });
    return;
  } catch (e3) {
    log('WARN', 'Failed to fetch time from worldclockapi.com', { 
      error: e3.message, 
      code: e3.code
    });
  }
  
  // All APIs failed - use system time with warning
  timeOffset = 0;
  log('WARN', 'All time APIs failed, using system time as fallback', {
    systemTime: systemTime.toISOString()
  });
}

function getCurrentUtcTime() {
  if (timeOffset === null) {
    throw new Error('Time offset not initialized. Call initializeTimeOffset() first.');
  }
  return new Date(Date.now() + timeOffset);
}

// Legacy function for backward compatibility
async function getInternetUtcDate() {
  await initializeTimeOffset();
  return getCurrentUtcTime();
}

// Telegram send via HTTP API
async function sendTelegramMessage(botToken, chatId, text) {
  const url = `https://api.telegram.org/bot${botToken}/sendMessage`;
  await axios.post(url, {
    chat_id: chatId,
    text,
    parse_mode: 'HTML',
    disable_web_page_preview: true
  }, { timeout: 15000 });
}

// Telegram send photo with caption
async function sendTelegramPhoto(botToken, chatId, photoPath, caption) {
  const form = new FormData();
  
  form.append('chat_id', chatId);
  form.append('photo', fs.createReadStream(photoPath));
  if (caption) {
    form.append('caption', caption);
    form.append('parse_mode', 'HTML');
  }
  
  const url = `https://api.telegram.org/bot${botToken}/sendPhoto`;
  await axios.post(url, form, {
    headers: form.getHeaders(),
    timeout: 15000
  });
}

// Schedule generation
function sampleUniqueSecondsWithinHour(count) {
  const max = 3600;
  if (count >= max) {
    return Array.from({ length: max }, (_, i) => i);
  }
  const picked = new Set();
  while (picked.size < count) {
    picked.add(Math.floor(Math.random() * max));
  }
  return Array.from(picked).sort((a, b) => a - b);
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function floorToHourTs(timestampMs) {
  const d = new Date(timestampMs);
  d.setUTCMinutes(0, 0, 0);
  return d.getTime();
}

function secondOfHour(timestampMs) {
  return Math.floor((timestampMs - floorToHourTs(timestampMs)) / 1000);
}

function enforceUniqueIntervals(scheduleIsoArray) {
  if (scheduleIsoArray.length <= 1) return scheduleIsoArray;
  const times = scheduleIsoArray.map(iso => new Date(iso).getTime());
  const usedIntervals = new Set();
  const hourToSeconds = new Map();

  // initialize hour second sets
  for (const t of times) {
    const hourKey = floorToHourTs(t);
    const sec = secondOfHour(t);
    if (!hourToSeconds.has(hourKey)) hourToSeconds.set(hourKey, new Set());
    hourToSeconds.get(hourKey).add(sec);
  }

  // iterate and adjust duplicates
  for (let i = 1; i < times.length; i += 1) {
    const prev = times[i - 1];
    let curr = times[i];
    let deltaSec = Math.max(1, Math.round((curr - prev) / 1000));
    if (!usedIntervals.has(deltaSec)) {
      usedIntervals.add(deltaSec);
      continue;
    }

    const hourKey = floorToHourTs(curr);
    const hourStart = hourKey;
    const hourEnd = hourStart + 3600_000 - 1;
    const lowerBound = Math.max(hourStart, prev + 1000); // at least 1s after prev
    const upperBound = (i + 1 < times.length)
      ? Math.min(hourEnd, times[i + 1] - 1000)
      : hourEnd;
    if (lowerBound > upperBound) {
      // cannot adjust safely; keep as is (rare)
      usedIntervals.add(deltaSec);
      continue;
    }

    const usedSecsInHour = hourToSeconds.get(hourKey) || new Set();
    const currentSecOfHour = secondOfHour(curr);
    // free up current second temporarily
    usedSecsInHour.delete(currentSecOfHour);

    let found = false;
    // try expanding radius search around current second within bounds
    const radiusLimit = 300; // up to 5 minutes move if room
    const baseSec = secondOfHour(curr);
    const lbSec = Math.ceil((lowerBound - hourStart) / 1000);
    const ubSec = Math.floor((upperBound - hourStart) / 1000);
    for (let r = 1; r <= radiusLimit && !found; r += 1) {
      const candidates = [];
      const plus = baseSec + r;
      const minus = baseSec - r;
      if (plus <= ubSec) candidates.push(plus);
      if (minus >= lbSec) candidates.push(minus);
      for (const candSec of candidates) {
        if (usedSecsInHour.has(candSec)) continue;
        const candTs = hourStart + candSec * 1000;
        const candDelta = Math.max(1, Math.round((candTs - prev) / 1000));
        if (usedIntervals.has(candDelta)) continue;
        // accept
        curr = candTs;
        deltaSec = candDelta;
        found = true;
        break;
      }
    }

    if (!found) {
      // fallback: scan sequentially within bounds to find any unique combo
      for (let sec = lbSec; sec <= ubSec; sec += 1) {
        if (usedSecsInHour.has(sec)) continue;
        const candTs = hourStart + sec * 1000;
        const candDelta = Math.max(1, Math.round((candTs - prev) / 1000));
        if (usedIntervals.has(candDelta)) continue;
        curr = candTs;
        deltaSec = candDelta;
        found = true;
        break;
      }
    }

    // commit
    times[i] = curr;
    usedIntervals.add(deltaSec);
    usedSecsInHour.add(secondOfHour(curr));
    hourToSeconds.set(hourKey, usedSecsInHour);
  }

  return times.map(ts => new Date(ts).toISOString());
}

function generatePerHourWeights(startIso, hours) {
  const weights = [];
  for (let i = 0; i < hours; i += 1) {
    const d = new Date(new Date(startIso).getTime() + i * 3600_000);
    const hour = d.getUTCHours();
    const day = d.getUTCDay();
    const diurnal = 0.95 + 0.35 * Math.sin((2 * Math.PI * (hour + 2)) / 24); // peak around UTC afternoon/evening
    const weekdayBias = [0.9, 0.95, 1.0, 1.05, 1.1, 1.2, 1.25][day]; // higher on Fri/Sat
    const noise = 0.9 + Math.random() * 0.3; // 0.9..1.2
    weights.push(diurnal * weekdayBias * noise);
  }
  return weights;
}

function generateSchedule(config) {
  const start = new Date(config.startTimeUtc);
  const desiredEnd = new Date(config.endTimeUtc);
  const amountPerMessage = config.amountPerMessageUsd;
  const totalMessagesNeeded = Math.ceil(config.targetTotalUsd / amountPerMessage);

  const initialHours = Math.ceil((desiredEnd.getTime() - start.getTime()) / 3600_000);
  const minPerHour = config.minPerHour;
  const maxPerHour = config.maxPerHour;

  let hours = Math.max(1, initialHours);
  const maxPossible = hours * maxPerHour;
  if (maxPossible < totalMessagesNeeded) {
    const additionalHours = Math.ceil((totalMessagesNeeded - maxPossible) / maxPerHour);
    hours += additionalHours;
  }

  const weights = generatePerHourWeights(start.toISOString(), hours);
  const sumWeights = weights.reduce((a, b) => a + b, 0);

  let perHour = weights.map(w => Math.round((w / sumWeights) * totalMessagesNeeded));

  // Enforce maxPerHour and re-balance
  let currentTotal = perHour.reduce((a, b) => a + b, 0);
  if (currentTotal !== totalMessagesNeeded) {
    // Correct rounding to exact total while soft-clamping to maxPerHour
    // First clamp to max and recalc deficit
    perHour = perHour.map(n => Math.min(n, maxPerHour));
    currentTotal = perHour.reduce((a, b) => a + b, 0);
    let diff = totalMessagesNeeded - currentTotal;
    if (diff > 0) {
      const idxs = Array.from({ length: hours }, (_, i) => i).sort(() => Math.random() - 0.5);
      for (const i of idxs) {
        if (diff <= 0) break;
        const headroom = maxPerHour - perHour[i];
        if (headroom > 0) {
          const add = Math.min(headroom, diff);
          perHour[i] += add;
          diff -= add;
        }
      }
    } else if (diff < 0) {
      let remaining = -diff;
      const idxs = Array.from({ length: hours }, (_, i) => i).sort(() => Math.random() - 0.5);
      for (const i of idxs) {
        if (remaining <= 0) break;
        const reducible = Math.max(0, perHour[i] - 0);
        if (reducible > 0) {
          const r = Math.min(reducible, remaining);
          perHour[i] -= r;
          remaining -= r;
        }
      }
    }
  }

  // Soft floor to minPerHour while still meeting the total by borrowing from other hours with surplus
  // This is best-effort and only for realism; not guaranteed across all distributions
  let deficit = 0;
  for (let i = 0; i < perHour.length; i += 1) {
    if (perHour[i] < minPerHour) {
      const need = minPerHour - perHour[i];
      perHour[i] += need;
      deficit += need;
    }
  }
  if (deficit > 0) {
    // Reduce from hours above minPerHour in random order, respecting not to go below 0
    const idxs = Array.from({ length: hours }, (_, i) => i).sort(() => Math.random() - 0.5);
    for (const i of idxs) {
      if (deficit <= 0) break;
      const surplus = Math.max(0, perHour[i] - minPerHour);
      if (surplus > 0) {
        const take = Math.min(surplus, deficit);
        perHour[i] -= take;
        deficit -= take;
      }
    }
  }

  // Final adjust to exact total
  let sumNow = perHour.reduce((a, b) => a + b, 0);
  let finalDiff = totalMessagesNeeded - sumNow;
  if (finalDiff !== 0) {
    if (finalDiff > 0) {
      const idxs = Array.from({ length: hours }, (_, i) => i).sort(() => Math.random() - 0.5);
      for (const i of idxs) {
        if (finalDiff <= 0) break;
        const headroom = maxPerHour - perHour[i];
        if (headroom > 0) {
          const add = Math.min(headroom, finalDiff);
          perHour[i] += add;
          finalDiff -= add;
        }
      }
    } else {
      let remaining = -finalDiff;
      const idxs = Array.from({ length: hours }, (_, i) => i).sort(() => Math.random() - 0.5);
      for (const i of idxs) {
        if (remaining <= 0) break;
        const reducible = Math.max(0, perHour[i]);
        if (reducible > 0) {
          const r = Math.min(reducible, remaining);
          perHour[i] -= r;
          remaining -= r;
        }
      }
    }
  }

  const schedule = [];
  for (let h = 0; h < hours; h += 1) {
    const hourStart = new Date(start.getTime() + h * 3600_000);
    const seconds = sampleUniqueSecondsWithinHour(perHour[h]);
    for (const s of seconds) {
      const fireAt = new Date(hourStart.getTime() + s * 1000);
      schedule.push(fireAt.toISOString());
    }
  }
  schedule.sort();

  // Enforce unique inter-message intervals globally for realism
  const uniqueSchedule = enforceUniqueIntervals(schedule);

  const effectiveEnd = new Date(start.getTime() + hours * 3600_000);

  return { schedule: uniqueSchedule, effectiveEnd: effectiveEnd.toISOString(), totalMessagesNeeded };
}

function sampleUniqueSeconds(count, spanSeconds) {
  const max = Math.max(1, spanSeconds);
  if (count >= max) {
    return Array.from({ length: max }, (_, i) => i);
  }
  const picked = new Set();
  while (picked.size < count) {
    picked.add(Math.floor(Math.random() * max));
  }
  return Array.from(picked).sort((a, b) => a - b);
}

function generateFullSchedule(config) {
  const amountPerMessage = config.amountPerMessageUsd;
  const totalMessagesNeeded = Math.ceil(config.targetTotalUsd / amountPerMessage);

  const entries = [];

  let remainingMessages = totalMessagesNeeded;
  let normalStartIso = config.startTimeUtc;

  // Special phase (countdown + start + initial burst)
  if (config.specialPhase && config.specialPhase.enabled) {
    const countdownStart = new Date(config.specialPhase.countdownStartUtc);
    const presaleStart = new Date(config.specialPhase.presaleStartUtc);
    const initialBurstMinutes = config.specialPhase.initialBurstMinutes || 10;
    const initialBurstCount = config.specialPhase.initialBurstCount || 0;

    // Add countdown messages at specific intervals
    const countdownMessages = [
      { minutes: 59, image: '59.png' },
      { minutes: 30, image: '30.png' },
      { minutes: 5, image: '5.png' },
      { minutes: 4, image: '4.png' },
      { minutes: 3, image: '3.png' },
      { minutes: 2, image: '2.png' },
      { minutes: 1, image: '1.png' }
    ];
    
    for (const { minutes, image } of countdownMessages) {
      const t = new Date(presaleStart.getTime() - minutes * 60_000);
      const countdownText = `🚀 ${minutes} MINUTE${minutes !== 1 ? 'S' : ''} TO LAUNCH! 🚀

⚡ First Come, First Served — Only 14,000 spots!

👥 Whitelisted: 85,312
💰 Max: $100 each
💎 Supply: 10,000,000 $BBLP
📊 Hard Cap: $1.4M

🔗 Be ready: <a href="https://www.bblip.io/presale">bblip.io/presale</a>`;
      entries.push({ at: t.toISOString(), kind: 'countdown', text: countdownText, image });
    }
    // Presale started marker at presaleStart
    const launchText = `🎯 BBLIP PRESALE IS LIVE! 🎯

⚡ First Come, First Served — Only the first 14,000 investors get in.
👥 Whitelisted: 85,312
💰 Max: $100 each

💎 Supply: 10,000,000 $BBLP
📊 Hard Cap: $1,400,000

🔗 Secure your spot now: <a href="https://www.bblip.io/presale">bblip.io/presale</a>`;
    entries.push({ at: presaleStart.toISOString(), kind: 'start', text: launchText, image: 'live.png' });

    // Initial burst buys in next initialBurstMinutes window
    if (initialBurstCount > 0) {
      const burstStart = presaleStart.getTime();
      const burstSpanSec = initialBurstMinutes * 60;
      const secs = sampleUniqueSeconds(initialBurstCount, burstSpanSec);
      for (const s of secs) {
        const t = new Date(burstStart + s * 1000);
        entries.push({ at: t.toISOString(), kind: 'buy' });
      }
      remainingMessages -= Math.min(initialBurstCount, remainingMessages);
      normalStartIso = new Date(burstStart + burstSpanSec * 1000).toISOString();
    } else {
      normalStartIso = presaleStart.toISOString();
    }
  }

  // Generate remaining buys with the existing randomized per-hour engine
  if (remainingMessages > 0) {
    const normalConfig = { ...config, startTimeUtc: normalStartIso, targetTotalUsd: remainingMessages * amountPerMessage };
    const { schedule, effectiveEnd } = generateSchedule(normalConfig);
    for (const iso of schedule) entries.push({ at: iso, kind: 'buy' });
    entries.sort((a, b) => new Date(a.at) - new Date(b.at));
    return { schedule: entries, effectiveEnd };
  }

  entries.sort((a, b) => new Date(a.at) - new Date(b.at));
  const lastAt = entries.length ? entries[entries.length - 1].at : config.startTimeUtc;
  return { schedule: entries, effectiveEnd: lastAt };
}

function formatMoney(amount) {
  return amount.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function buildMessage(nowUtcDate, nextTotalUsd, tokensPerHundred) {
  const dateLine = formatUtcDateToDisplay(nowUtcDate);
  const tokensText = `${tokensPerHundred.toFixed(2)} $BBLP`;
  
  // Calculate stats
  const totalInvestors = Math.floor(nextTotalUsd / 100);
  const totalTokensSold = totalInvestors * tokensPerHundred;
  const spotsRemaining = Math.max(0, 14000 - totalInvestors);
  
  const text = [
    '═════ BBLIP PRESALE [ PHASE 3 ] ═════',
    '',
    '🚀 NEW PURCHASE!',
    '',
    `💰 $100.00 (${tokensText})`,
    `📅 ${dateLine} UTC`,
    '',
    `📊 Raised: $${formatMoney(nextTotalUsd)} / $1,400,000`,
    `💎 Sold: ${formatMoney(totalTokensSold)} / 10,000,000 $BBLP`,
    `👥 Spots Filled: ${totalInvestors.toLocaleString()} / 14,000`,
    `⚡ Remaining: ${spotsRemaining.toLocaleString()}`,
    '',
    '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━',
    '<a href="https://x.com/BblipProtocol">X</a> | <a href="https://discord.com/invite/w982fWnhe9">Discord</a> | <a href="http://bblip.io/whitepaper">Whitepaper</a> | <a href="https://bblip.io/tokenomics">Tokenomics</a> | <a href="https://www.bblip.io/presale">Presale</a>'
  ].join('\n');
  
  return { text, image: 'feed.png' };
}

async function main() {
  ensureDirectoryExists(DATA_DIR);
  
  // Initialize time offset once at startup
  await initializeTimeOffset();

  const fileConfig = readJsonFile(CONFIG_FILE, {});
  const envConfig = {
    BOT_TOKEN: process.env.BOT_TOKEN,
    TARGET_CHAT_ID: process.env.TARGET_CHAT_ID,
    startTimeUtc: process.env.START_TIME_UTC,
    endTimeUtc: process.env.END_TIME_UTC,
    targetTotalUsd: process.env.TARGET_TOTAL_USD ? Number(process.env.TARGET_TOTAL_USD) : undefined,
    amountPerMessageUsd: process.env.AMOUNT_PER_MESSAGE_USD ? Number(process.env.AMOUNT_PER_MESSAGE_USD) : undefined,
    tokensPerHundred: process.env.TOKENS_PER_HUNDRED ? Number(process.env.TOKENS_PER_HUNDRED) : undefined,
    startRaisedUsd: process.env.START_RAISED_USD ? Number(process.env.START_RAISED_USD) : undefined,
    minPerHour: process.env.MIN_PER_HOUR ? Number(process.env.MIN_PER_HOUR) : undefined,
    maxPerHour: process.env.MAX_PER_HOUR ? Number(process.env.MAX_PER_HOUR) : undefined,
    enforceHardStopAtEnd: process.env.ENFORCE_HARD_STOP === 'true'
  };

  // Only merge env vars that are actually defined (not undefined)
  const cleanEnvConfig = Object.fromEntries(
    Object.entries(envConfig).filter(([key, value]) => value !== undefined)
  );
  const merged = { ...fileConfig, ...cleanEnvConfig };
  const defaulted = {
    startTimeUtc: merged.startTimeUtc || '2025-08-08T17:45:00Z',
    endTimeUtc: merged.endTimeUtc || '2025-08-16T12:00:00Z',
    targetTotalUsd: merged.targetTotalUsd ?? 1_400_000,
    amountPerMessageUsd: merged.amountPerMessageUsd ?? 100,
    minPerHour: merged.minPerHour ?? 30,
    maxPerHour: merged.maxPerHour ?? 60,
    tokensPerHundred: merged.tokensPerHundred ?? 714.28,
    startRaisedUsd: merged.startRaisedUsd ?? 0,
    enforceHardStopAtEnd: merged.enforceHardStopAtEnd ?? false,
    BOT_TOKEN: merged.BOT_TOKEN,
    TARGET_CHAT_ID: merged.TARGET_CHAT_ID,
    specialPhase: merged.specialPhase
  };

  if (!defaulted.BOT_TOKEN || !defaulted.TARGET_CHAT_ID) {
    console.error('Please set BOT_TOKEN and TARGET_CHAT_ID in .env or config.json');
    process.exit(1);
  }

  const configSig = getConfigSignature(defaulted);
  let state = readJsonFile(STATE_FILE, null);
  if (!state || state.configSignature !== configSig) {
    const { schedule, effectiveEnd } = generateFullSchedule(defaulted);
    state = {
      configSignature: configSig,
      schedule, // array of ISO strings or event objects { at, kind, text? }
      effectiveEnd,
      sentCount: 0,
      totalUsdRaised: defaulted.startRaisedUsd,
      completed: false
    };
    writeJsonFile(STATE_FILE, state);
    const counts = state.schedule.reduce((acc, item) => {
      const kind = typeof item === 'string' ? 'buy' : (item.kind || 'buy');
      acc[kind] = (acc[kind] || 0) + 1;
      return acc;
    }, {});
    const firstIso = state.schedule.length ? (typeof state.schedule[0] === 'string' ? state.schedule[0] : state.schedule[0].at) : null;
    const lastIso = state.schedule.length ? (typeof state.schedule[state.schedule.length - 1] === 'string' ? state.schedule[state.schedule.length - 1] : state.schedule[state.schedule.length - 1].at) : null;
    log('INFO', 'Planned schedule', { total: state.schedule.length, effectiveEnd, counts, firstAt: firstIso, lastAt: lastIso });
  }

  // Start lightweight public HTTP endpoint
  const server = http.createServer((req, res) => {
    try {
      // CORS for browser-based consumers
      res.setHeader('Access-Control-Allow-Origin', '*');
      res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
      res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
      if (req.method === 'OPTIONS') {
        res.writeHead(204);
        res.end();
        return;
      }

      if (req.method === 'GET' && req.url && req.url.startsWith('/total-raised')) {
        const body = JSON.stringify({ totalUsdRaised: state?.totalUsdRaised ?? 0 });
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(body);
        return;
      }

      if (req.method === 'GET' && req.url && req.url.startsWith('/status')) {
        const remaining = Math.max(0, (state?.schedule?.length || 0) - (state?.sentCount || 0));
        const firstPending = state && state.schedule && state.sentCount < state.schedule.length
          ? (typeof state.schedule[state.sentCount] === 'string' ? state.schedule[state.sentCount] : state.schedule[state.sentCount].at)
          : null;
        const resp = {
          totalUsdRaised: state?.totalUsdRaised ?? 0,
          sentCount: state?.sentCount ?? 0,
          remaining,
          completed: !!state?.completed,
          effectiveEnd: state?.effectiveEnd ?? null,
          nextAt: firstPending,
          countdownStartUtc: defaulted?.specialPhase?.countdownStartUtc ?? null,
          presaleStartUtc: defaulted?.specialPhase?.presaleStartUtc ?? null
        };
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(JSON.stringify(resp));
        return;
      }

      if (req.method === 'GET' && req.url && req.url.startsWith('/countdown-start')) {
        const body = JSON.stringify({ countdownStartUtc: defaulted?.specialPhase?.countdownStartUtc ?? null });
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(body);
        return;
      }

      if (req.method === 'GET' && req.url && req.url.startsWith('/presale-start')) {
        const body = JSON.stringify({ presaleStartUtc: defaulted?.specialPhase?.presaleStartUtc ?? null });
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(body);
        return;
      }

      if (req.method === 'GET' && req.url && req.url.startsWith('/phases')) {
        const body = JSON.stringify({
          startTimeUtc: defaulted?.startTimeUtc ?? null,
          endTimeUtc: defaulted?.endTimeUtc ?? null,
          countdownStartUtc: defaulted?.specialPhase?.countdownStartUtc ?? null,
          presaleStartUtc: defaulted?.specialPhase?.presaleStartUtc ?? null
        });
        res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
        res.end(body);
        return;
      }

      res.writeHead(404, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ error: 'Not Found' }));
    } catch (err) {
      res.writeHead(500, { 'Content-Type': 'application/json; charset=utf-8' });
      res.end(JSON.stringify({ error: 'Internal Server Error' }));
    }
  });
  server.listen(PUBLIC_PORT, () => {
    log('INFO', 'Public HTTP server listening', { port: PUBLIC_PORT });
  });

  const { BOT_TOKEN, TARGET_CHAT_ID } = defaulted;

  // Enforce strict schedule: do not reschedule based on local or current time.
  // All timing decisions will be evaluated against internet UTC only.

  async function sendNextIfDue() {
    if (!state || state.completed) return;
    if (state.sentCount >= state.schedule.length) {
      state.completed = true;
      writeJsonFile(STATE_FILE, state);
      log('INFO', 'All messages sent');
      return;
    }
    // Use cached offset-corrected UTC time
    const internetNow = getCurrentUtcTime();
    const nextRaw = state.schedule[state.sentCount];
    let nextIso = typeof nextRaw === 'string' ? nextRaw : nextRaw.at;
    const nextEvent = typeof nextRaw === 'string' ? { kind: 'buy', at: nextIso } : { ...nextRaw };
    let nextTs = new Date(nextIso).getTime();
    let msUntil = nextTs - internetNow.getTime();

    // Skip past events to avoid backlog sends, but allow recent events (within 30 seconds)
    const MAX_ALLOWED_DELAY_MS = 30 * 1000; // 30 seconds tolerance
    while (msUntil < -MAX_ALLOWED_DELAY_MS && state.sentCount < state.schedule.length) {
      log('WARN', 'Skipping past event (too old)', { kind: nextEvent.kind, at: nextIso, behindMs: -msUntil });
      state.sentCount += 1;
      writeJsonFile(STATE_FILE, state);
      if (state.sentCount >= state.schedule.length) {
        state.completed = true;
        writeJsonFile(STATE_FILE, state);
        log('INFO', 'All messages sent');
        return;
      }
      const nr = state.schedule[state.sentCount];
      const ni = typeof nr === 'string' ? nr : nr.at;
      const ne = typeof nr === 'string' ? { kind: 'buy', at: ni } : { ...nr };
      nextTs = new Date(ni).getTime();
      msUntil = nextTs - internetNow.getTime();
      nextIso = ni;
      nextEvent.kind = ne.kind;
      nextEvent.at = ne.at;
    }

    if (msUntil <= 0) {
      const wasLate = msUntil < 0;
      try {
        if (nextEvent.kind === 'buy') {
          const nextTotal = state.totalUsdRaised + defaulted.amountPerMessageUsd;
          const messageData = buildMessage(internetNow, nextTotal, defaulted.tokensPerHundred);
          const imagePath = path.join(ROOT_DIR, messageData.image);
          await sendTelegramPhoto(BOT_TOKEN, TARGET_CHAT_ID, imagePath, messageData.text);
          state.sentCount += 1;
          state.totalUsdRaised = nextTotal;
          const logData = { 
            progress: `${state.sentCount}/${state.schedule.length}`, 
            totalRaised: `$${formatMoney(nextTotal)}`
          };
          if (wasLate) logData.lateByMs = -msUntil;
          log('INFO', 'Buy sent', logData);
        } else if (nextEvent.kind === 'countdown' || nextEvent.kind === 'start') {
          let msg = nextEvent.text;
          let imagePath = null;
          
          if (nextEvent.image) {
            imagePath = path.join(ROOT_DIR, nextEvent.image);
          }
          
          if (!msg && nextEvent.kind === 'start') {
            msg = `🎯 BBLIP PRESALE IS LIVE! 🎯

⚡ First Come, First Served — Only the first 14,000 investors get in.
👥 Whitelisted: 85,312
💰 Max: $100 each

💎 Supply: 10,000,000 $BBLP
📊 Hard Cap: $1,400,000

🔗 Secure your spot now: <a href="https://www.bblip.io/presale">bblip.io/presale</a>`;
            imagePath = path.join(ROOT_DIR, 'live.png');
          }
          
          if (imagePath && fs.existsSync(imagePath)) {
            await sendTelegramPhoto(BOT_TOKEN, TARGET_CHAT_ID, imagePath, msg);
          } else {
            await sendTelegramMessage(BOT_TOKEN, TARGET_CHAT_ID, msg);
          }
          
          state.sentCount += 1;
          log('INFO', 'Event sent', { kind: nextEvent.kind, progress: `${state.sentCount}/${state.schedule.length}` });
        } else {
          // unknown type, skip
          state.sentCount += 1;
        }
        writeJsonFile(STATE_FILE, state);
      } catch (err) {
        log('ERROR', 'Send failed, will retry shortly', { error: err.message || String(err) });
      }
      setTimeout(sendNextIfDue, 1_000);
      return;
    }
    // Poll at a safe cadence to re-check against internet UTC, never trusting local clock
    log('INFO', 'Waiting for next event', { nextKind: nextEvent.kind, at: nextIso, msUntil });
    setTimeout(sendNextIfDue, Math.min(msUntil, 5_000));
  }

  // If hard stop is enforced, trim schedule beyond end (no local time usage)
  if (defaulted.enforceHardStopAtEnd) {
    const endTs = new Date(defaulted.endTimeUtc).getTime();
    state.schedule = state.schedule.filter(item => {
      const iso = typeof item === 'string' ? item : item.at;
      return new Date(iso).getTime() <= endTs;
    });
    writeJsonFile(STATE_FILE, state);
  }

  // Start the scheduler loop (internet-UTC driven)
  sendNextIfDue();
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});


