// Records the demo sessions for the VS Code screenshots, then drives VS Code through them
// and captures what it shows. Run by scripts/vscode-screens.sh, which starts everything.
//
//   node scripts/vscode-screens.mjs record --session NAME --direct URL --lossy URL
//       one agent session: an email whose answer is lost on the way to the agent, an append
//       whose answer is lost on the way from the tool, and a read
//   node scripts/vscode-screens.mjs drive --port N --out DIR --originals DIR
//       clicks through the Ledger view of a VS Code started with --remote-debugging-port=N,
//       writes PNG screenshots cropped to the Ledger view and what it opens, and an
//       animated GIF, to --out, and each screenshot's whole window to --originals
//
// The clicks and key presses are real input events sent to VS Code's window through the
// Chrome DevTools Protocol, and the screenshots are what that window renders. The GIF is
// encoded here, with no dependencies.

import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { inflateSync } from "node:zlib";

const [mode, ...rest] = process.argv.slice(2);
const arg = (name) => {
  const i = rest.indexOf(`--${name}`);
  if (i < 0) throw new Error(`--${name} is required`);
  return rest[i + 1];
};
const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// ---- Recording: a minimal MCP client over streamable HTTP ----

class Agent {
  constructor(url, session) {
    this.url = url;
    this.session = session;
    this.id = 0;
  }

  async post(body, step) {
    const headers = {
      "content-type": "application/json",
      accept: "application/json, text/event-stream",
      "x-ledger-session": this.session,
    };
    if (this.mcpSession) headers["mcp-session-id"] = this.mcpSession;
    if (this.version) headers["mcp-protocol-version"] = this.version;
    if (step) headers["x-ledger-step"] = step;
    const response = await fetch(this.url, { method: "POST", headers, body: JSON.stringify(body) });
    this.mcpSession ??= response.headers.get("mcp-session-id");
    const text = await response.text();
    if (body.id === undefined) return null;
    const messages = response.headers.get("content-type")?.includes("text/event-stream")
      ? text.split("\n").filter((l) => l.startsWith("data:") && l.slice(5).trim()).map((l) => JSON.parse(l.slice(5)))
      : [JSON.parse(text)];
    const reply = messages.find((m) => m.id === body.id);
    if (!reply) throw new Error(`no answer to request ${body.id}: ${text.slice(0, 200)}`);
    return reply;
  }

  async connect() {
    const init = await this.post({
      jsonrpc: "2.0",
      id: ++this.id,
      method: "initialize",
      params: { protocolVersion: "2025-06-18", capabilities: {}, clientInfo: { name: "demo-agent", version: "1" } },
    });
    this.version = init.result.protocolVersion;
    await this.post({ jsonrpc: "2.0", method: "notifications/initialized" });
    return this;
  }

  call(step, name, args) {
    return this.post({ jsonrpc: "2.0", id: ++this.id, method: "tools/call", params: { name, arguments: args } }, step);
  }
}

/** Calls until answered, as agents do, reconnecting directly after a lost answer. */
async function untilAnswered(first, direct, step, name, args) {
  let agent = first;
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const reply = await agent.call(step, name, args);
      const outcome = reply.error ? `error ${reply.error.code}` : "answered";
      console.log(`step ${step}, ${name}, attempt ${attempt}: ${outcome}`);
      if (!reply.error || attempt === 2) return reply;
    } catch (error) {
      console.log(`step ${step}, ${name}, attempt ${attempt}: no answer (${error.cause?.code ?? error.message})`);
    }
    agent = await new Agent(direct, agent.session).connect();
  }
}

async function record() {
  const direct = arg("direct");
  const session = arg("session");
  const lossy = await new Agent(arg("lossy"), session).connect();
  const agent = await new Agent(direct, session).connect();
  // The agent's own connection loses the email's answer: the retry must not send it again.
  await untilAnswered(lossy, direct, "1", "email__send_email", {
    to: "ops@example.com",
    subject: "Nightly report",
    body: "All systems nominal.",
  });
  // The tool's answer to the append is lost before Ledger can log it: the append may or may
  // not have happened, and it takes no key, so a retry is flagged rather than run again.
  await untilAnswered(agent, direct, "2", "files__append_file", { path: "audit.log", content: "report sent\n" });
  await untilAnswered(agent, direct, "3", "files__read_file", { path: "audit.log" });
}

// ---- Driving VS Code through the DevTools protocol ----

class Page {
  static async open(port) {
    const targets = await (await fetch(`http://127.0.0.1:${port}/json/list`)).json();
    const target = targets.find((t) => t.type === "page" && t.url.includes("workbench"));
    if (!target) throw new Error("no VS Code window on that port");
    const page = new Page(new WebSocket(target.webSocketDebuggerUrl));
    await new Promise((resolve, reject) => {
      page.ws.addEventListener("open", resolve, { once: true });
      page.ws.addEventListener("error", reject, { once: true });
    });
    return page;
  }

  constructor(ws) {
    this.ws = ws;
    this.id = 0;
    this.pending = new Map();
    ws.addEventListener("message", (message) => {
      const data = JSON.parse(message.data);
      this.pending.get(data.id)?.(data);
      this.pending.delete(data.id);
    });
  }

  send(method, params = {}) {
    return new Promise((resolve, reject) => {
      const id = ++this.id;
      this.pending.set(id, (data) => (data.error ? reject(new Error(`${method}: ${data.error.message}`)) : resolve(data.result)));
      this.ws.send(JSON.stringify({ id, method, params }));
    });
  }

  async eval(expression) {
    const result = await this.send("Runtime.evaluate", { expression, returnByValue: true });
    if (result.exceptionDetails) throw new Error(result.exceptionDetails.text);
    return result.result.value;
  }

  /**
   * Has the window draw a frame. A window that is not on screen (the display asleep, say)
   * draws none of its own, and the editor fills in its text only when it draws, so every
   * check comes after a frame drawn here.
   */
  async render() {
    await this.send("Page.captureScreenshot", { format: "png", clip: { x: 0, y: 0, width: 1, height: 1, scale: 1 } });
  }

  /** Waits for an expression to be truthy, and returns its value. */
  async until(expression, what, ms = 20000) {
    const deadline = Date.now() + ms;
    for (;;) {
      await this.render();
      const value = await this.eval(expression);
      if (value) return value;
      if (Date.now() > deadline) throw new Error(`gave up waiting for ${what}`);
      await wait(100);
    }
  }

  async click({ x, y }) {
    await this.send("Input.dispatchMouseEvent", { type: "mouseMoved", x, y });
    await wait(150);
    await this.send("Input.dispatchMouseEvent", { type: "mousePressed", x, y, button: "left", clickCount: 1 });
    await this.send("Input.dispatchMouseEvent", { type: "mouseReleased", x, y, button: "left", clickCount: 1 });
  }

  async drag(from, to) {
    await this.send("Input.dispatchMouseEvent", { type: "mouseMoved", ...from });
    await this.send("Input.dispatchMouseEvent", { type: "mousePressed", ...from, button: "left", clickCount: 1 });
    for (let i = 1; i <= 10; i++) {
      const x = from.x + ((to.x - from.x) * i) / 10;
      await this.send("Input.dispatchMouseEvent", { type: "mouseMoved", x, y: from.y, button: "left", buttons: 1 });
      await wait(20);
    }
    await this.send("Input.dispatchMouseEvent", { type: "mouseReleased", ...to, button: "left", clickCount: 1 });
  }

  async hover({ x, y }) {
    await this.send("Input.dispatchMouseEvent", { type: "mouseMoved", x, y });
  }

  async altRight() {
    const key = { key: "ArrowRight", code: "ArrowRight", windowsVirtualKeyCode: 39, nativeVirtualKeyCode: 39, modifiers: 1 };
    await this.send("Input.dispatchKeyEvent", { type: "rawKeyDown", ...key });
    await this.send("Input.dispatchKeyEvent", { type: "keyUp", ...key });
  }

  /** The window as a PNG, or only the part of it inside `clip`, in CSS pixels. */
  async png(clip) {
    const params = clip ? { format: "png", clip: { ...clip, scale: 1 } } : { format: "png" };
    return Buffer.from((await this.send("Page.captureScreenshot", params)).data, "base64");
  }
}

// Where an element is, by a selector and the text it contains; null until it is there.
const find = (selector, text) => `(() => {
  const el = [...document.querySelectorAll(${JSON.stringify(selector)})]
    .find((e) => e.offsetParent !== null && e.textContent.includes(${JSON.stringify(text)}));
  if (!el) return null;
  const r = el.getBoundingClientRect();
  return { x: r.left + Math.min(r.width / 2, 120), y: r.top + r.height / 2 };
})()`;
const row = (text) => find(".sidebar .monaco-list-row", text);
// A tree row by its whole label, so that "report-demo" does not find "report-demo.fork-...".
const session = (name) => `(() => {
  const el = [...document.querySelectorAll(".sidebar .monaco-list-row")]
    .find((e) => e.offsetParent !== null && e.querySelector(".label-name")?.textContent.trim() === ${JSON.stringify(name)});
  if (!el) return null;
  const r = el.getBoundingClientRect();
  return { x: r.left + 80, y: r.top + r.height / 2 };
})()`;
// The centre of the first visible element a selector matches, whatever its text.
const centre = (selector) => `(() => {
  const el = [...document.querySelectorAll(${JSON.stringify(selector)})].find((e) => e.offsetParent !== null);
  if (!el) return null;
  const r = el.getBoundingClientRect();
  return { x: r.left + r.width / 2, y: r.top + r.height / 2 };
})()`;
const VISIBLE_TEXT = `[...document.querySelectorAll(".editor-instance .view-lines")].filter((e) => e.offsetParent !== null).map((e) => e.textContent.replace(/\\u00a0/g, " ")).join("\\n")`;
const editorShows = (text) => `${VISIBLE_TEXT}.includes(${JSON.stringify(text)})`;
// What a still shows, in CSS pixels: the Ledger view and, when `through` is given, the
// editor beside it down to the first line containing `through` and `after` lines more,
// as wide as those lines. VS Code's own title bar and activity bar are left out, and so
// is the empty part of the window, so that the text can be shown at its own size.
const crop = (through, after = 0) => `(() => {
  const side = document.querySelector(".part.sidebar").getBoundingClientRect();
  const rows = [...document.querySelectorAll(".sidebar .monaco-list-row")].filter((e) => e.offsetParent !== null);
  let right = side.right;
  let bottom = Math.max(...rows.map((e) => e.getBoundingClientRect().bottom)) + 8;
  const through = ${JSON.stringify(through ?? null)};
  if (through !== null) {
    const lines = [...document.querySelectorAll(".editor-instance .view-line")]
      .filter((e) => e.offsetParent !== null)
      .map((e) => ({ text: e.textContent.replace(/\\u00a0/g, " "), box: e.getBoundingClientRect(), text_box: e.firstElementChild?.getBoundingClientRect() }))
      .sort((a, b) => a.box.top - b.box.top);
    const at = lines.findIndex((l) => l.text.includes(through));
    if (at < 0 || at + ${after} >= lines.length) return null;
    const shown = lines.slice(0, at + ${after} + 1);
    bottom = Math.max(bottom, shown[shown.length - 1].box.bottom);
    right = Math.max(right, ...shown.map((l) => (l.text_box?.right ?? l.box.left) + 16));
  }
  // The editor's last line ends the picture exactly, so none of the next one shows.
  right = Math.min(Math.ceil(right), innerWidth);
  bottom = Math.min(Math.floor(bottom), innerHeight);
  return { x: Math.floor(side.left), y: Math.floor(side.top), width: right - Math.floor(side.left), height: bottom - Math.floor(side.top) };
})()`;

async function drive(page) {
  const out = arg("out");
  const originals = arg("originals");
  mkdirSync(out, { recursive: true });
  mkdirSync(originals, { recursive: true });
  const frames = [];
  const crops = [];
  const settle = async () => {
    await wait(700);
  };
  const frame = async (seconds) => frames.push({ png: await page.png(), seconds });
  // Cropped for the README; the whole window is kept beside it, outside the repository.
  const still = async (name, through, after) => {
    const clip = await page.until(crop(through, after), `the part of the window to show in ${name}`);
    crops.push(clip);
    writeFileSync(join(out, `${name}.png`), await page.png(clip));
    writeFileSync(join(originals, `${name}.png`), await page.png());
    console.log(`wrote ${name}.png, ${clip.width}x${clip.height} CSS pixels`);
  };

  await page.click(await page.until(centre('.activitybar .action-label[aria-label="Ledger"]'), "the Ledger view in the activity bar", 60000));
  await page.until(row("retries-demo"), "the demo sessions");
  // Widen the side bar, as a person would, so that each call's status fits.
  const sash = await page.until(
    `(() => { const side = document.querySelector(".part.sidebar").getBoundingClientRect(); const s = [...document.querySelectorAll(".monaco-sash.vertical")].find((e) => Math.abs(e.getBoundingClientRect().left + e.getBoundingClientRect().width / 2 - side.right) < 6); if (!s) return null; const r = s.getBoundingClientRect(); return { x: r.left + r.width / 2, y: r.top + r.height / 2 }; })()`,
    "the side bar's edge",
  );
  await page.drag(sash, { x: sash.x + 150, y: sash.y });
  await settle();
  await frame(1.5);

  // The timeline: the session with a blocked duplicate and a flagged call.
  await page.click(await page.until(session("retries-demo"), "the retries session"));
  await page.until(row("files__append_file"), "the session's calls");
  await settle();
  await frame(1.5);

  // Inspect: the email that was asked for twice and sent once.
  await page.click(await page.until(row("email__send_email"), "the email call"));
  await page.until(editorShows('"tool": "email__send_email"'), "the email call's inspect view");
  await settle();
  await frame(2.5);
  await still("vscode-inspect-duplicate", '"idempotency_key": "ledger:retries-demo:1"', 2);

  // Inspect: the append whose outcome is unknown, flagged for a person.
  await page.click(await page.until(row("files__append_file"), "the append call"));
  await page.until(editorShows('"status": "in_doubt"'), "the append call's inspect view");
  await settle();
  await frame(2.5);
  await still("vscode-inspect-in-doubt", "not safe to run again", 1);

  // The timeline: every session open, the recorded run and its fork among them.
  await page.click(await page.until(session("report-demo"), "the recorded run"));
  await page.until(row("#7 email__send_email"), "the recorded run's calls");
  await page.click(await page.until(row(".fork-"), "the fork"));
  await page.until(row("refused"), "the fork's calls");
  await settle();
  await frame(2);
  await still("vscode-timeline");

  // Replay: step through the session one event at a time.
  const retries = await page.until(session("retries-demo"), "the retries session");
  await page.hover(retries);
  const replay = await page.until(
    `(() => { const row = [...document.querySelectorAll(".sidebar .monaco-list-row")].find((e) => e.querySelector(".label-name")?.textContent.trim() === "retries-demo"); const b = row && [...row.querySelectorAll('.action-label')].find((e) => (e.getAttribute('aria-label') || '').includes('Replay Session')); if (!b || b.offsetParent === null) return null; const r = b.getBoundingClientRect(); return { x: r.left + r.width / 2, y: r.top + r.height / 2 }; })()`,
    "the replay button",
  );
  await page.click(replay);
  await page.until(editorShows("Session retries-demo, step 1 of"), "the replay view");
  await settle();
  await frame(1.5);
  // Click into the replay text so the keys go to it, at the start of a line so that the
  // cursor does not sit in the middle of the text, then step forward.
  const text = await page.eval(find(".editor-instance .view-line", "Now:"));
  const start = await page.eval(`[...document.querySelectorAll(".editor-instance .view-lines")].find((e) => e.offsetParent !== null).getBoundingClientRect().left + 1`);
  await page.click({ x: start, y: text.y });
  const total = Number((await page.eval(VISIBLE_TEXT)).match(/step \d+ of (\d+)/)[1]);
  for (let step = 2; step <= total; step++) {
    await page.altRight();
    await page.until(editorShows(`step ${step} of`), `replay step ${step}`);
    await settle();
    await frame(step === total ? 3 : 1);
    await page.render();
    if (await page.eval(editorShows("answer to call #4: in_doubt"))) await still("vscode-replay", '"recorded_at"', 1);
  }

  // A fork: the run replayed up to step 5, then a different model, and its email turned down.
  await page.click(await page.until(row("refused"), "the refused email"));
  await page.until(editorShows('"refused"'), "the refused email's inspect view");
  await settle();
  await still("vscode-fork", '"resultType"', 2);
  await frame(3);

  // Every frame cut to the same part of the window: the smallest that holds every still.
  const left = Math.min(...crops.map((c) => c.x));
  const top = Math.min(...crops.map((c) => c.y));
  const union = {
    x: left,
    y: top,
    width: Math.max(...crops.map((c) => c.x + c.width)) - left,
    height: Math.max(...crops.map((c) => c.y + c.height)) - top,
  };
  const scale = await page.eval("devicePixelRatio");
  writeFileSync(join(out, "vscode-tour.gif"), gif(frames.map((f) => ({ ...cut(decodePng(f.png), union, scale), seconds: f.seconds }))));
  console.log(`wrote vscode-tour.gif, ${frames.length} frames, ${union.width}x${union.height} CSS pixels`);
  page.ws.close();
}

// ---- PNG decoding and GIF encoding ----

function decodePng(buffer) {
  let pos = 8;
  let width, height, colorType;
  const data = [];
  while (pos < buffer.length) {
    const length = buffer.readUInt32BE(pos);
    const type = buffer.toString("ascii", pos + 4, pos + 8);
    const body = buffer.subarray(pos + 8, pos + 8 + length);
    if (type === "IHDR") {
      width = body.readUInt32BE(0);
      height = body.readUInt32BE(4);
      colorType = body[9];
      if (body[8] !== 8 || (colorType !== 2 && colorType !== 6) || body[12] !== 0) throw new Error("unsupported PNG");
    } else if (type === "IDAT") data.push(body);
    pos += 12 + length;
  }
  const bpp = colorType === 6 ? 4 : 3;
  const raw = inflateSync(Buffer.concat(data));
  const stride = width * bpp;
  const rgb = Buffer.alloc(width * height * 3);
  let previous = Buffer.alloc(stride);
  for (let y = 0; y < height; y++) {
    const filter = raw[y * (stride + 1)];
    const line = Buffer.from(raw.subarray(y * (stride + 1) + 1, (y + 1) * (stride + 1)));
    for (let i = 0; i < stride; i++) {
      const a = i >= bpp ? line[i - bpp] : 0;
      const b = previous[i];
      const c = i >= bpp ? previous[i - bpp] : 0;
      let predictor = 0;
      if (filter === 1) predictor = a;
      else if (filter === 2) predictor = b;
      else if (filter === 3) predictor = (a + b) >> 1;
      else if (filter === 4) {
        const p = a + b - c;
        const pa = Math.abs(p - a), pb = Math.abs(p - b), pc = Math.abs(p - c);
        predictor = pa <= pb && pa <= pc ? a : pb <= pc ? b : c;
      }
      line[i] = (line[i] + predictor) & 255;
    }
    for (let x = 0; x < width; x++) {
      rgb[(y * width + x) * 3] = line[x * bpp];
      rgb[(y * width + x) * 3 + 1] = line[x * bpp + 1];
      rgb[(y * width + x) * 3 + 2] = line[x * bpp + 2];
    }
    previous = line;
  }
  return { width, height, rgb };
}

/** The part of an image inside a rectangle in CSS pixels, `scale` image pixels to each. */
function cut({ width, height, rgb }, rect, scale) {
  const x0 = Math.round(rect.x * scale), y0 = Math.round(rect.y * scale);
  const w = Math.min(Math.round(rect.width * scale), width - x0);
  const h = Math.min(Math.round(rect.height * scale), height - y0);
  const out = Buffer.alloc(w * h * 3);
  for (let y = 0; y < h; y++) rgb.copy(out, y * w * 3, ((y0 + y) * width + x0) * 3, ((y0 + y) * width + x0 + w) * 3);
  return { width: w, height: h, rgb: out };
}

function gif(frames) {
  const { width, height } = frames[0];
  // One palette for every frame: the 256 most common colours, at 5 bits a channel.
  const bin = (r, g, b) => ((r >> 3) << 10) | ((g >> 3) << 5) | (b >> 3);
  const counts = new Float64Array(32768), sums = new Float64Array(32768 * 3);
  for (const { rgb } of frames)
    for (let i = 0; i < rgb.length; i += 3) {
      const k = bin(rgb[i], rgb[i + 1], rgb[i + 2]);
      counts[k]++;
      sums[k * 3] += rgb[i];
      sums[k * 3 + 1] += rgb[i + 1];
      sums[k * 3 + 2] += rgb[i + 2];
    }
  const top = [...counts.keys()].filter((k) => counts[k] > 0).sort((a, b) => counts[b] - counts[a]).slice(0, 256);
  const palette = top.map((k) => [0, 1, 2].map((c) => Math.round(sums[k * 3 + c] / counts[k])));
  while (palette.length < 256) palette.push([0, 0, 0]);
  const nearest = new Int16Array(32768).fill(-1);
  const index = (r, g, b) => {
    const k = bin(r, g, b);
    if (nearest[k] < 0) {
      let best = 0, bestDistance = Infinity;
      for (let p = 0; p < palette.length; p++) {
        const [pr, pg, pb] = palette[p];
        const d = (pr - r) ** 2 + (pg - g) ** 2 + (pb - b) ** 2;
        if (d < bestDistance) [best, bestDistance] = [p, d];
      }
      nearest[k] = best;
    }
    return nearest[k];
  };
  const indexed = frames.map(({ rgb }) => {
    const pixels = new Uint8Array(width * height);
    for (let i = 0; i < pixels.length; i++) pixels[i] = index(rgb[i * 3], rgb[i * 3 + 1], rgb[i * 3 + 2]);
    return pixels;
  });

  const bytes = [];
  const word = (n) => bytes.push(n & 255, n >> 8);
  bytes.push(...Buffer.from("GIF89a"));
  word(width);
  word(height);
  bytes.push(0xf7, 0, 0);
  for (const [r, g, b] of palette) bytes.push(r, g, b);
  bytes.push(0x21, 0xff, 11, ...Buffer.from("NETSCAPE2.0"), 3, 1, 0, 0, 0);
  let previous = null;
  for (let f = 0; f < indexed.length; f++) {
    const pixels = indexed[f];
    // Only the rectangle that changed since the last frame.
    let [left, top_, right, bottom] = [0, 0, width - 1, height - 1];
    if (previous) {
      [left, top_, right, bottom] = [width, height, -1, -1];
      for (let y = 0; y < height; y++)
        for (let x = 0; x < width; x++)
          if (pixels[y * width + x] !== previous[y * width + x]) {
            left = Math.min(left, x);
            right = Math.max(right, x);
            top_ = Math.min(top_, y);
            bottom = Math.max(bottom, y);
          }
      if (right < 0) [left, top_, right, bottom] = [0, 0, 0, 0];
    }
    const w = right - left + 1, h = bottom - top_ + 1;
    const sub = new Uint8Array(w * h);
    for (let y = 0; y < h; y++) sub.set(pixels.subarray((top_ + y) * width + left, (top_ + y) * width + left + w), y * w);
    bytes.push(0x21, 0xf9, 4, 0x04);
    word(Math.round(frames[f].seconds * 100));
    bytes.push(0, 0, 0x2c);
    word(left);
    word(top_);
    word(w);
    word(h);
    bytes.push(0, 8);
    const data = lzw(sub);
    for (let i = 0; i < data.length; i += 255) {
      const block = data.subarray(i, i + 255);
      bytes.push(block.length, ...block);
    }
    bytes.push(0);
    previous = pixels;
  }
  bytes.push(0x3b);
  return Buffer.from(bytes);
}

/** GIF's LZW, with 8-bit pixels. */
function lzw(pixels) {
  const clear = 256, end = 257;
  const out = [];
  let buffer = 0, bits = 0, size = 9;
  const emit = (code) => {
    buffer |= code << bits;
    bits += size;
    while (bits >= 8) {
      out.push(buffer & 255);
      buffer >>= 8;
      bits -= 8;
    }
  };
  let table = new Map(), next = 258;
  emit(clear);
  let prefix = pixels[0];
  for (let i = 1; i < pixels.length; i++) {
    const k = pixels[i];
    const key = prefix * 256 + k;
    const code = table.get(key);
    if (code !== undefined) {
      prefix = code;
      continue;
    }
    emit(prefix);
    if (next < 4096) {
      table.set(key, next++);
      if (next > 1 << size && size < 12) size++;
    } else {
      emit(clear);
      table = new Map();
      next = 258;
      size = 9;
    }
    prefix = k;
  }
  emit(prefix);
  emit(end);
  if (bits > 0) out.push(buffer & 255);
  return Uint8Array.from(out);
}

if (mode === "record") await record();
else if (mode === "drive") {
  const page = await Page.open(Number(arg("port")));
  try {
    await drive(page);
  } catch (error) {
    // What the window showed when it went wrong, to see why.
    const seen = () => page.eval(`JSON.stringify([...document.querySelectorAll(".editor-instance .view-lines")].map((e) => ({ visible: e.offsetParent !== null, text: e.textContent.slice(0, 80) })))`);
    const before = await seen();
    writeFileSync(join(arg("out"), "failure.png"), await page.png());
    writeFileSync(join(arg("out"), "failure.txt"), `before the screenshot: ${before}\nafter it: ${await seen()}\nfocused: ${await page.eval("document.hasFocus()")}, visibility: ${await page.eval("document.visibilityState")}\n`);
    throw error;
  }
}
else throw new Error("usage: vscode-screens.mjs record|drive ...");
