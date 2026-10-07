/* 遥感档案站迁移演练场前端：全部操作走真实 HTTP API */
"use strict";

const $ = (id) => document.getElementById(id);
let currentDrill = null;
let pollTimer = null;

// ------------------------------------------------------------ drill editor
function recordRow(rid = "", payload = "") {
  const div = document.createElement("div");
  div.className = "rec-row";
  div.innerHTML = `
    <input placeholder="rid" value="${escapeHtml(rid)}">
    <input placeholder="观测载荷 payload" value="${escapeHtml(payload)}">
    <span></span><span></span>
    <button class="del-x" title="删除">×</button>`;
  div.querySelector(".del-x").onclick = () => div.remove();
  return div;
}

function updateRow(batch = 1, op = "upsert", rid = "", payload = "") {
  const div = document.createElement("div");
  div.className = "upd-row";
  div.innerHTML = `
    <select class="u-op">
      <option value="upsert">upsert</option>
      <option value="update">update</option>
      <option value="delete">delete</option>
    </select>
    <input type="number" min="1" class="u-batch" value="${batch}" title="复制批次">
    <input class="u-rid" placeholder="目标 rid" value="${escapeHtml(rid)}">
    <input class="u-payload" placeholder="${op === "delete" ? "—" : "新 payload"}"
           value="${op === "delete" ? "" : escapeHtml(payload)}">
    <button class="del-x">×</button>`;
  const opSel = div.querySelector(".u-op");
  opSel.value = op;
  opSel.onchange = () => {
    const p = div.querySelector(".u-payload");
    p.disabled = opSel.value === "delete";
    p.placeholder = opSel.value === "delete" ? "—" : "新 payload";
  };
  div.querySelector(".del-x").onclick = () => div.remove();
  return div;
}

function addRecord(rid, payload) {
  $("records").appendChild(recordRow(rid, payload));
}
function addUpdate(batch, op, rid, payload) {
  $("updates").appendChild(updateRow(batch, op, rid, payload));
}

function escapeHtml(s) {
  return String(s ?? "").replace(/[&<>"']/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}

function defaultDrill() {
  addRecord("RS-1001", "L1A 多光谱帧 · 2026-10-07T08:00Z · 云量 12%");
  addRecord("RS-1002", "SAR 条带 44 · VV/VH · 北海港区");
  addRecord("RS-1003", "高光谱推扫 · 红边波段校准 v3");
  addUpdate(1, "upsert", "RS-1002", "SAR 条带 44 · 重处理：配准修正 +2px");
  addUpdate(1, "upsert", "RS-1004", "新增红外帧 · 夜间热异常 03:12Z");
  addUpdate(2, "delete", "RS-1003", "");
  addUpdate(2, "upsert", "RS-1001", "L1A 多光谱帧 · 云量订正 9% · QA 通过");
}

// ------------------------------------------------------------ API helpers
async function api(path, opts = {}) {
  const res = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...opts,
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw Object.assign(new Error(data.message || res.statusText),
    { status: res.status, code: data.error });
  return data;
}

function collectSpec() {
  const records = [...$("records").children].map((row) => ({
    rid: row.children[0].value.trim(),
    payload: row.children[1].value,
  })).filter((r) => r.rid);
  const updates = [...$("updates").children].map((row) => ({
    op: row.querySelector(".u-op").value,
    batch: parseInt(row.querySelector(".u-batch").value, 10),
    rid: row.querySelector(".u-rid").value.trim(),
    payload: row.querySelector(".u-payload").value,
  })).filter((u) => u.rid && Number.isFinite(u.batch));
  return {
    name: $("d-name").value,
    records, updates,
    crash_point: $("d-crash").value || null,
  };
}

async function createDrill() {
  const msg = $("create-msg");
  msg.className = "msg";
  msg.textContent = "提交中…";
  try {
    const spec = collectSpec();
    const r = await api("/api/drills", {
      method: "POST", body: JSON.stringify(spec),
    });
    msg.className = "msg ok";
    msg.textContent = `已提交演练 ${r.drill_id}（定义已冻结，不可修改）`;
    await loadDrills(r.drill_id);
  } catch (e) {
    msg.className = "msg err";
    msg.textContent = "提交失败：" + e.message;
  }
}

async function loadDrills(selectId) {
  const r = await api("/api/drills");
  const sel = $("drill-select");
  sel.innerHTML = "";
  if (!r.drills.length) {
    sel.innerHTML = '<option value="">（尚无演练）</option>';
    return;
  }
  for (const d of r.drills) {
    const o = document.createElement("option");
    o.value = d.drill_id;
    o.textContent = `${d.name} · ${d.drill_id}`;
    sel.appendChild(o);
  }
  currentDrill = selectId || r.drills[r.drills.length - 1].drill_id;
  sel.value = currentDrill;
  selectDrill(false);
}

function selectDrill(regen = true) {
  currentDrill = $("drill-select").value || null;
  if (regen) genMid();
  refresh();
}

function genMid() {
  $("migration-id").value = "mig-" + Math.random().toString(36).slice(2, 10);
}

async function startMigration() {
  if (!currentDrill) return alert("请先选择演练");
  const mid = $("migration-id").value.trim();
  if (!mid) return alert("需要 migration_id（可点“生成”）");
  try {
    const r = await api(`/api/drills/${currentDrill}/migration`, {
      method: "POST", body: JSON.stringify({ migration_id: mid }),
    });
    console.log("migration", r);
  } catch (e) {
    alert("启动失败：" + e.message);
  }
  refresh();
}

async function resumeWorker() {
  if (!currentDrill) return;
  try { await api(`/api/drills/${currentDrill}/resume`, { method: "POST" }); }
  catch (e) { alert(e.message); }
  refresh();
}

async function manualWrite() {
  if (!currentDrill) return;
  const rid = "RS-" + Math.floor(1000 + Math.random() * 9000);
  try {
    await api(`/api/drills/${currentDrill}/writes`, {
      method: "POST",
      body: JSON.stringify({
        token: "manual-" + Date.now() + "-" + Math.random().toString(36).slice(2, 6),
        op: "upsert", rid, batch: 9,
        payload: "切换窗口外旧端补写 " + new Date().toISOString(),
      }),
    });
  } catch (e) { alert(e.message); }
  refresh();
}

// ------------------------------------------------------------ rendering
function fmtRows(tbodyId, rows, kind) {
  const tb = $(tbodyId).querySelector("tbody");
  tb.innerHTML = "";
  if (!rows.length) {
    tb.innerHTML = '<tr><td colspan="3" class="miss">（空）</td></tr>';
    return;
  }
  for (const r of rows) {
    const tr = document.createElement("tr");
    if (kind === "target") {
      tr.innerHTML = `<td>${escapeHtml(r.rid)}</td><td>${escapeHtml(r.payload)}</td>
        <td>${r.updated_seq}</td>`;
    } else {
      tr.innerHTML = `<td>${escapeHtml(r.rid)}</td><td>${escapeHtml(r.payload)}</td>
        <td>${r.batch ?? "—"}</td>`;
    }
    tb.appendChild(tr);
  }
}

function renderIncrements(incs, applied) {
  const tb = $("inc-table").querySelector("tbody");
  tb.innerHTML = "";
  if (!incs.length) {
    tb.innerHTML = '<tr><td colspan="6" class="miss">（边界后暂无增量）</td></tr>';
    return;
  }
  for (const i of incs) {
    const done = i.seq <= applied;
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${i.seq}</td>
      <td><span class="tag ${i.op}">${i.op}</span></td>
      <td>${escapeHtml(i.rid)}</td>
      <td>${i.batch ?? "—"}</td>
      <td>${i.payload == null ? "—" : escapeHtml(i.payload)}</td>
      <td><span class="tag ${done ? "yes" : "no"}">${done ? "已投影" : "待投影"}</span></td>`;
    tb.appendChild(tr);
  }
}

function setGauge(id, text, cls) {
  const el = $(id);
  el.textContent = text;
  el.className = cls || "";
}

function renderVerdict(s) {
  const v = $("verdict");
  const cov = s.coverage;
  const m = s.migration;
  v.classList.remove("hidden", "success", "pending", "failed");

  if (!m) {
    v.classList.add("pending");
    v.innerHTML = "<h4>⏳ 尚未冻结复制边界</h4><div>提交 migration_id 后开始：先冻结快照边界，再捕获边界后的有序增量。</div>";
    return;
  }
  if (cov.read_pointer === "target") {
    v.classList.add("success");
    v.innerHTML = `<h4>✅ 切换结论：已原子切换至 target</h4>
      <div>目标已覆盖快照与全部 <b>${cov.max_recorded_seq}</b> 条已记录增量，
      源/目标内容逐行一致（${cov.target_count} 行），无丢失、重复或回退。</div>
      <ul>
        <li>快照水位：已复制 ｜ 增量序号 ${cov.last_applied_seq}/${cov.max_recorded_seq}</li>
        <li>切换意图：已持久化 ｜ 指针：<b>target</b> ｜ 时间 ${new Date(cov.switched_at * 1000).toLocaleString()}</li>
      </ul>`;
    return;
  }
  const checks = [
    ["快照水位已置位", cov.snapshot_done],
    ["目标内容 == 快照 + 全部增量", cov.content_match],
    ["全部已记录增量已恰好一次投影", cov.all_increments_applied],
  ];
  v.classList.add("pending");
  v.innerHTML = `<h4>🟡 明确未切换：读取指针仍为 source</h4>
    <div>重开服务会从此持久化状态继续收敛；只有下面全部成立才允许原子切换。</div>
    <ul>${checks.map(([t, ok]) =>
      `<li>${ok ? "✅" : "⏳"} ${t}</li>`).join("")}
      <li>已投影 ${cov.last_applied_seq} / 已落盘 ${cov.max_recorded_seq}，
      源 ${cov.source_count} 行 · 目标 ${cov.target_count} 行 · 期望 ${cov.expected_count} 行</li>
    </ul>`;
}

async function refresh() {
  if (!currentDrill) return;
  try {
    const s = await api(`/api/drills/${currentDrill}`);
    const m = s.migration, cov = s.coverage;
    fmtRows("source-table", s.source, "source");
    fmtRows("target-table", s.target, "target");
    renderIncrements(s.increments, m ? m.last_applied_seq : 0);

    setGauge("g-frozen", m ? "已冻结" : "未冻结", m ? "good" : "warn");
    setGauge("g-snapshot", m ? (m.snapshot_done ? "已复制" : "复制中") : "—",
      m && m.snapshot_done ? "good" : "warn");
    setGauge("g-maxseq", cov.max_recorded_seq ?? 0);
    setGauge("g-applied", m ? m.last_applied_seq : 0);
    setGauge("g-intent", m ? (m.switch_intended ? "已写入" : "未写入")
      : "—", m && m.switch_intended ? "good" : "warn");
    setGauge("g-pointer", m ? m.read_pointer : "source",
      m && m.read_pointer === "target" ? "good" : "warn");

    const ev = $("events");
    ev.innerHTML = s.events.map((e) => {
      const cls = ["crash", "switch", "done", "gate"].includes(e.kind) ? e.kind : "";
      return `<div class="event ${cls}"><span class="t">${new Date(e.ts * 1000)
        .toLocaleTimeString()}</span><span>${escapeHtml(e.message)}</span></div>`;
    }).join("");
    ev.scrollTop = ev.scrollHeight;
    renderVerdict(s);
  } catch (e) {
    console.warn(e);
  }
}

// ------------------------------------------------------------ health
async function checkHealth() {
  const box = $("health"), txt = $("health-text");
  try {
    const h = await api("/healthz");
    box.className = "health ok";
    txt.textContent = `服务健康 · 运行 ${h.uptime_sec}s · ${h.drills} 个演练`;
  } catch {
    box.className = "health bad";
    txt.textContent = "服务不可用（中断后等待自动重开）";
  }
}

// ------------------------------------------------------------ boot
defaultDrill();
genMid();
loadDrills();
checkHealth();
setInterval(checkHealth, 3000);
pollTimer = setInterval(refresh, 1200);
