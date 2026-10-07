const $ = (id) => document.getElementById(id);
let current = null;
let pendingMid = null;   // mid whose submit may have been cut off by a crash
let pollTimer = null;

// ---- batch editor -------------------------------------------------------
function addBatchRow(count = '', afterPages = '') {
  const div = document.createElement('div');
  div.className = 'batchline';
  div.innerHTML =
    `<input class="b-count" type="number" min="1" placeholder="写入数" value="${count}" style="width:90px">` +
    `<input class="b-after" type="number" min="0" placeholder="第N页后(可空)" value="${afterPages}" style="width:130px">` +
    `<button class="x" type="button">✕</button>`;
  div.querySelector('.x').addEventListener('click', () => div.remove());
  $('f-batches').appendChild(div);
}
$('add-batch').addEventListener('click', () => addBatchRow());
addBatchRow(3, 1);
addBatchRow(2, '');

function genId() {
  return 'MIG-' + Math.random().toString(36).slice(2, 10) + '-' + Date.now().toString(36);
}
$('f-mid').value = genId();

// ---- health --------------------------------------------------------------
async function checkHealth() {
  try {
    const r = await fetch('/health');
    const j = await r.json();
    $('health').textContent = 'health: ' + j.status;
    $('health').className = 'pill ok';
    $('started').textContent = 'up since ' + j.started_at;
  } catch (e) {
    $('health').textContent = 'health: DOWN (等待容器重启…)';
    $('health').className = 'pill bad';
  }
}
setInterval(checkHealth, 2000);
checkHealth();

// ---- drill creation ------------------------------------------------------
$('create').addEventListener('click', async () => {
  $('create-err').textContent = '';
  const batches = [...document.querySelectorAll('#f-batches .batchline')].map(d => ({
    count: parseInt(d.querySelector('.b-count').value || '0', 10),
    after_pages: (() => {
      const v = d.querySelector('.b-after').value.trim();
      return v === '' ? null : parseInt(v, 10);
    })()
  })).filter(b => b.count >= 1);
  const body = {
    name: $('f-name').value,
    initial_records: parseInt($('f-init').value, 10),
    page_size: 4, // fixed? no — use field
  };
  body.page_size = parseInt($('f-page').value, 10);
  body.batches = batches;
  body.crash_point = $('f-crash').value || null;
  const mid = $('f-mid').value.trim();
  try {
    const r = await fetch('/api/drills', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body)
    });
    const j = await r.json();
    if (!r.ok) throw new Error(j.error || ('HTTP ' + r.status));
    current = j.drill_id;
    window.__nextBatch = 0;
    await loadDrills();
    render(j);
    await startMigration(mid);
  } catch (e) {
    $('create-err').textContent = e.message;
  }
});

async function startMigration(mid) {
  if (!current || !mid) return;
  pendingMid = mid;  // crash right after submit may cut the response off
  try {
    const r = await fetch(`/api/drills/${current}/migrate`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ migration_id: mid })
    });
    const j = await r.json();
    if (!r.ok) throw new Error(j.error || ('HTTP ' + r.status));
    render(j);
  } catch (e) {
    $('op-err').textContent = '提交中断（可能是演练的中断点触发，等待重启后自动重试）：' + e.message;
  }
}

// ---- migration launch / batch fire / refresh ----------------------------
$('btn-migrate').addEventListener('click', () => startMigration($('f-mid').value.trim()));
$('btn-fire').addEventListener('click', async () => {
  if (!current) return;
  $('op-err').textContent = '';
  try {
    const next = (window.__nextBatch ?? 0);
    const r = await fetch(`/api/drills/${current}/batches/fire`, {
      mode: 'same-origin',
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ batch_no: next })
    });
    const j = await r.json();
    if (!r.ok) throw new Error(j.error || ('HTTP ' + r.status));
    window.__nextBatch = next + 1;
    render(j);
  } catch (e) {
    $('op-err').textContent = e.message;
  }
});
$('btn-refresh').addEventListener('click', () => current && poll());

// ---- lists ---------------------------------------------------------------
async function loadDrills() {
  const r = await fetch('/api/drills');
  const j = await r.json();
  const box = $('drills');
  if (!j.drills.length) { box.textContent = '暂无'; return; }
  box.innerHTML = '';
  j.drills.forEach(d => {
    const a = document.createElement('a');
    a.textContent = `${d.name} [${d.state || '未启动'}]`;
    a.addEventListener('click', async () => {
      current = d.id;
      const rr = await fetch('/api/drills/' + d.id);
      render(await rr.json());
    });
    box.appendChild(a);
  });
}
loadDrills();

// ---- rendering -----------------------------------------------------------
function esc(s) { return String(s).replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c])); }

function renderTable(tbodyId, data) {
  const tb = $(tbodyId);
  tb.innerHTML = data.rows.map(r =>
    `<tr><td>${esc(r.rid)}</td><td>${esc(r.payload)}</td><td>${r.version}</td><td>${r.updated_at ?? r.seq}</td></tr>`
  ).join('');
}

function render(j) {
  $('panel').style.display = 'block';
  $('d-name').textContent = j.name;
  const m = j.migration;
  if (m) {
    $('d-mid').textContent = `migration_id=${m.migration_id} · started=${m.started_at}`;
    $('m-state').textContent = m.state;
    $('m-state').className = 'v state s-' + m.state;
    $('m-wm').textContent = m.watermark ?? '—';
    $('m-inc').textContent = `${j.increments.total} / ${j.increments.applied}`;
    $('m-ptr').textContent = m.pointer;
    const total = m.pages_total ?? Math.max(1, Math.ceil(j.config.initial_records / j.config.page_size));
    const pct = m.pages_total ? Math.round(100 * m.pages_done / m.pages_total) : 0;
    $('m-prog').style.width = pct + '%';
    $('m-pages').textContent = `快照分页 ${m.pages_done}/${m.pages_total ?? '?'} · 源写入序号 ${j.source_write_counter} · 已触发批次 ${j.fired_batches.join(', ') || '无'}`;
    const canAct = ['FROZEN', 'COPYING', 'CATCHING_UP'].includes(m.state);
    $('btn-fire').disabled = !canAct;
    $('btn-migrate').disabled = m.state !== 'INTENT';
    if (j.conclusion) {
      const v = $('verdict');
      v.style.display = 'block';
      v.className = 'verdict ' + (j.conclusion.switched ? 'ok' : 'no');
      v.innerHTML = `<b>切换结论：</b>${esc(j.conclusion.verdict)}<br>` +
        `差异行=${j.conclusion.divergent_rows} · 重复行=${j.conclusion.duplicate_target_rows}` +
        (m.switched_at ? ` · 切换时间=${m.switched_at}` : '');
    } else {
      $('verdict').style.display = 'none';
    }
  } else {
    $('d-mid').textContent = '迁移尚未提交';
    ['m-state', 'm-wm', 'm-inc', 'm-ptr'].forEach(k => $(k).textContent = '—');
    $('btn-fire').disabled = true;
    $('btn-migrate').disabled = false;
  }
  renderTable('t-src', j.source_projection);
  renderTable('t-tgt', j.target_projection);
  $('src-count').textContent = `共 ${j.source_projection.count} 行（展示前 8 行按 rid）`;
  $('tgt-count').textContent = `共 ${j.target_projection.count} 行（展示前 8 行）`;
  $('events').innerHTML = j.events.map(e =>
    `<div class="ev"><span>${e.at.slice(11, 23)}</span> <b>${esc(e.kind)}</b> ${esc(e.detail)}</div>`
  ).join('');
}

async function poll() {
  if (!current) return;
  try {
    const r = await fetch('/api/drills/' + current);
    if (r.ok) {
      const j = await r.json();
      render(j);
      // If a submit was in flight when the process hard-exited at the
      // after_submit interruption, resend the same id: the durable
      // UNIQUE(migration_id) makes it an idempotent no-op once it landed.
      if (pendingMid && (!j.migration || j.migration.state === 'INTENT')) {
        await startMigration(pendingMid);
      } else if (pendingMid && j.migration) {
        pendingMid = null;
        $('op-err').textContent = '';
      }
    }
  } catch (e) { /* service may be crashing/restarting */ }
}
setInterval(poll, 700);
setInterval(loadDrills, 3000);
