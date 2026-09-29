'use strict';

/* 情景注入台前端
 *
 * 同步协议：
 * 1. 打开 WebSocket 时带上 last_event_id（已应用的最大事件 ID，仅作客户端侧提示）；
 * 2. 服务端先推权威快照（snapshot）——全量替换本地状态，水位 = snapshot.last_event_id；
 *    该水位以服务端在状态锁内读到的「当前房间」事件水位为准，不会被本端传入值
 *    抬高（提示值可能来自旧库或其他房间，事件 ID 跨房间不可比）；
 * 3. 之后只应用 id > 水位的增量事件，旧事件一律丢弃，状态绝不回退。
 *
 * 三种角色：
 * - 主持人：房间码连接（全量视野，含观察分享管理与回执门槛编排——卡片可依赖
 *   前置卡片，前置集齐目标组回执后才揭示，可设虚拟时钟超时秒数，超时标记为
 *   「超时揭示」；门槛状态仅主持人可见，随 gate_updated 事件与快照下发）；
 *   另可逐卡锁定/重新开放处置答复，并实时看到各应答组的提交状态、正文与版本；
 * - 参与者：组加入码连接（服务端按组过滤快照与事件，不感知门槛编排）；
 *   可在已揭示卡片下提交/修改本组处置答复（每组一份，携带 revision 乐观并发，
 *   版本过期收到 409 与最新答复；锁定后只读），看不到他组答复正文；
 *   在情报核验板对本组命题提交「支持/反对 + 简短依据」（同组覆盖、可撤回），
 *   只见本组报告与汇总状态，看不到他组立场明细；
 * - 观察者：限时观察链接（#/observe/{token}）进入，只读——只能看到已揭示的
 *   全员卡片与分享所选组卡片，拿不到房间码、组加入码、未揭示卡片与回执明细；
 *   链接到期或被撤销时服务端主动断开，重连也会被拒绝。
 */

const $ = (sel) => document.querySelector(sel);

const state = {
  code: null,      // 主持人 = 房间码；参与者 = 组加入码；观察者 = 观察凭证
  role: null,      // 'host' | 'player' | 'observer'
  roomName: '',
  status: 'idle',
  cards: [],
  groups: [],      // 主持人：全部参与组；参与者：仅本组；观察者：分享所选组
  shares: [],      // 主持人：全部观察分享（有效/失效）
  anchors: [],     // 主持人：全部轮次的复盘锚点（参与者/观察者永远为空）
  polls: [],       // 主持人：全部轮次投票；参与者：本轮本组投票；观察者：永远为空
  vehicles: null,  // 主持人：{total, remaining, requests}；参与者：{my_request}；观察者：null
  claims: [],      // 主持人：全部命题（逐组报告）；参与者：白名单含本组的命题（本组报告+汇总）；观察者：永远为空
  groupId: null,   // 参与者本组 id（主持人/观察者为 null）
  round: 0,        // 当前演练轮次（= 此前 reset 次数；快照权威下发，reset 事件 +1）
  // 主持人响应时效面板（round=null 跟随当前轮）。
  // epoch：进入/离开房间时递增的会话代号，使上一房间在途请求整体失效；
  // seq：当前会话内已发起的报告请求序号，仅最新一次请求的响应允许写入面板。
  latency: { round: null, data: null, timer: 0, epoch: 0, seq: 0 },
  lastEventId: 0,
  clock: { status: 'idle', elapsedMs: 0, syncedAt: performance.now() },
  ws: null,
  reconnectDelay: 1000,
};

const STATUS_TEXT = { idle: '待机', running: '进行中', paused: '已暂停' };

/* ---------------------------------------------------------- 视图切换 */

function showLanding() {
  $('#view-landing').classList.remove('hidden');
  $('#view-room').classList.add('hidden');
}

function showRoom() {
  $('#view-landing').classList.add('hidden');
  $('#view-room').classList.remove('hidden');
  $('#host-console').classList.toggle('hidden', state.role !== 'host');
  // 现场决策投票对观察者完全不可见；主持人与参与者共用列表面板（内容按角色渲染）
  $('#polls-panel').classList.toggle('hidden', state.role === 'observer');
  // 应急车辆申请对观察者完全不可见；主持人看全量，参与组看本组
  $('#vehicles-panel').classList.toggle('hidden', state.role === 'observer');
  // 现场情报核验板对观察者完全不可见；主持人看逐组报告，参与组看本组与汇总状态
  $('#intel-panel').classList.toggle('hidden', state.role === 'observer');
  // 响应时效面板仅主持人可见（参与者与观察席拿不到分析数据）
  $('#latency-panel').classList.toggle('hidden', state.role !== 'host');
  $('#cards-title').textContent =
    state.role === 'host' ? '注入卡片（全部）'
    : state.role === 'observer' ? '已揭示的情景注入（观察席 · 只读）'
    : '已揭示的情景注入';
}

/* ---------------------------------------------------------- 渲染 */

function fmt(ms) {
  const s = Math.max(0, Math.floor(ms / 1000));
  const h = String(Math.floor(s / 3600)).padStart(2, '0');
  const m = String(Math.floor((s % 3600) / 60)).padStart(2, '0');
  const sec = String(s % 60).padStart(2, '0');
  return `${h}:${m}:${sec}`;
}

function groupName(id) {
  const g = state.groups.find((g) => g.id === id);
  return g ? g.name : `#${id}`;
}

function renderMeta() {
  $('#room-name').textContent = state.roomName || '未命名演练';
  // 观察者的 code 是观察凭证而非房间码，不显示「加入码」行
  $('.room-code').classList.toggle('hidden', state.role === 'observer');
  $('#room-code').textContent = state.code;
  const st = $('#status');
  st.textContent = STATUS_TEXT[state.status] || state.status;
  st.className = 'status' + (state.status === 'running' ? ' running' : state.status === 'paused' ? ' paused' : '');
  const pg = $('#player-group');
  if (state.role === 'player' && state.groupId != null) {
    pg.textContent = `参与组：${groupName(state.groupId)}`;
    pg.classList.remove('hidden');
  } else if (state.role === 'observer') {
    const names = state.groups.map((g) => g.name).join('、');
    pg.textContent = `观察席（只读）· 可见组：${names || '全员'}`;
    pg.classList.remove('hidden');
  } else {
    pg.classList.add('hidden');
  }
  renderControls();
}

function renderControls() {
  if (state.role !== 'host') return;
  const s = state.status;
  $('#btn-start').disabled = s !== 'idle';
  $('#btn-pause').disabled = s !== 'running';
  $('#btn-resume').disabled = s !== 'paused';
  $('#btn-reset').disabled = s === 'idle';
  const editing = s !== 'running';
  $('#card-form').querySelectorAll('input, textarea, select, button').forEach((el) => { el.disabled = !editing; });
  $('#card-form-hint').textContent = editing ? '' : '时钟运行中，暂停后才能编辑卡片';
}

/* 主持人：参与组列表 + 卡片目标下拉 */
function renderGroups() {
  if (state.role !== 'host') return;
  $('#group-count').textContent = state.groups.length;
  const list = $('#group-list');
  list.innerHTML = '';
  for (const g of state.groups) {
    const li = document.createElement('li');
    li.className = 'group-item';

    const name = document.createElement('span');
    name.className = 'group-name';
    name.textContent = g.name;
    li.appendChild(name);

    const code = document.createElement('button');
    code.className = 'code-chip group-code';
    code.textContent = g.join_code;
    code.title = '点击复制加入码';
    code.onclick = () => {
      navigator.clipboard?.writeText(g.join_code);
      code.textContent = '已复制';
      setTimeout(() => { code.textContent = g.join_code; }, 1200);
    };
    li.appendChild(code);

    const rename = document.createElement('button');
    rename.className = 'group-op';
    rename.textContent = '改名';
    rename.onclick = async () => {
      const name2 = (prompt('新的组名称', g.name) || '').trim();
      if (!name2 || name2 === g.name) return;
      try {
        await api('PATCH', `/api/rooms/${state.code}/groups/${g.id}`, { name: name2 });
      } catch (err) { showError(err); }
    };
    li.appendChild(rename);

    const del = document.createElement('button');
    del.className = 'group-op group-del';
    del.textContent = '删除';
    del.onclick = async () => {
      if (!confirm(`删除参与组「${g.name}」？其加入码将立即失效。`)) return;
      try {
        await api('DELETE', `/api/rooms/${state.code}/groups/${g.id}`);
      } catch (err) { showError(err); }
    };
    li.appendChild(del);

    list.appendChild(li);
  }

  // 卡片目标下拉：全员 + 各参与组；尽量保留当前选择
  const sel = $('#card-target');
  const cur = sel.value;
  sel.innerHTML = '';
  const all = document.createElement('option');
  all.value = '';
  all.textContent = '全员';
  sel.appendChild(all);
  for (const g of state.groups) {
    const opt = document.createElement('option');
    opt.value = String(g.id);
    opt.textContent = `仅：${g.name}`;
    sel.appendChild(opt);
  }
  sel.value = [...sel.options].some((o) => o.value === cur) ? cur : '';

  // 观察分享表单的组复选框（1～3 个）；尽量保留已勾选项
  const checked = new Set(
    [...document.querySelectorAll('#share-groups input:checked')].map((el) => el.value)
  );
  const sg = $('#share-groups');
  sg.innerHTML = '';
  for (const g of state.groups) {
    const label = document.createElement('label');
    label.className = 'share-group-chip';
    const cb = document.createElement('input');
    cb.type = 'checkbox';
    cb.value = String(g.id);
    cb.checked = checked.has(String(g.id));
    label.appendChild(cb);
    label.appendChild(document.createTextNode(g.name));
    sg.appendChild(label);
  }

  // 投票表单的组复选框（至少 1 个、至多全部组）；尽量保留已勾选项
  const pollChecked = new Set(
    [...document.querySelectorAll('#poll-groups input:checked')].map((el) => el.value)
  );
  const pg = $('#poll-groups');
  pg.innerHTML = '';
  for (const g of state.groups) {
    const label = document.createElement('label');
    label.className = 'share-group-chip';
    const cb = document.createElement('input');
    cb.type = 'checkbox';
    cb.value = String(g.id);
    cb.checked = pollChecked.has(String(g.id));
    label.appendChild(cb);
    label.appendChild(document.createTextNode(g.name));
    pg.appendChild(label);
  }

  // 情报核验表单的组复选框（至少 2 个）；尽量保留已勾选项
  const intelChecked = new Set(
    [...document.querySelectorAll('#intel-groups input:checked')].map((el) => el.value)
  );
  const ig = $('#intel-groups');
  ig.innerHTML = '';
  for (const g of state.groups) {
    const label = document.createElement('label');
    label.className = 'share-group-chip';
    const cb = document.createElement('input');
    cb.type = 'checkbox';
    cb.value = String(g.id);
    cb.checked = intelChecked.has(String(g.id));
    label.appendChild(cb);
    label.appendChild(document.createTextNode(g.name));
    ig.appendChild(label);
  }
}

/* 主持人：回执门槛的前置卡片下拉（已有卡片均可作前置）；尽量保留当前选择 */
function renderGateOptions() {
  if (state.role !== 'host') return;
  const sel = $('#card-gate');
  const cur = sel.value;
  sel.innerHTML = '';
  const none = document.createElement('option');
  none.value = '';
  none.textContent = '无门槛：到点即揭示';
  sel.appendChild(none);
  for (const c of sortedCards()) {
    const opt = document.createElement('option');
    opt.value = String(c.id);
    opt.textContent = `前置 #${c.seq} · ${c.title}（T+${fmt(c.trigger_seconds * 1000)}）`;
    sel.appendChild(opt);
  }
  sel.value = [...sel.options].some((o) => o.value === cur) ? cur : '';
}

function sortedCards() {
  return [...state.cards].sort((a, b) => a.trigger_seconds - b.trigger_seconds || a.seq - b.seq);
}

function renderCards() {
  const list = $('#card-list');
  // 重渲染会重建列表：先记住正在编辑的答复框，渲染后恢复焦点与未提交文本
  const active = document.activeElement;
  const editingId = active && active.classList && active.classList.contains('answer-input')
    ? active.dataset.cardId : null;
  const editingVal = editingId ? active.value : '';
  list.innerHTML = '';
  const all = sortedCards();
  const visible = state.role === 'host' ? all : all.filter((c) => c.revealed);
  $('#cards-empty').classList.toggle('hidden', visible.length > 0);
  if (state.role === 'host') {
    $('#cards-empty').textContent = '暂无卡片，请在上方添加';
  } else {
    $('#cards-empty').textContent = all.some((c) => c.revealed) ? '' : '等待主持人注入情景…';
  }
  for (const card of visible) {
    const li = document.createElement('li');
    li.className = 'card-item ' + (card.revealed ? 'revealed' : 'pending');

    const meta = document.createElement('div');
    meta.className = 'card-meta';
    const left = document.createElement('span');
    const trig = document.createElement('span');
    trig.className = 'card-trigger';
    trig.textContent = `T+${fmt(card.trigger_seconds * 1000)}`;
    left.appendChild(trig);
    left.appendChild(document.createTextNode(` · #${card.seq} `));
    const target = document.createElement('span');
    target.className = 'badge ' + (card.group_id == null ? 'badge-all' : 'badge-target');
    target.textContent = card.group_id == null ? '全员' : `仅 ${groupName(card.group_id)}`;
    left.appendChild(target);
    meta.appendChild(left);

    const right = document.createElement('span');
    if (card.revealed) {
      right.innerHTML = `<span class="badge badge-revealed">已揭示 ${fmt(card.revealed_at_ms || 0)}</span>`;
    } else {
      right.innerHTML = '<span class="badge badge-pending">待触发</span>';
    }
    if (state.role === 'player' && card.revealed) {
      right.appendChild(receiptButton(card));
    }
    if (state.role === 'host') {
      right.appendChild(answerLockButton(card));
    }
    if (state.role === 'host' && state.status !== 'running') {
      const del = document.createElement('button');
      del.className = 'card-del';
      del.textContent = '删除';
      del.onclick = () => api('DELETE', `/api/rooms/${state.code}/cards/${card.id}`).catch(showError);
      right.appendChild(del);
    }
    meta.appendChild(right);

    const title = document.createElement('div');
    title.className = 'card-title';
    title.textContent = card.title;

    li.appendChild(meta);
    li.appendChild(title);
    if (card.content) {
      const content = document.createElement('div');
      content.className = 'card-content';
      content.textContent = card.content;
      li.appendChild(content);
    }
    if (state.role === 'host' && card.gate) {
      li.appendChild(gateStatus(card));
    }
    if (state.role === 'host' && card.revealed) {
      const summary = receiptSummary(card);
      if (summary) li.appendChild(summary);
      const answers = answerSummary(card);
      if (answers) li.appendChild(answers);
    }
    if (state.role === 'player' && card.revealed) {
      li.appendChild(answerEditor(card));
    }
    list.appendChild(li);
  }
  if (editingId) {
    const ta = list.querySelector(`.answer-input[data-card-id="${editingId}"]`);
    if (ta && !ta.disabled) {
      ta.value = editingVal;
      ta.focus();
      ta.setSelectionRange(ta.value.length, ta.value.length);
    }
  }
}

/* 主持人：回执门槛状态行——等待（含未回执组与超时点）/ 已解锁 / 超时揭示 */
function gateStatus(card) {
  const g = card.gate;
  const box = document.createElement('div');
  box.className = `gate-status gate-${g.state}`;
  const parts = [];
  if (g.state === 'waiting') {
    parts.push(`⏳ 回执门槛：等待前置 #${g.depends_on_seq} 集齐回执`);
    if (g.pending_groups.length) {
      parts.push(`（未回执：${g.pending_groups.map(groupName).join('、')}）`);
    }
    if (g.gate_timeout_seconds != null) {
      parts.push(`· 超时点 T+${fmt((card.trigger_seconds + g.gate_timeout_seconds) * 1000)}`);
    }
  } else if (g.state === 'unlocked') {
    parts.push(`🔓 回执门槛已解锁（前置 #${g.depends_on_seq} 已集齐回执）`);
  } else {
    parts.push(`⏰ 超时揭示（前置 #${g.depends_on_seq} 回执未集齐）`);
  }
  box.textContent = parts.join(' ');
  return box;
}

/* 参与者：已揭示卡片上的「已收到」开关（可撤销） */
function receiptButton(card) {
  const btn = document.createElement('button');
  btn.className = 'receipt-btn' + (card.my_receipt ? ' acked' : '');
  btn.textContent = card.my_receipt ? '✓ 已收到（点击撤销）' : '标记已收到';
  btn.onclick = async () => {
    btn.disabled = true;
    try {
      const res = await api(card.my_receipt ? 'DELETE' : 'POST',
        `/api/join/${state.code}/cards/${card.id}/receipt`);
      card.my_receipt = res.acked; // 同组其他连接会通过 receipt_updated 事件同步
      renderCards();
    } catch (err) {
      showError(err);
      btn.disabled = false;
    }
  };
  return btn;
}

/* 主持人：已揭示卡片的每组回执/未回执汇总（全员卡统计所有组，定向卡只统计目标组） */
function receiptSummary(card) {
  const r = card.receipts;
  if (!r || r.expected.length === 0) return null; // 房间内还没有参与组
  const box = document.createElement('div');
  box.className = 'receipt-summary';

  const count = document.createElement('span');
  const done = r.acked.length === r.expected.length;
  count.className = 'receipt-count' + (done ? ' done' : '');
  count.textContent = `回执 ${r.acked.length}/${r.expected.length}`;
  box.appendChild(count);

  for (const gid of r.expected) {
    const got = r.acked.includes(gid);
    const chip = document.createElement('span');
    chip.className = 'receipt-chip ' + (got ? 'receipt-ok' : 'receipt-wait');
    chip.textContent = (got ? '✓ ' : '未回执 · ') + groupName(gid);
    box.appendChild(chip);
  }
  return box;
}

/* ---------------------------------------------------------- 处置答复 */

/* 主持人：逐卡锁定/重新开放答复的开关（锁定后参与者只读，写入被 409 拒绝） */
function answerLockButton(card) {
  const btn = document.createElement('button');
  btn.className = 'answer-lock-btn' + (card.answers_locked ? ' locked' : '');
  btn.textContent = card.answers_locked ? '🔒 答复已锁定' : '🔓 答复开放';
  btn.title = card.answers_locked
    ? '点击重新开放：参与者可继续提交/修改本组答复'
    : '点击锁定：参与者只读，提交/修改将被拒绝';
  btn.onclick = () => api('POST',
    `/api/rooms/${state.code}/cards/${card.id}/answers/${card.answers_locked ? 'unlock' : 'lock'}`)
    .catch(showError);
  return btn;
}

/* 主持人：已揭示卡片的各组答复状态、正文与版本（定向卡只看目标组，全员卡覆盖全部组） */
function answerSummary(card) {
  const a = card.answers;
  if (!a || a.expected.length === 0) return null; // 房间内还没有参与组
  const box = document.createElement('div');
  box.className = 'answer-summary';

  const head = document.createElement('div');
  head.className = 'answer-summary-head';
  const count = document.createElement('span');
  const done = a.submitted.length === a.expected.length;
  count.className = 'receipt-count' + (done ? ' done' : '');
  count.textContent = `答复 ${a.submitted.length}/${a.expected.length}`;
  head.appendChild(count);
  for (const gid of a.expected) {
    const got = a.submitted.find((s) => s.group_id === gid);
    const chip = document.createElement('span');
    chip.className = 'receipt-chip ' + (got ? 'receipt-ok' : 'receipt-wait');
    chip.textContent = (got ? `✓ v${got.revision} ` : '未提交 · ') + groupName(gid);
    head.appendChild(chip);
  }
  box.appendChild(head);

  for (const s of a.submitted) {
    const item = document.createElement('div');
    item.className = 'answer-item';
    const meta = document.createElement('div');
    meta.className = 'answer-item-meta';
    meta.textContent = `${groupName(s.group_id)} · 版本 ${s.revision}`;
    item.appendChild(meta);
    const body = document.createElement('div');
    body.className = 'answer-item-body';
    body.textContent = s.body || '（空）';
    item.appendChild(body);
    box.appendChild(item);
  }
  return box;
}

/* 参与者：已揭示卡片上的本组答复编辑器（每组一份；锁定后只读） */
function answerEditor(card) {
  const wrap = document.createElement('div');
  wrap.className = 'answer-area';
  const locked = card.answers_locked;
  const ans = card.my_answer || null;

  const head = document.createElement('div');
  head.className = 'answer-head';
  const label = document.createElement('span');
  label.className = 'answer-label';
  label.textContent = '处置答复（本组共享一份）';
  head.appendChild(label);
  const rev = document.createElement('span');
  rev.className = 'answer-rev';
  rev.textContent = locked ? '🔒 主持人已锁定，只读'
    : ans ? `已提交 · 版本 ${ans.revision}` : '尚未提交';
  head.appendChild(rev);
  wrap.appendChild(head);

  const ta = document.createElement('textarea');
  ta.className = 'answer-input';
  ta.dataset.cardId = card.id;
  ta.rows = 3;
  ta.maxLength = 4000;
  ta.placeholder = locked ? '答复已被主持人锁定' : '输入本组的处置答复…';
  ta.value = ans ? ans.body : '';
  ta.disabled = locked;
  wrap.appendChild(ta);

  if (card._answerWarn) {
    const warn = document.createElement('div');
    warn.className = 'answer-warn';
    warn.textContent = '同组其他窗口已提交新版本，继续保存将覆盖其内容';
    wrap.appendChild(warn);
  }

  if (!locked) {
    const save = document.createElement('button');
    save.className = 'primary answer-save';
    save.textContent = ans ? '更新答复' : '提交答复';
    save.onclick = () => submitAnswer(card, ta, save);
    wrap.appendChild(save);
  }
  return wrap;
}

/* 提交/修改本组答复：携带当前 revision 乐观并发；
   版本过期（409）时载入服务端返回的最新答复，绝不静默覆盖同组内容 */
async function submitAnswer(card, ta, btn) {
  btn.disabled = true;
  const cur = card.my_answer;
  try {
    const res = await api('PUT', `/api/join/${state.code}/cards/${card.id}/answer`, {
      body: ta.value,
      revision: cur ? cur.revision : 0,
    });
    card.my_answer = { body: res.body, revision: res.revision };
    card._selfSavedRevision = res.revision; // 自己的提交回显不再提示冲突
    card._answerWarn = false;
    renderCards();
  } catch (err) {
    btn.disabled = false;
    const d = err.detail;
    if (err.status === 409 && d && d.error === 'revision_conflict') {
      card.my_answer = d.answer ? { body: d.answer.body, revision: d.answer.revision } : null;
      card._answerWarn = false;
      showError(new Error('答复已被同组其他窗口更新，已为你载入最新版本'));
      renderCards();
    } else if (err.status === 409 && d && d.error === 'answers_locked') {
      card.answers_locked = true;
      showError(new Error('主持人已锁定该卡答复，当前为只读'));
      renderCards();
    } else {
      showError(err);
    }
  }
}

/* ---------------------------------------------------------- 观察席分享 */

const SHARE_STATUS_TEXT = { active: '有效', expired: '已过期', revoked: '已撤销' };

function shareStatus(s) {
  if (s.status === 'revoked') return 'revoked';
  return s.expires_at * 1000 > Date.now() ? 'active' : 'expired';
}

function shareUrl(s) {
  return `${location.origin}/#/observe/${s.token}`;
}

function upsertShare(s) {
  const i = state.shares.findIndex((x) => x.token === s.token);
  if (i >= 0) state.shares[i] = s;
  else state.shares.unshift(s); // 服务端按 id DESC 下发，新分享排最前
}

/* 主持人：分享列表（有效/失效），可复制链接、可撤销 */
function renderShares() {
  if (state.role !== 'host') return;
  const list = $('#share-list');
  list.innerHTML = '';
  $('#shares-empty').classList.toggle('hidden', state.shares.length > 0);
  for (const s of state.shares) {
    const st = shareStatus(s);
    const li = document.createElement('li');
    li.className = 'share-item';

    const main = document.createElement('div');
    main.className = 'share-main';

    const status = document.createElement('span');
    status.className = `badge share-status-${st}`;
    status.textContent = SHARE_STATUS_TEXT[st];
    main.appendChild(status);

    const groups = document.createElement('span');
    groups.className = 'share-groups-label';
    groups.textContent = (s.group_names || []).join('、');
    main.appendChild(groups);

    const mins = document.createElement('span');
    mins.className = 'share-remaining';
    mins.textContent = `${s.minutes} 分钟`;
    main.appendChild(mins);

    if (st === 'active') {
      const remain = document.createElement('span');
      remain.className = 'share-remaining';
      remain.dataset.expiresAt = s.expires_at;
      remain.dataset.token = s.token;
      remain.textContent = remainingText(s.expires_at);
      main.appendChild(remain);
    }
    li.appendChild(main);

    const ops = document.createElement('span');
    ops.className = 'share-ops';

    const copy = document.createElement('button');
    copy.className = 'group-op';
    copy.textContent = '复制链接';
    copy.onclick = () => {
      navigator.clipboard?.writeText(shareUrl(s));
      copy.textContent = '已复制';
      setTimeout(() => { copy.textContent = '复制链接'; }, 1200);
    };
    ops.appendChild(copy);

    if (st === 'active') {
      const revoke = document.createElement('button');
      revoke.className = 'group-op group-del';
      revoke.textContent = '撤销';
      revoke.onclick = async () => {
        if (!confirm('撤销该观察链接？在线观察者将立即断线，链接此后永久失效。')) return;
        try {
          await api('DELETE', `/api/rooms/${state.code}/shares/${s.token}`);
        } catch (err) { showError(err); }
      };
      ops.appendChild(revoke);
    }
    li.appendChild(ops);
    list.appendChild(li);
  }
}

function remainingText(expiresAt) {
  const mins = Math.ceil((expiresAt * 1000 - Date.now()) / 60000);
  return mins > 1 ? `剩余 ${mins} 分钟` : '剩余不足 1 分钟';
}

/* 倒计时文本就地更新（不重建列表，避免打断点击）；到期翻转时整体重渲染 */
function updateShareCountdowns() {
  let flip = false;
  for (const el of document.querySelectorAll('.share-remaining[data-expires-at]')) {
    const s = state.shares.find((x) => x.token === el.dataset.token);
    if (!s || s.status !== 'active') continue;
    const exp = parseFloat(el.dataset.expiresAt);
    if (exp * 1000 <= Date.now()) {
      s.status = 'expired';
      flip = true;
    } else {
      el.textContent = remainingText(exp);
    }
  }
  if (flip) renderShares();
}

/* ---------------------------------------------------------- 复盘锚点 */

/* 主持人：按 (轮次, 虚拟毫秒, id) 列出全部锚点；旧轮次锚点标记「第 N 轮」。
   服务端已保证锚点仅主持人可见（快照字段与 anchor_* 事件都按视野过滤）。 */
function renderAnchors() {
  if (state.role !== 'host') return;
  const list = $('#anchor-list');
  list.innerHTML = '';
  $('#anchors-empty').classList.toggle('hidden', state.anchors.length > 0);
  const ordered = [...state.anchors].sort(
    (a, b) => a.round - b.round || a.vtime_ms - b.vtime_ms || a.id - b.id);
  for (const a of ordered) {
    const li = document.createElement('li');
    li.className = 'anchor-item' + (a.round < currentRound() ? ' is-old' : '');

    const head = document.createElement('div');
    head.className = 'anchor-item-head';
    const title = document.createElement('span');
    title.className = 'anchor-item-title';
    title.textContent = `⚓ ${a.title}`;
    head.appendChild(title);
    if (a.round < currentRound()) {
      const rt = document.createElement('span');
      rt.className = 'anchor-round-tag';
      rt.textContent = `第 ${a.round + 1} 轮（历史）`;
      head.appendChild(rt);
    }
    const tm = document.createElement('span');
    tm.className = 'anchor-item-time';
    tm.textContent = `T+${fmt(a.vtime_ms)}`;
    head.appendChild(tm);
    li.appendChild(head);

    const note = document.createElement('div');
    note.className = 'anchor-item-note' + (a.note ? '' : ' empty-note');
    note.textContent = a.note || '（无备注）';
    li.appendChild(note);

    const meta = document.createElement('div');
    meta.className = 'anchor-item-meta';
    const info = document.createElement('span');
    info.textContent = `第 ${a.round + 1} 轮 · 事件水位 #${a.event_id}` +
      (a.updated_at && a.updated_at !== a.created_at ? ' · 备注已修改' : '');
    meta.appendChild(info);
    const ops = document.createElement('span');
    ops.className = 'anchor-ops';
    const edit = document.createElement('button');
    edit.className = 'anchor-op';
    edit.textContent = '修改备注';
    edit.onclick = async () => {
      const note2 = prompt('修改锚点备注（标题与绑定的轮次/时间/水位不可变）', a.note || '');
      if (note2 === null || note2 === a.note) return;
      try {
        await api('PATCH', `/api/rooms/${state.code}/anchors/${a.id}`, { note: note2 });
      } catch (err) { showError(err); }
    };
    ops.appendChild(edit);
    const del = document.createElement('button');
    del.className = 'anchor-op anchor-del';
    del.textContent = '删除';
    del.onclick = async () => {
      if (!confirm(`删除锚点「${a.title}」？删除后不可恢复。`)) return;
      try {
        await api('DELETE', `/api/rooms/${state.code}/anchors/${a.id}`);
      } catch (err) { showError(err); }
    };
    ops.appendChild(del);
    meta.appendChild(ops);
    li.appendChild(meta);

    list.appendChild(li);
  }
}

/* 当前轮次由快照权威下发（服务端按 reset 事件计数），reset 事件到达时 +1。 */
function currentRound() {
  return state.round;
}

/* ---------------------------------------------------------- 现场决策投票 */

/* 当前虚拟毫秒：以快照/事件同步点为基准，running 时本地外推，暂停即冻结。
   投票截止点是虚拟毫秒，倒计时与「已到期待结算」判定都走同一口径。 */
function currentElapsedMs() {
  const c = state.clock;
  return c.status === 'running'
    ? c.elapsedMs + (performance.now() - c.syncedAt)
    : c.elapsedMs;
}

function deadlineText(poll) {
  const remainMs = poll.deadline_ms - currentElapsedMs();
  if (poll.status === 'settled') return `已结算 · T+${fmt(poll.settled_at_ms || poll.deadline_ms)}`;
  if (poll.status === 'voided') return '已作废（重置时未结算）';
  return remainMs > 0 ? `截止 T+${fmt(poll.deadline_ms)}（剩余 ${fmt(remainMs)}）`
    : '已到截止点，等待结算…';
}

/* 投票列表：主持人看全部轮次（历史轮标记），参与者只看本轮本组投票 */
function renderPolls() {
  if (state.role === 'observer') return;
  const list = $('#poll-list');
  list.innerHTML = '';
  const ordered = [...state.polls].sort(
    (a, b) => a.round - b.round || a.id - b.id);
  $('#polls-empty').classList.toggle('hidden', ordered.length > 0);
  for (const poll of ordered) {
    list.appendChild(state.role === 'host' ? hostPollItem(poll) : playerPollItem(poll));
  }
}

/* 主持人视角：问题、轮次、截止/状态、选项汇总、逐组选票 */
function hostPollItem(poll) {
  const li = document.createElement('li');
  li.className = `poll-item poll-${poll.status}` +
    (poll.round < currentRound() ? ' is-old' : '');

  const head = document.createElement('div');
  head.className = 'poll-head';
  const q = document.createElement('span');
  q.className = 'poll-question';
  q.textContent = `🗳️ ${poll.question}`;
  head.appendChild(q);
  const tags = document.createElement('span');
  tags.className = 'poll-tags';
  if (poll.round < currentRound()) {
    const rt = document.createElement('span');
    rt.className = 'anchor-round-tag';
    rt.textContent = `第 ${poll.round + 1} 轮（历史）`;
    tags.appendChild(rt);
  }
  const st = document.createElement('span');
  st.className = 'badge ' + (poll.status === 'settled' ? 'badge-revealed'
    : poll.status === 'voided' ? 'share-status-revoked' : 'badge-pending');
  st.textContent = poll.status === 'settled' ? '已结算'
    : poll.status === 'voided' ? '已作废' : '投票中';
  tags.appendChild(st);
  head.appendChild(tags);
  li.appendChild(head);

  const meta = document.createElement('div');
  meta.className = 'poll-meta';
  const groups = (poll.group_ids || []).map(groupName).join('、') || '—';
  meta.textContent = `参与组：${groups} · ${deadlineText(poll)}`;
  li.appendChild(meta);

  // 选项与票数：投票中实时变化（每组只计一票，改投自动迁移）；
  // 已结算展示结算当时固定的票数（删组/重启均不变）。旧库缺结算事件时
  // tallies 为 null 且 tallies_recoverable=false，明确标示票数不可恢复，
  // 绝不显示由残余选票凑出的 0 票。
  const tallies = poll.tallies || poll.options.map((o) => ({ option_id: o.id, count: 0 }));
  const countsUnrecoverable = poll.status === 'settled' && poll.tallies_recoverable === false;
  for (const opt of poll.options) {
    const row = document.createElement('div');
    row.className = 'poll-option-row';
    const winner = poll.status === 'settled' && opt.id === poll.winner_option_id;
    row.textContent = countsUnrecoverable
      ? `${winner ? '🏆 ' : ''}${opt.text}　票数不可恢复`
      : `${winner ? '🏆 ' : ''}${opt.text}　${((tallies.find((t) => t.option_id === opt.id) || {}).count) || 0} 票`;
    if (winner) row.classList.add('poll-winner');
    li.appendChild(row);
  }
  if (countsUnrecoverable) {
    const warn = document.createElement('div');
    warn.className = 'poll-meta poll-counts-missing';
    warn.textContent = '该投票结算于旧版本且结算事件已缺失：保留获胜项，但各选项票数无法恢复，未按残余选票重算。';
    li.appendChild(warn);
  }

  if (poll.status === 'open') {
    // 逐组选票：哪一组投了哪个选项；未投组也列出
    const detail = document.createElement('div');
    detail.className = 'poll-votes-detail';
    const byGroup = new Map((poll.votes || []).map((v) => [v.group_id, v.option_id]));
    for (const gid of poll.group_ids || []) {
      const oid = byGroup.get(gid);
      const chip = document.createElement('span');
      chip.className = 'receipt-chip ' + (oid == null ? 'receipt-wait' : 'receipt-ok');
      chip.textContent = oid == null
        ? `未投票 · ${groupName(gid)}`
        : `✓ ${groupName(gid)} → ${optionText(poll, oid)}`;
      detail.appendChild(chip);
    }
    li.appendChild(detail);
  }
  return li;
}

function optionText(poll, optionId) {
  const o = poll.options.find((x) => x.id === optionId);
  return o ? o.text : `#${optionId}`;
}

/* 参与者视角：结算前只能投票/改投、只见自己的选择；结算后才显示结果与汇总 */
function playerPollItem(poll) {
  const li = document.createElement('li');
  li.className = `poll-item poll-${poll.status}`;

  const head = document.createElement('div');
  head.className = 'poll-head';
  const q = document.createElement('span');
  q.className = 'poll-question';
  q.textContent = `🗳️ ${poll.question}`;
  head.appendChild(q);
  const st = document.createElement('span');
  st.className = 'badge ' + (poll.status === 'settled' ? 'badge-revealed'
    : poll.status === 'voided' ? 'share-status-revoked' : 'badge-pending');
  st.textContent = poll.status === 'settled' ? '已结算'
    : poll.status === 'voided' ? '已作废' : '投票中';
  head.appendChild(st);
  li.appendChild(head);

  const meta = document.createElement('div');
  meta.className = 'poll-meta';
  meta.textContent = deadlineText(poll);
  li.appendChild(meta);

  // 结算前不显示任何票数（包括自己选项的总票数），只呈现本组的选择
  const pastDue = poll.status === 'open' && currentElapsedMs() >= poll.deadline_ms;
  for (const opt of poll.options) {
    const row = document.createElement('div');
    row.className = 'poll-option-row';
    const mine = poll.my_option_id === opt.id;
    if (poll.status === 'open') {
      const btn = document.createElement('button');
      btn.className = 'poll-vote-btn' + (mine ? ' chosen' : '');
      btn.textContent = (mine ? '● ' : '○ ') + opt.text;
      btn.disabled = pastDue;
      btn.title = pastDue ? '已到截止点，等待结算' : (mine ? '再次点击仍为改投到本项' : '投票 / 改投到本项');
      btn.onclick = () => submitPollVote(poll, opt.id, btn);
      row.appendChild(btn);
    } else if (poll.status === 'settled') {
      const winner = opt.id === poll.winner_option_id;
      if (winner) row.classList.add('poll-winner');
      if (poll.tallies_recoverable === false) {
        row.textContent = `${winner ? '🏆 ' : ''}${opt.text}　票数不可恢复`;
      } else {
        const n = (poll.tallies || []).find((t) => t.option_id === opt.id);
        row.textContent = `${winner ? '🏆 ' : ''}${opt.text}　${n ? n.count : 0} 票`
          + (mine ? '（本组选择）' : '');
      }
    } else {
      row.textContent = opt.text;
    }
    li.appendChild(row);
  }
  if (poll.status === 'settled' && poll.tallies_recoverable === false) {
    const warn = document.createElement('div');
    warn.className = 'poll-meta poll-counts-missing';
    warn.textContent = '该投票结算于旧版本且结算事件已缺失：获胜项保留，各选项票数无法恢复。';
    li.appendChild(warn);
  }
  if (poll.status === 'open' && pastDue) {
    const note = document.createElement('div');
    note.className = 'poll-meta';
    note.textContent = '已截止，正在等待主持人端时钟结算…';
    li.appendChild(note);
  }
  return li;
}

/* 提交/改投（每组只计一票，服务端原地更新）；成功由 poll_vote_updated 事件统一刷新 */
async function submitPollVote(poll, optionId, btn) {
  btn.disabled = true;
  try {
    await api('PUT', `/api/join/${state.code}/polls/${poll.id}/vote`, { option_id: optionId });
  } catch (err) {
    showError(err);
    renderPolls();
  }
}

/* ---------------------------------------------------------- 应急车辆申请 */

function vehicleStatusBadge(status, position) {
  const span = document.createElement('span');
  if (status === 'allocated') {
    span.className = 'badge vehicle-status-allocated';
    span.textContent = '已分配';
  } else {
    span.className = 'badge vehicle-status-queued';
    span.textContent = `排队中 · 第 ${position || '?'} 位`;
  }
  return span;
}

/* 主持人：车辆池总量/余量 + 各组申请与队列（服务端按受理顺序下发） */
function renderHostVehicles() {
  const v = state.vehicles;
  // 车辆池创建表单仅未创建时可见（每房间一个，创建后固定）
  $('#vehicle-pool-form').classList.toggle('hidden', !!v);
  const summary = $('#vehicle-pool-summary');
  const list = $('#vehicle-list');
  list.innerHTML = '';
  if (!v) {
    summary.textContent = '尚未创建车辆池：请在上方控制台设置总量（1～20 辆，创建后固定，重置演练保留）。';
    $('#vehicles-empty').classList.add('hidden');
    return;
  }
  const allocated = v.total - v.remaining;
  summary.textContent = `车辆池总量 ${v.total} 辆 · 已分配 ${allocated} 辆 · 剩余 ${v.remaining} 辆`
    + ` · 共 ${v.requests.length} 份申请`;
  $('#vehicles-empty').classList.toggle('hidden', v.requests.length > 0);
  for (const r of v.requests) {
    const li = document.createElement('li');
    li.className = 'vehicle-item ' + (r.status === 'allocated' ? 'vehicle-allocated' : 'vehicle-queued');

    const name = document.createElement('span');
    name.className = 'vehicle-group';
    name.textContent = r.group_name;
    li.appendChild(name);

    const qty = document.createElement('span');
    qty.className = 'vehicle-qty';
    qty.textContent = `申请 ${r.quantity} 辆`;
    li.appendChild(qty);

    li.appendChild(vehicleStatusBadge(r.status, r.position));
    list.appendChild(li);
  }
}

/* 参与者：仅本组申请状态与排位；无申请时显示提交表单 */
function renderPlayerVehicles() {
  const my = state.vehicles ? state.vehicles.my_request : null;
  const box = $('#vehicle-my');
  box.innerHTML = '';
  $('#vehicle-request-form').classList.toggle('hidden', !!my);
  if (!my) return;
  const div = document.createElement('div');
  div.className = 'vehicle-item ' + (my.status === 'allocated' ? 'vehicle-allocated' : 'vehicle-queued');

  const left = document.createElement('span');
  left.className = 'vehicle-group';
  left.appendChild(document.createTextNode(`本组申请 ${my.quantity} 辆　`));
  left.appendChild(vehicleStatusBadge(my.status, my.position));
  div.appendChild(left);

  const cancel = document.createElement('button');
  cancel.className = 'vehicle-cancel';
  cancel.textContent = '取消申请';
  cancel.title = my.status === 'allocated'
    ? '取消后已分配车辆释放并自动补位队首，本组可重新申请'
    : '取消后本组可重新申请';
  cancel.onclick = () => cancelVehicleRequest(cancel);
  div.appendChild(cancel);

  if (my.status === 'queued') {
    const note = document.createElement('div');
    note.className = 'hint';
    note.style.width = '100%';
    note.textContent = `当前排队第 ${my.position} 位：已分配申请取消释放车辆后，系统将从队首依次补位。`;
    div.appendChild(note);
  }
  box.appendChild(div);
}

function renderVehicles() {
  if (state.role === 'observer') return;
  const isHost = state.role === 'host';
  $('#vehicle-host-view').classList.toggle('hidden', !isHost);
  $('#vehicle-player-view').classList.toggle('hidden', isHost);
  if (isHost) renderHostVehicles();
  else renderPlayerVehicles();
}

async function cancelVehicleRequest(btn) {
  btn.disabled = true;
  try {
    await api('DELETE', `/api/join/${state.code}/vehicle-requests`);
    // vehicle_request_cancelled 事件也会到达本组；此处先即时更新
    if (state.vehicles) state.vehicles.my_request = null;
    renderVehicles();
  } catch (err) {
    showError(err);
    btn.disabled = false;
  }
}

/* ---------------------------------------------------------- 现场情报核验板 */

const INTEL_STATUS_TEXT = { pending: '待核验', confirmed: '已证实', refuted: '已否定', disputed: '有争议' };
const INTEL_STANCE_TEXT = { support: '支持', oppose: '反对' };

function intelStatusBadge(status) {
  const b = document.createElement('span');
  b.className = `badge intel-status-${status}`;
  b.textContent = INTEL_STATUS_TEXT[status] || status;
  return b;
}

function intelStanceBadge(stance) {
  const b = document.createElement('span');
  b.className = `badge intel-badge-${stance}`;
  b.textContent = INTEL_STANCE_TEXT[stance] || stance;
  return b;
}

/* 命题列表：主持人看逐组报告，参与者看本组报告与汇总状态（观察者面板整体隐藏） */
function renderIntel() {
  if (state.role === 'observer') return;
  const list = $('#intel-list');
  // 重渲染会重建列表：先记住正在编辑的依据框，渲染后恢复焦点与未提交文本
  const active = document.activeElement;
  const editingId = active && active.classList && active.classList.contains('intel-rationale')
    ? active.dataset.claimId : null;
  list.innerHTML = '';
  const ordered = [...state.claims].sort((a, b) => a.id - b.id);
  $('#intel-empty').classList.toggle('hidden', ordered.length > 0);
  for (const claim of ordered) {
    list.appendChild(state.role === 'host' ? hostIntelItem(claim) : playerIntelItem(claim));
  }
  if (editingId) {
    const ta = list.querySelector(`.intel-rationale[data-claim-id="${editingId}"]`);
    if (ta) {
      ta.focus();
      ta.setSelectionRange(ta.value.length, ta.value.length);
    }
  }
}

function intelSummaryText(summary) {
  const s = summary || { support: 0, oppose: 0 };
  return `支持 ${s.support} 组 · 反对 ${s.oppose} 组`;
}

/* 主持人视角：命题、白名单各组报告状态、逐组立场与依据正文、汇总状态 */
function hostIntelItem(claim) {
  const li = document.createElement('li');
  li.className = `intel-item intel-${claim.summary.status}`;

  const head = document.createElement('div');
  head.className = 'intel-head';
  const st = document.createElement('span');
  st.className = 'intel-statement';
  st.textContent = `🔎 ${claim.statement}`;
  head.appendChild(st);
  head.appendChild(intelStatusBadge(claim.summary.status));
  li.appendChild(head);

  const meta = document.createElement('div');
  meta.className = 'intel-meta';
  const groups = (claim.group_ids || []).map(groupName).join('、') || '—';
  meta.textContent = `参与组：${groups} · ${intelSummaryText(claim.summary)}`;
  li.appendChild(meta);

  // 逐组报告状态徽标：✓支持 / ✗反对 / 未报告
  const chips = document.createElement('div');
  chips.className = 'intel-chips';
  const byGroup = new Map((claim.reports || []).map((r) => [r.group_id, r]));
  for (const gid of claim.group_ids || []) {
    const rep = byGroup.get(gid);
    const chip = document.createElement('span');
    chip.className = 'receipt-chip ' + (rep
      ? (rep.stance === 'support' ? 'receipt-ok' : 'intel-chip-oppose')
      : 'receipt-wait');
    chip.textContent = rep
      ? `${rep.stance === 'support' ? '✓' : '✗'} ${groupName(gid)}`
      : `未报告 · ${groupName(gid)}`;
    chips.appendChild(chip);
  }
  li.appendChild(chips);

  // 逐组报告明细（立场、依据正文、版本）
  for (const rep of claim.reports || []) {
    const item = document.createElement('div');
    item.className = 'intel-report';
    const rmeta = document.createElement('div');
    rmeta.className = 'intel-report-meta';
    rmeta.appendChild(document.createTextNode(`${groupName(rep.group_id)} · `));
    rmeta.appendChild(intelStanceBadge(rep.stance));
    rmeta.appendChild(document.createTextNode(` · 版本 ${rep.revision}`));
    item.appendChild(rmeta);
    const body = document.createElement('div');
    body.className = 'intel-report-body';
    body.textContent = rep.rationale;
    item.appendChild(body);
    li.appendChild(item);
  }
  return li;
}

/* 参与者视角：命题、汇总状态（计数+状态，无他组明细）、本组报告编辑器 */
function playerIntelItem(claim) {
  const li = document.createElement('li');
  li.className = `intel-item intel-${claim.summary.status}`;

  const head = document.createElement('div');
  head.className = 'intel-head';
  const st = document.createElement('span');
  st.className = 'intel-statement';
  st.textContent = `🔎 ${claim.statement}`;
  head.appendChild(st);
  head.appendChild(intelStatusBadge(claim.summary.status));
  li.appendChild(head);

  const meta = document.createElement('div');
  meta.className = 'intel-meta';
  meta.textContent = `汇总状态：${intelSummaryText(claim.summary)}`;
  li.appendChild(meta);

  li.appendChild(intelEditor(claim));
  return li;
}

/* 参与者：本组报告编辑器——支持/反对 + 简短依据；同组修改覆盖原报告，可撤回。
   草稿存于 claim._draftStance/_draftRationale：事件重渲染不丢未提交内容；
   本组报告被同组其他窗口改动时（revision 变化）草稿被清除、载入最新报告。 */
function intelEditor(claim) {
  const wrap = document.createElement('div');
  wrap.className = 'intel-editor';
  const my = claim.my_report || null;
  const stance = claim._draftStance !== undefined
    ? claim._draftStance : (my ? my.stance : null);
  const rationale = claim._draftRationale !== undefined
    ? claim._draftRationale : (my ? my.rationale : '');

  const head = document.createElement('div');
  head.className = 'answer-head';
  const label = document.createElement('span');
  label.className = 'answer-label';
  label.textContent = '本组核验报告（同组共享一份）';
  head.appendChild(label);
  const rev = document.createElement('span');
  rev.className = 'answer-rev';
  rev.textContent = my
    ? `已提交：${INTEL_STANCE_TEXT[my.stance]} · 版本 ${my.revision}`
    : '尚未提交';
  head.appendChild(rev);
  wrap.appendChild(head);

  const stanceRow = document.createElement('div');
  stanceRow.className = 'intel-stance-row';
  for (const s of ['support', 'oppose']) {
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.className = `intel-stance-btn intel-stance-${s}` + (stance === s ? ' chosen' : '');
    btn.textContent = (stance === s ? '● ' : '○ ') + INTEL_STANCE_TEXT[s];
    btn.onclick = () => {
      claim._draftStance = s;
      renderIntel();
    };
    stanceRow.appendChild(btn);
  }
  wrap.appendChild(stanceRow);

  const ta = document.createElement('textarea');
  ta.className = 'intel-rationale';
  ta.dataset.claimId = claim.id;
  ta.rows = 2;
  ta.maxLength = 500;
  ta.placeholder = '简短依据（必填，1～500 字）…';
  ta.value = rationale;
  ta.oninput = () => { claim._draftRationale = ta.value; };
  wrap.appendChild(ta);

  const ops = document.createElement('div');
  ops.className = 'intel-editor-ops';
  const save = document.createElement('button');
  save.className = 'primary intel-save';
  save.textContent = my ? '更新报告（覆盖原报告）' : '提交报告';
  save.onclick = () => submitIntelReport(claim, stance, ta, save);
  ops.appendChild(save);
  if (my) {
    const withdraw = document.createElement('button');
    withdraw.className = 'intel-withdraw';
    withdraw.textContent = '撤回报告';
    withdraw.title = '撤回后本组不再计入任何一方，状态即时重算';
    withdraw.onclick = () => withdrawIntelReport(claim, withdraw);
    ops.appendChild(withdraw);
  }
  wrap.appendChild(ops);
  return wrap;
}

/* 提交/修改本组报告：同组覆盖原报告；成功由返回值与 intel_claim_updated 事件共同收敛 */
async function submitIntelReport(claim, stance, ta, btn) {
  if (!stance) {
    showError(new Error('请选择立场：支持或反对'));
    return;
  }
  if (!ta.value.trim()) {
    showError(new Error('请填写简短依据'));
    return;
  }
  btn.disabled = true;
  try {
    const res = await api('PUT', `/api/join/${state.code}/claims/${claim.id}/report`, {
      stance,
      rationale: ta.value.trim(),
    });
    upsertClaim(res); // 本组其他连接通过 intel_claim_updated 事件同步
  } catch (err) {
    showError(err);
    btn.disabled = false;
  }
}

/* 撤回本组报告：状态即时重算 */
async function withdrawIntelReport(claim, btn) {
  btn.disabled = true;
  try {
    const res = await api('DELETE', `/api/join/${state.code}/claims/${claim.id}/report`);
    upsertClaim(res);
  } catch (err) {
    showError(err);
    btn.disabled = false;
  }
}

/* 用服务端下发的命题视图（按当前角色脱敏）就地更新列表 */
function upsertClaim(view) {
  const i = state.claims.findIndex((c) => c.id === view.id);
  if (i >= 0) state.claims[i] = view;
  else state.claims.push(view);
  renderIntel();
}

/* ---------------------------------------------------------- 响应时效面板（仅主持人） */

/* 虚拟毫秒差格式化：mm:ss.mmm（超 1 小时带小时位）；null 由调用方显示「未达 / 无数据」 */
function fmtDelta(ms) {
  const v = Math.max(0, Math.round(ms));
  const s = Math.floor(v / 1000);
  const mmm = String(v % 1000).padStart(3, '0');
  const hh = Math.floor(s / 3600);
  const mm = String(Math.floor((s % 3600) / 60)).padStart(2, '0');
  const ss = String(s % 60).padStart(2, '0');
  return hh > 0 ? `${hh}:${mm}:${ss}.${mmm}` : `${mm}:${ss}.${mmm}`;
}

/* 换房、离开及重新进入时复位面板会话：取消挂起的防抖刷新并推进 epoch，
   使所有在途的旧请求（含上一房间的请求）在返回时判定失效。 */
function resetLatencySession() {
  clearTimeout(state.latency.timer);
  state.latency = { round: null, data: null, timer: 0,
                    epoch: state.latency.epoch + 1, seq: 0 };
}

/* 响应时效报告的陈旧判定：仅当响应仍对应「发起时的房间 + 当前轮次选择」
   且它是当前会话最新发出的请求时，才允许写入面板。
   - epoch 不同：换房/离开/重新进入后返回的旧请求，一律丢弃；
   - code 不同：响应来自上一房间（防止上一房间组名渲染进新房间面板）；
   - 轮次不一致：快速切换轮次、或跟随当前轮时发生 reset（轮次 +1）后，
     较早请求即便后返回也不得覆盖当前选择；
   - seq 不是最新：同选择下更早发出的请求后返回时丢弃。 */
function latencyResponseStale(req) {
  if (req.epoch !== state.latency.epoch) return true;
  if (req.code !== state.code) return true;
  const wantRound = state.latency.round == null ? state.round : state.latency.round;
  if (req.round !== wantRound) return true;
  return req.seq !== state.latency.seq;
}

/* 拉取所选轮次的响应时效报告。服务端在状态锁内固定事件水位后统计：
   暂停时长不计入，撤销回执/修改答复不改首次响应时间，未响应为 null（未达）。
   请求身份（房间码、所选轮次、会话代号、请求序号）在发起瞬间绑定，
   轮次选择触发的拉取与实时事件触发的防抖刷新走同一陈旧判定。 */
async function refreshLatency() {
  if (state.role !== 'host' || !state.code) return;
  // 发起瞬间固定身份：round 为发起时解析出的轮次（跟随当前轮也固化为具体值）
  const req = {
    code: state.code,
    round: state.latency.round == null ? state.round : state.latency.round,
    epoch: state.latency.epoch,
    seq: ++state.latency.seq,
  };
  let data;
  try {
    data = await api('GET', `/api/rooms/${req.code}/latency?round=${req.round}`);
  } catch (err) {
    // 拉取失败（轮次失效、网络抖动、换房后在途的旧请求等）一律静默忽略：
    // 既不清空也不覆盖当前报告，下一次轮次切换/事件刷新会重新拉取
    return;
  }
  if (latencyResponseStale(req)) return; // 较旧或跨房间响应不得写入面板
  state.latency.data = data;
  renderLatency();
}

/* 揭示/回执/答复/重置/组增删事件后防抖刷新（面板为拉取式分析，不走增量事件）。
   与轮次切换共用 refreshLatency 的身份绑定与陈旧判定。 */
function scheduleLatencyRefresh() {
  if (state.role !== 'host') return;
  clearTimeout(state.latency.timer);
  state.latency.timer = setTimeout(() => {
    state.latency.timer = 0;
    refreshLatency();
  }, 400);
}

function renderLatency() {
  if (state.role !== 'host') return;
  const data = state.latency.data;
  const cur = data ? data.current_round : state.round;
  // 选中值以当前轮次选择为准（而非已载入数据的轮次）：快速切轮次时旧数据仍在途，
  // 其他事件触发整体重渲染也不会把下拉回弹到旧轮次
  const selected = state.latency.round == null ? state.round : state.latency.round;
  const sel = $('#latency-round');
  sel.innerHTML = '';
  for (let r = 0; r <= cur; r++) {
    const opt = document.createElement('option');
    opt.value = String(r);
    opt.textContent = `第 ${r + 1} 轮` + (r === cur ? '（当前）' : '');
    sel.appendChild(opt);
  }
  sel.value = String(selected);
  // 渲染保险：缓存数据的轮次与当前选择不一致时（切换轮次/重置后、新响应尚未
  // 到达的在途窗口），绝不展示错轮内容。跨房间数据在写入时已被陈旧判定拦截，
  // 且进入/离开房间会清空缓存，不会到达这里
  const view = data && data.round === selected ? data : null;
  $('#latency-meta').textContent = view ? `事件水位 #${view.high_water_event_id}` : '';
  const box = $('#latency-groups');
  box.innerHTML = '';
  const groups = view ? view.groups : [];
  const hasCards = groups.some((g) => g.cards.length > 0);
  $('#latency-empty').classList.toggle('hidden', hasCards);
  for (const g of groups) {
    if (g.cards.length > 0) box.appendChild(latencyGroupBlock(g));
  }
}

/* 单个参与组的时效块：组名 + 两类中位时延（只取已响应样本）+ 逐卡明细表 */
function latencyGroupBlock(g) {
  const wrap = document.createElement('div');
  wrap.className = 'latency-group';

  const head = document.createElement('div');
  head.className = 'latency-group-head';
  const name = document.createElement('span');
  name.className = 'latency-group-name';
  name.textContent = g.group_name + (g.exists ? '' : '（已删除）');
  head.appendChild(name);
  const med = document.createElement('span');
  med.className = 'latency-medians';
  const recMed = g.receipt_median_ms == null ? '无数据' : fmtDelta(g.receipt_median_ms);
  const ansMed = g.answer_median_ms == null ? '无数据' : fmtDelta(g.answer_median_ms);
  med.textContent = `回执中位 ${recMed}（已响应 ${g.receipt_responded}/${g.cards.length}）`
    + ` · 答复中位 ${ansMed}（已响应 ${g.answer_responded}/${g.cards.length}）`;
  head.appendChild(med);
  wrap.appendChild(head);

  const table = document.createElement('table');
  table.className = 'latency-table';
  const thead = document.createElement('thead');
  thead.innerHTML = '<tr><th>卡片</th><th>揭示</th><th>首次回执</th><th>首次答复</th></tr>';
  table.appendChild(thead);
  const tbody = document.createElement('tbody');
  for (const c of g.cards) {
    const tr = document.createElement('tr');
    const cardCell = document.createElement('td');
    cardCell.textContent = `#${c.seq} ${c.title} `;
    const badge = document.createElement('span');
    badge.className = 'badge ' + (c.card_group_id == null ? 'badge-all' : 'badge-target');
    badge.textContent = c.card_group_id == null ? '全员' : '定向';
    cardCell.appendChild(badge);
    tr.appendChild(cardCell);
    const revCell = document.createElement('td');
    revCell.className = 'latency-num';
    revCell.textContent = `T+${fmt(c.revealed_at_ms || 0)}`;
    tr.appendChild(revCell);
    tr.appendChild(latencyCell(c.receipt_latency_ms));
    tr.appendChild(latencyCell(c.answer_latency_ms));
    tbody.appendChild(tr);
  }
  table.appendChild(tbody);
  wrap.appendChild(table);
  return wrap;
}

/* 单个时延单元格：已响应显示 +mm:ss.mmm，未响应显示「未达」（不以墙钟补足） */
function latencyCell(ms) {
  const td = document.createElement('td');
  td.className = 'latency-num';
  if (ms == null) {
    const miss = document.createElement('span');
    miss.className = 'latency-miss';
    miss.textContent = '未达';
    td.appendChild(miss);
  } else {
    td.textContent = `+${fmtDelta(ms)}`;
  }
  return td;
}

function renderAll() {
  renderMeta();
  renderGroups();
  renderGateOptions();
  renderShares();
  renderAnchors();
  renderPolls();
  renderVehicles();
  renderIntel();
  renderLatency();
  renderCards();
}

/* 本地时钟渲染：以快照/事件同步点为基准，running 时本地外推，暂停即冻结 */
setInterval(() => {
  if (!state.code) return;
  const c = state.clock;
  const elapsed = c.status === 'running' ? c.elapsedMs + (performance.now() - c.syncedAt) : c.elapsedMs;
  $('#clock').textContent = fmt(elapsed);
  if (state.role === 'host') updateShareCountdowns();
  updatePollCountdowns();
}, 200);

/* 投票截止倒计时就地更新（不重建列表，避免打断点击）；
   open→到点、到点→结算 的翻转由整体重渲染处理 */
function updatePollCountdowns() {
  if (state.role === 'observer' || !state.polls.length) return;
  const elapsed = currentElapsedMs();
  let needRender = false;
  for (const p of state.polls) {
    if (p.status !== 'open') continue;
    const flipKey = p.deadline_ms <= elapsed ? 1 : 0;
    if (p._pastDueKey !== flipKey) {
      p._pastDueKey = flipKey;
      needRender = true; // 到点瞬间需禁用按钮/显示「等待结算」
    }
  }
  if (needRender) renderPolls();
}

function syncClock(status, elapsedMs) {
  state.status = status;
  state.clock = { status, elapsedMs, syncedAt: performance.now() };
}

/* ---------------------------------------------------------- 状态应用 */

function applySnapshot(msg) {
  state.roomName = msg.room.name;
  state.cards = msg.cards;
  state.groups = msg.groups || [];
  state.shares = msg.shares || []; // 仅主持人快照携带
  state.anchors = msg.anchors || []; // 仅主持人快照携带；参与者/观察者字段缺省
  state.polls = msg.polls || [];  // 主持人全部轮次；参与者本轮本组；观察者缺省为空
  state.polls.forEach((p) => {
    p._pastDueKey = p.status === 'open' && p.deadline_ms <= msg.room.elapsed_ms ? 1 : 0;
  });
  state.vehicles = msg.vehicles || null; // 主持人：全量（无池为 null）；参与者：{my_request}；观察者：缺省
  state.claims = msg.claims || [];  // 主持人：全部命题（逐组报告）；参与者：白名单含本组的命题；观察者缺省为空
  state.groupId = msg.scope && msg.scope.role === 'player' ? msg.scope.group_id : null;
  state.round = Number.isInteger(msg.round) ? msg.round : 0;
  state.lastEventId = msg.last_event_id; // 权威水位
  syncClock(msg.room.status, msg.room.elapsed_ms);
  renderAll();
  if (state.role === 'host') refreshLatency(); // 快照就绪后拉取响应时效面板数据
}

function applyEvent(msg) {
  if (msg.id <= state.lastEventId) return; // 不回退：丢弃旧事件
  state.lastEventId = msg.id;
  const p = msg.payload;
  switch (msg.type) {
    case 'card_added':
      if (!state.cards.some((c) => c.id === p.id)) {
        initCardState(p); // 新卡必然零回执、零答复（未揭示不可回执）
        state.cards.push(p);
      }
      break;
    case 'card_removed':
      state.cards = state.cards.filter((c) => c.id !== p.id);
      break;
    case 'card_revealed': {
      const card = state.cards.find((c) => c.id === p.id);
      if (card) Object.assign(card, p);
      else { initCardState(p); state.cards.push(p); }
      break;
    }
    case 'receipt_updated': {
      // 服务端已按视野过滤：主持人收全部，参与者只收本组
      const card = state.cards.find((c) => c.id === p.card_id);
      if (!card) break;
      if (state.role === 'host' && card.receipts) {
        const i = card.receipts.acked.indexOf(p.group_id);
        if (p.acked && i < 0) card.receipts.acked.push(p.group_id);
        if (!p.acked && i >= 0) card.receipts.acked.splice(i, 1);
      } else if (state.role === 'player' && p.group_id === state.groupId) {
        card.my_receipt = p.acked;
      }
      break;
    }
    case 'answer_updated': {
      // 含答复正文：主持人收全部，参与者只收本组（服务端已过滤）
      const card = state.cards.find((c) => c.id === p.card_id);
      if (!card) break;
      if (state.role === 'host' && card.answers) {
        const i = card.answers.submitted.findIndex((s) => s.group_id === p.group_id);
        const entry = { group_id: p.group_id, body: p.body, revision: p.revision,
                        updated_at: p.updated_at };
        if (i >= 0) card.answers.submitted[i] = entry;
        else card.answers.submitted.push(entry);
      } else if (state.role === 'player' && p.group_id === state.groupId) {
        card.my_answer = { body: p.body, revision: p.revision };
        // 正在编辑时收到同组新版本：保留未提交文本并提示，而非直接覆盖
        const active = document.activeElement;
        const editing = active && active.classList && active.classList.contains('answer-input')
          && active.dataset.cardId === String(card.id);
        if (editing && p.revision !== card._selfSavedRevision) card._answerWarn = true;
      }
      break;
    }
    case 'answer_lock_updated': {
      const card = state.cards.find((c) => c.id === p.card_id);
      if (card) card.answers_locked = p.answers_locked;
      break;
    }
    case 'gate_updated': {
      // 仅主持人会收到：门槛状态由服务端权威计算，直接替换
      const card = state.cards.find((c) => c.id === p.card_id);
      if (card) card.gate = p.gate;
      break;
    }
    case 'group_added':
      if (!state.groups.some((g) => g.id === p.id)) state.groups.push(p);
      // 新组自动成为所有全员卡片的待回执方与待答复方
      state.cards.forEach((c) => {
        if (c.group_id == null) {
          if (c.receipts && !c.receipts.expected.includes(p.id)) {
            c.receipts.expected.push(p.id);
          }
          if (c.answers && !c.answers.expected.includes(p.id)) {
            c.answers.expected.push(p.id);
          }
        }
      });
      break;
    case 'group_renamed': {
      const g = state.groups.find((g) => g.id === p.id);
      if (g) g.name = p.name;
      break;
    }
    case 'group_removed':
      state.groups = state.groups.filter((g) => g.id !== p.id);
      state.cards.forEach((c) => {
        if (c.receipts) {
          c.receipts.expected = c.receipts.expected.filter((id) => id !== p.id);
          c.receipts.acked = c.receipts.acked.filter((id) => id !== p.id);
        }
        if (c.answers) {
          c.answers.expected = c.answers.expected.filter((id) => id !== p.id);
          c.answers.submitted = c.answers.submitted.filter((s) => s.group_id !== p.id);
        }
      });
      break;
    case 'share_created':
      upsertShare(p); // 仅主持人会收到（事件含观察凭证，服务端已过滤）
      break;
    case 'share_revoked':
    case 'share_expired': {
      const s = state.shares.find((x) => x.token === p.token);
      if (s) s.status = msg.type === 'share_revoked' ? 'revoked' : 'expired';
      break;
    }
    case 'anchor_created':
      // 仅主持人会收到（服务端 event_visible 对 anchor_* 已过滤参与者/观察者）
      if (!state.anchors.some((a) => a.id === p.id)) state.anchors.push(p);
      break;
    case 'anchor_updated': {
      const a = state.anchors.find((x) => x.id === p.id);
      if (a) Object.assign(a, p); // 仅备注/updated_at 会变；绑定字段不可变
      break;
    }
    case 'anchor_deleted':
      state.anchors = state.anchors.filter((a) => a.id !== p.id);
      break;
    case 'poll_created':
      // 主持人收全量，参与者收脱敏 open 载荷（服务端已按白名单过滤）
      if (!state.polls.some((x) => x.id === p.id)) {
        p._pastDueKey = p.status === 'open' && p.deadline_ms <= currentElapsedMs() ? 1 : 0;
        state.polls.push(p);
      }
      break;
    case 'poll_vote_updated': {
      // 主持人载荷带完整 poll（逐组选票）；参与者本组只收三字段，更新 my_option_id
      const poll = state.polls.find((x) => x.id === p.poll_id);
      if (state.role === 'host' && p.poll) {
        const i = state.polls.findIndex((x) => x.id === p.poll_id);
        if (i >= 0) state.polls[i] = { ...state.polls[i], ...p.poll };
      } else if (state.role === 'player' && poll) {
        poll.my_option_id = p.option_id; // 本组投票/改投（含同组其他窗口）
      }
      break;
    }
    case 'poll_settled': {
      // 主持人收全量（含 winner/tallies/votes），参与者收到按组脱敏的结果载荷
      const i = state.polls.findIndex((x) => x.id === p.id);
      if (i >= 0) state.polls[i] = { ...state.polls[i], ...p };
      else if (state.role === 'host') state.polls.push(p);
      break;
    }
    case 'poll_voided':
      // 仅主持人收到：标记历史未结算投票为作废
      if (state.role === 'host') {
        const poll = state.polls.find((x) => x.id === p.id);
        if (poll) poll.status = 'voided';
      }
      break;
    case 'vehicle_pool_created':
      // 仅主持人收到：车辆池总量/余量（事件载荷即主持人全量视图）
      state.vehicles = p;
      break;
    case 'vehicle_request_submitted':
    case 'vehicle_request_cancelled':
      // 主持人载荷带全量 vehicles；本组参与者只收 {my_request}（服务端已按组过滤）
      if (state.role === 'host') state.vehicles = p.vehicles;
      else if (state.role === 'player') state.vehicles = { my_request: p.my_request };
      break;
    case 'intel_claim_created':
      // 主持人收全量（逐组报告），白名单参与组收脱敏载荷（my_report 为空）
      if (!state.claims.some((c) => c.id === p.id)) state.claims.push(p);
      break;
    case 'intel_claim_updated': {
      // 报告提交/修改/撤回、白名单收缩后的即时重算结果：
      // 主持人收 {claim_id, group_ids, claim: 全量}；参与组收本组脱敏 {claim_id, claim}
      const i = state.claims.findIndex((c) => c.id === p.claim_id);
      const old = i >= 0 ? state.claims[i] : null;
      const oldRev = old && old.my_report ? old.my_report.revision : 0;
      const next = { ...(old || {}), ...p.claim };
      const newRev = next.my_report ? next.my_report.revision : 0;
      if (newRev !== oldRev) {
        // 本组报告已被（同组其他窗口）改动：丢弃未提交草稿，载入最新报告
        delete next._draftStance;
        delete next._draftRationale;
      } else if (old) {
        next._draftStance = old._draftStance;
        next._draftRationale = old._draftRationale;
      }
      if (i >= 0) state.claims[i] = next;
      else state.claims.push(next);
      break;
    }
    case 'clock':
      syncClock(p.status, p.elapsed_ms);
      break;
    case 'reset':
      state.cards.forEach((c) => {
        c.revealed = false;
        c.revealed_at_ms = null;
        if (c.receipts) c.receipts.acked = []; // 回执随揭示一并收回
        if ('my_receipt' in c) c.my_receipt = false;
        c.answers_locked = false;            // 答复锁定一并解除
        if (c.answers) c.answers.submitted = [];
        if ('my_answer' in c) c.my_answer = null;
        c._answerWarn = false;
      });
      // 投票：参与者清空本轮投票（作废投票与旧轮结果均不再下发）；
      // 主持人保留全部——未结算投票随后由 poll_voided 标为作废，已结算按轮次保留
      if (state.role !== 'host') state.polls = [];
      // 应急车辆：清空全部申请并恢复全部余量；车辆池保留（参与者回到未申请状态）
      if (state.vehicles) {
        if (state.role === 'host') {
          state.vehicles = { ...state.vehicles, remaining: state.vehicles.total, requests: [] };
        } else {
          state.vehicles = { my_request: null };
        }
      }
      // 情报核验：清空全部报告（命题保留，各命题回到待核验）
      state.claims.forEach((c) => {
        c.summary = { support: 0, oppose: 0, status: 'pending' };
        if (state.role === 'host') c.reports = [];
        else c.my_report = null;
        delete c._draftStance;
        delete c._draftRationale;
      });
      state.round += 1; // 重置开启新一轮；锚点不清空，旧锚点仍归属原轮次
      syncClock('idle', 0);
      break;
  }
  // 响应时效面板为拉取式分析：揭示/回执/答复/重置/组增删后防抖重取所选轮次数据
  if (['card_revealed', 'receipt_updated', 'answer_updated', 'reset',
       'group_added', 'group_removed'].includes(msg.type)) {
    scheduleLatencyRefresh();
  }
  renderAll();
}

/* 事件流里新增的卡片不带回执/答复字段（新卡/新揭示必然零回执、零答复），按角色补默认值；
   观察者只读，不附加任何回执与答复状态 */
function initCardState(card) {
  if (state.role === 'host') {
    card.receipts = {
      expected: card.group_id == null ? state.groups.map((g) => g.id) : [card.group_id],
      acked: [],
    };
    card.answers = {
      expected: card.group_id == null ? state.groups.map((g) => g.id) : [card.group_id],
      submitted: [],
    };
  } else if (state.role === 'player') {
    card.my_receipt = false;
    card.my_answer = null;
  }
}

/* ---------------------------------------------------------- WebSocket */

function setConn(up) {
  const el = $('#conn');
  el.textContent = up ? '已连接' : '连接断开，重连中…';
  el.className = 'conn ' + (up ? 'conn-up' : 'conn-down');
}

function connect() {
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  const path = state.role === 'observer' ? `/ws/observe/${state.code}` : `/ws/${state.code}`;
  const ws = new WebSocket(`${proto}://${location.host}${path}?last_event_id=${state.lastEventId}`);
  state.ws = ws;

  ws.onopen = () => { state.reconnectDelay = 1000; setConn(true); };
  ws.onmessage = (e) => {
    const msg = JSON.parse(e.data);
    if (msg.kind === 'snapshot') applySnapshot(msg);
    else if (msg.kind === 'event') applyEvent(msg);
    else if (msg.kind === 'error') { alert(msg.message); leaveRoom(); }
  };
  ws.onclose = () => {
    setConn(false);
    if (!state.code) return;
    setTimeout(connect, state.reconnectDelay);
    state.reconnectDelay = Math.min(state.reconnectDelay * 2, 5000);
  };
}

/* ---------------------------------------------------------- API */

async function api(method, path, body) {
  const res = await fetch(path, {
    method,
    headers: body ? { 'Content-Type': 'application/json' } : {},
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!res.ok) {
    const data = await res.json().catch(() => ({}));
    const d = data.detail;
    // 409 冲突等场景 detail 可能是结构化对象（如答复版本冲突携带最新答复）
    const err = new Error(typeof d === 'string' ? d : (d && d.error) || `请求失败 (${res.status})`);
    err.status = res.status;
    if (d && typeof d === 'object') err.detail = d;
    throw err;
  }
  return res.json();
}

function showError(err) {
  for (const sel of ['#landing-error', '#room-error']) {
    const el = $(sel);
    el.textContent = err.message || String(err);
    setTimeout(() => { el.textContent = ''; }, 4000);
  }
}

/* ---------------------------------------------------------- 流程 */

function enterRoom(code, role) {
  state.code = code;
  state.role = role;
  state.lastEventId = 0;
  state.cards = [];
  state.groups = [];
  state.shares = [];
  state.anchors = [];
  state.polls = [];
  state.vehicles = null;
  state.claims = [];
  state.round = 0;
  state.groupId = null;
  resetLatencySession();
  location.hash = `#/${role}/${code}`;
  localStorage.setItem(`inject-role-${code}`, role);
  showRoom();
  renderAll();
  connect();
}

/* 观察者：凭限时观察链接进入（只读视野，凭证失效后服务端拒绝重连） */
function enterObserver(token) {
  state.code = token;
  state.role = 'observer';
  state.lastEventId = 0;
  state.cards = [];
  state.groups = [];
  state.shares = [];
  state.anchors = [];
  state.polls = [];
  state.vehicles = null;
  state.claims = [];
  state.round = 0;
  state.groupId = null;
  resetLatencySession();
  location.hash = `#/observe/${token}`;
  showRoom();
  renderAll();
  connect();
}

function leaveRoom() {
  if (state.ws) { state.ws.onclose = null; state.ws.close(); }
  state.code = null;
  // 离开房间：取消挂起的防抖刷新并使所有在途报告请求失效，
  // 旧响应返回后既不写面板也不触发渲染
  resetLatencySession();
  location.hash = '';
  showLanding();
}

$('#btn-create').onclick = async () => {
  try {
    const name = $('#create-name').value.trim();
    const room = await api('POST', '/api/rooms', { name });
    enterRoom(room.code, 'host');
  } catch (err) { showError(err); }
};

$('#btn-join').onclick = async () => {
  const code = $('#join-code').value.trim().toUpperCase();
  if (!code) return;
  try {
    await api('GET', `/api/join/${code}`);
    enterRoom(code, 'player');
  } catch (err) { showError(new Error('加入码无效，请向主持人确认组加入码')); }
};

$('#room-code').onclick = () => {
  navigator.clipboard?.writeText(state.code);
  $('#room-code').textContent = '已复制';
  setTimeout(() => { $('#room-code').textContent = state.code; }, 1200);
};

$('#btn-start').onclick  = () => api('POST', `/api/rooms/${state.code}/start`).catch(showError);
$('#btn-pause').onclick  = () => api('POST', `/api/rooms/${state.code}/pause`).catch(showError);
$('#btn-resume').onclick = () => api('POST', `/api/rooms/${state.code}/resume`).catch(showError);
$('#btn-reset').onclick  = () => api('POST', `/api/rooms/${state.code}/reset`).catch(showError);

/* 响应时效面板：切换演练轮次 / 手动刷新（仅主持人；数据为拉取式分析） */
$('#latency-round').onchange = () => {
  const v = $('#latency-round').value;
  state.latency.round = v === '' ? null : parseInt(v, 10);
  refreshLatency();
};
$('#latency-refresh').onclick = () => refreshLatency();

/* 导出复盘 ZIP：按点击瞬间的事件水位打包（服务端在锁内定水位，生成期间新事件不混入）。
   直接走浏览器下载，无需解析 JSON；导出进行中禁用按钮并提示。 */
$('#btn-export').onclick = async () => {
  const btn = $('#btn-export');
  const hint = $('#export-hint');
  btn.disabled = true;
  hint.textContent = '正在按当前事件水位打包…';
  try {
    const res = await fetch(`/api/rooms/${state.code}/review.zip`, { cache: 'no-store' });
    if (!res.ok) {
      const data = await res.json().catch(() => ({}));
      throw new Error(typeof data.detail === 'string' ? data.detail : `导出失败 (${res.status})`);
    }
    const blob = await res.blob();
    const wantName = /filename\*=UTF-8''([^;]+)/i.exec(
      res.headers.get('content-disposition') || '');
    const name = wantName ? decodeURIComponent(wantName[1])
      : `review-${Date.now()}.zip`;
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = name;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
    hint.textContent = '已导出（含离线回放页 replay.html，可直接打开）';
    setTimeout(() => { hint.textContent = ''; }, 4000);
  } catch (err) {
    hint.textContent = '';
    showError(err);
  } finally {
    btn.disabled = false;
  }
};

$('#group-form').onsubmit = async (e) => {
  e.preventDefault();
  const name = $('#group-name').value.trim();
  if (!name) return;
  try {
    await api('POST', `/api/rooms/${state.code}/groups`, { name });
    $('#group-name').value = '';
    $('#group-name').focus();
  } catch (err) { showError(err); }
};

/* 记录复盘锚点：任意状态可提交，服务端在状态锁内绑定当前轮次/虚拟毫秒/事件水位。
   成功后由 anchor_created 事件（本连接也会收到广播）统一刷新，这里只清空表单。 */
$('#anchor-form').onsubmit = async (e) => {
  e.preventDefault();
  const title = $('#anchor-title').value.trim();
  if (!title) return;
  const note = $('#anchor-note').value;
  const btn = $('#anchor-form').querySelector('button[type=submit]');
  btn.disabled = true;
  try {
    await api('POST', `/api/rooms/${state.code}/anchors`, { title, note });
    $('#anchor-title').value = '';
    $('#anchor-note').value = '';
    $('#anchor-title').focus();
  } catch (err) { showError(err); } finally {
    btn.disabled = false;
  }
};

$('#card-form').onsubmit = async (e) => {
  e.preventDefault();
  const gateId = $('#card-gate').value;
  const gateTimeout = $('#card-gate-timeout').value.trim();
  if (gateTimeout && !gateId) {
    showError(new Error('设置超时秒数需先选择前置卡片'));
    return;
  }
  const body = {
    title: $('#card-title').value.trim(),
    content: $('#card-content').value.trim(),
    trigger_seconds: parseInt($('#card-trigger').value, 10) || 0,
    group_id: $('#card-target').value ? parseInt($('#card-target').value, 10) : null,
    depends_on_card_id: gateId ? parseInt(gateId, 10) : null,
    gate_timeout_seconds: gateTimeout ? parseInt(gateTimeout, 10) : null,
  };
  try {
    await api('POST', `/api/rooms/${state.code}/cards`, body);
    $('#card-title').value = '';
    $('#card-content').value = '';
    $('#card-trigger').value = '';
    $('#card-gate').value = '';
    $('#card-gate-timeout').value = '';
    $('#card-title').focus();
  } catch (err) { showError(err); }
};

$('#share-form').onsubmit = async (e) => {
  e.preventDefault();
  const ids = [...document.querySelectorAll('#share-groups input:checked')]
    .map((el) => parseInt(el.value, 10));
  const minutes = parseInt($('#share-minutes').value, 10);
  if (ids.length < 1 || ids.length > 3) {
    showError(new Error('请选择 1～3 个参与组'));
    return;
  }
  if (!Number.isInteger(minutes) || minutes < 5 || minutes > 120) {
    showError(new Error('有效期须为 5～120 分钟'));
    return;
  }
  try {
    const share = await api('POST', `/api/rooms/${state.code}/shares`,
      { group_ids: ids, minutes });
    upsertShare(share); // share_created 事件到达时按 token 去重
    renderShares();
    document.querySelectorAll('#share-groups input:checked').forEach((el) => { el.checked = false; });
    $('#share-minutes').value = 30;
  } catch (err) { showError(err); }
};

/* 选项条数 2～4：动态增减投票选项输入框 */
$('#poll-add-option').onclick = () => {
  const box = $('#poll-options');
  if (box.children.length >= 4) {
    showError(new Error('最多 4 个选项'));
    return;
  }
  const input = document.createElement('input');
  input.className = 'poll-option';
  input.type = 'text';
  input.placeholder = `选项 ${box.children.length + 1}`;
  input.required = true;
  input.maxLength = 200;
  box.appendChild(input);
  input.focus();
};

$('#poll-form').onsubmit = async (e) => {
  e.preventDefault();
  const question = $('#poll-question').value.trim();
  const options = [...document.querySelectorAll('#poll-options .poll-option')]
    .map((el) => el.value.trim())
    .filter((s) => s.length > 0);
  const groupIds = [...document.querySelectorAll('#poll-groups input:checked')]
    .map((el) => parseInt(el.value, 10));
  const deadline = parseInt($('#poll-deadline').value, 10);
  if (!question) return;
  if (options.length < 2 || options.length > 4) {
    showError(new Error('请填写 2～4 个非空选项'));
    return;
  }
  if (new Set(options).size !== options.length) {
    showError(new Error('选项文本不能重复'));
    return;
  }
  if (groupIds.length < 1) {
    showError(new Error('请至少勾选一个参与组'));
    return;
  }
  if (!Number.isInteger(deadline) || deadline < 1 || deadline > 86400) {
    showError(new Error('截止虚拟秒须为 1～86400'));
    return;
  }
  const btn = $('#poll-form').querySelector('button[type=submit]');
  btn.disabled = true;
  try {
    await api('POST', `/api/rooms/${state.code}/polls`, {
      question, options, group_ids: groupIds, deadline_seconds: deadline,
    });
    $('#poll-question').value = '';
    document.querySelectorAll('#poll-options .poll-option').forEach((el, i) => {
      if (i < 2) el.value = '';
      else el.remove();
    });
    document.querySelectorAll('#poll-groups input:checked').forEach((el) => { el.checked = false; });
    $('#poll-deadline').value = '';
    $('#poll-question').focus();
  } catch (err) { showError(err); } finally {
    btn.disabled = false;
  }
};

/* 主持人创建车辆池（每房间一个，1～20 辆，创建后固定；重置演练保留）。
   成功后由 vehicle_pool_created 事件统一刷新，这里也用返回值即时更新。 */
$('#vehicle-pool-form').onsubmit = async (e) => {
  e.preventDefault();
  const total = parseInt($('#vehicle-pool-total').value, 10);
  if (!Number.isInteger(total) || total < 1 || total > 20) {
    showError(new Error('车辆池总量须为 1～20 辆'));
    return;
  }
  const btn = e.target.querySelector('button[type=submit]');
  btn.disabled = true;
  try {
    const res = await api('POST', `/api/rooms/${state.code}/vehicle-pool`, { total });
    state.vehicles = res;
    $('#vehicle-pool-total').value = '';
    renderVehicles();
  } catch (err) { showError(err); } finally {
    btn.disabled = false;
  }
};

/* 参与组提交车辆申请（1～5 辆且不超过池总量）；每组一份，取消后才能重新申请。
   成功由返回值与 vehicle_request_submitted 事件共同保证视图收敛。 */
$('#vehicle-request-form').onsubmit = async (e) => {
  e.preventDefault();
  const quantity = parseInt($('#vehicle-quantity').value, 10);
  if (!Number.isInteger(quantity) || quantity < 1 || quantity > 5) {
    showError(new Error('申请数量须为 1～5 辆'));
    return;
  }
  const btn = e.target.querySelector('button[type=submit]');
  btn.disabled = true;
  try {
    const res = await api('POST', `/api/join/${state.code}/vehicle-requests`, { quantity });
    state.vehicles = { my_request: res };
    $('#vehicle-quantity').value = '';
    renderVehicles();
  } catch (err) { showError(err); } finally {
    btn.disabled = false;
  }
};

/* 主持人创建待核验命题（至少勾选两个参与组）。
   成功后由 intel_claim_created 事件（本连接也会收到广播）统一刷新，这里只清空表单。 */
$('#intel-form').onsubmit = async (e) => {
  e.preventDefault();
  const statement = $('#intel-statement').value.trim();
  const groupIds = [...document.querySelectorAll('#intel-groups input:checked')]
    .map((el) => parseInt(el.value, 10));
  if (!statement) return;
  if (groupIds.length < 2) {
    showError(new Error('请至少勾选两个参与组'));
    return;
  }
  const btn = e.target.querySelector('button[type=submit]');
  btn.disabled = true;
  try {
    await api('POST', `/api/rooms/${state.code}/claims`, {
      statement, group_ids: groupIds,
    });
    $('#intel-statement').value = '';
    document.querySelectorAll('#intel-groups input:checked').forEach((el) => { el.checked = false; });
    $('#intel-statement').focus();
  } catch (err) { showError(err); } finally {
    btn.disabled = false;
  }
};

/* 刷新恢复：根据 hash 与本地角色记录重进房间（走完整的快照+增量协议）；
   观察链接同样从 hash 恢复——凭证已失效时服务端会拒绝并提示 */
(function boot() {
  const m = location.hash.match(/^#\/(host|player)\/([A-Z0-9]{6})$/);
  if (m) { enterRoom(m[2], m[1]); return; }
  const mo = location.hash.match(/^#\/observe\/([A-Za-z0-9_-]+)$/);
  if (mo) { enterObserver(mo[1]); return; }
  showLanding();
})();
