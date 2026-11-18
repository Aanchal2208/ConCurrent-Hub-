
'use strict';

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------
const API_ORIGIN = location.port === '8000'
  ? location.origin
  : `${location.protocol === 'https:' ? 'https:' : 'http:'}//${location.hostname || 'localhost'}:8000`;
const API_BASE = API_ORIGIN === location.origin ? '' : API_ORIGIN;

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------
let authToken = localStorage.getItem('ch_token') || null;
let currentUser = null;
let ws = null;
let wsRetryTimer = null;
let tasks = [];

// ---------------------------------------------------------------------------
// Utility helpers
// ---------------------------------------------------------------------------
function qs(sel) { return document.querySelector(sel); }
function qsa(sel) { return document.querySelectorAll(sel); }

function show(el)   { el?.classList.remove('hidden'); }
function hide(el)   { el?.classList.add('hidden'); }
function toggle(el) { el?.classList.toggle('hidden'); }

function setError(el, msg) {
  if (!el) return;
  el.textContent = msg;
  el.classList.toggle('hidden', !msg);
}

function setSuccess(el, msg) {
  if (!el) return;
  el.textContent = msg;
  el.classList.toggle('hidden', !msg);
}

function apiErrorMessage(body, status, fallback) {
  let detail = body?.detail ?? body?.message ?? body?.error;
  if (Array.isArray(detail)) {
    detail = detail.map(item => {
      if (typeof item === 'string') return item;
      return item?.msg || JSON.stringify(item);
    }).join('; ');
  }
  if (typeof detail === 'string' && detail.trim()) return detail;
  if (status === 405) {
    return 'The registration API is not available at this address. Start ConcurrentHub with .\\start.ps1 and open http://localhost:8000.';
  }
  return status
    ? `${fallback} (HTTP ${status}).`
    : fallback;
}

async function apiFetch(path, options = {}) {
  const headers = { 'Content-Type': 'application/json', ...(options.headers || {}) };
  if (authToken) headers['Authorization'] = `Bearer ${authToken}`;
  let res;
  try {
    res = await fetch(`${API_BASE}${path}`, { ...options, headers });
  } catch {
    return {
      ok: false,
      status: 0,
      body: {
        detail: 'Cannot reach the ConcurrentHub API. Start the backend and open the app at http://localhost:8000.',
      },
    };
  }

  const responseText = await res.text();
  let body = {};
  if (responseText) {
    try {
      const parsed = JSON.parse(responseText);
      body = parsed && typeof parsed === 'object' ? parsed : {};
    } catch {
      body = {
        detail: `The backend returned a non-JSON response (HTTP ${res.status}). Open this app through the ConcurrentHub backend, not a static file server.`,
      };
    }
  }
  return { ok: res.ok, status: res.status, body };
}

// ---------------------------------------------------------------------------
// Auth-page tab switching
// ---------------------------------------------------------------------------
qsa('.tab-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    qsa('.tab-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    const tab = btn.dataset.tab;
    hide(qs('#login-form'));
    hide(qs('#register-form'));
    show(qs(`#${tab}-form`));
    setError(qs('#login-error'), '');
    setError(qs('#register-error'), '');
    setSuccess(qs('#register-success'), '');
  });
});

// ---------------------------------------------------------------------------
// Login
// ---------------------------------------------------------------------------
qs('#login-form').addEventListener('submit', async e => {
  e.preventDefault();
  setError(qs('#login-error'), '');
  const username = qs('#login-username').value.trim();
  const password = qs('#login-password').value;

  const { ok, body } = await apiFetch('/auth/login', {
    method: 'POST',
    body: JSON.stringify({ username, password }),
  });

  if (!ok) {
    setError(qs('#login-error'), body.detail || 'Login failed. Check your credentials.');
    return;
  }

  authToken = body.access_token;
  currentUser = body.user;
  localStorage.setItem('ch_token', authToken);
  enterApp();
});

// ---------------------------------------------------------------------------
// Register
// ---------------------------------------------------------------------------
qs('#register-form').addEventListener('submit', async e => {
  e.preventDefault();
  setError(qs('#register-error'), '');
  setSuccess(qs('#register-success'), '');

  const payload = {
    username: qs('#reg-username').value.trim(),
    email:    qs('#reg-email').value.trim(),
    full_name: qs('#reg-fullname').value.trim(),
    password: qs('#reg-password').value,
    role:     qs('#reg-role').value,
  };

  const { ok, status, body } = await apiFetch('/auth/register', {
    method: 'POST',
    body: JSON.stringify(payload),
  });

  if (!ok) {
    setError(qs('#register-error'), apiErrorMessage(body, status, 'Registration failed.'));
    return;
  }

  setSuccess(qs('#register-success'), 'Account created! You can now sign in.');
  qs('#reg-password').value = '';
  // Switch to login tab
  qs('[data-tab="login"]').click();
  qs('#login-username').value = payload.username;
});

// ---------------------------------------------------------------------------
// Enter app / logout
// ---------------------------------------------------------------------------
function enterApp() {
  hide(qs('#auth-page'));
  show(qs('#app-page'));
  qs('#topbar-username').textContent = currentUser?.username || '—';
  loadTasks();
  connectWebSocket();
  refreshMetrics();
}

qs('#logout-btn').addEventListener('click', () => {
  authToken = null;
  currentUser = null;
  localStorage.removeItem('ch_token');
  tasks = [];
  clearActivity();
  disconnectWebSocket();
  hide(qs('#app-page'));
  show(qs('#auth-page'));
  qs('#login-username').value = '';
  qs('#login-password').value = '';
});

// ---------------------------------------------------------------------------
// Auto-login if token is stored
// ---------------------------------------------------------------------------
async function tryAutoLogin() {
  if (!authToken) return;
  const { ok, body } = await apiFetch('/auth/me');
  if (ok && body.authenticated) {
    currentUser = body.user || { username: body.username };
    enterApp();
  } else {
    localStorage.removeItem('ch_token');
    authToken = null;
  }
}

// ---------------------------------------------------------------------------
// Task loading & rendering
// ---------------------------------------------------------------------------
async function loadTasks() {
  const status   = qs('#filter-status').value;
  const priority = qs('#filter-priority').value;
  const search   = qs('#search-input').value.trim();

  const params = new URLSearchParams();
  if (status)   params.set('status', status);
  if (priority) params.set('priority', priority);
  if (search)   params.set('search', search);

  const { ok, body } = await apiFetch(`/tasks?${params}`);
  if (!ok) {
    renderTasks([]);
    pushActivity({ event: 'error', message: body.detail || 'Failed to load tasks' });
    return;
  }
  tasks = body;
  renderTasks(tasks);
}

function renderTasks(list) {
  const container = qs('#tasks-list');
  if (!list.length) {
    container.innerHTML = '<div class="empty-state">No tasks found. Create one to get started.</div>';
    return;
  }
  container.innerHTML = '';
  list.forEach(t => container.appendChild(buildTaskCard(t)));
}

function buildTaskCard(task) {
  const card = document.createElement('div');
  card.className = 'task-card';
  card.dataset.taskId = task.id;

  const statusClass    = `badge-${task.status.toLowerCase()}`;
  const priorityClass  = `badge-${task.priority.toLowerCase()}`;
  const statusLabel    = task.status.replace('_', ' ');

  card.innerHTML = `
    <div class="task-card-top">
      <span class="task-card-title">${escHtml(task.title)}</span>
      <span class="task-id">#${task.id}</span>
    </div>
    <div class="task-card-meta">
      <span class="badge ${statusClass}">${escHtml(statusLabel)}</span>
      <span class="badge ${priorityClass}">${escHtml(task.priority)}</span>
      <span class="assignee-chip">@${escHtml(task.assignee)}</span>
      <span class="version-chip">v${task.version}</span>
    </div>
  `;

  card.addEventListener('click', () => openEditModal(task));
  return card;
}

function escHtml(str) {
  return String(str ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

// Flash card on update / conflict
function flashCard(taskId, type) {
  const card = qs(`.task-card[data-task-id="${taskId}"]`);
  if (!card) return;
  card.classList.add(`flash-${type}`);
  setTimeout(() => card.classList.remove(`flash-${type}`), 1500);
}

// ---------------------------------------------------------------------------
// Filters & search
// ---------------------------------------------------------------------------
['filter-status', 'filter-priority'].forEach(id => {
  qs(`#${id}`).addEventListener('change', loadTasks);
});

let searchDebounce;
qs('#search-input').addEventListener('input', () => {
  clearTimeout(searchDebounce);
  searchDebounce = setTimeout(loadTasks, 350);
});

qs('#refresh-btn').addEventListener('click', loadTasks);

// ---------------------------------------------------------------------------
// Create Task Modal
// ---------------------------------------------------------------------------
qs('#open-create-modal').addEventListener('click', () => {
  qs('#create-task-form').reset();
  setError(qs('#create-error'), '');
  show(qs('#create-modal'));
});

[qs('#close-create-modal'), qs('#cancel-create-modal')].forEach(btn => {
  btn?.addEventListener('click', () => hide(qs('#create-modal')));
});

qs('#create-modal').addEventListener('click', e => {
  if (e.target === qs('#create-modal')) hide(qs('#create-modal'));
});

qs('#create-task-form').addEventListener('submit', async e => {
  e.preventDefault();
  setError(qs('#create-error'), '');

  const payload = {
    title:       qs('#ct-title').value.trim(),
    description: qs('#ct-description').value.trim(),
    status:      qs('#ct-status').value,
    priority:    qs('#ct-priority').value,
    assignee:    qs('#ct-assignee').value.trim() || 'unassigned',
  };

  const { ok, body } = await apiFetch('/tasks', {
    method: 'POST',
    body: JSON.stringify(payload),
  });

  if (!ok) {
    setError(qs('#create-error'), body.detail || body.message || 'Failed to create task.');
    return;
  }

  hide(qs('#create-modal'));
  await loadTasks();
  pushActivity({ event: 'task_created', task_id: body.id, message: `Task #${body.id} created` });
});

// ---------------------------------------------------------------------------
// Edit Task Modal
// ---------------------------------------------------------------------------
function openEditModal(task) {
  qs('#et-task-id').value         = task.id;
  qs('#et-expected-version').value = task.version;
  qs('#et-title').value            = task.title;
  qs('#et-description').value      = task.description || '';
  qs('#et-status').value           = task.status;
  qs('#et-priority').value         = task.priority;
  qs('#et-assignee').value         = task.assignee || '';
  qs('#edit-task-id-label').textContent = `#${task.id}`;
  qs('#edit-version-info').textContent  = `Current version: ${task.version}`;
  setError(qs('#edit-error'), '');
  setSuccess(qs('#edit-success'), '');
  show(qs('#edit-modal'));
}

[qs('#close-edit-modal'), qs('#cancel-edit-modal')].forEach(btn => {
  btn?.addEventListener('click', () => hide(qs('#edit-modal')));
});

qs('#edit-modal').addEventListener('click', e => {
  if (e.target === qs('#edit-modal')) hide(qs('#edit-modal'));
});

qs('#edit-task-form').addEventListener('submit', async e => {
  e.preventDefault();
  setError(qs('#edit-error'), '');
  setSuccess(qs('#edit-success'), '');

  const taskId  = parseInt(qs('#et-task-id').value);
  const version = parseInt(qs('#et-expected-version').value);

  const payload = {
    expected_version: version,
    title:       qs('#et-title').value.trim(),
    description: qs('#et-description').value.trim(),
    status:      qs('#et-status').value,
    priority:    qs('#et-priority').value,
    assignee:    qs('#et-assignee').value.trim(),
  };

  const { ok, status, body } = await apiFetch(`/tasks/${taskId}`, {
    method: 'PUT',
    body: JSON.stringify(payload),
  });

  if (status === 409) {
    // Version conflict
    const currentVer = body.current_version;
    setError(qs('#edit-error'),
      `Version conflict. Another user updated this task (now at version ${currentVer}). ` +
      'Close the editor, refresh the task list, and try again.'
    );
    flashCard(taskId, 'conflict');
    pushActivity({
      event: 'version_conflict',
      task_id: taskId,
      message: `Conflict on task #${taskId}: your version ${version} vs server v${currentVer}`,
    });
    return;
  }

  if (!ok) {
    setError(qs('#edit-error'), body.detail || body.message || 'Update failed.');
    return;
  }

  setSuccess(qs('#edit-success'), `Saved (version → ${body.new_version})`);
  // Update version in form for subsequent saves without closing
  qs('#et-expected-version').value = body.new_version;
  qs('#edit-version-info').textContent = `Current version: ${body.new_version}`;
  flashCard(taskId, 'update');
  await loadTasks();
});

// Delete task
qs('#delete-task-btn').addEventListener('click', async () => {
  const taskId = parseInt(qs('#et-task-id').value);
  const ver    = parseInt(qs('#et-expected-version').value);
  if (!confirm(`Delete task #${taskId}? This cannot be undone.`)) return;

  const { ok, body } = await apiFetch(`/tasks/${taskId}?expected_version=${ver}`, {
    method: 'DELETE',
  });

  if (!ok) {
    setError(qs('#edit-error'), body.detail || body.message || 'Delete failed.');
    return;
  }

  hide(qs('#edit-modal'));
  await loadTasks();
  pushActivity({ event: 'task_deleted', task_id: taskId, message: `Task #${taskId} deleted` });
});

// ---------------------------------------------------------------------------
// Activity feed
// ---------------------------------------------------------------------------
const MAX_ACTIVITY = 60;

function pushActivity(ev) {
  const feed = qs('#activity-feed');
  if (!feed) return;

  const li = document.createElement('li');
  li.className = `activity-item ev-${(ev.event || 'unknown').replace(/[^a-z_]/g, '')}`;

  const ts = new Date().toLocaleTimeString();
  const label = formatEventLabel(ev.event);
  const detail = ev.message || buildEventDetail(ev);

  li.innerHTML = `
    <span class="activity-event">${escHtml(label)}</span>
    <span class="activity-detail">${escHtml(detail)}</span>
    <time class="activity-time">${ts}</time>
  `;

  feed.insertBefore(li, feed.firstChild);

  // Trim overflow
  while (feed.children.length > MAX_ACTIVITY) {
    feed.removeChild(feed.lastChild);
  }
}

function formatEventLabel(event) {
  const map = {
    task_created:    'Task created',
    task_updated:    'Task updated',
    task_deleted:    'Task deleted',
    version_conflict:'Version conflict',
    audit_event:     'Audit event',
    pong:            'Connection check',
    error:           'Connection error',
  };
  return map[event] || event;
}

function buildEventDetail(ev) {
  if (ev.task_id != null) return `Task #${ev.task_id}`;
  return '';
}

qs('#clear-activity-btn').addEventListener('click', clearActivity);

function clearActivity() {
  const feed = qs('#activity-feed');
  if (feed) feed.innerHTML = '';
}

// ---------------------------------------------------------------------------
// WebSocket
// ---------------------------------------------------------------------------
function connectWebSocket() {
  if (ws && ws.readyState < 2) return;  // already open/connecting

  const apiUrl = new URL(API_BASE || location.origin);
  const wsProto = apiUrl.protocol === 'https:' ? 'wss' : 'ws';
  const wsUrl = `${wsProto}://${apiUrl.host}/ws`;

  try {
    ws = new WebSocket(wsUrl);
  } catch {
    setWsDot('disconnected');
    return;
  }

  ws.onopen = () => {
    setWsDot('connected');
    pushActivity({ event: 'ws_connected', message: 'Real-time connection established' });
  };

  ws.onmessage = (e) => {
    let ev;
    try { ev = JSON.parse(e.data); } catch { return; }
    handleWsEvent(ev);
  };

  ws.onclose = () => {
    setWsDot('disconnected');
    // Reconnect after 4 seconds if user is still logged in
    if (authToken) {
      wsRetryTimer = setTimeout(connectWebSocket, 4000);
    }
  };

  ws.onerror = () => setWsDot('disconnected');
}

function disconnectWebSocket() {
  clearTimeout(wsRetryTimer);
  if (ws) { ws.close(); ws = null; }
  setWsDot('disconnected');
}

function setWsDot(state) {
  const dot = qs('#ws-dot');
  if (!dot) return;
  dot.className = `ws-dot ${state}`;
  dot.title = state === 'connected'
    ? 'WebSocket: connected'
    : 'WebSocket: disconnected';
}

function handleWsEvent(ev) {
  pushActivity(ev);
  const reloadEvents = ['task_created', 'task_updated', 'task_deleted'];
  if (reloadEvents.includes(ev.event)) {
    loadTasks();
  }
  if (ev.event === 'task_updated' && ev.task_id) {
    flashCard(ev.task_id, 'update');
  }
  if (ev.event === 'version_conflict' && ev.task_id) {
    flashCard(ev.task_id, 'conflict');
  }
}

// ---------------------------------------------------------------------------
// System metrics
// ---------------------------------------------------------------------------
async function refreshMetrics() {
  const { ok, body } = await apiFetch('/health');
  if (!ok) return;

  qs('#m-storage').textContent   = body.persisted_in_postgresql ? 'PostgreSQL' : body.storage_backend;
  qs('#m-ws').textContent        = body.active_websocket_clients ?? '—';
  qs('#m-pool-cap').textContent  = body.object_pool?.capacity ?? '—';
  qs('#m-pool-use').textContent  = body.object_pool?.in_use ?? '—';
  qs('#m-pool-reuse').textContent= body.object_pool?.reuse_ratio_pct != null
    ? `${body.object_pool.reuse_ratio_pct.toFixed(1)}%`
    : '—';

  const queues = body.scheduler?.active_queues ?? body.scheduler?.active_record_queues ?? '—';
  qs('#m-sched-q').textContent   = queues;

  // Update storage badge in topbar
  const badge = qs('#storage-badge');
  if (badge) {
    badge.textContent = body.persisted_in_postgresql ? 'PostgreSQL' : 'In-Memory';
    badge.style.background = body.persisted_in_postgresql ? '' : 'var(--coral-pale)';
    badge.style.color      = body.persisted_in_postgresql ? '' : 'var(--coral)';
  }
}

qs('#refresh-metrics-btn')?.addEventListener('click', refreshMetrics);

// About dialog
const aboutDialog = qs('#about-dialog');
let aboutOpener = null;

function openAboutDialog() {
  aboutOpener = document.activeElement;
  show(aboutDialog);
  qs('#close-about-dialog').focus();
}

function closeAboutDialog() {
  hide(aboutDialog);
  if (aboutOpener instanceof HTMLElement) aboutOpener.focus();
}

qs('#open-about-dialog').addEventListener('click', openAboutDialog);
qs('#close-about-dialog').addEventListener('click', closeAboutDialog);
qs('#about-done').addEventListener('click', closeAboutDialog);
aboutDialog.addEventListener('click', e => {
  if (e.target === aboutDialog) closeAboutDialog();
});

// Auto-refresh metrics every 30 seconds while logged in
setInterval(() => { if (authToken) refreshMetrics(); }, 30_000);

// ---------------------------------------------------------------------------
// Keyboard: Escape closes modals
// ---------------------------------------------------------------------------
document.addEventListener('keydown', e => {
  if (e.key !== 'Escape') return;
  hide(qs('#create-modal'));
  hide(qs('#edit-modal'));
  if (!aboutDialog.classList.contains('hidden')) closeAboutDialog();
});

aboutDialog.addEventListener('keydown', e => {
  if (e.key !== 'Tab') return;
  const focusable = aboutDialog.querySelectorAll('button:not([disabled]), [href], input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])');
  if (!focusable.length) return;
  const first = focusable[0];
  const last = focusable[focusable.length - 1];
  if (e.shiftKey && document.activeElement === first) {
    e.preventDefault();
    last.focus();
  } else if (!e.shiftKey && document.activeElement === last) {
    e.preventDefault();
    first.focus();
  }
});

// ---------------------------------------------------------------------------
// Bootstrap
// ---------------------------------------------------------------------------
tryAutoLogin();
