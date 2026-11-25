window.DashboardUI = (() => {
  const escape = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const labels = {
    INSERT:'新增', UPDATE:'修改', DELETE:'删除',
    PENDING:'等待处理', IN_FLIGHT:'正在投递', SUCCEEDED:'投递成功', RETRY_WAIT:'等待重试', FAILED:'处理失败',
    COMPLETED:'分发已完成', PROCESSING:'正在分发', INITIAL:'首次通知', REPLAY:'历史补发', HANDLED:'已处理'
  };
  const status = value => labels[value] || value;
  const badge = value => `<span class="status-tag ${['SUCCEEDED','COMPLETED','HANDLED'].includes(value) ? 'good' : value === 'FAILED' ? 'bad' : 'waiting'}">${escape(status(value))}</span>`;
  const date = value => value == null ? '暂无记录' : new Date(typeof value === 'number' ? value * 1000 : value).toLocaleString('zh-CN');
  const facts = entries => `<dl class="result-meta">${entries.map(([label, value]) => `<div><dt>${escape(label)}</dt><dd>${escape(value ?? '—')}</dd></div>`).join('')}</dl>`;
  const stats = entries => `<div class="result-stats">${entries.map(([label, value]) => `<div><span>${escape(label)}</span><strong>${escape(value)}</strong></div>`).join('')}</div>`;
  const technical = (value, label = '技术详情与完整 JSON') => `<details class="technical"><summary>${escape(label)}</summary><pre>${escape(JSON.stringify(value, null, 2))}</pre></details>`;
  const value = item => item === null ? '<span class="muted">NULL</span>' : item === '' ? '<span class="muted">空字符串</span>' : escape(typeof item === 'object' ? JSON.stringify(item) : String(item));
  function reason(message) {
    if (!message) return '暂无说明';
    const translations = [
      ['Pre-check passed; review before execution', '预检查通过，可下载 SQL，核对后手动执行'],
      ['Primary key conflict: target row exists', '当前已有相同主键的记录，无法直接恢复插入'],
      ['Target row not found', '当前记录不存在，无法按本次变更恢复'],
      ['Row has been further modified: column ', '该记录后来又被修改，冲突字段：'],
      ['Foreign key column modification is not supported for recovery: ', '本次修改涉及外键关联列，暂不支持恢复：'],
      ['INSERT/DELETE recovery is not supported for tables with foreign keys', '外键表暂不支持新增或删除事件的恢复'],
      ['Schema snapshot invalidated by DDL: ', '表结构快照已因结构变更失效：'],
      ['Recovery baseline not established', '尚未建立恢复基线'],
      ['Eligible for a current-state pre-check; not proof of safe execution', '结构和基线已就绪，生成恢复计划时会核对当前数据'],
      ['Schema snapshot not found', '找不到对应的表结构快照']
    ];
    for (const [original, translated] of translations) {
      if (message.startsWith(original)) return translated + message.slice(original.length);
    }
    return message;
  }
  function requestError(statusCode, details) {
    const messages = {400:'请求内容不符合要求，请核对输入',401:'登录已失效，请重新登录',
      403:'当前访问无权限',404:'找不到对应记录或接口',409:'数据或规则版本已更新，请刷新后重试',
      429:'请求太频繁，请稍后再试',
      500:'服务处理失败，请展开错误详情查看原因',502:'下游服务返回错误',503:'服务暂时不可用'};
    const error = new Error(`${messages[statusCode] || '请求失败'}（HTTP ${statusCode}）`);
    error.status = statusCode;
    error.details = details;
    return error;
  }
  // 页面由网关托管：接口统一加 /api 前缀，管理接口带登录换来的令牌（30 分钟有效，存在本标签页的 sessionStorage）。
  const TOKEN_KEY = 'cachepulse.token';
  const API_PREFIX = '/api';
  let pendingLogin = null;
  // 页面同时发出的多个请求都收到 401 时只弹一次登录框
  function login() {
    if (!pendingLogin) pendingLogin = showLogin().finally(() => { pendingLogin = null; });
    return pendingLogin;
  }
  function showLogin() {
    return new Promise((resolve, reject) => {
      const dialog = document.createElement('dialog');
      dialog.innerHTML = '<form method="dialog" class="login-form"><h2>管理员登录</h2>'
        + '<label>账号<input name="username" autocomplete="username" required value="admin"></label>'
        + '<label>密码<input name="password" type="password" autocomplete="current-password" required></label>'
        + '<p class="login-error" role="alert"></p><div><button value="ok">登录</button> <button value="cancel" formnovalidate>取消</button></div></form>';
      document.body.append(dialog);
      const form = dialog.querySelector('form');
      form.addEventListener('submit', async event => {
        if (event.submitter && event.submitter.value === 'cancel') { dialog.remove(); reject(requestError(401, 'Login cancelled')); return; }
        event.preventDefault();
        const response = await window.fetch('/auth/token', {method:'POST', headers:{'Content-Type':'application/json'},
          body: JSON.stringify({username: form.username.value, password: form.password.value})});
        if (!response.ok) { dialog.querySelector('.login-error').textContent = '账号或密码不正确'; return; }
        const body = await response.json();
        sessionStorage.setItem(TOKEN_KEY, body.accessToken);
        dialog.remove();
        resolve(body.accessToken);
      });
      dialog.showModal();
    });
  }
  async function apiFetch(path, options = {}, retried = false) {
    const token = sessionStorage.getItem(TOKEN_KEY);
    const headers = {...(options.headers || {})};
    // 不用 Authorization：公网入口的 Nginx 会把它换成访问服务器端口的凭据
    if (token) headers['X-CachePulse-Token'] = token;
    const response = await window.fetch(API_PREFIX + path, {...options, headers});
    if (response.status === 401 && !retried) {
      sessionStorage.removeItem(TOKEN_KEY);
      await login();
      return apiFetch(path, options, true);
    }
    return response;
  }
  /** 带令牌下载文件（window.open 没法带请求头）。 */
  async function download(path, filename) {
    const response = await apiFetch(path);
    if (!response.ok) throw requestError(response.status, await response.text());
    const url = URL.createObjectURL(await response.blob());
    const link = Object.assign(document.createElement('a'), {href: url, download: filename});
    document.body.append(link); link.click(); link.remove();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  }
  async function run(button, action, notice) {
    if (button.getAttribute('aria-busy') === 'true') return;
    button.setAttribute('aria-busy', 'true');
    notice('正在处理，请稍候…', false);
    try { await action(); }
    catch (error) { notice(error.message, true, error.details); }
    finally { button.removeAttribute('aria-busy'); }
  }
  async function copy(inputId, feedbackId) {
    const feedback = document.getElementById(feedbackId);
    try {
      await navigator.clipboard.writeText(document.getElementById(inputId).value);
      feedback.textContent = '已复制。';
    } catch (error) {
      feedback.textContent = '复制失败：' + error.message + '。可以选中内容手动复制。';
    }
  }
  return {escape, status, badge, date, facts, stats, technical, value, reason, requestError, run, copy, fetch: apiFetch, download, login};
})();
