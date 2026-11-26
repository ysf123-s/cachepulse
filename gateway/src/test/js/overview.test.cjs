// 首页微服务总览：服务卡片、架构图上的状态点、消费组卡片、顶部统计，以及服务名等文字的转义。
const fs = require('fs');
const vm = require('vm');
const assert = require('node:assert/strict');

const html = fs.readFileSync('src/main/resources/static/index.html', 'utf8');
const source = html.match(/<script>([\s\S]*?)<\/script>/)[1];
const elements = new Map();
const element = id => {
  if (!elements.has(id)) {
    elements.set(id, {id, innerHTML: '', textContent: '', className: '', children: {},
      querySelector(selector) { return this.children[selector] || (this.children[selector] = {textContent: ''}); }});
  }
  return elements.get(id);
};
const nodes = ['gateway', 'archive-service', 'delivery-service'].map(service => ({
  dataset: {service}, classes: new Set(),
  classList: {add(c) { this.owner.classes.add(c); }, remove(...cs) { cs.forEach(c => this.owner.classes.delete(c)); }}
}));
nodes.forEach(node => { node.classList.owner = node; });
const document = {
  getElementById: element,
  querySelectorAll(selector) { return selector === '[data-service]' ? nodes : []; },
  querySelector(selector) {
    const [id, rest] = selector.split(' ');
    return element(id.slice(1)).querySelector(rest);
  }
};

const now = new Date().toISOString();
const overview = {generatedAt: now, services: [
  {serviceId: 'gateway', instances: [{port: 18090, status: 'UP'}], circuitBreaker: null, registryError: null},
  {serviceId: 'archive-service', instances: [{port: 18092, status: 'UP'}, {port: 28092, status: 'DOWN'}],
    circuitBreaker: {state: 'OPEN', failureRate: 62.5, slowCallRate: 0, bufferedCalls: 20}, registryError: null},
  {serviceId: 'delivery-service', instances: [], circuitBreaker: {state: 'CLOSED', failureRate: -1, slowCallRate: -1, bufferedCalls: 0},
    registryError: null},
  {serviceId: '<img src=x onerror=alert(1)>', instances: [{port: 1, status: 'UP'}], circuitBreaker: null, registryError: null}
]};
const responses = {
  '/api/system/overview': overview,
  '/api/sync/status': {serviceStarted: true, activeConsumers: 6, configuredConsumers: 6, lastSuccessAt: now, lastError: null},
  '/api/cache/freshness': {state: 'FRESH'},
  '/api/platform/consumers/status': {archive: {activeConsumer: true, lastSuccessAt: now, acknowledgedNewRows: 10,
    acknowledgedRejectedEvents: 2, lastError: {message: '<b>boom</b>', occurredAt: now}}},
  '/api/platform/deliveries/consumer-status': null
};
const context = vm.createContext({
  window: {fetch: async url => responses[url] === null
    ? {ok: false, status: 503, json: async () => ({})}
    : {ok: true, status: 200, json: async () => responses[url]}},
  document, sessionStorage: {getItem() { return null; }, setItem() {}, removeItem() {}},
  console, setTimeout, setInterval() {}, Date, Math, Promise
});
vm.runInContext(fs.readFileSync('src/main/resources/static/dashboard-ui.js', 'utf8'), context);

(async () => {
  vm.runInContext(source, context);
  await new Promise(resolve => setTimeout(resolve, 20));

  const grid = element('serviceGrid').innerHTML;
  assert.ok(grid.includes('网关') && grid.includes('运行正常'));
  assert.ok(grid.includes('1 / 2 个实例正常') && grid.includes('已熔断') && grid.includes('失败率 63%'));
  assert.ok(grid.includes('未注册') && grid.includes('调用太少，未统计失败率'));
  assert.ok(!grid.includes('<img src=x'), 'unknown service names are escaped');
  assert.deepEqual([...nodes[0].classes], ['good']);
  assert.deepEqual([...nodes[1].classes], ['warn']);
  assert.deepEqual([...nodes[2].classes], ['bad']);

  const consumers = element('consumerGrid').innerHTML;
  assert.ok(consumers.includes('cachepulse-cache-sync') && consumers.includes('6 / 6') && consumers.includes('正常'));
  assert.ok(consumers.includes('跳过的坏事件') && consumers.includes('<dd>2</dd>'));
  assert.ok(consumers.includes('&lt;b&gt;boom&lt;/b&gt;') && !consumers.includes('<b>boom</b>'));
  assert.ok(consumers.includes('读不到') && consumers.includes('HTTP 503'));

  assert.equal(element('statServices').querySelector('strong').textContent, '2 / 4');
  assert.equal(element('statInstances').querySelector('strong').textContent, '3 / 4');
  assert.equal(element('statFreshness').querySelector('strong').textContent, '正常');
  assert.equal(element('statConsumers').querySelector('strong').textContent, '2 / 3');
  assert.equal(element('notice').className, 'error');
  console.log('PASS: service health and breakers, architecture status dots, consumer groups, summary stats and escaping');
})().catch(error => { console.error(error); process.exit(1); });
