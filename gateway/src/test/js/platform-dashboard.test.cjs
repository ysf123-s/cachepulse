const fs = require('fs');
const vm = require('vm');
const assert = require('node:assert/strict');
const source = fs.readFileSync('src/main/resources/static/platform-dashboard.html', 'utf8').match(/<script>([\s\S]*?)<\/script>/)[1];
const sharedSource = fs.readFileSync('src/main/resources/static/dashboard-ui.js', 'utf8');
const elements = new Map();
const requests = [];
const document = {
  querySelectorAll() { return []; },
  getElementById(id) {
    if (!elements.has(id)) elements.set(id, {value:'', innerHTML:'', textContent:'', disabled:true, checked:false, style:{},
      scrollIntoView() { this.scrolledIntoView = true; },
      attributes:{},getAttribute(name){return this.attributes[name]??null;},setAttribute(name,value){this.attributes[name]=value;},removeAttribute(name){delete this.attributes[name];},
      listeners:{}, addEventListener(name, fn) { this.listeners[name] = fn; }});
    return elements.get(id);
  }
};
const event = (n) => ({sequence:String(n),eventId:n.toString(16).padStart(64,'0'),table:'inventory',operation:'INSERT',
  primaryKeyValue:n === 21 ? '<img src=x onerror=alert(1)>' : String(n),eventTimeMillis:1700000000000});
const sub = {subscriptionId:'a'.repeat(64),table:'inventory',targetName:'hook',operations:'UPDATE',changedFields:'name,available',
  filterField:'name',filterValue:'<img src=x onerror=alert(1)>',ruleVersion:2,enabled:true};
let pageCalls = 0;
let stockoutHandled = false;
let enabledExamples = [];
let detailOperation = 'UPDATE';
let detailChanges = [{field:'available_qty',beforePresent:true,before:'5',afterPresent:true,after:'0'}];
let replayMatched=0;
let forcedStatus=0;
let deliveryDown=false;
const replayJob=()=>({jobId:'b'.repeat(64),status:'COMPLETED',totalEvents:1,matchedEvents:replayMatched,
  skippedEvents:1-replayMatched,failedEvents:0,lastError:null,subscriptionSnapshot:JSON.stringify({...sub,table:'departments',targetName:'hook'})});
const copiedEventIds = [];
const context = {document, navigator:{clipboard:{writeText:async(value)=>{copiedEventIds.push(value);}}},
  window:{confirm:()=>true,location:{search:''}}, URLSearchParams, Date, JSON, console, fetch:async(url,opts={}) => {
  requests.push({url,...opts});
  if(forcedStatus)return {ok:false,status:forcedStatus,text:async()=>'{"error":"Conflict"}'};
  let data;
  if (url === '/platform/examples') data=enabledExamples;
  else if (url === '/platform/tables') data=[];
  else if (url === '/platform/targets') data=['hook'];
  else if (url.startsWith('/platform/events?')) {
    pageCalls++;
    data=url.includes('cursor=2') ? {items:[event(1)],nextCursor:null}
      : {items:Array.from({length:20},(_,i)=>event(21-i)),nextCursor:'2'};
  } else if (url === '/platform/subscriptions') data = opts.method ? sub : [sub];
  else if (url.startsWith('/platform/subscriptions/')) data=sub;
  else if (url === '/sync/status') data={serviceStarted:true,activeConsumers:6,configuredConsumers:6,lastSuccessAt:null,lastError:null};
  else if (url === '/platform/consumers/status') data={archive:{serviceStarted:true,activeConsumer:true,acknowledgedNewRows:10,acknowledgedDuplicateRows:2,lastSuccessAt:null,lastError:null}};
  else if (url === '/platform/deliveries/consumer-status') {
    if (deliveryDown) return {ok:false,status:503,text:async()=>'{"code":"UPSTREAM_UNAVAILABLE"}'};
    data={serviceStarted:true,activeConsumer:true,acknowledgedNewRows:10,acknowledgedDuplicateRows:0,createdTasks:7,lastSuccessAt:null,lastError:null};
  }
  else if (url === '/platform/replay-jobs' || /^\/platform\/replay-jobs\/[a-f0-9]{64}$/.test(url)) data=replayJob();
  else if (url.endsWith('/delivery-stats')) data=replayMatched?{SUCCEEDED:1}:{};
  else if (url.startsWith('/platform/deliveries?')) {
    const query = new URL(url, 'http://localhost').searchParams;
    const before = Number(query.get('cursor') || 126);
    const tasks = Array.from({length:125}, (_,i) => ({taskId:String(125-i), deliveryId:'d'.repeat(64),
      eventId:event(125-i).eventId, targetName:'<img src=x onerror=alert(1)>', roundType:'INITIAL',
      status:125-i > 75 ? 'SUCCEEDED' : 'RETRY_WAIT', attemptCount:1,maxAttempts:3}))
      .filter(t => Number(t.taskId) < before && (!query.get('status') || query.get('status') === t.status)
        && (!query.get('eventId') || query.get('eventId') === t.eventId));
    data={items:tasks.slice(0,50), nextCursor:tasks.length > 50 ? tasks[49].taskId : null};
  }
  else if (url.startsWith('/platform/deliveries/') && url.endsWith('/attempts')) data=[{attemptNumber:1,httpStatus:503,errorMessage:'unavailable'}];
  else if (url.startsWith('/warehouse/stockout-tasks?')) {
    const q=new URL(url,'http://localhost').searchParams;
    let rows=Array.from({length:21},(_,i)=>({id:String(21-i),inventoryId:'3',sku:'<img src=x onerror=alert(1)>',
      eventId:event(21-i).eventId,eventTimeMillis:1700000000000,status:stockoutHandled&&i===0?'HANDLED':'PENDING'}));
    rows=rows.filter(t=>(!q.get('cursor')||Number(t.id)<Number(q.get('cursor')))
      &&(!q.get('eventId')||t.eventId===q.get('eventId'))&&(!q.get('status')||t.status===q.get('status')));
    data={items:rows.slice(0,20),nextCursor:rows.length>20?rows[19].id:null};
  }
  else if (url === '/warehouse/stockout-tasks/21/complete') { stockoutHandled=true;data={status:'HANDLED'}; }
  else if (url.startsWith('/platform/events/')) data={
    summary:{...event(21),eventId:url.split('/').at(-1),database:'demo_db',sourceId:'source-v1',
      primaryKey:'id',primaryKeyValue:'59',operation:detailOperation,archivedAt:'2026-11-16T09:04:42.860Z'},
    event:{binlogFile:'binlog.000007',binlogPosition:927346198,beforeColumns:[],afterColumns:[]},
    topic:'example',partition:4,offset:'9007199254740993',changes:detailChanges,recovery:{available:true,reason:'Available'}
  };
  else if (url === '/platform/recovery-plans') data={planId:'b'.repeat(64),executable:true,reason:'Pre-check passed',generatedSql:'-- preview',checkSummary:'{}'};
  else throw Error('Unexpected request: '+url);
  return {ok:true,headers:{get:()=> 'application/json'},json:async()=>data};
}};
// 页面由网关托管：每个接口都加 /api 前缀并带登录令牌（X-CachePulse-Token），桩函数按去掉前缀后的路径应答
const stubFetch = context.fetch;
context.sessionStorage = {getItem:key => key === 'cachepulse.token' ? 'test-token' : null, setItem(){}, removeItem(){}};
context.window.fetch = async (url, opts={}) => {
  assert.ok(url.startsWith('/api/'), 'API calls must use the gateway prefix: ' + url);
  assert.equal(opts.headers && opts.headers['X-CachePulse-Token'], 'test-token');
  return stubFetch(url.slice(4), opts);
};
vm.createContext(context);vm.runInContext(sharedSource,context);vm.runInContext(source,context);
(async () => {
  await context.init();
  assert.equal(document.getElementById('stockoutDemoTab').hidden,true);
  assert.ok(!document.getElementById('deliveryList').innerHTML.includes('viewEventStockouts'));
  await context.resetSearch();
  assert.ok(!document.getElementById('eventList').innerHTML.includes('viewEventStockouts'));
  assert.ok(!requests.some(r=>r.url.startsWith('/warehouse/')));
  assert.equal(document.getElementById('btnNextCursor').disabled,false);
  assert.equal(document.getElementById('btnPrevCursor').disabled,true);
  assert.ok(!document.getElementById('eventList').innerHTML.includes('<img'));
  await context.nextCursor();
  assert.ok(requests.at(-1).url.includes('cursor=2'));
  assert.equal(document.getElementById('btnPrevCursor').disabled,false);
  assert.equal(document.getElementById('btnNextCursor').disabled,true);
  await context.prevCursor();
  assert.ok(!requests.at(-1).url.includes('cursor='));
  document.getElementById('filterKey').listeners.change();
  assert.equal(document.getElementById('btnNextCursor').disabled,true);
  await context.loadSubscriptions();
  assert.ok(!document.getElementById('subList').innerHTML.includes('<img'));
  await context.toggleSub(sub.subscriptionId,false,2);
  const toggle=requests.find(r=>r.url.includes('/enabled?'));
  assert.equal(toggle.method,'PUT');assert.deepEqual(JSON.parse(toggle.body),{enabled:false});
  context.editSub(sub.subscriptionId);
  document.getElementById('newSubChanged').value='';
  document.getElementById('newSubFilterField').value='';
  await context.createSubscription();
  const edit=requests.find(r=>r.method === 'PUT' && !r.url.includes('/enabled?'));
  assert.equal(JSON.parse(edit.body).filterField,null);
  assert.equal(JSON.parse(edit.body).changedFields,null);
  document.getElementById('recoveryEventId').value='a'.repeat(64);
  await context.generatePlan();
  assert.ok(document.getElementById('planExecutable').textContent.includes('仍需复核'));
  assert.equal(pageCalls,3);
  await context.resetDeliveries();
  assert.equal(document.getElementById('cardTasks').textContent,0);
  assert.equal(document.getElementById('btnNextDelivery').disabled,false);
  assert.equal(document.getElementById('btnPrevDelivery').disabled,true);
  assert.ok(!document.getElementById('deliveryList').innerHTML.includes('<img'));
  await context.nextDelivery();
  assert.ok(requests.at(-1).url.includes('cursor=76'));
  assert.equal(document.getElementById('cardTasks').textContent,50);
  await context.nextDelivery();
  assert.ok(requests.at(-1).url.includes('cursor=26'));
  assert.equal(document.getElementById('cardTasks').textContent,25);
  assert.equal(document.getElementById('btnNextDelivery').disabled,true);
  await context.prevDelivery();
  assert.ok(requests.at(-1).url.includes('cursor=76'));
  document.getElementById('delivStatus').value='RETRY_WAIT';
  await document.getElementById('delivStatus').listeners.change();
  assert.ok(requests.at(-1).url.includes('status=RETRY_WAIT'));
  assert.ok(!requests.at(-1).url.includes('cursor='));
  assert.equal(document.getElementById('btnPrevDelivery').disabled,true);
  context.showTab = () => {};
  await context.viewEventDeliveries(event(1).eventId);
  assert.ok(requests.at(-1).url.includes('eventId='+event(1).eventId));
  assert.equal(document.getElementById('cardTasks').textContent,1);
  const html = fs.readFileSync('src/main/resources/static/platform-dashboard.html','utf8');
  assert.ok(html.includes('当前页待处理/重试'));
  assert.ok(html.includes('追加一次投递'));
  await context.loadAttempts('1');
  assert.ok(document.getElementById('attemptDetail').innerHTML.includes('503'));
  assert.ok(document.getElementById('attemptDetail').innerHTML.includes('class="technical"'));
  document.getElementById('replayJobId').value='b'.repeat(64);
  await context.viewReplayDeliveries();
  assert.ok(requests.at(-1).url.includes('roundId='+'b'.repeat(64)));
  assert.ok(!requests.at(-1).url.includes('eventId='));
  await context.refreshReplay();
  assert.ok(document.getElementById('replayResult').innerHTML.includes('未生成 HTTP 通知'));
  assert.ok(document.getElementById('replayResult').innerHTML.includes('departments'));
  assert.ok(document.getElementById('replayFeedback').textContent.includes('查询完成'));
  replayMatched=1;
  await context.refreshReplay();
  assert.ok(document.getElementById('replayResult').innerHTML.includes('已为 1 条匹配事件生成通知任务'));
  assert.ok(document.getElementById('replayFeedback').textContent.includes('成功 1 条'));
  context.newReplayRound();
  assert.equal(document.getElementById('replayJobId').value,'');
  await context.loadConsumerStatus();
  assert.ok(document.getElementById('consumerStatus').innerHTML.includes('6 / 6'));
  assert.ok(document.getElementById('consumerStatus').innerHTML.includes('已确认重复事件'));
  assert.ok(document.getElementById('consumerStatus').innerHTML.includes('订阅投递'));
  assert.ok(document.getElementById('consumerStatus').innerHTML.includes('生成投递任务'));
  // 投递服务不可用时，其余两项照常显示
  deliveryDown = true;
  await context.loadConsumerStatus();
  assert.ok(document.getElementById('consumerStatus').innerHTML.includes('6 / 6'));
  assert.ok(document.getElementById('consumerStatus').innerHTML.includes('服务暂时不可用：HTTP 503'));
  deliveryDown = false;
  enabledExamples=['stockout'];
  await context.init();
  assert.equal(document.getElementById('stockoutDemoTab').hidden,false);
  assert.ok(document.getElementById('deliveryList').innerHTML.includes('viewEventStockouts'));
  await context.resetSearch();
  assert.ok(document.getElementById('eventList').innerHTML.includes('viewEventStockouts'));
  document.getElementById('stockoutStatus').value='PENDING';
  await context.resetStockouts();
  assert.equal(document.getElementById('stockoutNext').disabled,false);
  assert.ok(!document.getElementById('stockoutList').innerHTML.includes('<img'));
  await context.nextStockout();
  assert.ok(requests.at(-1).url.includes('cursor=2'));
  assert.equal(document.getElementById('stockoutNext').disabled,true);
  await context.prevStockout();
  await context.completeStockout('21');
  assert.equal(stockoutHandled,true);
  assert.ok(!document.getElementById('stockoutList').innerHTML.includes("completeStockout('21')"));
  await context.viewEventStockouts(event(21).eventId);
  assert.ok(document.getElementById('stockoutList').innerHTML.includes('已处理'));
  await context.openEvent(event(21).eventId);
  assert.equal(document.getElementById('detailEventId').value,event(21).eventId);
  assert.equal(document.getElementById('detailTable').textContent,'demo_db.inventory');
  assert.equal(document.getElementById('detailOperation').textContent,'修改（UPDATE）');
  assert.equal(document.getElementById('detailPrimaryKey').textContent,'id = 59');
  assert.equal(document.getElementById('eventDetail').hidden,false);
  assert.equal(document.getElementById('eventDetailEmpty').hidden,true);
  assert.equal(document.getElementById('eventDetail').scrolledIntoView,true);
  assert.equal(document.getElementById('eventTechnicalDetails').open,false);
  assert.ok(document.getElementById('detailChanges').innerHTML.includes('available_qty'));
  assert.ok(document.getElementById('detailChanges').innerHTML.includes('<td>5</td><td>0</td>'));
  assert.ok(document.getElementById('eventTechnicalMeta').innerHTML.includes('9007199254740993'));
  assert.ok(document.getElementById('eventRawJson').textContent.includes(event(21).eventId));
  await context.copyEventId();
  assert.equal(copiedEventIds.at(-1),event(21).eventId);
  assert.ok(document.getElementById('copyEventIdStatus').textContent.includes('已复制'));
  detailOperation='INSERT';
  detailChanges=[{field:'<img src=x onerror=alert(1)>',beforePresent:false,before:null,afterPresent:true,after:null}];
  await context.viewDetail(event(20).eventId);
  assert.equal(document.getElementById('detailOperation').textContent,'新增（INSERT）');
  assert.ok(document.getElementById('detailChanges').innerHTML.includes('不存在'));
  assert.ok(document.getElementById('detailChanges').innerHTML.includes('NULL'));
  assert.ok(!document.getElementById('detailChanges').innerHTML.includes('<img'));
  assert.equal(document.getElementById('copyEventIdStatus').textContent,'');
  detailOperation='DELETE';
  detailChanges=[{field:'remark',beforePresent:true,before:'<script>alert(1)</script>',afterPresent:false,after:null}];
  await context.viewDetail(event(19).eventId,true);
  assert.equal(document.getElementById('detailOperation').textContent,'删除（DELETE）');
  assert.ok(!document.getElementById('detailChanges').innerHTML.includes('<script>'));
  assert.ok(document.getElementById('detailChanges').innerHTML.includes('不存在'));
  assert.equal(document.getElementById('eventTechnicalDetails').open,true);
  assert.equal(document.getElementById('eventTechnicalDetails').scrolledIntoView,true);
  detailOperation='UPDATE';
  detailChanges=[{field:'remark',beforePresent:true,before:null,afterPresent:true,after:''}];
  await context.viewDetail(event(18).eventId);
  assert.ok(document.getElementById('detailChanges').innerHTML.includes('空字符串'));
  assert.equal(document.getElementById('eventTechnicalDetails').open,false);
  detailChanges=[];
  await context.viewDetail(event(17).eventId);
  assert.ok(document.getElementById('detailChanges').innerHTML.includes('没有字段值变化'));
  forcedStatus=409;
  const button=document.getElementById('testAction');
  await context.runAction(button,()=>context.api('/platform/subscriptions/stale'));
  assert.ok(document.getElementById('notice').textContent.includes('版本已更新'));
  assert.equal(document.getElementById('errorTechnical').hidden,false);
  assert.equal(button.getAttribute('aria-busy'),null);
  forcedStatus=0;
  assert.equal(context.window.DashboardUI.reason('Foreign key column modification is not supported for recovery: user_id'),
    '本次修改涉及外键关联列，暂不支持恢复：user_id');
  assert.equal(context.window.DashboardUI.reason('INSERT/DELETE recovery is not supported for tables with foreign keys'),
    '外键表暂不支持新增或删除事件的恢复');
  console.log('PASS: 21-row pagination, filter reset, text escaping, enabled request, rule editing, recovery wording');
  console.log('PASS: 125-task pagination, pending count scope, delivery filters and event linkage');
  console.log('PASS: replay-round linkage, 21-stockout pagination, completion, source event and escaped SKU');
  console.log('PASS: stockout example disabled by default, no warehouse requests until enabled, conditional entry links');
  console.log('PASS: readable event details, ID clipboard, UPDATE/INSERT/DELETE differences, NULL/absent/empty values, escaped text and collapsed technical details');
  console.log('PASS: readable replay counts and skip explanation, delivery attempts, consumer state and version-conflict feedback');
})().catch(error=>{console.error(error);process.exitCode=1;});
