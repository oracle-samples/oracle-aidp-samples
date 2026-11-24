import assert from 'node:assert/strict';
import test from 'node:test';
import {webcrypto} from 'node:crypto';
import {a2appClient} from '../web/a2app.mjs';
import {chart} from '../web/charts.mjs';

class Element {
  constructor(tag, text = '') {this.tagName = tag.toUpperCase(); this.children = []; this.text = text; this.disabled = false; this.value = ''; this.attributes = {}; this.style = {};}
  append(...children) {for (const child of children) {child.parentElement = this; this.children.push(child);}}
  replaceChildren(...children) {for (const child of this.children) child.parentElement = null; this.children = []; this.append(...children);}
  set textContent(value) {this.text = value; this.children = [];}
  get textContent() {return this.text + this.children.map(child => child.textContent).join('');}
  setAttribute(key, value) {this.attributes[key] = value;}
  getAttribute(key) {return this.attributes[key] ?? null;}
  contains(other) {return this === other || this.children.some(child => child.contains(other));}
  matches(selector) {return selector.split(',').some(tag => tag.trim().toUpperCase() === this.tagName);}
  closest(selector) {return this.matches(selector) ? this : this.parentElement?.closest(selector) ?? null;}
  focus() {document.activeElement = this;}
  blur() {if (document.activeElement === this) document.activeElement = null;}
  click() {if (!this.disabled) this.onclick?.({preventDefault() {}});}
}
class Input extends Element {constructor() {super('input');}}
const walk = root => [root, ...root.children.flatMap(walk)];
const flush = async () => {for (let i = 0; i < 20; i++) await Promise.resolve();};
const deferred = () => {let resolve; const promise = new Promise(done => {resolve = done;}); return {promise, resolve};};
const catalogId = 'https://example.test/catalog';
const action = label => ({label, effect: 'read', inputSchema: {properties: {}}});
const definition = {catalogId, components: [
  {id: 'overview', label: 'Overview', actions: {display: action('Display'), status: action('Status'), run: action('Run')}},
  {id: 'review', label: 'Review', actions: {display: action('Display')}},
]};
function reply(status = 'accepted', version = 1) {
  return {status, definitionRevision: 'release-one', result: {definition},
    process: {runId: 'run-one', stateVersion: version, completed: 0, total: 2},
    presentation: {a2ui: [
      {surfaceUpdate: {surfaceId: 'overview', components: [
        {id: 'root', component: {Column: {children: {explicitList: ['notes', 'run']}}}},
        {id: 'notes', component: {TextField: {label: {literalString: 'Notes'}, text: {path: '/notes', literalString: ''}}}},
        {id: 'run-label', component: {Text: {text: {literalString: 'Run'}}}},
        {id: 'run', component: {Button: {child: 'run-label', action: {name: 'overview.run', context: []}}}},
      ]}},
      {beginRendering: {surfaceId: 'overview', root: 'root', catalogId}},
    ]}};
}
async function client(t, initial = reply()) {
  const keys = ['document', 'HTMLInputElement', 'HTMLElement', 'fetch', 'setTimeout', 'clearTimeout', 'crypto'];
  const descriptors = new Map(keys.map(key => [key, Object.getOwnPropertyDescriptor(globalThis, key)]));
  const target = new Element('main'), requests = [], timers = new Map(), slow = deferred();
  let timerId = 0;
  const globals = {
    document: {activeElement: null, createElement: tag => tag === 'input' ? new Input() : new Element(tag), createTextNode: text => new Element('#text', text)},
    HTMLInputElement: Input, HTMLElement: Element, crypto: webcrypto,
    setTimeout(callback) {timers.set(++timerId, callback); return timerId;},
    clearTimeout(id) {timers.delete(id);},
    async fetch(_url, options) {
      requests.push(JSON.parse(options.body));
      const value = requests.length === 1 ? initial : requests.length === 2 ? await slow.promise : reply('ok', 3);
      return {ok: true, json: async () => value};
    },
  };
  for (const [key, value] of Object.entries(globals)) Object.defineProperty(globalThis, key, {configurable: true, writable: true, value});
  t.after(async () => {
    slow.resolve(reply('ok', 2)); await flush();
    for (const key of keys) {const descriptor = descriptors.get(key); if (descriptor) Object.defineProperty(globalThis, key, descriptor); else delete globalThis[key];}
  });
  await a2appClient('sample', target, () => new Element('figure'), catalogId);
  return {target, requests, slow,
    button: label => walk(target).find(node => node.tagName === 'BUTTON' && node.textContent === label),
    input: label => walk(target).find(node => node.tagName === 'INPUT' && node.getAttribute('aria-label') === label),
    async poll() {const [id, callback] = timers.entries().next().value; timers.delete(id); callback(); await flush();},
  };
}

test('background status does not disable navigation or actions while the response is slow', async t => {
  const page = await client(t);
  await page.poll();
  assert.equal(page.requests.length, 2);
  for (const label of ['Overview', 'Review', 'Run', 'Send']) {
    assert.equal(page.button(label).disabled, false, `${label} was disabled by a background status request`);
  }
});

test('foreground navigation waits for the outstanding status and uses its latest process version', async t => {
  const page = await client(t);
  await page.poll();
  page.button('Review').click();
  await flush();
  assert.equal(page.requests.length, 2, 'navigation raced the outstanding status request');
  page.slow.resolve(reply('ok', 2));
  await flush();
  assert.equal(page.requests.length, 3, 'navigation was dropped while polling');
  assert.equal(page.requests[2].componentId, 'review');
  assert.equal(page.requests[2].process.expectedStateVersion, 2);
});

test('a focused form is not replaced or polled while the user is typing', async t => {
  const page = await client(t), input = page.input('Notes');
  input.value = 'Unsaved notes'; input.oninput(); input.focus();
  await page.poll();
  assert.equal(page.requests.length, 1, 'a background request started while editing');
  assert.ok(page.target.contains(input), 'polling replaced the focused input');
  assert.equal(input.value, 'Unsaved notes');
});

test('a status response arriving during typing preserves the input and polling resumes after blur', async t => {
  const page = await client(t);
  await page.poll();
  const input = page.input('Notes');
  input.focus(); input.value = 'Notes typed while polling'; input.oninput();
  page.slow.resolve(reply('ok', 2));
  await flush();
  assert.ok(page.target.contains(input), 'the arriving response replaced the focused input');
  assert.equal(page.input('Notes').value, 'Notes typed while polling', 'the arriving response reset the draft');
  input.blur();
  await page.poll();
  assert.equal(page.requests.length, 3, 'status polling did not resume after editing');
  assert.equal(page.requests[2].actionId, 'status');
});

test('chat waits for an outstanding status and sends its latest release and run context', async t => {
  const page = await client(t);
  await page.poll();
  const input = page.input('Chat message');
  input.value = 'Explain the saved result.';
  input.parentElement.onsubmit({preventDefault() {}});
  await flush();
  assert.equal(page.requests.length, 2, 'chat raced the outstanding status request');
  const updated = reply('ok', 2);
  updated.definitionRevision = 'release-two'; updated.process.runId = 'run-two';
  page.slow.resolve(updated);
  await flush();
  assert.equal(page.requests.length, 3, 'the submitted chat was dropped');
  assert.equal(page.requests[2].type, 'chat');
  assert.equal(page.requests[2].text, 'Explain the saved result.');
  assert.equal(page.requests[2].definitionRevision, 'release-two');
  assert.deepEqual(page.requests[2].context, {componentId: 'overview', runId: 'run-two'});
});

for (const [choice, expected] of [['True', true], ['False', false]]) {
  test(`boolean enum choice ${choice} is sent as a boolean`, async t => {
    const initial = structuredClone(reply('ok'));
    initial.result.definition.components[0].actions.run.inputSchema.properties = {approved: {type: 'boolean', enum: [true, false]}};
    initial.presentation.a2ui[0].surfaceUpdate.components = [
      {id: 'root', component: {Column: {children: {explicitList: ['approved', 'run']}}}},
      {id: 'approved', component: {MultipleChoice: {selections: {path: '/approved', literalArray: [choice]},
        options: [{label: {literalString: 'True'}, value: 'True'}, {label: {literalString: 'False'}, value: 'False'}]}}},
      {id: 'run-label', component: {Text: {text: {literalString: 'Run'}}}},
      {id: 'run', component: {Button: {child: 'run-label', action: {name: 'overview.run',
        context: [{key: 'approved', value: {path: '/approved/0'}}]}}}},
    ];
    const page = await client(t, initial);
    page.button('Run').click();
    await flush();
    assert.equal(page.requests[1].data.userAction.context.approved, expected);
  });
}

test('chart renders text safely and keeps signed values in its complete table', async t => {
  await client(t);
  const result = chart({rows: [{label: '<script>example</script>', amount: -12.5}, {label: 'Second', amount: '25'}], label: 'label', value: 'amount'});
  assert.ok(result.textContent.includes('<script>example</script>'));
  assert.ok(result.textContent.includes('-12.5'));
  assert.ok(result.textContent.includes('View 2 values'));
  assert.ok(!result.textContent.includes('USD'), 'numeric charts acquired a business currency');
});

test('chart rejects missing and nonnumeric measures instead of displaying invented zeros', async t => {
  await client(t);
  for (const amount of [null, false, '', 'invalid', Infinity]) {
    assert.throws(() => chart({rows: [{label: 'Item', amount}], label: 'label', value: 'amount'}), /finite numbers/);
  }
});
