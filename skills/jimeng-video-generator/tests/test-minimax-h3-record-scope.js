const assert = require('assert');
const fs = require('fs');
const path = require('path');
const vm = require('vm');
const { createRequire } = require('module');

const workerPath = path.join(__dirname, '..', 'minimax-h3-feishu-worker.js');
const source = fs.readFileSync(workerPath, 'utf8');
const workerRequire = createRequire(workerPath);

async function exercise(args, missing = false) {
  const calls = { get: [], list: 0, updates: [] };
  const rows = new Map(['selected', 'unrelated'].map(id => [id, {
    record_id: id,
    fields: { '渠道': 'METASO', '状态': '待处理', '模型': 'MiniMax H3' }
  }]));
  const config = { appId: 'offline', appSecret: 'offline', appToken: 'offline', tableId: 'offline' };
  const modules = {
    fs: { readFileSync: () => JSON.stringify(config) },
    './lib/feishu-client': {
      loadOpenclawFeishuCredentials: () => ({}),
      setActiveConfigForNetwork: () => {},
      getAccessToken: async () => 'offline',
      getRecord: async (_config, _token, id) => {
        calls.get.push(id);
        return missing ? null : structuredClone(rows.get(id));
      },
      listAllRecords: async () => {
        calls.list++;
        return structuredClone([...rows.values()]);
      },
      updateRecord: async (_config, _token, id, fields) => {
        calls.updates.push(id);
        Object.assign(rows.get(id).fields, fields);
      }
    },
    './trace-state': {
      expandHome: value => value,
      listSubmissionRecords: () => ['selected', 'unrelated'].map(id => ({
        channel: 'minimax_h3', record_id: id, platform_task_id: `task-${id}`,
        trace_id: `trace-${id}`, status: 'uploaded', uploaded_file_token: `video-${id}`
      }))
    },
    './platforms/minimax-h3/credentials': { requireApiKey: () => 'offline' },
    './platforms/minimax-h3/client': {},
    './platforms/minimax-h3/feishu-upload': {}
  };
  const loaded = { exports: {} };
  vm.runInNewContext(source, {
    require: name => Object.hasOwn(modules, name) ? modules[name] : workerRequire(name),
    module: loaded, __dirname: path.dirname(workerPath),
    console: { log: () => {} }
  }, { filename: workerPath });
  const result = await loaded.exports.run(args);
  return { calls, result };
}

(async () => {
  const scoped = await exercise(['--record-id', 'selected', '--poll-only']);
  assert.deepStrictEqual(scoped.calls.get, ['selected', 'selected'], 'initial and reconciliation reread must use exact ID');
  assert.strictEqual(scoped.calls.list, 0, 'exact-ID runs must never scan the table');
  assert.deepStrictEqual(scoped.calls.updates, ['selected'], 'unrelated local state must never be reconciled');

  const dry = await exercise(['--record-id', 'selected', '--dry-run']);
  assert.deepStrictEqual(dry.calls, { get: ['selected'], list: 0, updates: [] });
  assert.strictEqual(dry.result.pending, 1);

  const absent = await exercise(['--record-id', 'selected', '--poll-only'], true);
  assert.deepStrictEqual(absent.calls, { get: ['selected'], list: 0, updates: [] });

  const unscoped = await exercise(['--poll-only']);
  assert.deepStrictEqual(unscoped.calls.get, []);
  assert.strictEqual(unscoped.calls.list, 2, 'default full-table behavior must remain unchanged');
  assert.deepStrictEqual(unscoped.calls.updates, ['selected', 'unrelated']);
  console.log('MiniMax H3 record scope: 4 offline cases passed');
})().catch(error => { console.error(error); process.exitCode = 1; });
