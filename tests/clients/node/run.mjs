// Drives one service workflow with the pinned Node.js Google Cloud clients
// against gcp-local, at each client's default transport. scripts/run_clients.py
// starts the emulator and runs `node run.mjs SERVICE` once per service; the
// process exits 0 when the workflow passes and prints the failing step otherwise.
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {BigQuery} from '@google-cloud/bigquery';
import {Bigtable} from '@google-cloud/bigtable';
import {Firestore} from '@google-cloud/firestore';
import {KeyManagementServiceClient} from '@google-cloud/kms';
import {Logging} from '@google-cloud/logging';
import {PubSub} from '@google-cloud/pubsub';
import {SecretManagerServiceClient} from '@google-cloud/secret-manager';
import {Spanner} from '@google-cloud/spanner';
import {CloudSchedulerClient} from '@google-cloud/scheduler';
import {Storage} from '@google-cloud/storage';
import {CloudTasksClient} from '@google-cloud/tasks';
import {iam} from '@googleapis/iam';
import {GoogleAuth} from 'google-auth-library';
import {grpc} from 'google-gax';

const project = process.env.GOOGLE_CLOUD_PROJECT;
const http = process.env.GCP_LOCAL_HTTP;
const scopes = ['https://www.googleapis.com/auth/cloud-platform'];
const deniedADC = process.env.GCP_LOCAL_DENIED_ADC;
// The granted identity is plain ADC; the ungranted one is a second ADC file.
// gRPC clients get these credentials through sslCreds, which bypasses their
// own credential loading, so they request cloud-platform; so does
// @googleapis/iam, which has no default scopes.
const owner = new GoogleAuth({scopes});
const denied = new GoogleAuth({scopes, keyFilename: deniedADC});

const authClients = new Map([[owner, await owner.getClient()], [denied, await denied.getClient()]]);

// The one setting every gRPC client needs. grpc-js attaches call credentials
// only to a TLS channel, so clients use the gateway's TLS port with channel
// credentials that trust the environment's CA and carry ADC. Passing them
// explicitly also overrides the plaintext channel Bigtable picks for any
// custom endpoint.
const ca = readFileSync(process.env.GCP_LOCAL_GRPC_TLS_CA);
function grpcOptions(auth) {
  const client = authClients.get(auth);
  const [apiEndpoint, port] = process.env.GCP_LOCAL_GRPC_TLS.split(':');
  const sslCreds = grpc.credentials.combineChannelCredentials(
    grpc.credentials.createSsl(ca),
    // grpc-js copies a plain header object; google-auth-library returns Headers.
    grpc.credentials.createFromGoogleCredential({
      getRequestHeaders: async url => Object.fromEntries(await client.getRequestHeaders(url)),
    }),
  );
  return {projectId: project, apiEndpoint, port: Number(port), sslCreds};
}

// Pub/Sub reads the port from apiEndpoint and treats any host outside
// googleapis.com as an anonymous plaintext emulator unless emulatorMode is false.
const pubsubOptions = auth => ({...grpcOptions(auth), apiEndpoint: process.env.GCP_LOCAL_GRPC_TLS, emulatorMode: false});

// JSON API clients skip authentication on a custom endpoint unless told not
// to. They load ADC themselves, with their default scopes.
const restOptions = auth => ({
  projectId: project, apiEndpoint: http, useAuthWithCustomEndpoint: true,
  ...(auth === denied ? {keyFilename: deniedADC} : {}),
});

async function expectDenied(step, call) {
  try {
    await call();
  } catch (error) {
    if (error.code === 403 || error.code === 7 || error.status === 403) return;
    throw new Error(`${step}: want permission denied, got: ${error.message}`);
  }
  throw new Error(`${step}: the ungranted identity was allowed`);
}

async function storage() {
  const client = new Storage(restOptions(owner));
  const [bucket] = await client.createBucket(`node-matrix-${project}`, {
    location: 'US-CENTRAL1',
    iamConfiguration: {uniformBucketLevelAccess: {enabled: true}, publicAccessPrevention: 'enforced'},
    softDeletePolicy: {retentionDurationSeconds: 0},
  });
  const file = bucket.file('hello.txt');
  // @google-cloud/storage 8.4.0 drops credentials from resumable uploads to a
  // custom endpoint (File.startResumableUpload_ omits useAuthWithCustomEndpoint),
  // so uploads must be single-request.
  await file.save('hello from node', {resumable: false});
  const [data] = await file.download();
  assert.equal(data.toString(), 'hello from node', 'download');
  const [files] = await bucket.getFiles();
  assert.deepEqual(files.map(f => f.name), ['hello.txt'], 'list objects');
  const other = new Storage(restOptions(denied));
  await expectDenied('ungranted object get', () => other.bucket(bucket.name).file('hello.txt').getMetadata());
  await file.delete();
  await bucket.delete();
}

async function bigquery() {
  const client = new BigQuery(restOptions(owner));
  const [dataset] = await client.createDataset('node_matrix', {location: 'US'});
  const [table] = await dataset.createTable('people', {schema: 'name:STRING,age:INTEGER'});
  await table.insert([{name: 'ada', age: 36}, {name: 'grace', age: 45}]);
  const [rows] = await client.query({query: `SELECT name, age FROM \`${project}.node_matrix.people\` WHERE name = 'ada'`});
  assert.deepEqual(rows, [{name: 'ada', age: 36}], 'query');
  const [tables] = await dataset.getTables();
  assert.deepEqual(tables.map(t => t.id), ['people'], 'list tables');
  const other = new BigQuery(restOptions(denied));
  await expectDenied('ungranted table get', () => other.dataset('node_matrix').table('people').getMetadata());
  await dataset.delete({force: true});
}

async function pubsub() {
  const client = new PubSub(pubsubOptions(owner));
  const [topic] = await client.createTopic('node-matrix');
  const [subscription] = await topic.createSubscription('node-matrix');
  await topic.publishMessage({data: Buffer.from('hello from node')});
  const received = await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('receive: timed out')), 30000);
    subscription.on('error', reject);
    subscription.on('message', message => {
      clearTimeout(timer);
      message.ack();
      resolve(message.data.toString());
    });
  });
  await subscription.close();
  assert.equal(received, 'hello from node', 'receive');
  const [topics] = await client.getTopics();
  assert.ok(topics.some(t => t.name === topic.name), 'list topics');
  const other = new PubSub(pubsubOptions(denied));
  await expectDenied('ungranted publish', () => other.topic('node-matrix').publishMessage({data: Buffer.from('no')}));
  await subscription.delete();
  await topic.delete();
}

async function firestore() {
  const client = new Firestore(grpcOptions(owner));
  const doc = client.collection('node-matrix-cities').doc('sf');
  await doc.set({name: 'San Francisco', population: 870000});
  assert.equal((await doc.get()).get('name'), 'San Francisco', 'get document');
  const query = await client.collection('node-matrix-cities').where('population', '>', 100000).get();
  assert.deepEqual(query.docs.map(d => d.id), ['sf'], 'query');
  const other = new Firestore(grpcOptions(denied));
  await expectDenied('ungranted get', () => other.collection('node-matrix-cities').doc('sf').get());
  await doc.delete();
}

async function secretmanager() {
  const client = new SecretManagerServiceClient(grpcOptions(owner));
  const parent = `projects/${project}`;
  const [secret] = await client.createSecret({parent, secretId: 'node-matrix', secret: {replication: {automatic: {}}}});
  await client.addSecretVersion({parent: secret.name, payload: {data: Buffer.from('hello from node')}});
  const latest = `${secret.name}/versions/latest`;
  const [access] = await client.accessSecretVersion({name: latest});
  assert.equal(Buffer.from(access.payload.data).toString(), 'hello from node', 'access version');
  const [secrets] = await client.listSecrets({parent});
  assert.ok(secrets.some(s => s.name === secret.name), 'list secrets');
  const other = new SecretManagerServiceClient(grpcOptions(denied));
  await expectDenied('ungranted access', () => other.accessSecretVersion({name: latest}));
  await client.deleteSecret({name: secret.name});
}

async function cloudkms() {
  const client = new KeyManagementServiceClient(grpcOptions(owner));
  const [ring] = await client.createKeyRing({parent: `projects/${project}/locations/us-central1`, keyRingId: 'node-matrix', keyRing: {}});
  const [key] = await client.createCryptoKey({parent: ring.name, cryptoKeyId: 'key', cryptoKey: {purpose: 'ENCRYPT_DECRYPT'}});
  const [encrypted] = await client.encrypt({name: key.name, plaintext: Buffer.from('hello from node')});
  const [decrypted] = await client.decrypt({name: key.name, ciphertext: encrypted.ciphertext});
  assert.equal(Buffer.from(decrypted.plaintext).toString(), 'hello from node', 'decrypt');
  const [keys] = await client.listCryptoKeys({parent: ring.name});
  assert.ok(keys.some(k => k.name === key.name), 'list crypto keys');
  const other = new KeyManagementServiceClient(grpcOptions(denied));
  await expectDenied('ungranted encrypt', () => other.encrypt({name: key.name, plaintext: Buffer.from('no')}));
  await client.destroyCryptoKeyVersion({name: `${key.name}/cryptoKeyVersions/1`});
}

async function cloudtasks() {
  const client = new CloudTasksClient(grpcOptions(owner));
  const location = `projects/${project}/locations/us-central1`;
  const [queue] = await client.createQueue({parent: location, queue: {name: `${location}/queues/node-matrix`}});
  // Scheduled an hour out, so the task stays queued while it is read.
  const task = {
    scheduleTime: {seconds: Math.floor(Date.now() / 1000) + 3600},
    httpRequest: {url: 'http://127.0.0.1:9/task', httpMethod: 'POST', body: Buffer.from('hello from node')},
  };
  const [created] = await client.createTask({parent: queue.name, task});
  await client.getTask({name: created.name});
  const [tasks] = await client.listTasks({parent: queue.name});
  assert.ok(tasks.some(t => t.name === created.name), 'list tasks');
  const other = new CloudTasksClient(grpcOptions(denied));
  await expectDenied('ungranted create task', () => other.createTask({parent: queue.name, task}));
  await client.deleteTask({name: created.name});
  await client.deleteQueue({name: queue.name});
}

// A yearly HTTP job: it never runs, so its unreachable target is never called.
async function cloudscheduler() {
  const client = new CloudSchedulerClient(grpcOptions(owner));
  const location = `projects/${project}/locations/us-central1`;
  const [job] = await client.createJob({parent: location, job: {
    name: `${location}/jobs/node-matrix`, schedule: '0 0 1 1 *', timeZone: 'America/New_York',
    httpTarget: {uri: 'http://127.0.0.1:9/job', httpMethod: 'POST', body: Buffer.from('hello from node')},
  }});
  const [got] = await client.getJob({name: job.name});
  assert.equal(got.state, 'ENABLED', 'get job');
  const [jobs] = await client.listJobs({parent: location});
  assert.ok(jobs.some(j => j.name === job.name), 'list jobs');
  const [paused] = await client.pauseJob({name: job.name});
  assert.equal(paused.state, 'PAUSED', 'pause job');
  await client.resumeJob({name: job.name});
  const other = new CloudSchedulerClient(grpcOptions(denied));
  await expectDenied('ungranted run job', () => other.runJob({name: job.name}));
  await client.deleteJob({name: job.name});
}

async function spanner() {
  // Built-in metrics would export to monitoring.googleapis.com; nothing may leave loopback.
  const client = new Spanner({...grpcOptions(owner), disableBuiltInMetrics: true});
  const [instance, instanceOperation] = await client.createInstance('node-matrix', {config: 'emulator-config', nodes: 1, displayName: 'Node matrix'});
  await instanceOperation.promise();
  const [database, databaseOperation] = await instance.createDatabase('matrix', {
    schema: ['CREATE TABLE Singers (Id INT64 NOT NULL, Name STRING(100)) PRIMARY KEY (Id)'],
  });
  await databaseOperation.promise();
  await database.table('Singers').insert({Id: 1, Name: 'Ada'});
  const [rows] = await database.run({sql: 'SELECT Name FROM Singers WHERE Id = 1', json: true});
  assert.deepEqual(rows, [{Name: 'Ada'}], 'query');
  const [databases] = await instance.getDatabases();
  assert.ok(databases.some(d => d.formattedName_ === database.formattedName_), 'list databases');
  const other = new Spanner({...grpcOptions(denied), disableBuiltInMetrics: true});
  const otherDatabase = other.instance('node-matrix').database('matrix');
  await expectDenied('ungranted read', () => otherDatabase.run({sql: 'SELECT Name FROM Singers'}));
  await otherDatabase.close();
  await database.close();
  await database.delete();
  await instance.delete();
}

async function bigtable() {
  const client = new Bigtable({...grpcOptions(owner), metricsEnabled: false});
  const instance = client.instance('node-matrix');
  const [, operation] = await instance.create({
    displayName: 'Node matrix',
    clusters: [{id: 'node-matrix-c1', location: 'us-central1-b', nodes: 1, storage: 'ssd'}],
  });
  await operation.promise();
  const [table] = await instance.createTable('matrix', {families: ['cf']});
  await table.insert([{key: 'row-1', data: {cf: {greeting: 'hello from node'}}}]);
  const [row] = await table.row('row-1').get();
  assert.equal(row.data.cf.greeting[0].value, 'hello from node', 'read row');
  const [tables] = await instance.getTables();
  assert.deepEqual(tables.map(t => t.id), ['matrix'], 'list tables');
  const other = new Bigtable({...grpcOptions(denied), metricsEnabled: false});
  await expectDenied('ungranted read', () => other.instance('node-matrix').table('matrix').row('row-1').get());
  await table.delete();
  await instance.delete();
}

async function logging() {
  const client = new Logging(grpcOptions(owner));
  const log = client.log('node-matrix');
  await log.write(log.entry({resource: {type: 'global'}}, 'hello from node'));
  const [entries] = await client.getEntries({filter: `logName="projects/${project}/logs/node-matrix"`});
  // The first write also logs the client's instrumentation diagnostic entry.
  const written = entries.map(e => e.data).filter(data => !data['logging.googleapis.com/diagnostic']);
  assert.deepEqual(written, ['hello from node'], 'list entries');
  const [logs] = await client.getLogs();
  assert.ok(logs.some(l => l.name === 'node-matrix'), 'list logs');
  const other = new Logging(grpcOptions(denied));
  const deniedLog = other.log('node-matrix');
  await expectDenied('ungranted write', () => deniedLog.write(deniedLog.entry({resource: {type: 'global'}}, 'no')));
  await log.delete();
}

async function iamService() {
  // The discovery-based REST client; Google publishes no Node GAPIC client for IAM admin v1.
  const client = iam({version: 'v1', auth: owner, rootUrl: `${http}/`});
  const name = `projects/${project}`;
  const {data: account} = await client.projects.serviceAccounts.create({
    name, requestBody: {accountId: 'node-matrix', serviceAccount: {displayName: 'Node matrix'}},
  });
  const resource = `${name}/serviceAccounts/${account.email}`;
  await client.projects.serviceAccounts.get({name: resource});
  const {data: listed} = await client.projects.serviceAccounts.list({name});
  assert.ok(listed.accounts.some(a => a.email === account.email), 'list service accounts');
  const other = iam({version: 'v1', auth: denied, rootUrl: `${http}/`});
  await expectDenied('ungranted get', () => other.projects.serviceAccounts.get({name: resource}));
  await client.projects.serviceAccounts.delete({name: resource});
}

const workflows = {bigquery, bigtable, cloudkms, cloudscheduler, cloudtasks, firestore, iam: iamService, logging, pubsub, secretmanager, spanner, storage};
const service = process.argv[2];
if (!workflows[service]) {
  console.error(`usage: node run.mjs SERVICE; services: ${Object.keys(workflows).join(', ')}`);
  process.exit(2);
}
const timer = setTimeout(() => {
  console.error('workflow timed out after 90s');
  process.exit(1);
}, 90000);
try {
  await workflows[service]();
  clearTimeout(timer);
  process.exit(0);
} catch (error) {
  console.error(error.stack || String(error));
  console.error(error.message);
  process.exit(1);
}
