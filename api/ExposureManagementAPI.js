// ExposureManagementAPI - CRUD for Exposure Management Tracker entries
// Uses direct Azure Table Storage REST API with SAS token authentication
const { app } = require('@azure/functions');
const axios = require('axios');
const crypto = require('crypto');

console.log('[ExposureManagementAPI] Module loading...');

const TABLE_NAME_ENV = 'EXPOSURE_TABLE_NAME';
const DEFAULT_TABLE = 'ExposureManagement';
const PARTITION_KEY = 'EXPOSURE';

// CORS headers
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  'Content-Type': 'application/json'
};

// Generate Account SAS token for Azure Table Storage
function generateTableSAS(accountName, accountKey) {
  const version = '2019-02-02';
  const now = new Date();
  const start = new Date(now.getTime() - 5 * 60 * 1000).toISOString().replace(/\.\d{3}Z$/, 'Z');
  const expiry = new Date(now.getTime() + 60 * 60 * 1000).toISOString().replace(/\.\d{3}Z$/, 'Z');

  const stringToSign = [
    accountName, 'raud', 't', 'sco', start, expiry, '', '', version, ''
  ].join('\n');

  const signature = crypto
    .createHmac('sha256', Buffer.from(accountKey, 'base64'))
    .update(stringToSign, 'utf-8')
    .digest('base64');

  return new URLSearchParams({
    sv: version, ss: 't', srt: 'sco', sp: 'raud',
    st: start, se: expiry, sig: signature
  }).toString();
}

// Make REST API call to Azure Table Storage
async function callTableAPI(method, path, body, context) {
  const account = process.env.AZURE_STORAGE_ACCOUNT_NAME;
  const accountKey = process.env.AZURE_STORAGE_ACCOUNT_KEY;

  if (!account || !accountKey) {
    throw new Error('Azure Storage credentials not configured');
  }

  const sasToken = generateTableSAS(account, accountKey);
  const separator = path.includes('?') ? '&' : '?';
  const url = `https://${account}.table.core.windows.net${path}${separator}${sasToken}`;

  const headers = {
    'Accept': 'application/json;odata=nometadata',
    'DataServiceVersion': '3.0'
  };

  if (body) {
    headers['Content-Type'] = 'application/json';
    headers['Content-Length'] = Buffer.byteLength(JSON.stringify(body)).toString();
  }

  context.log(`[ExposureAPI] ${method} ${path}`);

  const response = await axios({
    method, url, headers, data: body,
    validateStatus: () => true
  });

  if (response.status !== 200 && response.status !== 201 && response.status !== 204) {
    context.log(`[ExposureAPI] Error ${response.status}:`, JSON.stringify(response.data));
  }

  return response;
}

// Generate unique ID
function generateId() {
  return `${Date.now()}-${crypto.randomBytes(6).toString('hex')}`;
}

// Sanitize ID for OData queries
function sanitizeId(id) {
  return id.replace(/'/g, "''");
}

// Get user from request
function getUserFromRequest(request) {
  const clientPrincipal = request.headers.get('x-ms-client-principal');
  if (clientPrincipal) {
    try {
      const decoded = Buffer.from(clientPrincipal, 'base64').toString('utf8');
      const user = JSON.parse(decoded);
      return user.userDetails || 'authenticated-user';
    } catch (e) {
      return 'authenticated-user';
    }
  }
  return 'system';
}

// Get table name from env
function getTableName() {
  return process.env[TABLE_NAME_ENV] || DEFAULT_TABLE;
}

// Auto-create table if it doesn't exist (idempotent - 409 means it already exists)
let tableEnsured = false;
async function ensureTable(context) {
  if (tableEnsured) return;
  const tableName = getTableName();
  const account = process.env.AZURE_STORAGE_ACCOUNT_NAME;
  const accountKey = process.env.AZURE_STORAGE_ACCOUNT_KEY;
  if (!account || !accountKey) return;

  const sasToken = generateTableSAS(account, accountKey);
  const url = `https://${account}.table.core.windows.net/Tables?${sasToken}`;

  try {
    const res = await axios({
      method: 'POST',
      url,
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json;odata=nometadata',
        'DataServiceVersion': '3.0',
      },
      data: { TableName: tableName },
      validateStatus: () => true,
    });

    if (res.status === 201) {
      context.log(`[ExposureAPI] Table '${tableName}' created successfully`);
    } else if (res.status === 409) {
      context.log(`[ExposureAPI] Table '${tableName}' already exists`);
    } else {
      context.warn(`[ExposureAPI] Table creation returned ${res.status}:`, JSON.stringify(res.data));
    }
  } catch (e) {
    context.warn(`[ExposureAPI] Table creation check failed: ${e.message}`);
  }

  tableEnsured = true;
}

// ── LIST all entries ────────────────────────────────────────────────────────

async function listEntries(request, context) {
  const tableName = getTableName();
  const response = await callTableAPI('GET', `/${tableName}()`, null, context);

  if (response.status !== 200) {
    throw new Error(`Query failed: ${response.status}`);
  }

  const entities = response.data.value || [];
  const entries = entities
    .filter(e => !e.isDeleted)
    .map(e => ({
      id: e.RowKey,
      date: e.date || '',
      client: e.client || '',
      ticket: e.ticket || '-',
      status: e.status || 'Open',
      initiative: e.initiative || '',
      scope: e.scope || '',
      scoreBefore: e.scoreBefore || '',
      scoreAfter: e.scoreAfter || '',
      findings: e.findings || '',
      actions: e.actions || '',
      notes: e.notes || '',
      createdBy: e.createdBy || 'system',
      createdAt: e.createdAt || '',
      updatedBy: e.updatedBy || '',
      updatedAt: e.updatedAt || '',
    }))
    .sort((a, b) => new Date(b.date) - new Date(a.date));

  return {
    status: 200,
    headers: corsHeaders,
    jsonBody: { entries, count: entries.length }
  };
}

// ── GET single entry ────────────────────────────────────────────────────────

async function getEntry(request, context, id) {
  const tableName = getTableName();
  const safeId = sanitizeId(id);
  const response = await callTableAPI(
    'GET', `/${tableName}(PartitionKey='${PARTITION_KEY}',RowKey='${safeId}')`, null, context
  );

  if (response.status === 404 || response.data?.isDeleted) {
    return { status: 404, headers: corsHeaders, jsonBody: { error: 'Entry not found' } };
  }
  if (response.status !== 200) {
    throw new Error(`Get failed: ${response.status}`);
  }

  const e = response.data;
  return {
    status: 200,
    headers: corsHeaders,
    jsonBody: {
      id: e.RowKey, date: e.date, client: e.client, ticket: e.ticket,
      status: e.status, initiative: e.initiative, scope: e.scope,
      scoreBefore: e.scoreBefore, scoreAfter: e.scoreAfter,
      findings: e.findings, actions: e.actions, notes: e.notes,
      createdBy: e.createdBy, createdAt: e.createdAt,
      updatedBy: e.updatedBy, updatedAt: e.updatedAt,
    }
  };
}

// ── CREATE entry ────────────────────────────────────────────────────────────

async function createEntry(request, context) {
  const body = await request.json();
  const user = getUserFromRequest(request);
  const now = new Date().toISOString();
  const id = generateId();

  if (!body.client) {
    return { status: 400, headers: corsHeaders, jsonBody: { error: 'Client is required' } };
  }
  if (!body.date) {
    return { status: 400, headers: corsHeaders, jsonBody: { error: 'Date is required' } };
  }

  const entity = {
    PartitionKey: PARTITION_KEY,
    RowKey: id,
    date: body.date,
    client: body.client,
    ticket: body.ticket || '-',
    status: body.status || 'Open',
    initiative: body.initiative || '',
    scope: body.scope || '',
    scoreBefore: body.scoreBefore || '',
    scoreAfter: body.scoreAfter || '',
    findings: body.findings || '',
    actions: body.actions || '',
    notes: body.notes || '',
    createdBy: user,
    createdAt: now,
    updatedBy: '',
    updatedAt: '',
    isDeleted: false,
  };

  const tableName = getTableName();
  const response = await callTableAPI('POST', `/${tableName}`, entity, context);

  if (response.status !== 201 && response.status !== 204) {
    throw new Error(`Create failed: ${response.status}`);
  }

  return {
    status: 201,
    headers: corsHeaders,
    jsonBody: {
      message: 'Entry created',
      entry: {
        id, date: entity.date, client: entity.client, ticket: entity.ticket,
        status: entity.status, initiative: entity.initiative, scope: entity.scope,
        scoreBefore: entity.scoreBefore, scoreAfter: entity.scoreAfter,
        findings: entity.findings, actions: entity.actions, notes: entity.notes,
        createdBy: entity.createdBy, createdAt: entity.createdAt,
        updatedBy: '', updatedAt: '',
      }
    }
  };
}

// ── UPDATE entry ────────────────────────────────────────────────────────────

async function updateEntry(request, context, id) {
  const body = await request.json();
  const user = getUserFromRequest(request);
  const now = new Date().toISOString();

  const tableName = getTableName();
  const safeId = sanitizeId(id);
  const getResp = await callTableAPI(
    'GET', `/${tableName}(PartitionKey='${PARTITION_KEY}',RowKey='${safeId}')`, null, context
  );

  if (getResp.status === 404 || getResp.data?.isDeleted) {
    return { status: 404, headers: corsHeaders, jsonBody: { error: 'Entry not found' } };
  }

  const existing = getResp.data;

  const updated = {
    ...existing,
    date: body.date !== undefined ? body.date : existing.date,
    client: body.client !== undefined ? body.client : existing.client,
    ticket: body.ticket !== undefined ? body.ticket : existing.ticket,
    status: body.status !== undefined ? body.status : existing.status,
    initiative: body.initiative !== undefined ? body.initiative : existing.initiative,
    scope: body.scope !== undefined ? body.scope : existing.scope,
    scoreBefore: body.scoreBefore !== undefined ? body.scoreBefore : existing.scoreBefore,
    scoreAfter: body.scoreAfter !== undefined ? body.scoreAfter : existing.scoreAfter,
    findings: body.findings !== undefined ? body.findings : existing.findings,
    actions: body.actions !== undefined ? body.actions : existing.actions,
    notes: body.notes !== undefined ? body.notes : existing.notes,
    updatedBy: user,
    updatedAt: now,
  };

  const putResp = await callTableAPI(
    'PUT', `/${tableName}(PartitionKey='${PARTITION_KEY}',RowKey='${safeId}')`, updated, context
  );

  if (putResp.status !== 204) {
    throw new Error(`Update failed: ${putResp.status}`);
  }

  return {
    status: 200,
    headers: corsHeaders,
    jsonBody: {
      message: 'Entry updated',
      entry: {
        id, date: updated.date, client: updated.client, ticket: updated.ticket,
        status: updated.status, initiative: updated.initiative, scope: updated.scope,
        scoreBefore: updated.scoreBefore, scoreAfter: updated.scoreAfter,
        findings: updated.findings, actions: updated.actions, notes: updated.notes,
        createdBy: updated.createdBy, createdAt: updated.createdAt,
        updatedBy: updated.updatedBy, updatedAt: updated.updatedAt,
      }
    }
  };
}

// ── DELETE entry (soft delete) ──────────────────────────────────────────────

async function deleteEntry(request, context, id) {
  const user = getUserFromRequest(request);
  const now = new Date().toISOString();

  const tableName = getTableName();
  const safeId = sanitizeId(id);
  const getResp = await callTableAPI(
    'GET', `/${tableName}(PartitionKey='${PARTITION_KEY}',RowKey='${safeId}')`, null, context
  );

  if (getResp.status === 404 || getResp.data?.isDeleted) {
    return { status: 404, headers: corsHeaders, jsonBody: { error: 'Entry not found' } };
  }

  const updated = {
    ...getResp.data,
    isDeleted: true,
    status: 'deleted',
    updatedBy: user,
    updatedAt: now,
  };

  const putResp = await callTableAPI(
    'PUT', `/${tableName}(PartitionKey='${PARTITION_KEY}',RowKey='${safeId}')`, updated, context
  );

  if (putResp.status !== 204) {
    throw new Error(`Delete failed: ${putResp.status}`);
  }

  return {
    status: 200,
    headers: corsHeaders,
    jsonBody: { message: 'Entry deleted' }
  };
}

// ── ROUTE HANDLER ───────────────────────────────────────────────────────────

app.http('ExposureManagementAPI', {
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  authLevel: 'anonymous',
  route: 'exposure/{id?}',
  handler: async (request, context) => {
    context.log('[ExposureAPI] Request:', request.method, request.url);

    if (request.method === 'OPTIONS') {
      return { status: 200, headers: corsHeaders };
    }

    try {
      // Auto-create table on first request (idempotent)
      await ensureTable(context);
      const id = request.params.id;

      if (!id) {
        // /api/exposure
        if (request.method === 'GET') return await listEntries(request, context);
        if (request.method === 'POST') return await createEntry(request, context);
      } else {
        // /api/exposure/{id}
        if (request.method === 'GET') return await getEntry(request, context, id);
        if (request.method === 'PUT') return await updateEntry(request, context, id);
        if (request.method === 'DELETE') return await deleteEntry(request, context, id);
      }

      return { status: 405, headers: corsHeaders, jsonBody: { error: 'Method not allowed' } };
    } catch (error) {
      context.error('[ExposureAPI] Error:', error);
      return { status: 500, headers: corsHeaders, jsonBody: { error: error.message } };
    }
  }
});

console.log('[ExposureManagementAPI] Module loaded successfully');
