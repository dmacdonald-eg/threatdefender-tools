const { app } = require('@azure/functions');
const { ACTION_CATALOG, getHandler, resolveDeviceId } = require('./lib/soarHandlers');

// ── GET /api/SoarExecute — Return action catalog ────────────────────────────

app.http('SoarCatalog', {
  methods: ['GET', 'OPTIONS'],
  authLevel: 'anonymous',
  route: 'SoarExecute',
  handler: async (request) => {
    if (request.method === 'OPTIONS') {
      return {
        status: 200,
        headers: {
          'Access-Control-Allow-Origin': '*',
          'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
          'Access-Control-Allow-Headers': 'Content-Type',
        },
      };
    }

    return {
      status: 200,
      headers: { 'Access-Control-Allow-Origin': '*', 'Content-Type': 'application/json' },
      jsonBody: { catalog: ACTION_CATALOG },
    };
  },
});

// ── POST /api/SoarExecute — Execute a SOAR action ───────────────────────────

app.http('SoarExecute', {
  methods: ['POST'],
  authLevel: 'anonymous',
  route: 'SoarExecute',
  handler: async (request, context) => {
    context.log('SoarExecute triggered');

    let body;
    try {
      body = await request.json();
    } catch {
      return {
        status: 400,
        headers: { 'Access-Control-Allow-Origin': '*' },
        jsonBody: { error: 'Invalid JSON body' },
      };
    }

    const { tenantId, action, entityData, analystEmail, workspaceName } = body;

    // Validate tenantId
    if (!tenantId || typeof tenantId !== 'string') {
      return {
        status: 400,
        headers: { 'Access-Control-Allow-Origin': '*' },
        jsonBody: { error: 'tenantId is required' },
      };
    }

    // Validate action
    const actionMeta = ACTION_CATALOG[action];
    if (!actionMeta) {
      return {
        status: 400,
        headers: { 'Access-Control-Allow-Origin': '*' },
        jsonBody: { error: `Unknown action: ${action}` },
      };
    }

    // Validate entity data
    if (!entityData || typeof entityData !== 'object') {
      return {
        status: 400,
        headers: { 'Access-Control-Allow-Origin': '*' },
        jsonBody: { error: 'entityData is required' },
      };
    }

    // For user actions, require upn
    if (actionMeta.entityType === 'account' && !entityData.upn) {
      return {
        status: 400,
        headers: { 'Access-Control-Allow-Origin': '*' },
        jsonBody: { error: 'Missing required field: upn' },
      };
    }

    // For device actions, resolve hostname to deviceId if needed
    if (actionMeta.entityType === 'host') {
      if (!entityData.deviceId && !entityData.hostname) {
        return {
          status: 400,
          headers: { 'Access-Control-Allow-Origin': '*' },
          jsonBody: { error: 'Missing required field: hostname or deviceId' },
        };
      }

      if (!entityData.deviceId && entityData.hostname) {
        try {
          context.log(`Resolving hostname "${entityData.hostname}" to MDE device ID...`);
          entityData.deviceId = await resolveDeviceId(tenantId, entityData.hostname);
          context.log(`Resolved to deviceId: ${entityData.deviceId}`);
        } catch (err) {
          return {
            status: 400,
            headers: { 'Access-Control-Allow-Origin': '*' },
            jsonBody: { error: err.message },
          };
        }
      }
    }

    // Execute the action
    const handler = getHandler(action);
    if (!handler) {
      return {
        status: 500,
        headers: { 'Access-Control-Allow-Origin': '*' },
        jsonBody: { error: 'Handler not found for action' },
      };
    }

    const target = entityData.upn || entityData.hostname || entityData.deviceId;

    try {
      const result = await handler(tenantId, entityData);

      context.log(`SOAR action ${action} succeeded | Target: ${target} | By: ${analystEmail || 'unknown'} | Workspace: ${workspaceName || 'unknown'}`);

      return {
        status: 200,
        headers: { 'Access-Control-Allow-Origin': '*', 'Content-Type': 'application/json' },
        jsonBody: {
          success: result.success,
          message: result.message,
          details: result.details,
          action: actionMeta.label,
          target,
        },
      };
    } catch (err) {
      context.error(`SOAR action ${action} failed | Target: ${target} | Error: ${err.message}`);

      return {
        status: 500,
        headers: { 'Access-Control-Allow-Origin': '*' },
        jsonBody: {
          success: false,
          error: err.message,
          action: actionMeta.label,
          target,
        },
      };
    }
  },
});
