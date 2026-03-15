// SOAR action handlers — Graph API (user actions) and MDE API (device actions).
// Uses direct OAuth2 client credentials flow (no Azure SDK — avoids SWA crypto issues).
// Each handler: async (tokenFn, entityData) => { success, message, details }

const axios = require('axios');

// ── Token Acquisition ────────────────────────────────────────────────────────

const tokenCache = new Map();

async function acquireToken(tenantId, scope) {
  const cacheKey = `${tenantId}:${scope}`;
  const cached = tokenCache.get(cacheKey);
  if (cached && cached.expiresAt > Date.now() + 60_000) {
    return cached.token;
  }

  const clientId = process.env.SOAR_CLIENT_ID;
  const clientSecret = process.env.SOAR_CLIENT_SECRET;
  if (!clientId || !clientSecret) {
    throw new Error('SOAR_CLIENT_ID and SOAR_CLIENT_SECRET must be configured');
  }

  const params = new URLSearchParams({
    grant_type: 'client_credentials',
    client_id: clientId,
    client_secret: clientSecret,
    scope,
  });

  const res = await axios.post(
    `https://login.microsoftonline.com/${tenantId}/oauth2/v2.0/token`,
    params.toString(),
    { headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, timeout: 15000 }
  );

  const token = res.data.access_token;
  const expiresIn = res.data.expires_in || 3600;
  tokenCache.set(cacheKey, { token, expiresAt: Date.now() + expiresIn * 1000 });
  return token;
}

// ── API Helpers ──────────────────────────────────────────────────────────────

const GRAPH_BASE = 'https://graph.microsoft.com/v1.0';
const MDE_BASE = 'https://api.securitycenter.windows.com/api';

async function graphCall(tenantId, method, path, body) {
  const token = await acquireToken(tenantId, 'https://graph.microsoft.com/.default');
  const res = await axios({
    method,
    url: `${GRAPH_BASE}${path}`,
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    data: body || undefined,
    timeout: 30000,
    validateStatus: (s) => s < 500,
  });
  if (res.status >= 400) {
    const errMsg = typeof res.data === 'string' ? res.data : JSON.stringify(res.data);
    throw new Error(`Graph API ${method} ${path} failed (${res.status}): ${errMsg}`);
  }
  if (res.status === 204) return null;
  return res.data;
}

async function mdeCall(tenantId, method, path, body) {
  const token = await acquireToken(tenantId, 'https://api.securitycenter.windows.com/.default');
  const res = await axios({
    method,
    url: `${MDE_BASE}${path}`,
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    data: body || undefined,
    timeout: 30000,
    validateStatus: (s) => s < 500,
  });
  if (res.status >= 400) {
    const errMsg = typeof res.data === 'string' ? res.data : JSON.stringify(res.data);
    throw new Error(`MDE API ${method} ${path} failed (${res.status}): ${errMsg}`);
  }
  if (res.status === 204) return null;
  return res.data;
}

// ── Hostname → MDE Device ID Resolution ─────────────────────────────────────

async function resolveDeviceId(tenantId, hostname) {
  const token = await acquireToken(tenantId, 'https://api.securitycenter.windows.com/.default');
  const res = await axios.get(
    `${MDE_BASE}/machines?$filter=computerDnsName eq '${hostname}'&$top=1`,
    { headers: { Authorization: `Bearer ${token}` }, timeout: 15000 }
  );
  const machines = res.data?.value || [];
  if (machines.length === 0) {
    throw new Error(`Device "${hostname}" not found in MDE for this tenant. Verify the hostname is onboarded to Defender for Endpoint.`);
  }
  return machines[0].id;
}

// ── Temp Password Generator ─────────────────────────────────────────────────

function generateTempPassword() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZabcdefghjkmnpqrstuvwxyz23456789!@#$%';
  let password = 'Td!';
  for (let i = 0; i < 13; i++) {
    password += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return password;
}

// ── User Actions (Graph API) ─────────────────────────────────────────────────

const userActions = {
  async disableUser(tenantId, { upn }) {
    await graphCall(tenantId, 'PATCH', `/users/${encodeURIComponent(upn)}`, { accountEnabled: false });
    return { success: true, message: `User ${upn} disabled`, details: { upn, accountEnabled: false } };
  },

  async enableUser(tenantId, { upn }) {
    await graphCall(tenantId, 'PATCH', `/users/${encodeURIComponent(upn)}`, { accountEnabled: true });
    return { success: true, message: `User ${upn} enabled`, details: { upn, accountEnabled: true } };
  },

  async forcePasswordReset(tenantId, { upn }) {
    const tempPassword = generateTempPassword();
    await graphCall(tenantId, 'PATCH', `/users/${encodeURIComponent(upn)}`, {
      passwordProfile: { password: tempPassword, forceChangePasswordNextSignIn: true },
    });
    return {
      success: true,
      message: `Password reset for ${upn}`,
      details: { upn, tempPassword, forceChangePasswordNextSignIn: true },
    };
  },

  async resetMfa(tenantId, { upn }) {
    const encodedUpn = encodeURIComponent(upn);
    const methods = await graphCall(tenantId, 'GET', `/users/${encodedUpn}/authentication/methods`);
    const nonPasswordMethods = (methods.value || []).filter(
      (m) => !m['@odata.type']?.includes('passwordAuthenticationMethod')
    );

    let deletedCount = 0;
    const errors = [];
    for (const method of nonPasswordMethods) {
      const methodType = method['@odata.type']?.replace('#microsoft.graph.', '').replace('AuthenticationMethod', '');
      const pathMap = {
        phone: 'phoneMethods',
        microsoftAuthenticator: 'microsoftAuthenticatorMethods',
        fido2: 'fido2Methods',
        softwareOath: 'softwareOathMethods',
        email: 'emailMethods',
        temporaryAccessPass: 'temporaryAccessPassMethods',
        windowsHelloForBusiness: 'windowsHelloForBusinessMethods',
      };
      const segment = pathMap[methodType];
      if (!segment) continue;
      try {
        await graphCall(tenantId, 'DELETE', `/users/${encodedUpn}/authentication/${segment}/${method.id}`);
        deletedCount++;
      } catch (err) {
        errors.push(`${segment}/${method.id}: ${err.message}`);
      }
    }

    return {
      success: true,
      message: `Reset MFA for ${upn} — removed ${deletedCount} method(s)`,
      details: { upn, deletedCount, totalMethods: nonPasswordMethods.length, errors: errors.length ? errors : undefined },
    };
  },

  async revokeSessions(tenantId, { upn }) {
    await graphCall(tenantId, 'POST', `/users/${encodeURIComponent(upn)}/revokeSignInSessions`);
    return { success: true, message: `All sessions revoked for ${upn}`, details: { upn } };
  },
};

// ── Device Actions (MDE API) ─────────────────────────────────────────────────

const deviceActions = {
  async quickScan(tenantId, { deviceId }) {
    const result = await mdeCall(tenantId, 'POST', `/machines/${deviceId}/runAntiVirusScan`, {
      Comment: 'OPS Suite SOAR — Quick Scan', ScanType: 'Quick',
    });
    return { success: true, message: `Quick scan initiated on device`, details: { deviceId, machineActionId: result?.id } };
  },

  async fullScan(tenantId, { deviceId }) {
    const result = await mdeCall(tenantId, 'POST', `/machines/${deviceId}/runAntiVirusScan`, {
      Comment: 'OPS Suite SOAR — Full Scan', ScanType: 'Full',
    });
    return { success: true, message: `Full scan initiated on device`, details: { deviceId, machineActionId: result?.id } };
  },

  async restrictExecution(tenantId, { deviceId }) {
    const result = await mdeCall(tenantId, 'POST', `/machines/${deviceId}/restrictCodeExecution`, {
      Comment: 'OPS Suite SOAR — Restrict Code Execution',
    });
    return { success: true, message: `Code execution restricted on device`, details: { deviceId, machineActionId: result?.id } };
  },

  async removeRestriction(tenantId, { deviceId }) {
    const result = await mdeCall(tenantId, 'POST', `/machines/${deviceId}/unrestrictCodeExecution`, {
      Comment: 'OPS Suite SOAR — Remove Execution Restriction',
    });
    return { success: true, message: `Execution restriction removed on device`, details: { deviceId, machineActionId: result?.id } };
  },

  async collectForensics(tenantId, { deviceId }) {
    const result = await mdeCall(tenantId, 'POST', `/machines/${deviceId}/collectInvestigationPackage`, {
      Comment: 'OPS Suite SOAR — Collect Investigation Package',
    });
    return { success: true, message: `Forensics collection initiated on device`, details: { deviceId, machineActionId: result?.id } };
  },

  async startInvestigation(tenantId, { deviceId }) {
    const result = await mdeCall(tenantId, 'POST', `/machines/${deviceId}/startInvestigation`, {
      Comment: 'OPS Suite SOAR — Auto Investigation',
    });
    return { success: true, message: `Auto-investigation started on device`, details: { deviceId, machineActionId: result?.id } };
  },
};

// ── Compound Actions ─────────────────────────────────────────────────────────

const compoundActions = {
  async fullEndpointRemediation(tenantId, { deviceId }) {
    const results = [];
    results.push(await deviceActions.restrictExecution(tenantId, { deviceId }));
    results.push(await deviceActions.fullScan(tenantId, { deviceId }));
    results.push(await deviceActions.collectForensics(tenantId, { deviceId }));
    return {
      success: results.every((r) => r.success),
      message: `Full endpoint remediation: Restrict + Full Scan + Collect Forensics`,
      details: { deviceId, steps: results.map((r) => r.message) },
    };
  },

  async becTriage(tenantId, { upn }) {
    const results = [];
    results.push(await userActions.disableUser(tenantId, { upn }));
    results.push(await userActions.revokeSessions(tenantId, { upn }));
    const resetResult = await userActions.forcePasswordReset(tenantId, { upn });
    results.push(resetResult);
    return {
      success: results.every((r) => r.success),
      message: `BEC triage on ${upn}: Disable + Revoke Sessions + Reset Password`,
      details: { upn, tempPassword: resetResult.details.tempPassword, steps: results.map((r) => r.message) },
    };
  },
};

// ── Action Catalog ───────────────────────────────────────────────────────────

const ACTION_CATALOG = {
  disableUser:    { label: 'Disable User',               category: 'user',     entityType: 'account', severity: 'high',   requires: ['upn'],      description: 'Disable the user account in Entra ID' },
  enableUser:     { label: 'Enable User',                category: 'user',     entityType: 'account', severity: 'low',    requires: ['upn'],      description: 'Re-enable a disabled user account' },
  forcePasswordReset: { label: 'Force Password Reset',   category: 'user',     entityType: 'account', severity: 'medium', requires: ['upn'],      description: 'Reset password and force change on next sign-in' },
  resetMfa:       { label: 'Reset MFA Methods',           category: 'user',     entityType: 'account', severity: 'medium', requires: ['upn'],      description: 'Remove all MFA authentication methods' },
  revokeSessions: { label: 'Revoke All Sessions',         category: 'user',     entityType: 'account', severity: 'medium', requires: ['upn'],      description: 'Invalidate all active sessions and refresh tokens' },
  quickScan:      { label: 'Quick AV Scan',               category: 'device',   entityType: 'host',    severity: 'low',    requires: ['deviceId'], description: 'Run a quick antivirus scan via MDE' },
  fullScan:       { label: 'Full AV Scan',                category: 'device',   entityType: 'host',    severity: 'low',    requires: ['deviceId'], description: 'Run a full antivirus scan via MDE' },
  restrictExecution: { label: 'Restrict Code Execution',  category: 'device',   entityType: 'host',    severity: 'high',   requires: ['deviceId'], description: 'Restrict to Microsoft-signed binaries only' },
  removeRestriction: { label: 'Remove Execution Restriction', category: 'device', entityType: 'host', severity: 'low',    requires: ['deviceId'], description: 'Lift code execution restrictions' },
  collectForensics:  { label: 'Collect Investigation Package', category: 'device', entityType: 'host', severity: 'low',   requires: ['deviceId'], description: 'Collect forensic investigation package from device' },
  startInvestigation: { label: 'Start Auto-Investigation', category: 'device',  entityType: 'host',    severity: 'low',    requires: ['deviceId'], description: 'Initiate MDE automated investigation' },
  fullEndpointRemediation: { label: 'Full Endpoint Remediation', category: 'compound', entityType: 'host', severity: 'high', requires: ['deviceId'], description: 'Restrict execution + Full scan + Collect forensics', steps: ['Restrict code execution to Microsoft-signed binaries', 'Run full antivirus scan', 'Collect forensic investigation package'] },
  becTriage: { label: 'BEC Triage', category: 'compound', entityType: 'account', severity: 'high', requires: ['upn'], description: 'Disable user + Revoke sessions + Reset password', steps: ['Disable user account (blocks all sign-ins)', 'Revoke all active sessions and refresh tokens', 'Reset password and force change on next sign-in'] },
};

// ── Dispatch ─────────────────────────────────────────────────────────────────

const ALL_HANDLERS = { ...userActions, ...deviceActions, ...compoundActions };

function getHandler(actionName) {
  return ALL_HANDLERS[actionName] || null;
}

module.exports = { ACTION_CATALOG, getHandler, resolveDeviceId };
