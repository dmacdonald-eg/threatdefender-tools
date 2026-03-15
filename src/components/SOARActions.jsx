import React, { useState, useEffect, useCallback, useMemo } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { useAuth } from '../contexts/AuthContext';

// ── Action Catalog (static — mirrors backend) ───────────────────────────────

const ACTION_CATALOG = {
  disableUser:    { label: 'Disable User',               category: 'user',     entityType: 'account', severity: 'high',   icon: '🚫', description: 'Disable the user account in Entra ID' },
  enableUser:     { label: 'Enable User',                category: 'user',     entityType: 'account', severity: 'low',    icon: '✅', description: 'Re-enable a disabled user account' },
  forcePasswordReset: { label: 'Force Password Reset',   category: 'user',     entityType: 'account', severity: 'medium', icon: '🔑', description: 'Reset password and force change on next sign-in' },
  resetMfa:       { label: 'Reset MFA Methods',           category: 'user',     entityType: 'account', severity: 'medium', icon: '📱', description: 'Remove all MFA authentication methods' },
  revokeSessions: { label: 'Revoke All Sessions',         category: 'user',     entityType: 'account', severity: 'medium', icon: '🔄', description: 'Invalidate all active sessions and refresh tokens' },
  quickScan:      { label: 'Quick AV Scan',               category: 'device',   entityType: 'host',    severity: 'low',    icon: '🔍', description: 'Run a quick antivirus scan via MDE' },
  fullScan:       { label: 'Full AV Scan',                category: 'device',   entityType: 'host',    severity: 'low',    icon: '🛡️', description: 'Run a full antivirus scan via MDE' },
  restrictExecution: { label: 'Restrict Code Execution',  category: 'device',   entityType: 'host',    severity: 'high',   icon: '🔒', description: 'Restrict to Microsoft-signed binaries only' },
  removeRestriction: { label: 'Remove Execution Restriction', category: 'device', entityType: 'host', severity: 'low',    icon: '🔓', description: 'Lift code execution restrictions' },
  collectForensics:  { label: 'Collect Investigation Package', category: 'device', entityType: 'host', severity: 'low',   icon: '📦', description: 'Collect forensic investigation package from device' },
  startInvestigation: { label: 'Start Auto-Investigation', category: 'device',  entityType: 'host',    severity: 'low',    icon: '🔬', description: 'Initiate MDE automated investigation' },
  fullEndpointRemediation: { label: 'Full Endpoint Remediation', category: 'compound', entityType: 'host', severity: 'high', icon: '⚡', description: 'Restrict execution + Full scan + Collect forensics', steps: ['Restrict code execution to Microsoft-signed binaries', 'Run full antivirus scan', 'Collect forensic investigation package'] },
  becTriage: { label: 'BEC Triage', category: 'compound', entityType: 'account', severity: 'high', icon: '⚡', description: 'Disable user + Revoke sessions + Reset password', steps: ['Disable user account (blocks all sign-ins)', 'Revoke all active sessions and refresh tokens', 'Reset password and force change on next sign-in'] },
};

// ── Entity Type Detection ────────────────────────────────────────────────────

function detectEntityType(input) {
  if (!input || !input.trim()) return null;
  return input.includes('@') ? 'account' : 'host';
}

// ── Severity Badge ───────────────────────────────────────────────────────────

function SeverityBadge({ severity }) {
  const colors = {
    high: 'bg-red-500/20 text-red-400 border-red-500/30',
    medium: 'bg-amber-500/20 text-amber-400 border-amber-500/30',
    low: 'bg-green-500/20 text-green-400 border-green-500/30',
  };
  return (
    <span className={`text-[10px] font-semibold uppercase px-1.5 py-0.5 rounded border ${colors[severity] || colors.low}`}>
      {severity}
    </span>
  );
}

// ── Main Component ───────────────────────────────────────────────────────────

export default function SOARActions({ darkMode }) {
  const { isAuthenticated, account, login, getSentinelWorkspaces } = useAuth();

  // State
  const [workspaces, setWorkspaces] = useState([]);
  const [workspacesLoading, setWorkspacesLoading] = useState(false);
  const [selectedWorkspace, setSelectedWorkspace] = useState(null);
  const [entityInput, setEntityInput] = useState('');
  const [selectedAction, setSelectedAction] = useState(null);
  const [executing, setExecuting] = useState(false);
  const [result, setResult] = useState(null);
  const [error, setError] = useState(null);

  // Derived
  const entityType = useMemo(() => detectEntityType(entityInput), [entityInput]);

  const availableActions = useMemo(() => {
    if (!entityType) return [];
    return Object.entries(ACTION_CATALOG).filter(([, meta]) => meta.entityType === entityType);
  }, [entityType]);

  const userActions = useMemo(() => availableActions.filter(([, m]) => m.category === 'user'), [availableActions]);
  const deviceActions = useMemo(() => availableActions.filter(([, m]) => m.category === 'device'), [availableActions]);
  const compoundActions = useMemo(() => availableActions.filter(([, m]) => m.category === 'compound'), [availableActions]);

  // Styles
  const cardBg = darkMode ? 'bg-gray-800 border-gray-700' : 'bg-white border-gray-200';
  const inputBg = darkMode ? 'bg-gray-700 border-gray-600 text-white placeholder-gray-400' : 'bg-white border-gray-300 text-gray-900 placeholder-gray-500';
  const textPrimary = darkMode ? 'text-white' : 'text-gray-900';
  const textSecondary = darkMode ? 'text-gray-300' : 'text-gray-700';
  const textMuted = darkMode ? 'text-gray-400' : 'text-gray-600';

  // Load workspaces
  useEffect(() => {
    if (!isAuthenticated) return;
    let cancelled = false;

    const load = async () => {
      setWorkspacesLoading(true);
      try {
        const ws = await getSentinelWorkspaces();
        if (!cancelled) {
          const sorted = ws.sort((a, b) => (a.name || '').localeCompare(b.name || ''));
          setWorkspaces(sorted);
        }
      } catch (err) {
        console.error('Failed to load workspaces:', err);
      } finally {
        if (!cancelled) setWorkspacesLoading(false);
      }
    };

    load();
    return () => { cancelled = true; };
  }, [isAuthenticated, getSentinelWorkspaces]);

  // Clear action selection when entity type changes
  useEffect(() => {
    setSelectedAction(null);
    setResult(null);
    setError(null);
  }, [entityType]);

  // Execute action
  const handleExecute = useCallback(async () => {
    if (!selectedWorkspace || !selectedAction || !entityInput.trim()) return;

    const actionMeta = ACTION_CATALOG[selectedAction];
    if (!actionMeta) return;

    setExecuting(true);
    setResult(null);
    setError(null);

    const entityData = entityType === 'account'
      ? { upn: entityInput.trim() }
      : { hostname: entityInput.trim() };

    try {
      const res = await fetch('/api/SoarExecute', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          tenantId: selectedWorkspace.tenantId,
          action: selectedAction,
          entityData,
          analystEmail: account?.username || 'unknown',
          workspaceName: selectedWorkspace.name,
        }),
      });

      const data = await res.json();

      if (!res.ok || !data.success) {
        setError(data.error || data.message || `Action failed (HTTP ${res.status})`);
      } else {
        setResult(data);
      }
    } catch (err) {
      setError(err.message || 'Network error — could not reach backend');
    } finally {
      setExecuting(false);
    }
  }, [selectedWorkspace, selectedAction, entityInput, entityType, account]);

  // Cancel confirmation
  const handleCancel = useCallback(() => {
    setSelectedAction(null);
    setResult(null);
    setError(null);
  }, []);

  // ── Render ─────────────────────────────────────────────────────────────────

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h2 className={`text-2xl font-bold ${textPrimary}`}>SOAR Actions</h2>
        <p className={`text-sm mt-1 ${textMuted}`}>
          Execute response actions across client environments
        </p>
      </div>

      {/* Auth Gate */}
      {!isAuthenticated ? (
        <div className={`rounded-lg border ${cardBg} p-8 text-center`}>
          <p className={`text-lg mb-4 ${textSecondary}`}>Sign in to access SOAR actions</p>
          <button
            onClick={login}
            className="px-6 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-lg font-medium transition-colors"
          >
            Sign in with Microsoft
          </button>
        </div>
      ) : (
        <>
          {/* Environment + Entity Input */}
          <div className={`rounded-lg border ${cardBg} p-6 space-y-4`}>
            {/* Workspace Picker */}
            <div>
              <label className={`block text-sm font-medium mb-1.5 ${textSecondary}`}>Environment</label>
              {workspacesLoading ? (
                <div className={`flex items-center gap-2 ${textMuted}`}>
                  <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-blue-500"></div>
                  <span className="text-sm">Loading workspaces...</span>
                </div>
              ) : workspaces.length === 0 ? (
                <p className={`text-sm ${textMuted}`}>No Sentinel workspaces found</p>
              ) : (
                <select
                  value={selectedWorkspace?.id || ''}
                  onChange={(e) => {
                    const ws = workspaces.find(w => w.id === e.target.value);
                    setSelectedWorkspace(ws || null);
                    setSelectedAction(null);
                    setResult(null);
                    setError(null);
                  }}
                  className={`w-full px-3 py-2 rounded-lg border text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 ${inputBg}`}
                >
                  <option value="">Select workspace...</option>
                  {workspaces.map(ws => (
                    <option key={ws.id} value={ws.id}>
                      {ws.name} ({ws.subscriptionName})
                    </option>
                  ))}
                </select>
              )}
            </div>

            {/* Entity Input */}
            {selectedWorkspace && (
              <motion.div initial={{ opacity: 0, y: -5 }} animate={{ opacity: 1, y: 0 }}>
                <label className={`block text-sm font-medium mb-1.5 ${textSecondary}`}>Target Entity</label>
                <div className="relative">
                  <input
                    type="text"
                    value={entityInput}
                    onChange={(e) => setEntityInput(e.target.value)}
                    placeholder="Enter UPN (user@domain.com) or hostname..."
                    className={`w-full px-3 py-2 pr-24 rounded-lg border text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 ${inputBg}`}
                    onKeyDown={(e) => { if (e.key === 'Escape') { setEntityInput(''); } }}
                  />
                  {/* Entity type badge */}
                  {entityType && (
                    <span className={`absolute right-2 top-1/2 -translate-y-1/2 text-xs font-medium px-2 py-0.5 rounded-full ${
                      entityType === 'account'
                        ? 'bg-blue-500/20 text-blue-400 border border-blue-500/30'
                        : 'bg-purple-500/20 text-purple-400 border border-purple-500/30'
                    }`}>
                      {entityType === 'account' ? 'User' : 'Device'}
                    </span>
                  )}
                </div>
                {entityType === 'host' && (
                  <p className={`text-xs mt-1 ${textMuted}`}>
                    Hostname will be resolved to an MDE device ID when the action executes
                  </p>
                )}
              </motion.div>
            )}

            {/* Tenant warning */}
            {selectedWorkspace && !selectedWorkspace.tenantId && (
              <div className="p-3 bg-amber-900/20 border border-amber-600/30 rounded-lg text-amber-400 text-sm">
                This workspace's subscription does not have a tenant ID mapped. SOAR actions may fail.
              </div>
            )}
          </div>

          {/* Actions Grid */}
          <AnimatePresence mode="wait">
            {entityType && selectedWorkspace && availableActions.length > 0 && (
              <motion.div
                key={entityType}
                initial={{ opacity: 0, y: 10 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, y: -10 }}
                transition={{ duration: 0.2 }}
                className="space-y-4"
              >
                {/* User / Device Actions */}
                {(entityType === 'account' ? userActions : deviceActions).length > 0 && (
                  <ActionGroup
                    title={entityType === 'account' ? 'User Actions' : 'Device Actions'}
                    actions={entityType === 'account' ? userActions : deviceActions}
                    selectedAction={selectedAction}
                    onSelect={setSelectedAction}
                    darkMode={darkMode}
                  />
                )}

                {/* Compound Actions */}
                {compoundActions.length > 0 && (
                  <ActionGroup
                    title="Compound Actions"
                    actions={compoundActions}
                    selectedAction={selectedAction}
                    onSelect={setSelectedAction}
                    darkMode={darkMode}
                  />
                )}
              </motion.div>
            )}
          </AnimatePresence>

          {/* Confirmation + Execution Panel */}
          <AnimatePresence>
            {selectedAction && selectedWorkspace && entityInput.trim() && (
              <motion.div
                initial={{ opacity: 0, y: 10 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, y: -10 }}
                className={`rounded-lg border ${cardBg} p-6 space-y-4`}
              >
                <ConfirmationPanel
                  action={selectedAction}
                  actionMeta={ACTION_CATALOG[selectedAction]}
                  entityInput={entityInput.trim()}
                  entityType={entityType}
                  workspaceName={selectedWorkspace.name}
                  executing={executing}
                  onExecute={handleExecute}
                  onCancel={handleCancel}
                  darkMode={darkMode}
                />

                {/* Error */}
                {error && (
                  <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} className="p-4 bg-red-900/20 border border-red-600/30 rounded-lg text-red-400 text-sm">
                    <span className="font-semibold">Error:</span> {error}
                  </motion.div>
                )}

                {/* Result */}
                {result && (
                  <ResultPanel result={result} darkMode={darkMode} />
                )}
              </motion.div>
            )}
          </AnimatePresence>
        </>
      )}
    </div>
  );
}

// ── Action Group ─────────────────────────────────────────────────────────────

function ActionGroup({ title, actions, selectedAction, onSelect, darkMode }) {
  const textPrimary = darkMode ? 'text-white' : 'text-gray-900';
  const textMuted = darkMode ? 'text-gray-400' : 'text-gray-600';

  return (
    <div>
      <h3 className={`text-sm font-semibold uppercase tracking-wide mb-3 ${textMuted}`}>{title}</h3>
      <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-3">
        {actions.map(([key, meta]) => {
          const isSelected = selectedAction === key;
          return (
            <motion.button
              key={key}
              whileHover={{ y: -2 }}
              whileTap={{ y: 0 }}
              onClick={() => onSelect(isSelected ? null : key)}
              className={`relative text-left p-4 rounded-lg border transition-all ${
                isSelected
                  ? 'border-blue-500 bg-blue-500/10 ring-1 ring-blue-500/50'
                  : darkMode
                    ? 'border-gray-700 bg-gray-800 hover:border-gray-600 hover:bg-gray-750'
                    : 'border-gray-200 bg-white hover:border-gray-300 hover:bg-gray-50'
              }`}
            >
              <div className="flex items-start justify-between mb-2">
                <span className="text-xl">{meta.icon}</span>
                <SeverityBadge severity={meta.severity} />
              </div>
              <div className={`text-sm font-medium ${textPrimary}`}>{meta.label}</div>
              <div className={`text-xs mt-1 ${textMuted} line-clamp-2`}>{meta.description}</div>
              {meta.steps && (
                <div className={`mt-2 pt-2 border-t ${darkMode ? 'border-gray-700' : 'border-gray-200'}`}>
                  <div className={`text-[10px] uppercase font-semibold ${textMuted}`}>Steps</div>
                  {meta.steps.map((step, i) => (
                    <div key={i} className={`text-xs ${textMuted} mt-0.5`}>
                      {i + 1}. {step}
                    </div>
                  ))}
                </div>
              )}
            </motion.button>
          );
        })}
      </div>
    </div>
  );
}

// ── Confirmation Panel ───────────────────────────────────────────────────────

function ConfirmationPanel({ action, actionMeta, entityInput, entityType, workspaceName, executing, onExecute, onCancel, darkMode }) {
  const textPrimary = darkMode ? 'text-white' : 'text-gray-900';
  const textMuted = darkMode ? 'text-gray-400' : 'text-gray-600';

  return (
    <div>
      <div className="flex items-center gap-3 mb-3">
        <span className="text-2xl">{actionMeta.icon}</span>
        <div>
          <div className={`font-semibold ${textPrimary}`}>{actionMeta.label}</div>
          <div className={`text-sm ${textMuted}`}>{actionMeta.description}</div>
        </div>
      </div>

      {actionMeta.severity === 'high' && (
        <div className="p-3 mb-3 bg-red-900/20 border border-red-600/30 rounded-lg text-red-400 text-sm flex items-center gap-2">
          <span className="text-lg">&#9888;</span>
          <span><strong>High severity action</strong> — this will immediately impact the target. Proceed with caution.</span>
        </div>
      )}

      <div className={`text-sm space-y-1 mb-4 ${textMuted}`}>
        <div><span className="font-medium">Target:</span> <span className={textPrimary}>{entityInput}</span> <span className="text-xs">({entityType === 'account' ? 'User' : 'Device'})</span></div>
        <div><span className="font-medium">Environment:</span> <span className={textPrimary}>{workspaceName}</span></div>
      </div>

      <div className="flex items-center gap-3">
        <button
          onClick={onCancel}
          disabled={executing}
          className={`px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
            darkMode ? 'bg-gray-700 hover:bg-gray-600 text-gray-300' : 'bg-gray-200 hover:bg-gray-300 text-gray-700'
          } disabled:opacity-50`}
        >
          Cancel
        </button>
        <button
          onClick={onExecute}
          disabled={executing}
          className={`px-4 py-2 rounded-lg text-sm font-medium text-white transition-colors disabled:opacity-50 flex items-center gap-2 ${
            actionMeta.severity === 'high'
              ? 'bg-red-600 hover:bg-red-700'
              : 'bg-blue-600 hover:bg-blue-700'
          }`}
        >
          {executing ? (
            <>
              <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-white"></div>
              Executing...
            </>
          ) : (
            'Confirm & Execute'
          )}
        </button>
      </div>
    </div>
  );
}

// ── Result Panel ─────────────────────────────────────────────────────────────

function ResultPanel({ result, darkMode }) {
  const textPrimary = darkMode ? 'text-white' : 'text-gray-900';
  const textMuted = darkMode ? 'text-gray-400' : 'text-gray-600';

  return (
    <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} className="p-4 bg-green-900/20 border border-green-600/30 rounded-lg space-y-2">
      <div className="flex items-center gap-2 text-green-400 font-semibold text-sm">
        <span>&#10003;</span> {result.message}
      </div>

      {/* Temp password display */}
      {result.details?.tempPassword && (
        <div className={`p-3 rounded-lg ${darkMode ? 'bg-gray-900' : 'bg-gray-100'}`}>
          <div className={`text-xs font-medium mb-1 ${textMuted}`}>Temporary Password (copy now — will not be shown again)</div>
          <code className={`text-sm font-mono select-all cursor-text ${textPrimary}`}>
            {result.details.tempPassword}
          </code>
        </div>
      )}

      {/* Compound action steps */}
      {result.details?.steps && (
        <div className="space-y-1">
          <div className={`text-xs font-medium ${textMuted}`}>Completed Steps</div>
          {result.details.steps.map((step, i) => (
            <div key={i} className="text-sm text-green-400 flex items-center gap-1.5">
              <span>&#10003;</span> {step}
            </div>
          ))}
        </div>
      )}

      {/* MDE action ID */}
      {result.details?.machineActionId && (
        <div className={`text-xs ${textMuted}`}>
          MDE Action ID: <code className="font-mono">{result.details.machineActionId}</code>
        </div>
      )}

      {/* MFA reset details */}
      {result.details?.deletedCount !== undefined && (
        <div className={`text-xs ${textMuted}`}>
          Removed {result.details.deletedCount} of {result.details.totalMethods} MFA method(s)
          {result.details.errors?.length > 0 && (
            <span className="text-amber-400"> ({result.details.errors.length} failed)</span>
          )}
        </div>
      )}
    </motion.div>
  );
}
