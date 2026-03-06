import React, { useState, useCallback, useMemo } from 'react';
import { useAuth } from '../contexts/AuthContext';
import { motion } from 'framer-motion';
import {
  BarChart, Bar, PieChart, Pie, Cell, LineChart, Line,
  XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend
} from 'recharts';

// ── KQL Queries ──────────────────────────────────────────────────────────────

function buildIncidentQuery(startDate, endDate) {
  return `
SecurityIncident
| where CreatedTime between (datetime('${startDate}') .. datetime('${endDate}'))
| summarize arg_max(TimeGenerated, *) by IncidentNumber
| extend ClosedTimeActual = iff(Status == "Closed", ClosedTime, datetime(null))
| extend ResolutionMinutes = iff(isnotempty(ClosedTimeActual), datetime_diff('minute', ClosedTimeActual, CreatedTime), int(null))
| extend FirstModified = iff(FirstModifiedTime != CreatedTime, FirstModifiedTime, datetime(null))
| extend TriageMinutes = iff(isnotempty(FirstModified), datetime_diff('minute', FirstModified, CreatedTime), int(null))
| extend Tactics = tostring(AdditionalData.tactics)
| extend Techniques = tostring(AdditionalData.techniques)
| project IncidentNumber, Title, Severity, Status, Classification,
          CreatedTime, ClosedTime = ClosedTimeActual, ResolutionMinutes,
          FirstModifiedTime = FirstModified, TriageMinutes,
          Owner, Labels, Tactics, Techniques, IncidentUrl
| order by CreatedTime desc
  `.trim();
}

// ── Helpers ──────────────────────────────────────────────────────────────────

const SEVERITY_ORDER = { High: 0, Medium: 1, Low: 2, Informational: 3 };
const SEVERITY_COLORS = { High: '#ef4444', Medium: '#f59e0b', Low: '#3b82f6', Informational: '#6b7280' };
const PIE_COLORS = ['#ef4444', '#f59e0b', '#3b82f6', '#6b7280', '#10b981', '#8b5cf6', '#ec4899'];

function formatMinutes(mins) {
  if (mins == null || isNaN(mins)) return '—';
  if (mins < 60) return `${Math.round(mins)}m`;
  const h = Math.floor(mins / 60);
  const m = Math.round(mins % 60);
  return m > 0 ? `${h}h ${m}m` : `${h}h`;
}

function getMonthOptions() {
  const opts = [];
  const now = new Date();
  for (let i = 0; i < 12; i++) {
    const d = new Date(now.getFullYear(), now.getMonth() - i, 1);
    const label = d.toLocaleString('default', { month: 'long', year: 'numeric' });
    const start = d.toISOString().split('T')[0];
    const end = new Date(d.getFullYear(), d.getMonth() + 1, 0);
    end.setHours(23, 59, 59);
    opts.push({ label, start, end: end.toISOString().split('T')[0] });
  }
  return opts;
}

function parseLogAnalyticsRows(response) {
  if (!response?.tables?.[0]) return [];
  const table = response.tables[0];
  const cols = table.columns.map(c => c.name);
  return table.rows.map(row => {
    const obj = {};
    cols.forEach((col, i) => { obj[col] = row[i]; });
    return obj;
  });
}

function classifyCategory(title) {
  const t = (title || '').toLowerCase();
  if (t.includes('phish') || t.includes('email') || t.includes('zap') || t.includes('mail') || t.includes('spam')) return 'Email / Phishing';
  if (t.includes('sign-in') || t.includes('login') || t.includes('brute') || t.includes('mfa') || t.includes('identity') || t.includes('password') || t.includes('credential') || t.includes('entra') || t.includes('aad')) return 'Identity';
  if (t.includes('malware') || t.includes('ransomware') || t.includes('endpoint') || t.includes('defender') || t.includes('software') || t.includes('process')) return 'Endpoint';
  if (t.includes('dlp') || t.includes('exfil') || t.includes('data') || t.includes('sharepoint') || t.includes('onedrive')) return 'Data';
  if (t.includes('log') || t.includes('ingestion') || t.includes('monitor') || t.includes('health')) return 'Operations';
  return 'Other';
}

function parseTactics(tacticsStr) {
  if (!tacticsStr || tacticsStr === '[]') return [];
  try {
    const parsed = JSON.parse(tacticsStr);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return tacticsStr.split(',').map(s => s.trim()).filter(Boolean);
  }
}

// ── Cache ────────────────────────────────────────────────────────────────────

const CACHE_TTL = 15 * 60 * 1000; // 15 min

function getCached(key) {
  try {
    const raw = sessionStorage.getItem(key);
    if (!raw) return null;
    const { data, ts } = JSON.parse(raw);
    if (Date.now() - ts > CACHE_TTL) { sessionStorage.removeItem(key); return null; }
    return data;
  } catch { return null; }
}

function setCache(key, data) {
  try { sessionStorage.setItem(key, JSON.stringify({ data, ts: Date.now() })); } catch { /* full */ }
}

// ── Component ────────────────────────────────────────────────────────────────

export default function MonthlyReportDashboard({ darkMode }) {
  const { isAuthenticated, isMsalAvailable, login, getSentinelWorkspaces, fetchFromLogAnalytics } = useAuth();

  const [workspaces, setWorkspaces] = useState([]);
  const [selectedWorkspace, setSelectedWorkspace] = useState(null);
  const [selectedMonth, setSelectedMonth] = useState(0);
  const [incidents, setIncidents] = useState(null);
  const [loading, setLoading] = useState(false);
  const [loadingWorkspaces, setLoadingWorkspaces] = useState(false);
  const [error, setError] = useState(null);

  const monthOptions = useMemo(() => getMonthOptions(), []);

  // Load workspaces
  const loadWorkspaces = useCallback(async () => {
    setLoadingWorkspaces(true);
    setError(null);
    try {
      const ws = await getSentinelWorkspaces();
      setWorkspaces(ws);
      if (ws.length > 0) setSelectedWorkspace(ws[0]);
    } catch (err) {
      setError(`Failed to load workspaces: ${err.message}`);
    } finally {
      setLoadingWorkspaces(false);
    }
  }, [getSentinelWorkspaces]);

  // Fetch incident data
  const fetchReport = useCallback(async () => {
    if (!selectedWorkspace) return;
    setLoading(true);
    setError(null);

    const month = monthOptions[selectedMonth];
    const cacheKey = `monthly_report_${selectedWorkspace.customerId}_${month.start}`;
    const cached = getCached(cacheKey);
    if (cached) {
      setIncidents(cached);
      setLoading(false);
      return;
    }

    try {
      const query = buildIncidentQuery(month.start, month.end);
      const result = await fetchFromLogAnalytics(selectedWorkspace.customerId, query);
      const rows = parseLogAnalyticsRows(result);
      setIncidents(rows);
      setCache(cacheKey, rows);
    } catch (err) {
      setError(`Query failed: ${err.message}`);
      setIncidents(null);
    } finally {
      setLoading(false);
    }
  }, [selectedWorkspace, selectedMonth, monthOptions, fetchFromLogAnalytics]);

  // ── Computed metrics ─────────────────────────────────────────────────────

  const metrics = useMemo(() => {
    if (!incidents || incidents.length === 0) return null;

    const total = incidents.length;
    const bySeverity = {};
    const byCategory = {};
    const tacticsMap = {};
    let analystTouched = 0;
    let truePositives = 0;
    let falsePositives = 0;
    let benignPositives = 0;

    // Resolution times for Medium+High only
    const analystResolutions = [];
    // Triage times for Medium+High only
    const analystTriageTimes = [];
    // All resolution times
    const allResolutions = [];

    incidents.forEach(inc => {
      const sev = inc.Severity || 'Informational';
      bySeverity[sev] = (bySeverity[sev] || 0) + 1;

      const cat = classifyCategory(inc.Title);
      if (!byCategory[cat]) byCategory[cat] = { count: 0, incidents: [] };
      byCategory[cat].count++;
      byCategory[cat].incidents.push(inc);

      const isAnalyst = sev === 'High' || sev === 'Medium';
      if (isAnalyst) analystTouched++;

      const classification = (inc.Classification || '').toLowerCase();
      if (classification.includes('true')) truePositives++;
      else if (classification.includes('false')) falsePositives++;
      else if (classification.includes('benign')) benignPositives++;

      if (inc.ResolutionMinutes != null && inc.ResolutionMinutes > 0) {
        allResolutions.push(inc.ResolutionMinutes);
        if (isAnalyst) analystResolutions.push(inc.ResolutionMinutes);
      }

      if (inc.TriageMinutes != null && inc.TriageMinutes > 0 && isAnalyst) {
        analystTriageTimes.push(inc.TriageMinutes);
      }

      parseTactics(inc.Tactics).forEach(t => {
        tacticsMap[t] = (tacticsMap[t] || 0) + 1;
      });
    });

    const avgMTTR = analystResolutions.length > 0
      ? analystResolutions.reduce((a, b) => a + b, 0) / analystResolutions.length
      : null;

    const avgMTTT = analystTriageTimes.length > 0
      ? analystTriageTimes.reduce((a, b) => a + b, 0) / analystTriageTimes.length
      : null;

    const overallMTTR = allResolutions.length > 0
      ? allResolutions.reduce((a, b) => a + b, 0) / allResolutions.length
      : null;

    const classifiedCount = truePositives + falsePositives + benignPositives;
    const fpRate = classifiedCount > 0
      ? ((falsePositives + benignPositives) / classifiedCount * 100).toFixed(1)
      : null;

    // Severity table data
    const severityTable = Object.entries(bySeverity)
      .sort((a, b) => (SEVERITY_ORDER[a[0]] ?? 99) - (SEVERITY_ORDER[b[0]] ?? 99))
      .map(([sev, count]) => {
        const sevIncidents = incidents.filter(i => i.Severity === sev);
        const resolutions = sevIncidents
          .map(i => i.ResolutionMinutes)
          .filter(r => r != null && r > 0);
        const avgRes = resolutions.length > 0
          ? resolutions.reduce((a, b) => a + b, 0) / resolutions.length
          : null;
        const isAnalyst = sev === 'High' || sev === 'Medium';
        return {
          severity: sev,
          count,
          pct: ((count / total) * 100).toFixed(1),
          avgResolution: avgRes,
          handling: isAnalyst ? 'Analyst-Investigated' : sev === 'Low' ? 'Automation + Review' : 'Fully Automated',
        };
      });

    // Category table
    const categoryTable = Object.entries(byCategory)
      .sort((a, b) => b[1].count - a[1].count)
      .map(([cat, data]) => ({ category: cat, count: data.count }));

    // Tactics chart data
    const tacticsData = Object.entries(tacticsMap)
      .sort((a, b) => b[1] - a[1])
      .map(([tactic, count]) => ({ tactic, count }));

    // Notable incidents (High severity TPs or anything High)
    const notable = incidents
      .filter(i => i.Severity === 'High' || (i.Classification || '').toLowerCase().includes('true'))
      .sort((a, b) => (SEVERITY_ORDER[a.Severity] ?? 99) - (SEVERITY_ORDER[b.Severity] ?? 99))
      .slice(0, 5);

    return {
      total, analystTouched, truePositives, fpRate,
      avgMTTR, avgMTTT, overallMTTR,
      severityTable, categoryTable, tacticsData, notable,
      bySeverity, byCategory,
    };
  }, [incidents]);

  // ── Chart data ───────────────────────────────────────────────────────────

  const severityPieData = useMemo(() => {
    if (!metrics) return [];
    return Object.entries(metrics.bySeverity)
      .sort((a, b) => (SEVERITY_ORDER[a[0]] ?? 99) - (SEVERITY_ORDER[b[0]] ?? 99))
      .map(([name, value]) => ({ name, value }));
  }, [metrics]);

  const categoryBarData = useMemo(() => {
    if (!metrics) return [];
    return metrics.categoryTable;
  }, [metrics]);

  // ── Styles ───────────────────────────────────────────────────────────────

  const card = `rounded-xl border ${darkMode ? 'bg-gray-800 border-gray-700' : 'bg-white border-gray-200'}`;
  const cardHeader = `text-lg font-semibold mb-4 ${darkMode ? 'text-white' : 'text-gray-900'}`;
  const subtle = darkMode ? 'text-gray-400' : 'text-gray-500';
  const tableHeader = `text-left text-xs font-semibold uppercase tracking-wider ${darkMode ? 'text-gray-400' : 'text-gray-500'}`;
  const tableCell = `py-2 text-sm ${darkMode ? 'text-gray-300' : 'text-gray-700'}`;

  // ── Auth gate ────────────────────────────────────────────────────────────

  if (!isMsalAvailable) {
    return (
      <div className={`${card} p-8 text-center`}>
        <p className={`text-lg ${darkMode ? 'text-gray-300' : 'text-gray-700'}`}>
          Azure AD authentication is required for the Monthly Report Dashboard.
        </p>
        <p className={`text-sm mt-2 ${subtle}`}>Configure MSAL in your environment to enable this feature.</p>
      </div>
    );
  }

  if (!isAuthenticated) {
    return (
      <div className={`${card} p-8 text-center`}>
        <h2 className={cardHeader}>Monthly Incident Report</h2>
        <p className={`mb-4 ${subtle}`}>Sign in to access Sentinel incident data across your client workspaces.</p>
        <button onClick={login} className="px-6 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors">
          Sign In with Azure AD
        </button>
      </div>
    );
  }

  // ── Render ───────────────────────────────────────────────────────────────

  return (
    <div className="space-y-6">
      {/* Page Header */}
      <div>
        <h2 className={`text-2xl font-bold ${darkMode ? 'text-white' : 'text-gray-900'}`}>
          📊 Monthly Incident Summary Report
        </h2>
        <p className={subtle}>Proof-of-value reporting across client Sentinel workspaces</p>
      </div>

      {/* Controls */}
      <div className={`${card} p-5`}>
        <div className="flex flex-wrap items-end gap-4">
          {/* Workspace selector */}
          <div className="flex-1 min-w-[200px]">
            <label className={`block text-sm font-medium mb-1 ${darkMode ? 'text-gray-300' : 'text-gray-700'}`}>
              Client Workspace
            </label>
            {workspaces.length > 0 ? (
              <select
                value={selectedWorkspace?.customerId || ''}
                onChange={(e) => setSelectedWorkspace(workspaces.find(w => w.customerId === e.target.value))}
                className={`w-full px-3 py-2 rounded-lg border text-sm ${
                  darkMode
                    ? 'bg-gray-700 border-gray-600 text-white'
                    : 'bg-white border-gray-300 text-gray-900'
                }`}
              >
                {workspaces.map(ws => (
                  <option key={ws.customerId} value={ws.customerId}>
                    {ws.name} ({ws.subscriptionName})
                  </option>
                ))}
              </select>
            ) : (
              <button
                onClick={loadWorkspaces}
                disabled={loadingWorkspaces}
                className="px-4 py-2 bg-blue-600 text-white text-sm rounded-lg hover:bg-blue-700 disabled:opacity-50 transition-colors"
              >
                {loadingWorkspaces ? 'Loading Workspaces...' : 'Load Workspaces'}
              </button>
            )}
          </div>

          {/* Month selector */}
          <div className="min-w-[180px]">
            <label className={`block text-sm font-medium mb-1 ${darkMode ? 'text-gray-300' : 'text-gray-700'}`}>
              Reporting Period
            </label>
            <select
              value={selectedMonth}
              onChange={(e) => setSelectedMonth(parseInt(e.target.value))}
              className={`w-full px-3 py-2 rounded-lg border text-sm ${
                darkMode
                  ? 'bg-gray-700 border-gray-600 text-white'
                  : 'bg-white border-gray-300 text-gray-900'
              }`}
            >
              {monthOptions.map((opt, i) => (
                <option key={opt.start} value={i}>{opt.label}</option>
              ))}
            </select>
          </div>

          {/* Generate button */}
          <button
            onClick={fetchReport}
            disabled={loading || !selectedWorkspace}
            className="px-6 py-2 bg-blue-600 text-white text-sm font-medium rounded-lg hover:bg-blue-700 disabled:opacity-50 transition-colors"
          >
            {loading ? (
              <span className="flex items-center gap-2">
                <span className="inline-block w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin"></span>
                Querying Sentinel...
              </span>
            ) : 'Generate Report'}
          </button>
        </div>
      </div>

      {/* Error */}
      {error && (
        <div className="p-4 rounded-lg bg-red-900/30 border border-red-700 text-red-300 text-sm">
          {error}
        </div>
      )}

      {/* Loading */}
      {loading && (
        <div className="flex items-center justify-center py-16">
          <div className="text-center">
            <div className="inline-block animate-spin rounded-full h-12 w-12 border-b-2 border-blue-500 mb-4"></div>
            <p className={subtle}>Querying incident data from Sentinel...</p>
          </div>
        </div>
      )}

      {/* Report Content */}
      {metrics && !loading && (
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.3 }}
          className="space-y-6"
        >
          {/* KPI Cards */}
          <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
            <KPICard darkMode={darkMode} label="Total Incidents" sublabel="All severities" value={metrics.total} />
            <KPICard darkMode={darkMode} label="Analyst-Investigated" sublabel="Medium + High" value={metrics.analystTouched} />
            <KPICard darkMode={darkMode} label="Mean Time to Triage" sublabel="Medium + High only" value={formatMinutes(metrics.avgMTTT)} />
            <KPICard darkMode={darkMode} label="Human MTTR" sublabel="Mean Time to Resolve" value={formatMinutes(metrics.avgMTTR)} />
            <KPICard darkMode={darkMode} label="True Positives" sublabel="Confirmed threats" value={metrics.truePositives} />
            <KPICard darkMode={darkMode} label="Benign/FP Rate" sublabel="Of classified incidents" value={metrics.fpRate != null ? `${metrics.fpRate}%` : '—'} />
          </div>

          {/* Charts Row */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            {/* Severity Breakdown Pie */}
            <div className={`${card} p-5`}>
              <h3 className={cardHeader}>Incidents by Severity</h3>
              <div className="h-64">
                <ResponsiveContainer width="100%" height="100%">
                  <PieChart>
                    <Pie
                      data={severityPieData}
                      cx="50%"
                      cy="50%"
                      innerRadius={60}
                      outerRadius={100}
                      dataKey="value"
                      label={({ name, value, percent }) => `${name}: ${value} (${(percent * 100).toFixed(0)}%)`}
                    >
                      {severityPieData.map((entry) => (
                        <Cell key={entry.name} fill={SEVERITY_COLORS[entry.name] || '#6b7280'} />
                      ))}
                    </Pie>
                    <Tooltip
                      contentStyle={{
                        backgroundColor: darkMode ? '#1f2937' : '#fff',
                        border: `1px solid ${darkMode ? '#374151' : '#e5e7eb'}`,
                        borderRadius: '8px',
                        color: darkMode ? '#e5e7eb' : '#111827',
                      }}
                    />
                  </PieChart>
                </ResponsiveContainer>
              </div>
            </div>

            {/* Category Bar Chart */}
            <div className={`${card} p-5`}>
              <h3 className={cardHeader}>Incidents by Category</h3>
              <div className="h-64">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={categoryBarData} layout="vertical" margin={{ left: 20 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke={darkMode ? '#374151' : '#e5e7eb'} />
                    <XAxis type="number" tick={{ fill: darkMode ? '#9ca3af' : '#6b7280', fontSize: 12 }} />
                    <YAxis type="category" dataKey="category" width={120} tick={{ fill: darkMode ? '#9ca3af' : '#6b7280', fontSize: 12 }} />
                    <Tooltip
                      contentStyle={{
                        backgroundColor: darkMode ? '#1f2937' : '#fff',
                        border: `1px solid ${darkMode ? '#374151' : '#e5e7eb'}`,
                        borderRadius: '8px',
                        color: darkMode ? '#e5e7eb' : '#111827',
                      }}
                    />
                    <Bar dataKey="count" fill="#3b82f6" radius={[0, 4, 4, 0]} />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </div>
          </div>

          {/* Severity Table */}
          <div className={`${card} p-5`}>
            <h3 className={cardHeader}>Incident Breakdown by Severity</h3>
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead>
                  <tr className={`border-b ${darkMode ? 'border-gray-700' : 'border-gray-200'}`}>
                    <th className={`${tableHeader} pb-3 pr-4`}>Severity</th>
                    <th className={`${tableHeader} pb-3 pr-4 text-right`}>Count</th>
                    <th className={`${tableHeader} pb-3 pr-4 text-right`}>% of Total</th>
                    <th className={`${tableHeader} pb-3 pr-4 text-right`}>Avg. Resolution</th>
                    <th className={`${tableHeader} pb-3`}>Handling</th>
                  </tr>
                </thead>
                <tbody>
                  {metrics.severityTable.map(row => (
                    <tr key={row.severity} className={`border-b ${darkMode ? 'border-gray-700/50' : 'border-gray-100'}`}>
                      <td className={tableCell}>
                        <span className="inline-flex items-center gap-2">
                          <span className="w-2.5 h-2.5 rounded-full" style={{ backgroundColor: SEVERITY_COLORS[row.severity] }}></span>
                          {row.severity}
                        </span>
                      </td>
                      <td className={`${tableCell} text-right font-medium`}>{row.count}</td>
                      <td className={`${tableCell} text-right`}>{row.pct}%</td>
                      <td className={`${tableCell} text-right`}>{formatMinutes(row.avgResolution)}</td>
                      <td className={tableCell}>
                        <span className={`inline-block px-2 py-0.5 rounded text-xs font-medium ${
                          row.handling === 'Analyst-Investigated'
                            ? 'bg-blue-500/20 text-blue-400'
                            : row.handling === 'Fully Automated'
                            ? 'bg-green-500/20 text-green-400'
                            : 'bg-yellow-500/20 text-yellow-400'
                        }`}>
                          {row.handling}
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <p className={`mt-3 text-xs ${subtle}`}>
              Note: Informational incidents (email ZAP removals, quarantine releases) are handled entirely by automation playbooks
              and do not require analyst intervention.
            </p>
          </div>

          {/* MITRE ATT&CK Tactics */}
          {metrics.tacticsData.length > 0 && (
            <div className={`${card} p-5`}>
              <h3 className={cardHeader}>MITRE ATT&CK Tactics Observed</h3>
              <div className="h-64">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={metrics.tacticsData} margin={{ bottom: 60 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke={darkMode ? '#374151' : '#e5e7eb'} />
                    <XAxis
                      dataKey="tactic"
                      tick={{ fill: darkMode ? '#9ca3af' : '#6b7280', fontSize: 11 }}
                      angle={-35}
                      textAnchor="end"
                      interval={0}
                    />
                    <YAxis tick={{ fill: darkMode ? '#9ca3af' : '#6b7280', fontSize: 12 }} allowDecimals={false} />
                    <Tooltip
                      contentStyle={{
                        backgroundColor: darkMode ? '#1f2937' : '#fff',
                        border: `1px solid ${darkMode ? '#374151' : '#e5e7eb'}`,
                        borderRadius: '8px',
                        color: darkMode ? '#e5e7eb' : '#111827',
                      }}
                    />
                    <Bar dataKey="count" fill="#8b5cf6" radius={[4, 4, 0, 0]} />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </div>
          )}

          {/* Notable Incidents */}
          {metrics.notable.length > 0 && (
            <div className={`${card} p-5`}>
              <h3 className={cardHeader}>Notable Incidents</h3>
              <div className="space-y-3">
                {metrics.notable.map(inc => (
                  <NotableIncidentCard key={inc.IncidentNumber} incident={inc} darkMode={darkMode} />
                ))}
              </div>
            </div>
          )}

          {/* Category Details Table */}
          <div className={`${card} p-5`}>
            <h3 className={cardHeader}>Incidents by Category</h3>
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead>
                  <tr className={`border-b ${darkMode ? 'border-gray-700' : 'border-gray-200'}`}>
                    <th className={`${tableHeader} pb-3 pr-4`}>Category</th>
                    <th className={`${tableHeader} pb-3 text-right`}>Count</th>
                  </tr>
                </thead>
                <tbody>
                  {metrics.categoryTable.map(row => (
                    <tr key={row.category} className={`border-b ${darkMode ? 'border-gray-700/50' : 'border-gray-100'}`}>
                      <td className={tableCell}>{row.category}</td>
                      <td className={`${tableCell} text-right font-medium`}>{row.count}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          {/* Export */}
          <div className={`${card} p-5`}>
            <div className="flex items-center justify-between">
              <div>
                <h3 className={cardHeader}>Export Report</h3>
                <p className={`text-sm ${subtle}`}>Download a formatted report for client delivery</p>
              </div>
              <div className="flex gap-3">
                <button
                  onClick={() => exportReport(metrics, incidents, selectedWorkspace, monthOptions[selectedMonth], 'html')}
                  className={`px-5 py-2.5 text-sm font-medium rounded-lg transition-colors border ${
                    darkMode
                      ? 'border-gray-600 text-gray-300 hover:bg-gray-700'
                      : 'border-gray-300 text-gray-700 hover:bg-gray-50'
                  }`}
                >
                  Download HTML
                </button>
                <button
                  onClick={() => exportReport(metrics, incidents, selectedWorkspace, monthOptions[selectedMonth], 'pdf')}
                  className="px-5 py-2.5 bg-blue-600 text-white text-sm font-medium rounded-lg hover:bg-blue-700 transition-colors"
                >
                  Save as PDF
                </button>
              </div>
            </div>
          </div>
        </motion.div>
      )}

      {/* Empty state */}
      {incidents && incidents.length === 0 && !loading && (
        <div className={`${card} p-12 text-center`}>
          <p className={`text-lg ${darkMode ? 'text-gray-300' : 'text-gray-700'}`}>No incidents found for this period.</p>
          <p className={`text-sm mt-2 ${subtle}`}>Try selecting a different month or workspace.</p>
        </div>
      )}
    </div>
  );
}

// ── Sub-components ───────────────────────────────────────────────────────────

function KPICard({ darkMode, label, sublabel, value }) {
  return (
    <div className={`rounded-xl border p-4 ${darkMode ? 'bg-gray-800 border-gray-700' : 'bg-white border-gray-200'}`}>
      <p className={`text-2xl font-bold ${darkMode ? 'text-white' : 'text-gray-900'}`}>{value}</p>
      <p className={`text-sm font-medium mt-1 ${darkMode ? 'text-gray-300' : 'text-gray-700'}`}>{label}</p>
      <p className={`text-xs ${darkMode ? 'text-gray-500' : 'text-gray-400'}`}>{sublabel}</p>
    </div>
  );
}

function NotableIncidentCard({ incident, darkMode }) {
  const [expanded, setExpanded] = useState(false);
  const classification = (incident.Classification || 'Undetermined');
  const tactics = parseTactics(incident.Tactics);

  return (
    <div
      className={`rounded-lg border p-4 cursor-pointer transition-colors ${
        darkMode
          ? 'bg-gray-900/50 border-gray-700 hover:border-gray-600'
          : 'bg-gray-50 border-gray-200 hover:border-gray-300'
      }`}
      onClick={() => setExpanded(!expanded)}
    >
      <div className="flex items-start justify-between gap-4">
        <div className="flex-1">
          <div className="flex items-center gap-2 flex-wrap">
            <span className="inline-block w-2.5 h-2.5 rounded-full" style={{ backgroundColor: SEVERITY_COLORS[incident.Severity] }}></span>
            <span className={`text-sm font-semibold ${darkMode ? 'text-white' : 'text-gray-900'}`}>
              #{incident.IncidentNumber} — {incident.Title}
            </span>
          </div>
          <div className={`flex items-center gap-3 mt-1 text-xs ${darkMode ? 'text-gray-400' : 'text-gray-500'}`}>
            <span>Severity: {incident.Severity}</span>
            <span>Classification: {classification}</span>
            {tactics.length > 0 && <span>MITRE: {tactics.join(', ')}</span>}
          </div>
        </div>
        <span className={`text-xs ${darkMode ? 'text-gray-500' : 'text-gray-400'}`}>
          {expanded ? '▲' : '▼'}
        </span>
      </div>
      {expanded && (
        <div className={`mt-3 pt-3 border-t text-sm ${darkMode ? 'border-gray-700 text-gray-300' : 'border-gray-200 text-gray-600'}`}>
          <div className="grid grid-cols-2 gap-2 text-xs">
            <div><strong>Created:</strong> {new Date(incident.CreatedTime).toLocaleString()}</div>
            <div><strong>Closed:</strong> {incident.ClosedTime ? new Date(incident.ClosedTime).toLocaleString() : 'Open'}</div>
            <div><strong>Resolution:</strong> {formatMinutes(incident.ResolutionMinutes)}</div>
            <div><strong>Status:</strong> {incident.Status}</div>
          </div>
        </div>
      )}
    </div>
  );
}

// ── Export ────────────────────────────────────────────────────────────────────

function exportReport(metrics, incidents, workspace, month, mode = 'html') {
  const html = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Monthly Incident Summary Report - ${workspace.name} - ${month.label}</title>
<style>
  body { font-family: 'Segoe UI', system-ui, -apple-system, sans-serif; max-width: 900px; margin: 0 auto; padding: 40px 20px; color: #1f2937; line-height: 1.6; }
  h1 { color: #1e40af; border-bottom: 3px solid #1e40af; padding-bottom: 12px; }
  h2 { color: #1e3a5f; margin-top: 32px; border-bottom: 1px solid #e5e7eb; padding-bottom: 8px; }
  .header { text-align: center; margin-bottom: 40px; }
  .header .brand { font-size: 14px; color: #6b7280; text-transform: uppercase; letter-spacing: 2px; }
  .header .subtitle { color: #6b7280; }
  .kpis { display: grid; grid-template-columns: repeat(6, 1fr); gap: 16px; margin: 24px 0; }
  .kpi { background: #f9fafb; border: 1px solid #e5e7eb; border-radius: 8px; padding: 16px; text-align: center; }
  .kpi .value { font-size: 28px; font-weight: 700; color: #1e40af; }
  .kpi .label { font-size: 13px; color: #374151; margin-top: 4px; }
  .kpi .sublabel { font-size: 11px; color: #9ca3af; }
  table { width: 100%; border-collapse: collapse; margin: 16px 0; }
  th { text-align: left; padding: 10px 12px; background: #f3f4f6; border-bottom: 2px solid #d1d5db; font-size: 12px; text-transform: uppercase; color: #6b7280; }
  td { padding: 10px 12px; border-bottom: 1px solid #e5e7eb; font-size: 14px; }
  .severity-dot { display: inline-block; width: 10px; height: 10px; border-radius: 50%; margin-right: 6px; }
  .badge { display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 12px; font-weight: 500; }
  .badge-analyst { background: #dbeafe; color: #1e40af; }
  .badge-auto { background: #d1fae5; color: #065f46; }
  .badge-review { background: #fef3c7; color: #92400e; }
  .notable { background: #f9fafb; border: 1px solid #e5e7eb; border-radius: 8px; padding: 16px; margin: 12px 0; }
  .notable h3 { margin: 0 0 8px 0; font-size: 15px; }
  .notable .meta { font-size: 12px; color: #6b7280; }
  .footer { text-align: center; margin-top: 48px; padding-top: 24px; border-top: 1px solid #e5e7eb; color: #9ca3af; font-size: 13px; }
  .confidential { text-align: center; color: #ef4444; font-weight: 600; font-size: 12px; text-transform: uppercase; letter-spacing: 2px; margin-top: 8px; }
  @media print { body { padding: 20px; } .kpis { grid-template-columns: repeat(6, 1fr); } }
</style>
</head>
<body>
<div class="header">
  <div class="brand">ThreatDefender Managed Security Services</div>
  <h1>Monthly Incident Summary Report</h1>
  <p style="font-size: 18px; font-weight: 600;">${workspace.name}</p>
  <p class="subtitle">${month.label}</p>
  <p class="confidential">Confidential</p>
  <p class="subtitle" style="font-size: 12px;">Prepared by eGroup | Enabling Technologies</p>
</div>

<h2>Key Performance Metrics</h2>
<div class="kpis">
  <div class="kpi"><div class="value">${metrics.total}</div><div class="label">Total Incidents</div><div class="sublabel">All severities</div></div>
  <div class="kpi"><div class="value">${metrics.analystTouched}</div><div class="label">Analyst-Investigated</div><div class="sublabel">Medium + High</div></div>
  <div class="kpi"><div class="value">${formatMinutes(metrics.avgMTTT)}</div><div class="label">Mean Time to Triage</div><div class="sublabel">Medium + High only</div></div>
  <div class="kpi"><div class="value">${formatMinutes(metrics.avgMTTR)}</div><div class="label">Human MTTR</div><div class="sublabel">Mean Time to Resolve</div></div>
  <div class="kpi"><div class="value">${metrics.truePositives}</div><div class="label">True Positives</div><div class="sublabel">Confirmed threats</div></div>
  <div class="kpi"><div class="value">${metrics.fpRate != null ? metrics.fpRate + '%' : '—'}</div><div class="label">Benign/FP Rate</div><div class="sublabel">Of classified</div></div>
</div>

<h2>Incident Breakdown by Severity</h2>
<table>
  <tr><th>Severity</th><th style="text-align:right">Count</th><th style="text-align:right">% of Total</th><th style="text-align:right">Avg. Resolution</th><th>Handling</th></tr>
  ${metrics.severityTable.map(r => `<tr>
    <td><span class="severity-dot" style="background:${SEVERITY_COLORS[r.severity]}"></span>${r.severity}</td>
    <td style="text-align:right;font-weight:600">${r.count}</td>
    <td style="text-align:right">${r.pct}%</td>
    <td style="text-align:right">${formatMinutes(r.avgResolution)}</td>
    <td><span class="badge ${r.handling === 'Analyst-Investigated' ? 'badge-analyst' : r.handling === 'Fully Automated' ? 'badge-auto' : 'badge-review'}">${r.handling}</span></td>
  </tr>`).join('')}
</table>
<p style="font-size:12px;color:#9ca3af">Note: Informational incidents are handled entirely by automation playbooks and do not require analyst intervention.</p>

<h2>Incidents by Category</h2>
<table>
  <tr><th>Category</th><th style="text-align:right">Count</th></tr>
  ${metrics.categoryTable.map(r => `<tr><td>${r.category}</td><td style="text-align:right;font-weight:600">${r.count}</td></tr>`).join('')}
</table>

${metrics.tacticsData.length > 0 ? `
<h2>MITRE ATT&CK Tactics Observed</h2>
<table>
  <tr><th>Tactic</th><th style="text-align:right">Occurrences</th></tr>
  ${metrics.tacticsData.map(r => `<tr><td>${r.tactic}</td><td style="text-align:right;font-weight:600">${r.count}</td></tr>`).join('')}
</table>` : ''}

${metrics.notable.length > 0 ? `
<h2>Notable Incidents</h2>
${metrics.notable.map(inc => `<div class="notable">
  <h3>#${inc.IncidentNumber} — ${inc.Title}</h3>
  <div class="meta">Severity: ${inc.Severity} | Classification: ${inc.Classification || 'Undetermined'} | Created: ${new Date(inc.CreatedTime).toLocaleString()}${inc.ClosedTime ? ` | Closed: ${new Date(inc.ClosedTime).toLocaleString()}` : ''}</div>
</div>`).join('')}` : ''}

<div class="footer">
  <p>Questions about this report? Contact your ThreatDefender team.</p>
  <p>soc@egroup-us.com | egroup-us.com/threatdefender</p>
  <p style="margin-top:8px">ThreatDefender Operations Suite | eGroup Enabling Technologies &copy; ${new Date().getFullYear()}</p>
</div>
</body>
</html>`;

  if (mode === 'pdf') {
    // Open in a new window and trigger print dialog (Save as PDF)
    const printWindow = window.open('', '_blank');
    printWindow.document.write(html);
    printWindow.document.close();
    printWindow.onload = () => { printWindow.print(); };
  } else {
    const blob = new Blob([html], { type: 'text/html' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `Monthly_Report_${workspace.name.replace(/\s+/g, '_')}_${month.label.replace(/\s+/g, '_')}.html`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  }
}
