import React, { useState, useCallback, useMemo } from 'react';
import { useAuth } from '../contexts/AuthContext';
import { motion, AnimatePresence } from 'framer-motion';
import {
  BarChart, Bar, LineChart, Line, AreaChart, Area,
  XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend, Cell
} from 'recharts';

// ── KQL Queries ──────────────────────────────────────────────────────────────

function buildIngestionOverviewQuery(timeRange) {
  return `
Usage
| where TimeGenerated > ago(${timeRange})
| where IsBillable == true
| summarize VolumeGB = sum(Quantity) / 1024.0,
            RecordCount = sum(Quantity),
            LastLog = max(TimeGenerated)
  by DataType
| order by VolumeGB desc
  `.trim();
}

function buildIngestionTrendQuery(timeRange) {
  return `
Usage
| where TimeGenerated > ago(${timeRange})
| where IsBillable == true
| summarize VolumeGB = sum(Quantity) / 1024.0 by bin(TimeGenerated, 1d), DataType
| order by TimeGenerated asc
  `.trim();
}

function buildEpsTrendQuery(timeRange) {
  return `
Usage
| where TimeGenerated > ago(${timeRange})
| where IsBillable == true
| summarize EventCount = sum(Quantity) by bin(TimeGenerated, 1h)
| extend EPS = EventCount / 3600.0
| order by TimeGenerated asc
  `.trim();
}

function buildAnomalyQuery(timeRange) {
  return `
let startTime = ago(${timeRange});
let sampleInterval = 1h;
Usage
| where TimeGenerated > startTime
| where IsBillable == true
| summarize VolumeGB = sum(Quantity) / 1024.0 by bin(TimeGenerated, sampleInterval), DataType
| order by DataType asc, TimeGenerated asc
| summarize TimeGenerated = make_list(TimeGenerated),
            VolumeGB = make_list(VolumeGB)
  by DataType
| extend (anomalies, score, baseline) = series_decompose_anomalies(VolumeGB, 1.5, -1, 'linefit')
| mv-expand TimeGenerated to typeof(datetime),
            VolumeGB to typeof(double),
            anomalies to typeof(int),
            score to typeof(double),
            baseline to typeof(double)
| where anomalies != 0
| project TimeGenerated, DataType, VolumeGB, AnomalyDirection = iff(anomalies > 0, 'Spike', 'Drop'), Score = round(score, 2), Baseline = round(baseline, 4)
| order by abs(Score) desc
  `.trim();
}

function buildConnectorHealthQuery(timeRange) {
  return `
SentinelHealth
| where TimeGenerated > ago(${timeRange})
| where SentinelResourceType == "Data connector"
| summarize arg_max(TimeGenerated, Status, Description),
            FailureCount = countif(Status == "Failure"),
            SuccessCount = countif(Status == "Success"),
            TotalEvents = count()
  by SentinelResourceName, SentinelResourceId
| extend HealthPct = round(100.0 * SuccessCount / TotalEvents, 1)
| project SentinelResourceName, LastStatus = Status,
          LastChecked = TimeGenerated,
          HealthPct, FailureCount, SuccessCount,
          LastDescription = Description
| order by FailureCount desc, SentinelResourceName asc
  `.trim();
}

function buildConnectorTimelineQuery(timeRange) {
  return `
SentinelHealth
| where TimeGenerated > ago(${timeRange})
| where SentinelResourceType == "Data connector"
| summarize FailureCount = countif(Status == "Failure"),
            SuccessCount = countif(Status == "Success")
  by bin(TimeGenerated, 1h), SentinelResourceName
| order by TimeGenerated asc
  `.trim();
}

function buildAgentHealthQuery() {
  return `
Heartbeat
| summarize LastHeartbeat = max(TimeGenerated),
            OSType = any(OSType),
            OSName = any(OSName),
            Version = any(Version),
            ComputerIP = any(ComputerIP)
  by Computer
| extend Status = iff(LastHeartbeat > ago(5m), 'Healthy',
                  iff(LastHeartbeat > ago(30m), 'Warning', 'Critical'))
| extend MinutesSinceHeartbeat = datetime_diff('minute', now(), LastHeartbeat)
| order by Status asc, MinutesSinceHeartbeat desc
  `.trim();
}

function buildAgentPerformanceQuery() {
  return `
Heartbeat
| where TimeGenerated > ago(24h)
| summarize AvgLatencyMs = avg(toint(RemoteIPLongitude)),
            HeartbeatCount = count()
  by Computer, bin(TimeGenerated, 1h)
| order by Computer asc, TimeGenerated asc
  `.trim();
}

function buildLastLogPerTableQuery(timeRange) {
  return `
Usage
| where TimeGenerated > ago(${timeRange})
| summarize LastLog = max(TimeGenerated), VolumeGB = sum(Quantity) / 1024.0 by DataType
| order by DataType asc
  `.trim();
}

// Map ARM connector kind to a friendly display name and associated tables
const CONNECTOR_KIND_MAP = {
  'Office365': { name: 'Office 365', tables: ['OfficeActivity'] },
  'MicrosoftCloudAppSecurity': { name: 'Microsoft Defender for Cloud Apps', tables: ['McasShadowItReporting'] },
  'AzureActiveDirectory': { name: 'Azure Active Directory', tables: ['SigninLogs', 'AuditLogs'] },
  'AzureSecurityCenter': { name: 'Microsoft Defender for Cloud', tables: ['SecurityAlert'] },
  'MicrosoftDefenderAdvancedThreatProtection': { name: 'Microsoft Defender for Endpoint', tables: ['SecurityAlert'] },
  'ThreatIntelligence': { name: 'Threat Intelligence', tables: ['ThreatIntelligenceIndicator'] },
  'ThreatIntelligenceTaxii': { name: 'Threat Intelligence (TAXII)', tables: ['ThreatIntelligenceIndicator'] },
  'AzureAdvancedThreatProtection': { name: 'Microsoft Defender for Identity', tables: ['SecurityAlert'] },
  'MicrosoftThreatProtection': { name: 'Microsoft 365 Defender', tables: ['AlertInfo', 'AlertEvidence', 'DeviceEvents'] },
  'OfficeATP': { name: 'Microsoft Defender for Office 365', tables: ['EmailEvents', 'EmailUrlInfo'] },
  'OfficeIRM': { name: 'Office 365 IRM', tables: ['OfficeActivity'] },
  'AmazonWebServicesCloudTrail': { name: 'AWS CloudTrail', tables: ['AWSCloudTrail'] },
  'AmazonWebServicesS3': { name: 'AWS S3', tables: ['AWSCloudTrail'] },
  'Syslog': { name: 'Syslog', tables: ['Syslog'] },
  'SecurityEvents': { name: 'Windows Security Events', tables: ['SecurityEvent'] },
  'WindowsFirewall': { name: 'Windows Firewall', tables: ['WindowsFirewall'] },
  'CEF': { name: 'Common Event Format (CEF)', tables: ['CommonSecurityLog'] },
  'MicrosoftThreatIntelligence': { name: 'Microsoft Threat Intelligence', tables: ['ThreatIntelligenceIndicator'] },
  'AzureActivity': { name: 'Azure Activity', tables: ['AzureActivity'] },
  'AADUserRiskEvents': { name: 'Entra ID Risk Events', tables: ['AADUserRiskEvents'] },
  'IOT': { name: 'IoT Defender', tables: ['SecurityAlert'] },
  'GenericUI': { name: 'Custom Connector', tables: [] },
  'APIPolling': { name: 'API Polling Connector', tables: [] },
};

function getConnectorDisplayName(connector) {
  const kind = connector.kind || '';
  const mapped = CONNECTOR_KIND_MAP[kind];
  if (mapped) return mapped.name;
  // Fall back to name from properties or kind
  return connector.properties?.connectorUiConfig?.title
    || connector.properties?.displayName
    || kind
    || connector.name
    || 'Unknown Connector';
}

function getConnectorTables(connector) {
  const kind = connector.kind || '';
  const mapped = CONNECTOR_KIND_MAP[kind];
  if (mapped) return mapped.tables;
  return [];
}

// ── Helpers ──────────────────────────────────────────────────────────────────

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

function formatBytes(gb) {
  if (gb == null || isNaN(gb)) return '0 B';
  if (gb >= 1) return `${gb.toFixed(2)} GB`;
  const mb = gb * 1024;
  if (mb >= 1) return `${mb.toFixed(1)} MB`;
  return `${(mb * 1024).toFixed(0)} KB`;
}

function formatNumber(n) {
  if (n == null) return '0';
  return n.toLocaleString();
}

function timeAgo(dateStr) {
  if (!dateStr) return 'Never';
  const d = new Date(dateStr);
  const mins = Math.floor((Date.now() - d.getTime()) / 60000);
  if (mins < 1) return 'Just now';
  if (mins < 60) return `${mins}m ago`;
  const hours = Math.floor(mins / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}

const CACHE_DURATION = 10 * 60 * 1000; // 10 minutes

function getCached(key) {
  try {
    const raw = sessionStorage.getItem(key);
    if (!raw) return null;
    const { data, ts } = JSON.parse(raw);
    if (Date.now() - ts < CACHE_DURATION) return data;
  } catch { /* ignore */ }
  return null;
}

function setCache(key, data) {
  try {
    sessionStorage.setItem(key, JSON.stringify({ data, ts: Date.now() }));
  } catch { /* ignore */ }
}

const STATUS_COLORS = {
  Healthy: '#10b981',
  Warning: '#f59e0b',
  Critical: '#ef4444',
  Success: '#10b981',
  Failure: '#ef4444',
};

const TAB_ITEMS = [
  { id: 'overview', label: 'Overview' },
  { id: 'anomalies', label: 'Anomalies' },
  { id: 'connectors', label: 'Connector Health' },
  { id: 'agents', label: 'Agent Health' },
];

const TIME_RANGES = [
  { value: '24h', label: 'Last 24 Hours' },
  { value: '3d', label: 'Last 3 Days' },
  { value: '7d', label: 'Last 7 Days' },
  { value: '14d', label: 'Last 14 Days' },
  { value: '30d', label: 'Last 30 Days' },
];

const CHART_COLORS = ['#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6', '#ec4899', '#06b6d4', '#84cc16', '#f97316', '#6366f1'];

// ── Main Component ───────────────────────────────────────────────────────────

export default function DataCollectionHealth({ darkMode }) {
  const { isAuthenticated, isMsalAvailable, login, getSentinelWorkspaces, fetchFromLogAnalytics, fetchFromArm } = useAuth();

  const [workspaces, setWorkspaces] = useState([]);
  const [selectedWorkspace, setSelectedWorkspace] = useState(null);
  const [loadingWorkspaces, setLoadingWorkspaces] = useState(false);

  const [activeTab, setActiveTab] = useState('overview');
  const [timeRange, setTimeRange] = useState('7d');

  // Data states
  const [ingestionData, setIngestionData] = useState(null);
  const [trendData, setTrendData] = useState(null);
  const [epsData, setEpsData] = useState(null);
  const [anomalyData, setAnomalyData] = useState(null);
  const [connectorData, setConnectorData] = useState(null);
  const [connectorTimeline, setConnectorTimeline] = useState(null);
  const [agentData, setAgentData] = useState(null);

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [lastRefresh, setLastRefresh] = useState(null);

  // Load workspaces
  const loadWorkspaces = useCallback(async () => {
    setLoadingWorkspaces(true);
    try {
      const ws = await getSentinelWorkspaces();
      setWorkspaces(ws || []);
      if (ws?.length === 1) setSelectedWorkspace(ws[0]);
    } catch (err) {
      setError(`Failed to load workspaces: ${err.message}`);
    } finally {
      setLoadingWorkspaces(false);
    }
  }, [getSentinelWorkspaces]);

  // Run a cached KQL query. If throwOnError is false, returns [] on failure.
  const runQuery = useCallback(async (query, cacheKey, { throwOnError = true } = {}) => {
    const cached = getCached(cacheKey);
    if (cached) return cached;

    try {
      const result = await fetchFromLogAnalytics(selectedWorkspace.customerId, query);
      const rows = parseLogAnalyticsRows(result);
      setCache(cacheKey, rows);
      return rows;
    } catch (err) {
      if (!throwOnError) {
        // Table likely doesn't exist in this workspace
        return { error: err.message };
      }
      throw err;
    }
  }, [fetchFromLogAnalytics, selectedWorkspace]);

  // Fetch tab data
  const fetchData = useCallback(async (tab) => {
    if (!selectedWorkspace) return;
    setLoading(true);
    setError(null);

    const wsKey = selectedWorkspace.customerId;

    try {
      if (tab === 'overview') {
        const [ingestion, trend, eps] = await Promise.all([
          runQuery(buildIngestionOverviewQuery(timeRange), `dch_ingestion_${wsKey}_${timeRange}`),
          runQuery(buildIngestionTrendQuery(timeRange), `dch_trend_${wsKey}_${timeRange}`),
          runQuery(buildEpsTrendQuery(timeRange), `dch_eps_${wsKey}_${timeRange}`),
        ]);
        setIngestionData(ingestion);
        setTrendData(trend);
        setEpsData(eps);
      } else if (tab === 'anomalies') {
        const anomalies = await runQuery(buildAnomalyQuery(timeRange), `dch_anomaly_${wsKey}_${timeRange}`);
        setAnomalyData(anomalies);
      } else if (tab === 'connectors') {
        // Hybrid approach: ARM API for all connectors + SentinelHealth + Usage for activity
        const wsResourceId = selectedWorkspace.id;
        const [armConnectorsRaw, healthData, lastLogData] = await Promise.all([
          fetchFromArm(
            `https://management.azure.com${wsResourceId}/providers/Microsoft.SecurityInsights/dataConnectors?api-version=2023-11-01`
          ).catch(() => ({ value: [] })),
          runQuery(buildConnectorHealthQuery(timeRange), `dch_connectors_${wsKey}_${timeRange}`, { throwOnError: false }),
          runQuery(buildLastLogPerTableQuery(timeRange), `dch_lastlog_${wsKey}_${timeRange}`, { throwOnError: false }),
        ]);

        const armConnectors = armConnectorsRaw.value || [];
        const healthRows = healthData?.error ? [] : (healthData || []);
        const lastLogRows = lastLogData?.error ? [] : (lastLogData || []);

        // Build lookup: SentinelResourceName -> health row
        const healthLookup = {};
        healthRows.forEach(h => { healthLookup[h.SentinelResourceName] = h; });

        // Build lookup: DataType -> last log info
        const tableLookup = {};
        lastLogRows.forEach(r => { tableLookup[r.DataType] = r; });

        // Merge: start with ARM connectors, enrich with health + usage data
        const merged = armConnectors.map(c => {
          const displayName = getConnectorDisplayName(c);
          const tables = getConnectorTables(c);
          const kind = c.kind || '';

          // Try to match SentinelHealth by connector name patterns
          const healthMatch = healthLookup[displayName]
            || healthLookup[kind]
            || Object.values(healthLookup).find(h =>
              h.SentinelResourceName?.toLowerCase().includes(kind.toLowerCase())
            );

          // Check if any associated tables have recent data
          let lastDataReceived = null;
          let tableActivity = [];
          tables.forEach(t => {
            const tInfo = tableLookup[t];
            if (tInfo) {
              tableActivity.push({ table: t, lastLog: tInfo.LastLog, volumeGB: parseFloat(tInfo.VolumeGB) || 0 });
              const logTime = new Date(tInfo.LastLog);
              if (!lastDataReceived || logTime > lastDataReceived) lastDataReceived = logTime;
            }
          });

          // Determine status from multiple signals
          let status, healthPct;
          if (healthMatch) {
            status = healthMatch.LastStatus || 'Unknown';
            healthPct = parseFloat(healthMatch.HealthPct) || 0;
          } else if (lastDataReceived) {
            const minutesAgo = (Date.now() - lastDataReceived.getTime()) / 60000;
            status = minutesAgo < 60 ? 'Active' : minutesAgo < 1440 ? 'Active' : 'Stale';
            healthPct = status === 'Active' ? 100 : 50;
          } else if (tables.length > 0) {
            status = 'No Data';
            healthPct = 0;
          } else {
            status = 'Unknown';
            healthPct = null;
          }

          return {
            name: displayName,
            kind,
            status,
            healthPct,
            failureCount: healthMatch ? parseInt(healthMatch.FailureCount) || 0 : 0,
            successCount: healthMatch ? parseInt(healthMatch.SuccessCount) || 0 : 0,
            lastChecked: healthMatch?.LastChecked || null,
            lastDataReceived: lastDataReceived?.toISOString() || null,
            tables: tableActivity,
            hasHealthData: !!healthMatch,
            enabled: c.properties?.dataTypes
              ? Object.values(c.properties.dataTypes).some(dt =>
                  dt?.state === 'Enabled' || dt?.state === 'enabled')
              : true,
          };
        });

        // Sort: failures first, then no-data, then active
        merged.sort((a, b) => {
          if (a.failureCount !== b.failureCount) return b.failureCount - a.failureCount;
          if (a.status === 'No Data' && b.status !== 'No Data') return -1;
          if (b.status === 'No Data' && a.status !== 'No Data') return 1;
          return a.name.localeCompare(b.name);
        });

        setConnectorData(merged);
        setConnectorTimeline(healthRows);
      } else if (tab === 'agents') {
        const agents = await runQuery(buildAgentHealthQuery(), `dch_agents_${wsKey}`, { throwOnError: false });
        setAgentData(agents?.error ? [] : agents);
        if (agents?.error) {
          setError('Heartbeat table is not available. This workspace may not have agents (MMA/AMA) reporting to it.');
        }
      }
      setLastRefresh(new Date());
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }, [selectedWorkspace, timeRange, runQuery]);

  // Refresh on tab/workspace/timeRange change
  const handleRefresh = useCallback(() => {
    // Clear cache for current queries to force fresh data
    if (selectedWorkspace) {
      const wsKey = selectedWorkspace.customerId;
      const prefix = `dch_`;
      for (let i = sessionStorage.length - 1; i >= 0; i--) {
        const key = sessionStorage.key(i);
        if (key?.startsWith(prefix) && key.includes(wsKey)) {
          sessionStorage.removeItem(key);
        }
      }
    }
    fetchData(activeTab);
  }, [fetchData, activeTab, selectedWorkspace]);

  // Auto-fetch when tab, workspace, or time range changes
  const handleTabChange = (tab) => {
    setActiveTab(tab);
    fetchData(tab);
  };

  const handleWorkspaceSelect = (ws) => {
    setSelectedWorkspace(ws);
    // Reset data
    setIngestionData(null);
    setTrendData(null);
    setEpsData(null);
    setAnomalyData(null);
    setConnectorData(null);
    setConnectorTimeline(null);
    setAgentData(null);
    setError(null);
  };

  const handleTimeRangeChange = (range) => {
    setTimeRange(range);
    // Data will be refreshed when user clicks Load or via effect
  };

  // ── Computed values ────────────────────────────────────────────────────────

  const totalVolumeGB = useMemo(() => {
    if (!ingestionData) return 0;
    return ingestionData.reduce((sum, r) => sum + (parseFloat(r.VolumeGB) || 0), 0);
  }, [ingestionData]);

  const topTables = useMemo(() => {
    if (!ingestionData) return [];
    return ingestionData.slice(0, 10);
  }, [ingestionData]);

  const avgEps = useMemo(() => {
    if (!epsData || epsData.length === 0) return 0;
    const total = epsData.reduce((sum, r) => sum + (parseFloat(r.EPS) || 0), 0);
    return total / epsData.length;
  }, [epsData]);

  const trendChartData = useMemo(() => {
    if (!trendData) return [];
    // Pivot data: group by date, columns are DataTypes
    const dateMap = {};
    const dataTypes = new Set();
    trendData.forEach(r => {
      const date = new Date(r.TimeGenerated).toLocaleDateString();
      dataTypes.add(r.DataType);
      if (!dateMap[date]) dateMap[date] = { date };
      dateMap[date][r.DataType] = parseFloat(r.VolumeGB) || 0;
    });
    return Object.values(dateMap);
  }, [trendData]);

  const trendDataTypes = useMemo(() => {
    if (!trendData) return [];
    const types = new Set();
    trendData.forEach(r => types.add(r.DataType));
    // Return top 8 by total volume
    const typeVolumes = {};
    trendData.forEach(r => {
      typeVolumes[r.DataType] = (typeVolumes[r.DataType] || 0) + (parseFloat(r.VolumeGB) || 0);
    });
    return [...types].sort((a, b) => (typeVolumes[b] || 0) - (typeVolumes[a] || 0)).slice(0, 8);
  }, [trendData]);

  const epsChartData = useMemo(() => {
    if (!epsData) return [];
    return epsData.map(r => ({
      time: new Date(r.TimeGenerated).toLocaleString(undefined, { month: 'short', day: 'numeric', hour: '2-digit' }),
      eps: parseFloat(r.EPS) || 0,
    }));
  }, [epsData]);

  // ── Shared UI pieces ───────────────────────────────────────────────────────

  const cardClass = `rounded-lg border ${darkMode ? 'bg-gray-800 border-gray-700' : 'bg-white border-gray-200'}`;
  const textPrimary = darkMode ? 'text-white' : 'text-gray-900';
  const textSecondary = darkMode ? 'text-gray-400' : 'text-gray-600';
  const textMuted = darkMode ? 'text-gray-500' : 'text-gray-400';

  // ── Auth gate ──────────────────────────────────────────────────────────────

  if (!isMsalAvailable) {
    return (
      <div className={`p-6 text-center ${textSecondary}`}>
        <p className="text-lg font-medium mb-2">Authentication Not Available</p>
        <p>Azure AD authentication is required. This feature is only available when deployed.</p>
      </div>
    );
  }

  if (!isAuthenticated) {
    return (
      <div className="flex flex-col items-center justify-center py-16 gap-4">
        <div className={`text-5xl mb-2`}>🔌</div>
        <h2 className={`text-xl font-semibold ${textPrimary}`}>Data Collection Health Monitor</h2>
        <p className={textSecondary}>Sign in to view data collection health across your Sentinel workspaces.</p>
        <motion.button
          whileHover={{ scale: 1.02 }}
          whileTap={{ scale: 0.98 }}
          onClick={login}
          className="mt-2 px-6 py-2.5 bg-blue-600 text-white rounded-lg font-medium hover:bg-blue-700 transition-colors"
        >
          Sign In with Azure AD
        </motion.button>
      </div>
    );
  }

  // ── Workspace selector ─────────────────────────────────────────────────────

  if (!selectedWorkspace) {
    return (
      <div className="space-y-6">
        <div>
          <h2 className={`text-xl font-semibold ${textPrimary}`}>Data Collection Health Monitor</h2>
          <p className={`text-sm mt-1 ${textSecondary}`}>
            Monitor ingestion volume, detect anomalies, and track connector and agent health.
          </p>
        </div>

        {!workspaces.length && !loadingWorkspaces && (
          <motion.button
            whileHover={{ scale: 1.02 }}
            whileTap={{ scale: 0.98 }}
            onClick={loadWorkspaces}
            className="px-6 py-2.5 bg-blue-600 text-white rounded-lg font-medium hover:bg-blue-700 transition-colors"
          >
            Load Sentinel Workspaces
          </motion.button>
        )}

        {loadingWorkspaces && (
          <div className={`flex items-center gap-3 ${textSecondary}`}>
            <div className="animate-spin rounded-full h-5 w-5 border-b-2 border-blue-500" />
            <span>Discovering Sentinel workspaces...</span>
          </div>
        )}

        {workspaces.length > 0 && (
          <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
            {workspaces.map(ws => (
              <motion.button
                key={ws.id}
                whileHover={{ scale: 1.01 }}
                whileTap={{ scale: 0.99 }}
                onClick={() => handleWorkspaceSelect(ws)}
                className={`text-left p-4 rounded-lg border transition-colors ${
                  darkMode
                    ? 'bg-gray-800 border-gray-700 hover:border-blue-500'
                    : 'bg-white border-gray-200 hover:border-blue-400'
                }`}
              >
                <div className={`font-medium ${textPrimary}`}>{ws.name}</div>
                <div className={`text-xs mt-1 ${textMuted}`}>{ws.subscriptionName}</div>
                <div className={`text-xs ${textMuted}`}>{ws.location}</div>
              </motion.button>
            ))}
          </div>
        )}

        {error && (
          <div className="p-4 rounded-lg bg-red-500/10 border border-red-500/30 text-red-400 text-sm">{error}</div>
        )}
      </div>
    );
  }

  // ── Main dashboard ─────────────────────────────────────────────────────────

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <div>
          <h2 className={`text-xl font-semibold ${textPrimary}`}>Data Collection Health Monitor</h2>
          <p className={`text-sm mt-1 ${textSecondary}`}>
            {selectedWorkspace.name}
            <button
              onClick={() => { setSelectedWorkspace(null); setError(null); }}
              className="ml-2 text-blue-500 hover:text-blue-400 text-xs"
            >
              (change)
            </button>
          </p>
        </div>
        <div className="flex items-center gap-3">
          {/* Time Range Selector */}
          <select
            value={timeRange}
            onChange={(e) => handleTimeRangeChange(e.target.value)}
            className={`text-sm rounded-lg px-3 py-1.5 border ${
              darkMode
                ? 'bg-gray-800 border-gray-700 text-white'
                : 'bg-white border-gray-200 text-gray-900'
            }`}
          >
            {TIME_RANGES.map(r => (
              <option key={r.value} value={r.value}>{r.label}</option>
            ))}
          </select>

          {/* Load / Refresh */}
          <motion.button
            whileHover={{ scale: 1.02 }}
            whileTap={{ scale: 0.98 }}
            onClick={() => fetchData(activeTab)}
            disabled={loading}
            className="px-4 py-1.5 bg-blue-600 text-white text-sm rounded-lg font-medium hover:bg-blue-700 transition-colors disabled:opacity-50"
          >
            {loading ? 'Loading...' : lastRefresh ? 'Refresh' : 'Load Data'}
          </motion.button>

          {lastRefresh && !loading && (
            <button
              onClick={handleRefresh}
              className={`text-xs ${textMuted} hover:text-blue-500`}
              title="Force refresh (clear cache)"
            >
              Force Refresh
            </button>
          )}
        </div>
      </div>

      {/* Tabs */}
      <div className={`flex gap-1 p-1 rounded-lg ${darkMode ? 'bg-gray-800' : 'bg-gray-100'}`}>
        {TAB_ITEMS.map(tab => (
          <button
            key={tab.id}
            onClick={() => handleTabChange(tab.id)}
            className={`flex-1 px-4 py-2 text-sm font-medium rounded-md transition-colors ${
              activeTab === tab.id
                ? darkMode
                  ? 'bg-gray-700 text-white'
                  : 'bg-white text-gray-900 shadow-sm'
                : darkMode
                ? 'text-gray-400 hover:text-gray-200'
                : 'text-gray-600 hover:text-gray-900'
            }`}
          >
            {tab.label}
          </button>
        ))}
      </div>

      {/* Error */}
      {error && (
        <div className="p-4 rounded-lg bg-red-500/10 border border-red-500/30 text-red-400 text-sm">{error}</div>
      )}

      {/* Loading indicator */}
      {loading && (
        <div className={`flex items-center justify-center py-12 ${textSecondary}`}>
          <div className="text-center">
            <div className="inline-block animate-spin rounded-full h-10 w-10 border-b-2 border-blue-500 mb-3" />
            <p>Querying Log Analytics...</p>
          </div>
        </div>
      )}

      {/* Tab content */}
      {!loading && (
        <AnimatePresence mode="wait">
          <motion.div
            key={activeTab}
            initial={{ opacity: 0, y: 8 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -8 }}
            transition={{ duration: 0.15 }}
          >
            {activeTab === 'overview' && <OverviewTab
              ingestionData={ingestionData} topTables={topTables} totalVolumeGB={totalVolumeGB}
              avgEps={avgEps} trendChartData={trendChartData} trendDataTypes={trendDataTypes}
              epsChartData={epsChartData} darkMode={darkMode} cardClass={cardClass}
              textPrimary={textPrimary} textSecondary={textSecondary} textMuted={textMuted}
            />}
            {activeTab === 'anomalies' && <AnomaliesTab
              anomalyData={anomalyData} darkMode={darkMode} cardClass={cardClass}
              textPrimary={textPrimary} textSecondary={textSecondary} textMuted={textMuted}
              timeRange={timeRange}
            />}
            {activeTab === 'connectors' && <ConnectorsTab
              connectorData={connectorData} connectorTimeline={connectorTimeline}
              darkMode={darkMode} cardClass={cardClass}
              textPrimary={textPrimary} textSecondary={textSecondary} textMuted={textMuted}
            />}
            {activeTab === 'agents' && <AgentsTab
              agentData={agentData} darkMode={darkMode} cardClass={cardClass}
              textPrimary={textPrimary} textSecondary={textSecondary} textMuted={textMuted}
            />}
          </motion.div>
        </AnimatePresence>
      )}

      {/* Last refresh info */}
      {lastRefresh && !loading && (
        <p className={`text-xs text-center ${textMuted}`}>
          Last refreshed: {lastRefresh.toLocaleTimeString()} (cached for 10 minutes)
        </p>
      )}
    </div>
  );
}

// ── Overview Tab ─────────────────────────────────────────────────────────────

function OverviewTab({ ingestionData, topTables, totalVolumeGB, avgEps, trendChartData, trendDataTypes, epsChartData, darkMode, cardClass, textPrimary, textSecondary, textMuted }) {
  if (!ingestionData) {
    return <EmptyState message="Click 'Load Data' to view ingestion overview." darkMode={darkMode} />;
  }

  const tableCount = ingestionData.length;

  return (
    <div className="space-y-6">
      {/* KPI Cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <KpiCard label="Total Ingestion" value={formatBytes(totalVolumeGB)} darkMode={darkMode} cardClass={cardClass} textPrimary={textPrimary} textSecondary={textSecondary} />
        <KpiCard label="Active Tables" value={tableCount} darkMode={darkMode} cardClass={cardClass} textPrimary={textPrimary} textSecondary={textSecondary} />
        <KpiCard label="Avg EPS" value={avgEps.toFixed(1)} darkMode={darkMode} cardClass={cardClass} textPrimary={textPrimary} textSecondary={textSecondary} />
        <KpiCard label="Top Table" value={topTables[0]?.DataType || '—'} subtitle={formatBytes(parseFloat(topTables[0]?.VolumeGB || 0))} darkMode={darkMode} cardClass={cardClass} textPrimary={textPrimary} textSecondary={textSecondary} />
      </div>

      {/* Ingestion Trend Chart */}
      {trendChartData.length > 0 && (
        <div className={`${cardClass} p-5`}>
          <h3 className={`text-sm font-semibold mb-4 ${textPrimary}`}>Daily Ingestion Volume by Table</h3>
          <ResponsiveContainer width="100%" height={300}>
            <AreaChart data={trendChartData}>
              <CartesianGrid strokeDasharray="3 3" stroke={darkMode ? '#374151' : '#e5e7eb'} />
              <XAxis dataKey="date" tick={{ fontSize: 11, fill: darkMode ? '#9ca3af' : '#6b7280' }} />
              <YAxis tick={{ fontSize: 11, fill: darkMode ? '#9ca3af' : '#6b7280' }} tickFormatter={v => `${v.toFixed(1)} GB`} />
              <Tooltip
                contentStyle={{ backgroundColor: darkMode ? '#1f2937' : '#fff', border: `1px solid ${darkMode ? '#374151' : '#e5e7eb'}`, borderRadius: '8px' }}
                labelStyle={{ color: darkMode ? '#fff' : '#111' }}
                formatter={(val) => [`${parseFloat(val).toFixed(3)} GB`, undefined]}
              />
              <Legend wrapperStyle={{ fontSize: 11 }} />
              {trendDataTypes.map((dt, i) => (
                <Area key={dt} type="monotone" dataKey={dt} stackId="1" fill={CHART_COLORS[i % CHART_COLORS.length]} stroke={CHART_COLORS[i % CHART_COLORS.length]} fillOpacity={0.6} />
              ))}
            </AreaChart>
          </ResponsiveContainer>
        </div>
      )}

      {/* EPS Trend */}
      {epsChartData.length > 0 && (
        <div className={`${cardClass} p-5`}>
          <h3 className={`text-sm font-semibold mb-4 ${textPrimary}`}>Events Per Second (Hourly)</h3>
          <ResponsiveContainer width="100%" height={200}>
            <LineChart data={epsChartData}>
              <CartesianGrid strokeDasharray="3 3" stroke={darkMode ? '#374151' : '#e5e7eb'} />
              <XAxis dataKey="time" tick={{ fontSize: 10, fill: darkMode ? '#9ca3af' : '#6b7280' }} interval="preserveStartEnd" />
              <YAxis tick={{ fontSize: 11, fill: darkMode ? '#9ca3af' : '#6b7280' }} />
              <Tooltip
                contentStyle={{ backgroundColor: darkMode ? '#1f2937' : '#fff', border: `1px solid ${darkMode ? '#374151' : '#e5e7eb'}`, borderRadius: '8px' }}
                formatter={(val) => [`${parseFloat(val).toFixed(1)} EPS`, 'Events/sec']}
              />
              <Line type="monotone" dataKey="eps" stroke="#3b82f6" strokeWidth={2} dot={false} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}

      {/* Top Tables */}
      <div className={`${cardClass} p-5`}>
        <h3 className={`text-sm font-semibold mb-4 ${textPrimary}`}>Top Data Tables by Volume</h3>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className={`border-b ${darkMode ? 'border-gray-700' : 'border-gray-200'}`}>
                <th className={`text-left py-2 px-3 font-medium ${textSecondary}`}>Data Type</th>
                <th className={`text-right py-2 px-3 font-medium ${textSecondary}`}>Volume</th>
                <th className={`text-right py-2 px-3 font-medium ${textSecondary}`}>% of Total</th>
                <th className={`text-right py-2 px-3 font-medium ${textSecondary}`}>Last Log</th>
                <th className={`text-left py-2 px-3 font-medium ${textSecondary}`}>Volume Bar</th>
              </tr>
            </thead>
            <tbody>
              {topTables.map((row, i) => {
                const vol = parseFloat(row.VolumeGB) || 0;
                const pct = totalVolumeGB > 0 ? (vol / totalVolumeGB * 100) : 0;
                return (
                  <tr key={i} className={`border-b ${darkMode ? 'border-gray-700/50' : 'border-gray-100'}`}>
                    <td className={`py-2 px-3 font-medium ${textPrimary}`}>{row.DataType}</td>
                    <td className={`py-2 px-3 text-right ${textSecondary}`}>{formatBytes(vol)}</td>
                    <td className={`py-2 px-3 text-right ${textSecondary}`}>{pct.toFixed(1)}%</td>
                    <td className={`py-2 px-3 text-right ${textMuted}`}>{timeAgo(row.LastLog)}</td>
                    <td className="py-2 px-3 w-40">
                      <div className={`h-2 rounded-full ${darkMode ? 'bg-gray-700' : 'bg-gray-200'}`}>
                        <div
                          className="h-2 rounded-full bg-blue-500"
                          style={{ width: `${Math.min(pct, 100)}%` }}
                        />
                      </div>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
        {ingestionData.length > 10 && (
          <p className={`text-xs mt-3 ${textMuted}`}>Showing top 10 of {ingestionData.length} tables</p>
        )}
      </div>
    </div>
  );
}

// ── Anomalies Tab ────────────────────────────────────────────────────────────

function AnomaliesTab({ anomalyData, darkMode, cardClass, textPrimary, textSecondary, textMuted, timeRange }) {
  if (!anomalyData) {
    return <EmptyState message="Click 'Load Data' to detect ingestion anomalies." darkMode={darkMode} />;
  }

  const spikes = anomalyData.filter(r => r.AnomalyDirection === 'Spike');
  const drops = anomalyData.filter(r => r.AnomalyDirection === 'Drop');

  // Group anomalies by DataType for the chart
  const anomalyByType = {};
  anomalyData.forEach(r => {
    if (!anomalyByType[r.DataType]) anomalyByType[r.DataType] = { DataType: r.DataType, Spikes: 0, Drops: 0 };
    if (r.AnomalyDirection === 'Spike') anomalyByType[r.DataType].Spikes++;
    else anomalyByType[r.DataType].Drops++;
  });
  const anomalyChartData = Object.values(anomalyByType).sort((a, b) => (b.Spikes + b.Drops) - (a.Spikes + a.Drops)).slice(0, 10);

  return (
    <div className="space-y-6">
      {/* Summary */}
      <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
        <KpiCard label="Total Anomalies" value={anomalyData.length} darkMode={darkMode} cardClass={cardClass} textPrimary={textPrimary} textSecondary={textSecondary} />
        <KpiCard label="Volume Spikes" value={spikes.length} color="#ef4444" darkMode={darkMode} cardClass={cardClass} textPrimary={textPrimary} textSecondary={textSecondary} />
        <KpiCard label="Volume Drops" value={drops.length} color="#f59e0b" darkMode={darkMode} cardClass={cardClass} textPrimary={textPrimary} textSecondary={textSecondary} />
      </div>

      {anomalyData.length === 0 ? (
        <div className={`${cardClass} p-8 text-center`}>
          <div className="text-4xl mb-3">&#10003;</div>
          <p className={`font-medium ${textPrimary}`}>No Anomalies Detected</p>
          <p className={`text-sm mt-1 ${textSecondary}`}>Ingestion volumes are within expected ranges for the last {timeRange}.</p>
        </div>
      ) : (
        <>
          {/* Anomaly chart */}
          {anomalyChartData.length > 0 && (
            <div className={`${cardClass} p-5`}>
              <h3 className={`text-sm font-semibold mb-4 ${textPrimary}`}>Anomalies by Data Type</h3>
              <ResponsiveContainer width="100%" height={250}>
                <BarChart data={anomalyChartData} layout="vertical">
                  <CartesianGrid strokeDasharray="3 3" stroke={darkMode ? '#374151' : '#e5e7eb'} />
                  <XAxis type="number" tick={{ fontSize: 11, fill: darkMode ? '#9ca3af' : '#6b7280' }} />
                  <YAxis dataKey="DataType" type="category" width={150} tick={{ fontSize: 11, fill: darkMode ? '#9ca3af' : '#6b7280' }} />
                  <Tooltip contentStyle={{ backgroundColor: darkMode ? '#1f2937' : '#fff', border: `1px solid ${darkMode ? '#374151' : '#e5e7eb'}`, borderRadius: '8px' }} />
                  <Legend wrapperStyle={{ fontSize: 11 }} />
                  <Bar dataKey="Spikes" fill="#ef4444" stackId="a" />
                  <Bar dataKey="Drops" fill="#f59e0b" stackId="a" />
                </BarChart>
              </ResponsiveContainer>
            </div>
          )}

          {/* Anomaly table */}
          <div className={`${cardClass} p-5`}>
            <h3 className={`text-sm font-semibold mb-4 ${textPrimary}`}>Detected Anomalies ({anomalyData.length})</h3>
            <div className="overflow-x-auto max-h-96 overflow-y-auto">
              <table className="w-full text-sm">
                <thead className="sticky top-0">
                  <tr className={`border-b ${darkMode ? 'border-gray-700 bg-gray-800' : 'border-gray-200 bg-white'}`}>
                    <th className={`text-left py-2 px-3 font-medium ${textSecondary}`}>Time</th>
                    <th className={`text-left py-2 px-3 font-medium ${textSecondary}`}>Data Type</th>
                    <th className={`text-left py-2 px-3 font-medium ${textSecondary}`}>Direction</th>
                    <th className={`text-right py-2 px-3 font-medium ${textSecondary}`}>Actual</th>
                    <th className={`text-right py-2 px-3 font-medium ${textSecondary}`}>Baseline</th>
                    <th className={`text-right py-2 px-3 font-medium ${textSecondary}`}>Score</th>
                  </tr>
                </thead>
                <tbody>
                  {anomalyData.slice(0, 50).map((row, i) => (
                    <tr key={i} className={`border-b ${darkMode ? 'border-gray-700/50' : 'border-gray-100'}`}>
                      <td className={`py-2 px-3 ${textMuted}`}>{new Date(row.TimeGenerated).toLocaleString()}</td>
                      <td className={`py-2 px-3 font-medium ${textPrimary}`}>{row.DataType}</td>
                      <td className="py-2 px-3">
                        <span className={`px-2 py-0.5 rounded-full text-xs font-medium ${
                          row.AnomalyDirection === 'Spike'
                            ? 'bg-red-500/10 text-red-400'
                            : 'bg-yellow-500/10 text-yellow-400'
                        }`}>
                          {row.AnomalyDirection === 'Spike' ? '^ Spike' : 'v Drop'}
                        </span>
                      </td>
                      <td className={`py-2 px-3 text-right ${textSecondary}`}>{formatBytes(parseFloat(row.VolumeGB) || 0)}</td>
                      <td className={`py-2 px-3 text-right ${textMuted}`}>{formatBytes(parseFloat(row.Baseline) || 0)}</td>
                      <td className={`py-2 px-3 text-right font-mono ${textSecondary}`}>{row.Score}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            {anomalyData.length > 50 && (
              <p className={`text-xs mt-3 ${textMuted}`}>Showing top 50 of {anomalyData.length} anomalies (sorted by score)</p>
            )}
          </div>
        </>
      )}
    </div>
  );
}

// ── Connectors Tab ───────────────────────────────────────────────────────────

function ConnectorsTab({ connectorData, connectorTimeline, darkMode, cardClass, textPrimary, textSecondary, textMuted }) {
  const [expandedRow, setExpandedRow] = useState(null);

  if (!connectorData) {
    return <EmptyState message="Click 'Load Data' to view connector health." darkMode={darkMode} />;
  }

  const active = connectorData.filter(r => r.status === 'Success' || r.status === 'Active');
  const withIssues = connectorData.filter(r => r.status === 'Failure' || r.status === 'Stale' || r.status === 'No Data');
  const withHealth = connectorData.filter(r => r.hasHealthData);

  function getStatusColor(row) {
    if (row.status === 'Success' || row.status === 'Active') return STATUS_COLORS.Healthy;
    if (row.status === 'Failure' || row.status === 'No Data') return STATUS_COLORS.Critical;
    if (row.status === 'Stale' || row.status === 'Warning') return STATUS_COLORS.Warning;
    return '#6b7280';
  }

  function getStatusLabel(row) {
    if (!row.enabled) return 'Disabled';
    return row.status;
  }

  return (
    <div className="space-y-6">
      {/* Summary */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <KpiCard label="Total Connectors" value={connectorData.length} darkMode={darkMode} cardClass={cardClass} textPrimary={textPrimary} textSecondary={textSecondary} />
        <KpiCard label="Active" value={active.length} color="#10b981" darkMode={darkMode} cardClass={cardClass} textPrimary={textPrimary} textSecondary={textSecondary} />
        <KpiCard label="Issues / No Data" value={withIssues.length} color={withIssues.length > 0 ? '#ef4444' : '#10b981'} darkMode={darkMode} cardClass={cardClass} textPrimary={textPrimary} textSecondary={textSecondary} />
        <KpiCard label="Health Reporting" value={withHealth.length} subtitle={`of ${connectorData.length} report to SentinelHealth`} darkMode={darkMode} cardClass={cardClass} textPrimary={textPrimary} textSecondary={textSecondary} />
      </div>

      {connectorData.length === 0 ? (
        <div className={`${cardClass} p-8 text-center`}>
          <p className={`font-medium ${textPrimary}`}>No Connectors Found</p>
          <p className={`text-sm mt-1 ${textSecondary}`}>
            No data connectors are configured in this Sentinel workspace.
          </p>
        </div>
      ) : (
        <div className={`${cardClass} p-5`}>
          <h3 className={`text-sm font-semibold mb-4 ${textPrimary}`}>All Connectors ({connectorData.length})</h3>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className={`border-b ${darkMode ? 'border-gray-700' : 'border-gray-200'}`}>
                  <th className={`text-left py-2 px-3 font-medium ${textSecondary}`}>Connector</th>
                  <th className={`text-left py-2 px-3 font-medium ${textSecondary}`}>Status</th>
                  <th className={`text-left py-2 px-3 font-medium ${textSecondary}`}>Last Data</th>
                  <th className={`text-right py-2 px-3 font-medium ${textSecondary}`}>Health %</th>
                  <th className={`text-right py-2 px-3 font-medium ${textSecondary}`}>Failures</th>
                  <th className={`text-left py-2 px-3 font-medium ${textSecondary}`}>Source</th>
                </tr>
              </thead>
              <tbody>
                {connectorData.map((row, i) => {
                  const statusColor = getStatusColor(row);
                  const isExpanded = expandedRow === i;
                  return (
                    <React.Fragment key={i}>
                      <tr
                        className={`border-b cursor-pointer transition-colors ${
                          darkMode ? 'border-gray-700/50 hover:bg-gray-700/30' : 'border-gray-100 hover:bg-gray-50'
                        }`}
                        onClick={() => setExpandedRow(isExpanded ? null : i)}
                      >
                        <td className={`py-2 px-3 font-medium ${textPrimary}`}>
                          <span className="flex items-center gap-2">
                            <span className="text-xs">{isExpanded ? '▼' : '▶'}</span>
                            {row.name}
                          </span>
                        </td>
                        <td className="py-2 px-3">
                          <span className="flex items-center gap-2">
                            <span className="w-2 h-2 rounded-full flex-shrink-0" style={{ backgroundColor: row.enabled ? statusColor : '#6b7280' }} />
                            <span className={textSecondary}>{getStatusLabel(row)}</span>
                          </span>
                        </td>
                        <td className={`py-2 px-3 ${row.lastDataReceived ? textSecondary : textMuted}`}>
                          {row.lastDataReceived ? timeAgo(row.lastDataReceived) : '—'}
                        </td>
                        <td className={`py-2 px-3 text-right font-medium`}>
                          {row.healthPct != null ? (
                            <span style={{ color: row.hasHealthData ? statusColor : textSecondary }}>{row.healthPct}%</span>
                          ) : (
                            <span className={textMuted}>—</span>
                          )}
                        </td>
                        <td className={`py-2 px-3 text-right ${row.failureCount > 0 ? 'text-red-400' : textMuted}`}>
                          {row.hasHealthData ? row.failureCount : '—'}
                        </td>
                        <td className={`py-2 px-3`}>
                          <span className={`text-xs px-1.5 py-0.5 rounded ${
                            row.hasHealthData
                              ? darkMode ? 'bg-blue-500/10 text-blue-400' : 'bg-blue-50 text-blue-600'
                              : darkMode ? 'bg-gray-700 text-gray-500' : 'bg-gray-100 text-gray-400'
                          }`}>
                            {row.hasHealthData ? 'Health + Usage' : 'Usage Only'}
                          </span>
                        </td>
                      </tr>
                      {isExpanded && (
                        <tr className={darkMode ? 'bg-gray-800/50' : 'bg-gray-50/50'}>
                          <td colSpan={6} className="px-6 py-3">
                            <div className="space-y-2 text-xs">
                              <div className="flex gap-6 flex-wrap">
                                <span className={textMuted}>Kind: <span className={textSecondary}>{row.kind || '—'}</span></span>
                                <span className={textMuted}>Enabled: <span className={row.enabled ? 'text-green-400' : 'text-red-400'}>{row.enabled ? 'Yes' : 'No'}</span></span>
                                {row.lastChecked && <span className={textMuted}>Last Health Check: <span className={textSecondary}>{timeAgo(row.lastChecked)}</span></span>}
                                {row.hasHealthData && <span className={textMuted}>Success: <span className={textSecondary}>{row.successCount}</span> | Failures: <span className={row.failureCount > 0 ? 'text-red-400' : textSecondary}>{row.failureCount}</span></span>}
                              </div>
                              {row.tables.length > 0 && (
                                <div>
                                  <span className={textMuted}>Associated Tables:</span>
                                  <div className="flex gap-2 mt-1 flex-wrap">
                                    {row.tables.map(t => (
                                      <span key={t.table} className={`px-2 py-0.5 rounded ${darkMode ? 'bg-gray-700' : 'bg-gray-200'} ${textSecondary}`}>
                                        {t.table}: {formatBytes(t.volumeGB)} — {timeAgo(t.lastLog)}
                                      </span>
                                    ))}
                                  </div>
                                </div>
                              )}
                              {row.tables.length === 0 && !row.hasHealthData && (
                                <p className={textMuted}>No table mapping or health data available for this connector type.</p>
                              )}
                            </div>
                          </td>
                        </tr>
                      )}
                    </React.Fragment>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}

// ── Agents Tab ───────────────────────────────────────────────────────────────

function AgentsTab({ agentData, darkMode, cardClass, textPrimary, textSecondary, textMuted }) {
  if (!agentData) {
    return <EmptyState message="Click 'Load Data' to view agent health. Requires agents reporting to this workspace." darkMode={darkMode} />;
  }

  const healthy = agentData.filter(r => r.Status === 'Healthy');
  const warning = agentData.filter(r => r.Status === 'Warning');
  const critical = agentData.filter(r => r.Status === 'Critical');

  return (
    <div className="space-y-6">
      {/* Summary */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <KpiCard label="Total Agents" value={agentData.length} darkMode={darkMode} cardClass={cardClass} textPrimary={textPrimary} textSecondary={textSecondary} />
        <KpiCard label="Healthy" value={healthy.length} color="#10b981" darkMode={darkMode} cardClass={cardClass} textPrimary={textPrimary} textSecondary={textSecondary} />
        <KpiCard label="Warning" value={warning.length} color="#f59e0b" darkMode={darkMode} cardClass={cardClass} textPrimary={textPrimary} textSecondary={textSecondary} />
        <KpiCard label="Critical" value={critical.length} color="#ef4444" darkMode={darkMode} cardClass={cardClass} textPrimary={textPrimary} textSecondary={textSecondary} />
      </div>

      {agentData.length === 0 ? (
        <div className={`${cardClass} p-8 text-center`}>
          <p className={`font-medium ${textPrimary}`}>No Agent Data</p>
          <p className={`text-sm mt-1 ${textSecondary}`}>
            No agents are reporting heartbeats to this workspace.
          </p>
        </div>
      ) : (
        <div className={`${cardClass} p-5`}>
          <h3 className={`text-sm font-semibold mb-4 ${textPrimary}`}>Agent Status</h3>
          <div className="overflow-x-auto max-h-[500px] overflow-y-auto">
            <table className="w-full text-sm">
              <thead className="sticky top-0">
                <tr className={`border-b ${darkMode ? 'border-gray-700 bg-gray-800' : 'border-gray-200 bg-white'}`}>
                  <th className={`text-left py-2 px-3 font-medium ${textSecondary}`}>Computer</th>
                  <th className={`text-left py-2 px-3 font-medium ${textSecondary}`}>Status</th>
                  <th className={`text-left py-2 px-3 font-medium ${textSecondary}`}>OS</th>
                  <th className={`text-left py-2 px-3 font-medium ${textSecondary}`}>IP Address</th>
                  <th className={`text-left py-2 px-3 font-medium ${textSecondary}`}>Agent Version</th>
                  <th className={`text-left py-2 px-3 font-medium ${textSecondary}`}>Last Heartbeat</th>
                </tr>
              </thead>
              <tbody>
                {agentData.map((row, i) => {
                  const statusColor = STATUS_COLORS[row.Status] || STATUS_COLORS.Warning;
                  return (
                    <tr key={i} className={`border-b ${darkMode ? 'border-gray-700/50' : 'border-gray-100'}`}>
                      <td className={`py-2 px-3 font-medium ${textPrimary}`}>{row.Computer}</td>
                      <td className="py-2 px-3">
                        <span className={`inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full text-xs font-medium`}
                          style={{ backgroundColor: `${statusColor}20`, color: statusColor }}>
                          <span className="w-1.5 h-1.5 rounded-full" style={{ backgroundColor: statusColor }} />
                          {row.Status}
                        </span>
                      </td>
                      <td className={`py-2 px-3 ${textSecondary}`}>{row.OSName || row.OSType || '—'}</td>
                      <td className={`py-2 px-3 font-mono text-xs ${textMuted}`}>{row.ComputerIP || '—'}</td>
                      <td className={`py-2 px-3 ${textMuted}`}>{row.Version || '—'}</td>
                      <td className={`py-2 px-3 ${textMuted}`}>
                        {timeAgo(row.LastHeartbeat)}
                        {row.MinutesSinceHeartbeat > 30 && (
                          <span className="ml-2 text-xs text-red-400">({row.MinutesSinceHeartbeat}m ago)</span>
                        )}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}

// ── Shared Components ────────────────────────────────────────────────────────

function KpiCard({ label, value, subtitle, color, darkMode, cardClass, textPrimary, textSecondary }) {
  return (
    <div className={`${cardClass} p-4`}>
      <p className={`text-xs font-medium uppercase tracking-wide ${textSecondary}`}>{label}</p>
      <p className={`text-2xl font-bold mt-1 ${textPrimary}`} style={color ? { color } : undefined}>{value}</p>
      {subtitle && <p className={`text-xs mt-0.5 ${textSecondary}`}>{subtitle}</p>}
    </div>
  );
}

function EmptyState({ message, darkMode }) {
  return (
    <div className={`text-center py-16 ${darkMode ? 'text-gray-500' : 'text-gray-400'}`}>
      <div className="text-4xl mb-3">📊</div>
      <p>{message}</p>
    </div>
  );
}
