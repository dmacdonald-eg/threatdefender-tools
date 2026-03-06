import React, { useState, useCallback, useMemo, useEffect, useRef } from 'react';
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

// ── Analytics Rule Health Queries ────────────────────────────────────────────

function buildAnalyticsRuleHealthQuery(timeRange) {
  return `
SentinelHealth
| where TimeGenerated > ago(${timeRange})
| where SentinelResourceType == "Analytics Rule"
| summarize arg_max(TimeGenerated, Status, Description, ExtendedProperties),
            FailureCount = countif(Status == "Failure"),
            SuccessCount = countif(Status == "Success"),
            TotalRuns = count()
  by SentinelResourceName, SentinelResourceId
| extend HealthPct = round(100.0 * SuccessCount / TotalRuns, 1)
| project RuleName = SentinelResourceName, RuleId = SentinelResourceId,
          LastStatus = Status, LastRun = TimeGenerated,
          HealthPct, FailureCount, SuccessCount, TotalRuns,
          LastDescription = Description
| order by FailureCount desc, RuleName asc
  `.trim();
}

function buildRuleFailureTimelineQuery(timeRange) {
  return `
SentinelHealth
| where TimeGenerated > ago(${timeRange})
| where SentinelResourceType == "Analytics Rule"
| where Status == "Failure"
| summarize Failures = count() by bin(TimeGenerated, 1h)
| order by TimeGenerated asc
  `.trim();
}

function buildRuleFailureDetailsQuery(timeRange) {
  return `
SentinelHealth
| where TimeGenerated > ago(${timeRange})
| where SentinelResourceType == "Analytics Rule"
| where Status == "Failure"
| extend Issues = parse_json(ExtendedProperties).Issues
| extend IssueCode = tostring(Issues[0].Code),
         IssueDetail = tostring(Issues[0].Description)
| project TimeGenerated, RuleName = SentinelResourceName,
          Description, Reason, IssueCode, IssueDetail
| order by TimeGenerated desc
  `.trim();
}

// ── Data Freshness Queries ───────────────────────────────────────────────────

function buildDataFreshnessQuery() {
  // Get actual last-event time per table using find (1d lookback to avoid timeout)
  return `
find withsource=DataType in (*) where TimeGenerated > ago(1d)
| summarize LastLog = max(TimeGenerated) by DataType
  `.trim();
}

function buildDataVolumeQuery() {
  // Get volume stats and table list from Usage (30d lookback for full picture)
  return `
Usage
| where TimeGenerated > ago(30d)
| where IsBillable == true
| summarize LastUsageBatch = max(TimeGenerated),
            AvgDailyGB = sum(Quantity) / 1024.0 / 30.0,
            DaysWithData = dcount(bin(TimeGenerated, 1d))
  by DataType
  `.trim();
}

// ── Ingestion Latency Queries ────────────────────────────────────────────────

function buildIngestionLatencyQuery(timeRange) {
  // Estimate ingestion latency from Usage table.
  // TimeGenerated in Usage = when the usage batch was recorded (proxy for ingestion time)
  // EndTime = latest event timestamp in that batch
  // Difference approximates ingestion delay
  return `
Usage
| where TimeGenerated > ago(${timeRange})
| where IsBillable == true
| extend EstimatedLatencyMin = datetime_diff('minute', TimeGenerated, EndTime)
| where EstimatedLatencyMin >= 0 and EstimatedLatencyMin < 1440
| summarize AvgLatencyMin = round(avg(EstimatedLatencyMin), 1),
            P50Latency = round(percentile(EstimatedLatencyMin, 50), 1),
            P95Latency = round(percentile(EstimatedLatencyMin, 95), 1),
            P99Latency = round(percentile(EstimatedLatencyMin, 99), 1),
            MaxLatency = max(EstimatedLatencyMin),
            SampleCount = count()
  by DataType
| where SampleCount > 3
| order by AvgLatencyMin desc
  `.trim();
}

function buildLatencyTrendQuery(timeRange) {
  return `
Usage
| where TimeGenerated > ago(${timeRange})
| where IsBillable == true
| extend EstimatedLatencyMin = datetime_diff('minute', TimeGenerated, EndTime)
| where EstimatedLatencyMin >= 0 and EstimatedLatencyMin < 1440
| summarize AvgLatencyMin = round(avg(EstimatedLatencyMin), 1),
            P95Latency = round(percentile(EstimatedLatencyMin, 95), 1)
  by bin(TimeGenerated, 1h)
| order by TimeGenerated asc
  `.trim();
}

// ── Rate Limit / Throttling Queries ──────────────────────────────────────────

function buildThrottlingQuery(timeRange) {
  return `
Operation
| where TimeGenerated > ago(${timeRange})
| where OperationCategory == "Ingestion"
| where Level == "Warning" or Level == "Error"
| summarize EventCount = count(),
            LastOccurrence = max(TimeGenerated)
  by OperationKey = strcat(Detail, " | ", Solution),
     Level, Detail, Solution
| order by EventCount desc
  `.trim();
}

function buildThrottlingTrendQuery(timeRange) {
  return `
Operation
| where TimeGenerated > ago(${timeRange})
| where OperationCategory == "Ingestion"
| where Level == "Warning" or Level == "Error"
| summarize Warnings = countif(Level == "Warning"),
            Errors = countif(Level == "Error")
  by bin(TimeGenerated, 1h)
| order by TimeGenerated asc
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

// ── Executive Health Summary Query (one per workspace) ───────────────────────

function buildExecutiveHealthQuery() {
  return `
let ingestion = Usage
| where TimeGenerated > ago(24h)
| where IsBillable == true
| summarize VolumeGB_24h = round(sum(Quantity) / 1024.0, 2), TableCount = dcount(DataType);
let ruleHealth = SentinelHealth
| where TimeGenerated > ago(24h)
| where SentinelResourceType == "Analytics Rule"
| summarize RuleFailures = countif(Status == "Failure"), RuleRuns = count(), RulesTracked = dcount(SentinelResourceName);
let connHealth = SentinelHealth
| where TimeGenerated > ago(24h)
| where SentinelResourceType == "Data connector"
| summarize ConnFailures = countif(Status == "Failure"), ConnTotal = dcount(SentinelResourceName);
let batchOrEventTables = dynamic(["IntuneDevices","IntuneOperationalLogs","UserPeerAnalytics","BehaviorAnalytics","Anomalies","IdentityInfo","AADRiskyUsers","AADUserRiskEvents","Watchlist","SentinelAudit","SentinelHealth","ThreatIntelligenceIndicator","SecurityRecommendation","SecurityBaseline","SecurityBaselineSummary","Update","UpdateSummary","InsightsMetrics","ConfigurationData","SqlVulnerabilityAssessmentScanStatus","SqlVulnerabilityAssessmentResult","AddonAzureBackupJobs","AddonAzureBackupPolicy","AddonAzureBackupStorage","CoreAzureBackup","AzureBackupOperations","Usage","OfficeActivity","AuditLogs","MicrosoftGraphActivityLogs","AzureActivity","AzureDiagnostics","SecurityAlert","SecurityIncident","LAQueryLogs","EmailPostDeliveryEvents","EmailEvents","EmailUrlInfo","EmailAttachmentInfo","AlertEvidence","AlertInfo","CloudAppEvents"]);
let staleTables = Usage
| where TimeGenerated > ago(30d)
| where IsBillable == true
| where DataType !in (batchOrEventTables)
| summarize LastSeen = max(TimeGenerated) by DataType
| where datetime_diff('hour', now(), LastSeen) > 24
| summarize StaleTables = count();
ingestion | extend p=1
| join kind=fullouter (ruleHealth | extend p=1) on p
| join kind=fullouter (connHealth | extend p=1) on p
| join kind=fullouter (staleTables | extend p=1) on p
| project VolumeGB_24h, TableCount, RuleFailures, RuleRuns, RulesTracked, ConnFailures, ConnTotal, StaleTables
  `.trim();
}

function computeOverallHealth(data) {
  if (!data) return { score: 0, status: 'Unknown', color: '#6b7280' };
  const ruleFailures = parseInt(data.RuleFailures) || 0;
  const ruleRuns = parseInt(data.RuleRuns) || 0;
  const connFailures = parseInt(data.ConnFailures) || 0;
  const staleTables = parseInt(data.StaleTables) || 0;
  const volumeGB = parseFloat(data.VolumeGB_24h) || 0;

  // No ingestion at all = critical
  if (volumeGB === 0) return { score: 0, status: 'Critical', color: '#ef4444' };

  let score = 100;
  // Rule health penalty
  if (ruleRuns > 0) score -= Math.min(30, (ruleFailures / ruleRuns) * 300);
  // Connector failure penalty
  if (connFailures > 0) score -= Math.min(20, connFailures * 10);
  // Stale table penalty
  score -= Math.min(20, staleTables * 5);

  score = Math.max(0, Math.round(score));
  if (score >= 90) return { score, status: 'Healthy', color: '#10b981' };
  if (score >= 70) return { score, status: 'Warning', color: '#f59e0b' };
  return { score, status: 'Critical', color: '#ef4444' };
}

const STATUS_COLORS = {
  Healthy: '#10b981',
  Warning: '#f59e0b',
  Critical: '#ef4444',
  Success: '#10b981',
  Failure: '#ef4444',
};

const TAB_ITEMS = [
  { id: 'executive', label: 'All Clients' },
  { id: 'overview', label: 'Overview' },
  { id: 'anomalies', label: 'Anomalies' },
  { id: 'freshness', label: 'Data Freshness' },
  { id: 'latency', label: 'Latency' },
  { id: 'rules', label: 'Rule Health' },
  { id: 'throttling', label: 'Throttling' },
  { id: 'connectors', label: 'Connectors' },
  { id: 'agents', label: 'Agents' },
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

  const [activeTab, setActiveTab] = useState('executive');
  const [timeRange, setTimeRange] = useState('7d');

  // Executive overview state (all-client scan)
  const [execHealthData, setExecHealthData] = useState({});  // { workspaceId: { data, loading, error } }
  const [execScanning, setExecScanning] = useState(false);

  // Data states
  const [ingestionData, setIngestionData] = useState(null);
  const [trendData, setTrendData] = useState(null);
  const [epsData, setEpsData] = useState(null);
  const [anomalyData, setAnomalyData] = useState(null);
  const [connectorData, setConnectorData] = useState(null);
  const [connectorTimeline, setConnectorTimeline] = useState(null);
  const [agentData, setAgentData] = useState(null);
  const [ruleHealthData, setRuleHealthData] = useState(null);
  const [ruleFailureTrend, setRuleFailureTrend] = useState(null);
  const [ruleFailureDetails, setRuleFailureDetails] = useState(null);
  const [freshnessData, setFreshnessData] = useState(null);
  const [latencyData, setLatencyData] = useState(null);
  const [latencyTrend, setLatencyTrend] = useState(null);
  const [throttlingData, setThrottlingData] = useState(null);
  const [throttlingTrend, setThrottlingTrend] = useState(null);

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

  // Executive scan — runs health query against all workspaces in parallel
  const scanAllWorkspaces = useCallback(async (wsList) => {
    if (!wsList?.length) return;
    setExecScanning(true);
    const query = buildExecutiveHealthQuery();

    // Process in batches of 5 to avoid overwhelming the API
    const batchSize = 5;
    for (let i = 0; i < wsList.length; i += batchSize) {
      const batch = wsList.slice(i, i + batchSize);
      const promises = batch.map(async (ws) => {
        const cacheKey = `dch_exec_${ws.customerId}`;
        const cached = getCached(cacheKey);
        if (cached) {
          setExecHealthData(prev => ({ ...prev, [ws.customerId]: { data: cached[0] || {}, loading: false, error: null } }));
          return;
        }
        setExecHealthData(prev => ({ ...prev, [ws.customerId]: { data: null, loading: true, error: null } }));
        try {
          const result = await fetchFromLogAnalytics(ws.customerId, query);
          const rows = parseLogAnalyticsRows(result);
          setCache(cacheKey, rows);
          setExecHealthData(prev => ({ ...prev, [ws.customerId]: { data: rows[0] || {}, loading: false, error: null } }));
        } catch (err) {
          setExecHealthData(prev => ({ ...prev, [ws.customerId]: { data: null, loading: false, error: err.message } }));
        }
      });
      await Promise.all(promises);
    }
    setExecScanning(false);
  }, [fetchFromLogAnalytics]);

  // Fetch tab data
  const fetchData = useCallback(async (tab) => {
    if (tab === 'executive') {
      if (workspaces.length > 0) scanAllWorkspaces(workspaces);
      return;
    }
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

        // Track which tables are already claimed by ARM connectors
        const claimedTables = new Set();
        merged.forEach(c => c.tables.forEach(t => claimedTables.add(t.table)));
        // Also claim tables from the CONNECTOR_KIND_MAP even if no data
        armConnectors.forEach(c => {
          const tables = getConnectorTables(c);
          tables.forEach(t => claimedTables.add(t));
        });

        // Create virtual connectors from Usage data for tables not covered by ARM
        const TABLE_TO_SOURCE = {
          'SecurityAlert': 'Security Alerts (Multiple Sources)',
          'SecurityEvent': 'Windows Security Events',
          'Syslog': 'Syslog',
          'CommonSecurityLog': 'Common Event Format (CEF)',
          'SigninLogs': 'Entra ID Sign-in Logs',
          'AuditLogs': 'Entra ID Audit Logs',
          'AADNonInteractiveUserSignInLogs': 'Entra ID Non-Interactive Sign-ins',
          'AADServicePrincipalSignInLogs': 'Entra ID Service Principal Sign-ins',
          'AADManagedIdentitySignInLogs': 'Entra ID Managed Identity Sign-ins',
          'AADProvisioningLogs': 'Entra ID Provisioning',
          'AzureActivity': 'Azure Activity',
          'AzureDiagnostics': 'Azure Diagnostics',
          'DeviceEvents': 'M365 Defender - Device Events',
          'DeviceProcessEvents': 'M365 Defender - Process Events',
          'DeviceNetworkEvents': 'M365 Defender - Network Events',
          'DeviceFileEvents': 'M365 Defender - File Events',
          'DeviceRegistryEvents': 'M365 Defender - Registry Events',
          'DeviceLogonEvents': 'M365 Defender - Logon Events',
          'DeviceImageLoadEvents': 'M365 Defender - Image Load Events',
          'DeviceInfo': 'M365 Defender - Device Info',
          'EmailEvents': 'M365 Defender - Email Events',
          'EmailUrlInfo': 'M365 Defender - Email URL Info',
          'EmailAttachmentInfo': 'M365 Defender - Email Attachments',
          'EmailPostDeliveryEvents': 'M365 Defender - Post Delivery',
          'AlertInfo': 'M365 Defender - Alert Info',
          'AlertEvidence': 'M365 Defender - Alert Evidence',
          'IdentityLogonEvents': 'M365 Defender - Identity Logon',
          'IdentityQueryEvents': 'M365 Defender - Identity Query',
          'IdentityDirectoryEvents': 'M365 Defender - Identity Directory',
          'CloudAppEvents': 'M365 Defender - Cloud App Events',
          'UrlClickEvents': 'M365 Defender - URL Click Events',
          'ThreatIntelligenceIndicator': 'Threat Intelligence Indicators',
          'Heartbeat': 'Agent Heartbeat',
          'W3CIISLog': 'IIS Logs',
          'DnsEvents': 'DNS Events',
          'WindowsFirewall': 'Windows Firewall',
          'OfficeActivity': 'Office 365 Activity',
          'SecurityIncident': 'Sentinel Incidents',
          'SentinelHealth': 'Sentinel Health',
        };

        // Group unclaimed tables by source name to avoid one row per M365D table
        const sourceGroups = {};
        lastLogRows.forEach(r => {
          if (claimedTables.has(r.DataType)) return;
          const sourceName = TABLE_TO_SOURCE[r.DataType] || r.DataType;
          // Group M365 Defender tables together
          const groupKey = sourceName.startsWith('M365 Defender') ? 'Microsoft 365 Defender (Advanced Hunting)' : sourceName;
          if (!sourceGroups[groupKey]) {
            sourceGroups[groupKey] = { tables: [], lastDataReceived: null };
          }
          const vol = parseFloat(r.VolumeGB) || 0;
          const logTime = new Date(r.LastLog);
          sourceGroups[groupKey].tables.push({ table: r.DataType, lastLog: r.LastLog, volumeGB: vol });
          if (!sourceGroups[groupKey].lastDataReceived || logTime > sourceGroups[groupKey].lastDataReceived) {
            sourceGroups[groupKey].lastDataReceived = logTime;
          }
        });

        Object.entries(sourceGroups).forEach(([name, group]) => {
          const minutesAgo = group.lastDataReceived ? (Date.now() - group.lastDataReceived.getTime()) / 60000 : Infinity;
          const status = minutesAgo < 1440 ? 'Active' : 'Stale';

          // Check if any health data matches
          const healthMatch = Object.values(healthLookup).find(h =>
            name.toLowerCase().includes(h.SentinelResourceName?.toLowerCase() || '___')
            || h.SentinelResourceName?.toLowerCase().includes(name.toLowerCase().split(' ')[0] || '___')
          );

          merged.push({
            name,
            kind: 'Usage-detected',
            status: healthMatch ? (healthMatch.LastStatus || status) : status,
            healthPct: healthMatch ? parseFloat(healthMatch.HealthPct) || (status === 'Active' ? 100 : 50) : (status === 'Active' ? 100 : 50),
            failureCount: healthMatch ? parseInt(healthMatch.FailureCount) || 0 : 0,
            successCount: healthMatch ? parseInt(healthMatch.SuccessCount) || 0 : 0,
            lastChecked: healthMatch?.LastChecked || null,
            lastDataReceived: group.lastDataReceived?.toISOString() || null,
            tables: group.tables,
            hasHealthData: !!healthMatch,
            enabled: true,
          });
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
      } else if (tab === 'rules') {
        const [rules, trend, details] = await Promise.all([
          runQuery(buildAnalyticsRuleHealthQuery(timeRange), `dch_rules_${wsKey}_${timeRange}`, { throwOnError: false }),
          runQuery(buildRuleFailureTimelineQuery(timeRange), `dch_rulefailures_${wsKey}_${timeRange}`, { throwOnError: false }),
          runQuery(buildRuleFailureDetailsQuery(timeRange), `dch_ruledetails_${wsKey}_${timeRange}`, { throwOnError: false }),
        ]);
        setRuleHealthData(rules?.error ? [] : rules);
        setRuleFailureTrend(trend?.error ? [] : trend);
        setRuleFailureDetails(details?.error ? [] : details);
        if (rules?.error) {
          setError('SentinelHealth table is not available. Enable Health Monitoring in Sentinel Settings to track analytics rule health.');
        }
      } else if (tab === 'freshness') {
        const [freshness, volume] = await Promise.all([
          runQuery(buildDataFreshnessQuery(), `dch_freshness_${wsKey}`, { throwOnError: false }),
          runQuery(buildDataVolumeQuery(), `dch_volume_${wsKey}`, { throwOnError: false }),
        ]);
        // Build merged freshness from both sources
        // find query gives real-time last event (1d window)
        // Usage gives volume stats + fallback last-log for older tables
        const findMap = {};
        if (freshness && !freshness.error) {
          freshness.forEach(f => { findMap[f.DataType] = f.LastLog; });
        }
        const volumeRows = (volume && !volume.error) ? volume : [];
        if (volumeRows.length === 0 && (!freshness || freshness.error)) {
          setFreshnessData([]);
          setError('Data freshness queries failed. This may be a permissions issue.');
        } else {
          // Start with all tables from Usage, overlay with find results
          const tableSet = new Set([
            ...volumeRows.map(v => v.DataType),
            ...Object.keys(findMap),
          ]);
          const merged = Array.from(tableSet).map(dt => {
            const vol = volumeRows.find(v => v.DataType === dt) || {};
            const lastLog = findMap[dt] || vol.LastUsageBatch || null;
            const mins = lastLog ? Math.max(0, Math.round((Date.now() - new Date(lastLog).getTime()) / 60000)) : 99999;
            const status = mins <= 60 ? 'Fresh' : mins <= 1440 ? 'Aging' : mins <= 4320 ? 'Stale' : 'Critical';
            return {
              DataType: dt,
              LastLog: lastLog,
              MinutesSinceLastLog: String(mins),
              FreshnessStatus: status,
              AvgDailyGB: vol.AvgDailyGB || '0',
              DaysWithData: vol.DaysWithData || '0',
            };
          });
          merged.sort((a, b) => parseInt(b.MinutesSinceLastLog) - parseInt(a.MinutesSinceLastLog));
          setFreshnessData(merged);
        }
      } else if (tab === 'latency') {
        const [latency, trend] = await Promise.all([
          runQuery(buildIngestionLatencyQuery(timeRange), `dch_latency_${wsKey}_${timeRange}`, { throwOnError: false }),
          runQuery(buildLatencyTrendQuery(timeRange), `dch_latencytrend_${wsKey}_${timeRange}`, { throwOnError: false }),
        ]);
        setLatencyData(latency?.error ? [] : latency);
        setLatencyTrend(trend?.error ? [] : trend);
        if (latency?.error) {
          setError('Latency query failed. The Usage table may not be accessible in this workspace.');
        }
      } else if (tab === 'throttling') {
        const [throttle, trend] = await Promise.all([
          runQuery(buildThrottlingQuery(timeRange), `dch_throttle_${wsKey}_${timeRange}`, { throwOnError: false }),
          runQuery(buildThrottlingTrendQuery(timeRange), `dch_throttletrend_${wsKey}_${timeRange}`, { throwOnError: false }),
        ]);
        setThrottlingData(throttle?.error ? [] : throttle);
        setThrottlingTrend(trend?.error ? [] : trend);
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
    setRuleHealthData(null);
    setRuleFailureTrend(null);
    setRuleFailureDetails(null);
    setFreshnessData(null);
    setLatencyData(null);
    setLatencyTrend(null);
    setThrottlingData(null);
    setThrottlingTrend(null);
    setError(null);
  };

  const handleTimeRangeChange = (range) => {
    setTimeRange(range);
    // Data will be refreshed when user clicks Load or via effect
  };

  // Auto-load workspaces on mount, then auto-scan
  const hasAutoScanned = useRef(false);
  useEffect(() => {
    if (isAuthenticated && !workspaces.length && !loadingWorkspaces) {
      loadWorkspaces();
    }
  }, [isAuthenticated, loadWorkspaces, workspaces.length, loadingWorkspaces]);

  useEffect(() => {
    if (workspaces.length > 0 && !hasAutoScanned.current && Object.keys(execHealthData).length === 0) {
      hasAutoScanned.current = true;
      scanAllWorkspaces(workspaces);
    }
  }, [workspaces, scanAllWorkspaces, execHealthData]);

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

  // ── Drill-down handler ─────────────────────────────────────────────────────

  const handleDrillDown = (ws) => {
    handleWorkspaceSelect(ws);
    setActiveTab('overview');
    // fetchData will be triggered by the tab change effect
  };

  const handleBackToExecutive = () => {
    setSelectedWorkspace(null);
    setActiveTab('executive');
    setError(null);
  };

  // Determine if detail tabs should be available
  const isDetailTab = activeTab !== 'executive';

  // ── Main dashboard ─────────────────────────────────────────────────────────

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <div>
          <h2 className={`text-xl font-semibold ${textPrimary}`}>Data Collection Health Monitor</h2>
          <p className={`text-sm mt-1 ${textSecondary}`}>
            {activeTab === 'executive' ? (
              'All client workspaces at a glance'
            ) : selectedWorkspace ? (
              <>
                {selectedWorkspace.name}
                <button onClick={handleBackToExecutive} className="ml-2 text-blue-500 hover:text-blue-400 text-xs">(all clients)</button>
              </>
            ) : (
              <span>Select a client from the All Clients tab to view details</span>
            )}
          </p>
        </div>
        <div className="flex items-center gap-3">
          {activeTab === 'executive' ? (
            <>
              {!workspaces.length && !loadingWorkspaces && (
                <motion.button
                  whileHover={{ scale: 1.02 }}
                  whileTap={{ scale: 0.98 }}
                  onClick={async () => { const ws = await loadWorkspaces(); }}
                  className="px-4 py-1.5 bg-blue-600 text-white text-sm rounded-lg font-medium hover:bg-blue-700 transition-colors"
                >
                  Load Workspaces
                </motion.button>
              )}
              {workspaces.length > 0 && (
                <motion.button
                  whileHover={{ scale: 1.02 }}
                  whileTap={{ scale: 0.98 }}
                  onClick={() => scanAllWorkspaces(workspaces)}
                  disabled={execScanning}
                  className="px-4 py-1.5 bg-blue-600 text-white text-sm rounded-lg font-medium hover:bg-blue-700 transition-colors disabled:opacity-50"
                >
                  {execScanning ? 'Scanning...' : Object.keys(execHealthData).length > 0 ? 'Rescan All' : 'Scan All Clients'}
                </motion.button>
              )}
            </>
          ) : (
            <>
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
                disabled={loading || !selectedWorkspace}
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
            </>
          )}
        </div>
      </div>

      {/* Tabs */}
      <div className={`flex gap-1 p-1 rounded-lg overflow-x-auto ${darkMode ? 'bg-gray-800' : 'bg-gray-100'}`}>
        {TAB_ITEMS.map(tab => {
          const isDisabled = tab.id !== 'executive' && !selectedWorkspace;
          return (
            <button
              key={tab.id}
              onClick={() => !isDisabled && handleTabChange(tab.id)}
              disabled={isDisabled}
              className={`px-3 py-2 text-sm font-medium rounded-md transition-colors whitespace-nowrap ${
                activeTab === tab.id
                  ? darkMode
                    ? 'bg-gray-700 text-white'
                    : 'bg-white text-gray-900 shadow-sm'
                  : isDisabled
                  ? 'text-gray-600 cursor-not-allowed opacity-40'
                  : darkMode
                  ? 'text-gray-400 hover:text-gray-200'
                  : 'text-gray-600 hover:text-gray-900'
              }`}
            >
              {tab.label}
            </button>
          );
        })}
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
            {activeTab === 'executive' && <ExecutiveOverview
              workspaces={workspaces} execHealthData={execHealthData}
              execScanning={execScanning} loadingWorkspaces={loadingWorkspaces}
              onDrillDown={handleDrillDown} onLoadWorkspaces={loadWorkspaces}
              onScanAll={scanAllWorkspaces}
              darkMode={darkMode} cardClass={cardClass}
              textPrimary={textPrimary} textSecondary={textSecondary} textMuted={textMuted}
            />}
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
            {activeTab === 'rules' && <RuleHealthTab
              ruleHealthData={ruleHealthData} ruleFailureTrend={ruleFailureTrend}
              ruleFailureDetails={ruleFailureDetails}
              darkMode={darkMode} cardClass={cardClass}
              textPrimary={textPrimary} textSecondary={textSecondary} textMuted={textMuted}
            />}
            {activeTab === 'freshness' && <FreshnessTab
              freshnessData={freshnessData} darkMode={darkMode} cardClass={cardClass}
              textPrimary={textPrimary} textSecondary={textSecondary} textMuted={textMuted}
            />}
            {activeTab === 'latency' && <LatencyTab
              latencyData={latencyData} latencyTrend={latencyTrend}
              darkMode={darkMode} cardClass={cardClass}
              textPrimary={textPrimary} textSecondary={textSecondary} textMuted={textMuted}
            />}
            {activeTab === 'throttling' && <ThrottlingTab
              throttlingData={throttlingData} throttlingTrend={throttlingTrend}
              darkMode={darkMode} cardClass={cardClass}
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

// ── Executive Overview (All Clients) ─────────────────────────────────────────

function ExecutiveOverview({ workspaces, execHealthData, execScanning, loadingWorkspaces, onDrillDown, onLoadWorkspaces, onScanAll, darkMode, cardClass, textPrimary, textSecondary, textMuted }) {
  if (!workspaces.length) {
    return (
      <div className={`${cardClass} p-12 text-center`}>
        {loadingWorkspaces ? (
          <div className={`flex flex-col items-center gap-3 ${textSecondary}`}>
            <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500" />
            <span>Discovering Sentinel workspaces...</span>
          </div>
        ) : (
          <>
            <p className={`font-medium text-lg mb-2 ${textPrimary}`}>Welcome to Health Monitor</p>
            <p className={`text-sm mb-4 ${textSecondary}`}>Load your Sentinel workspaces to begin the health scan.</p>
            <motion.button
              whileHover={{ scale: 1.02 }}
              whileTap={{ scale: 0.98 }}
              onClick={onLoadWorkspaces}
              className="px-6 py-2.5 bg-blue-600 text-white rounded-lg font-medium hover:bg-blue-700 transition-colors"
            >
              Load Workspaces
            </motion.button>
          </>
        )}
      </div>
    );
  }

  const scannedCount = Object.values(execHealthData).filter(v => v.data && !v.loading).length;
  const noData = scannedCount === 0;

  // Aggregate stats
  const allHealthScores = workspaces.map(ws => {
    const entry = execHealthData[ws.customerId];
    return entry?.data ? computeOverallHealth(entry.data) : null;
  }).filter(Boolean);

  const healthyCt = allHealthScores.filter(h => h.status === 'Healthy').length;
  const warningCt = allHealthScores.filter(h => h.status === 'Warning').length;
  const criticalCt = allHealthScores.filter(h => h.status === 'Critical').length;
  const totalVolume = workspaces.reduce((sum, ws) => {
    const d = execHealthData[ws.customerId]?.data;
    return sum + (parseFloat(d?.VolumeGB_24h) || 0);
  }, 0);
  const totalRuleFailures = workspaces.reduce((sum, ws) => {
    const d = execHealthData[ws.customerId]?.data;
    return sum + (parseInt(d?.RuleFailures) || 0);
  }, 0);

  // Sort: critical first, then warning, then healthy, then unscanned
  const sortedWorkspaces = [...workspaces].sort((a, b) => {
    const aEntry = execHealthData[a.customerId];
    const bEntry = execHealthData[b.customerId];
    const aHealth = aEntry?.data ? computeOverallHealth(aEntry.data) : null;
    const bHealth = bEntry?.data ? computeOverallHealth(bEntry.data) : null;
    if (!aHealth && !bHealth) return 0;
    if (!aHealth) return 1;
    if (!bHealth) return -1;
    return aHealth.score - bHealth.score;
  });

  return (
    <div className="space-y-6">
      {/* Summary KPIs */}
      {!noData && (
        <div className="grid grid-cols-2 md:grid-cols-6 gap-4">
          <KpiCard label="Clients Scanned" value={`${scannedCount}/${workspaces.length}`} darkMode={darkMode} cardClass={cardClass} textPrimary={textPrimary} textSecondary={textSecondary} />
          <KpiCard label="Healthy" value={healthyCt} color="#10b981" darkMode={darkMode} cardClass={cardClass} textPrimary={textPrimary} textSecondary={textSecondary} />
          <KpiCard label="Warning" value={warningCt} color={warningCt > 0 ? '#f59e0b' : '#10b981'} darkMode={darkMode} cardClass={cardClass} textPrimary={textPrimary} textSecondary={textSecondary} />
          <KpiCard label="Critical" value={criticalCt} color={criticalCt > 0 ? '#ef4444' : '#10b981'} darkMode={darkMode} cardClass={cardClass} textPrimary={textPrimary} textSecondary={textSecondary} />
          <KpiCard label="Total Ingestion (24h)" value={`${totalVolume.toFixed(2)} GB`} darkMode={darkMode} cardClass={cardClass} textPrimary={textPrimary} textSecondary={textSecondary} />
          <KpiCard label="Rule Failures (24h)" value={totalRuleFailures} color={totalRuleFailures > 0 ? '#f59e0b' : '#10b981'} darkMode={darkMode} cardClass={cardClass} textPrimary={textPrimary} textSecondary={textSecondary} />
        </div>
      )}

      {noData && !execScanning && (
        <div className={`${cardClass} p-8 text-center`}>
          <p className={`font-medium mb-2 ${textPrimary}`}>{workspaces.length} workspaces discovered</p>
          <p className={`text-sm ${textSecondary}`}>Click "Scan All Clients" to run a health check across all workspaces.</p>
        </div>
      )}

      {/* Client health cards */}
      <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
        {sortedWorkspaces.map(ws => {
          const entry = execHealthData[ws.customerId];
          const data = entry?.data;
          const isLoading = entry?.loading;
          const wsError = entry?.error;
          const health = data ? computeOverallHealth(data) : null;

          return (
            <motion.button
              key={ws.customerId}
              whileHover={{ scale: 1.01 }}
              whileTap={{ scale: 0.99 }}
              onClick={() => onDrillDown(ws)}
              className={`text-left p-4 rounded-lg border transition-all ${
                darkMode
                  ? 'bg-gray-800 border-gray-700 hover:border-blue-500'
                  : 'bg-white border-gray-200 hover:border-blue-400'
              }`}
            >
              {/* Header row */}
              <div className="flex items-center justify-between mb-3">
                <div className={`font-medium truncate ${textPrimary}`}>{ws.name}</div>
                {health && (
                  <span className="inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full text-xs font-semibold"
                    style={{ backgroundColor: `${health.color}20`, color: health.color }}>
                    <span className="w-1.5 h-1.5 rounded-full" style={{ backgroundColor: health.color }} />
                    {health.score}%
                  </span>
                )}
                {isLoading && (
                  <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-blue-500" />
                )}
              </div>

              {wsError && (
                <p className="text-red-400 text-xs truncate">{wsError}</p>
              )}

              {data && (
                <div className="grid grid-cols-2 gap-x-4 gap-y-1.5 text-xs">
                  <div className="flex justify-between">
                    <span className={textMuted}>Ingestion</span>
                    <span className={textSecondary}>{parseFloat(data.VolumeGB_24h || 0).toFixed(2)} GB</span>
                  </div>
                  <div className="flex justify-between">
                    <span className={textMuted}>Tables</span>
                    <span className={textSecondary}>{data.TableCount || 0}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className={textMuted}>Rules</span>
                    <span className={textSecondary}>{data.RulesTracked || 0} tracked</span>
                  </div>
                  <div className="flex justify-between">
                    <span className={textMuted}>Rule Failures</span>
                    <span className={parseInt(data.RuleFailures) > 0 ? 'text-red-400 font-medium' : textSecondary}>
                      {data.RuleFailures || 0}
                    </span>
                  </div>
                  <div className="flex justify-between">
                    <span className={textMuted}>Connectors</span>
                    <span className={textSecondary}>{data.ConnTotal || 0}</span>
                  </div>
                  <div className="flex justify-between">
                    <span className={textMuted}>Stale Tables</span>
                    <span className={parseInt(data.StaleTables) > 0 ? 'text-yellow-400 font-medium' : textSecondary}>
                      {data.StaleTables || 0}
                    </span>
                  </div>
                </div>
              )}

              {!data && !isLoading && !wsError && (
                <p className={`text-xs ${textMuted}`}>Not yet scanned</p>
              )}

              <div className={`text-[10px] mt-2 ${textMuted}`}>Click to drill down</div>
            </motion.button>
          );
        })}
      </div>
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

// ── Rule Health Tab ──────────────────────────────────────────────────────────

function RuleHealthTab({ ruleHealthData, ruleFailureTrend, ruleFailureDetails, darkMode, cardClass, textPrimary, textSecondary, textMuted }) {
  const [expandedRule, setExpandedRule] = useState(null);

  if (!ruleHealthData) {
    return <EmptyState message="Click 'Load Data' to view analytics rule health." darkMode={darkMode} />;
  }

  const totalRules = ruleHealthData.length;
  const failingRules = ruleHealthData.filter(r => parseInt(r.FailureCount) > 0);
  const healthyRules = ruleHealthData.filter(r => parseInt(r.FailureCount) === 0);
  const avgHealth = totalRules > 0
    ? (ruleHealthData.reduce((sum, r) => sum + (parseFloat(r.HealthPct) || 0), 0) / totalRules).toFixed(1)
    : '—';

  const failureTrendData = (ruleFailureTrend || []).map(r => ({
    time: new Date(r.TimeGenerated).toLocaleString(undefined, { month: 'short', day: 'numeric', hour: '2-digit' }),
    failures: parseInt(r.Failures) || 0,
  }));

  // Group failure details by rule name for expandable rows
  const failuresByRule = {};
  (ruleFailureDetails || []).forEach(d => {
    if (!failuresByRule[d.RuleName]) failuresByRule[d.RuleName] = [];
    failuresByRule[d.RuleName].push(d);
  });

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <KpiCard label="Rules Tracked" value={totalRules} darkMode={darkMode} cardClass={cardClass} textPrimary={textPrimary} textSecondary={textSecondary} />
        <KpiCard label="Healthy" value={healthyRules.length} color="#10b981" darkMode={darkMode} cardClass={cardClass} textPrimary={textPrimary} textSecondary={textSecondary} />
        <KpiCard label="With Failures" value={failingRules.length} color={failingRules.length > 0 ? '#ef4444' : '#10b981'} darkMode={darkMode} cardClass={cardClass} textPrimary={textPrimary} textSecondary={textSecondary} />
        <KpiCard label="Avg Health" value={`${avgHealth}%`} darkMode={darkMode} cardClass={cardClass} textPrimary={textPrimary} textSecondary={textSecondary} />
      </div>

      {/* Failure trend chart */}
      {failureTrendData.length > 0 && (
        <div className={`${cardClass} p-5`}>
          <h3 className={`text-sm font-semibold mb-4 ${textPrimary}`}>Rule Failures Over Time</h3>
          <ResponsiveContainer width="100%" height={200}>
            <BarChart data={failureTrendData}>
              <CartesianGrid strokeDasharray="3 3" stroke={darkMode ? '#374151' : '#e5e7eb'} />
              <XAxis dataKey="time" tick={{ fontSize: 10, fill: darkMode ? '#9ca3af' : '#6b7280' }} interval="preserveStartEnd" />
              <YAxis tick={{ fontSize: 11, fill: darkMode ? '#9ca3af' : '#6b7280' }} allowDecimals={false} />
              <Tooltip contentStyle={{ backgroundColor: darkMode ? '#1f2937' : '#fff', border: `1px solid ${darkMode ? '#374151' : '#e5e7eb'}`, borderRadius: '8px' }} />
              <Bar dataKey="failures" fill="#ef4444" name="Failures" />
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}

      {totalRules === 0 ? (
        <div className={`${cardClass} p-8 text-center`}>
          <p className={`font-medium ${textPrimary}`}>No Analytics Rule Health Data</p>
          <p className={`text-sm mt-1 ${textSecondary}`}>Enable Health Monitoring in Sentinel Settings to track rule execution health.</p>
        </div>
      ) : (
        <div className={`${cardClass} p-5`}>
          <h3 className={`text-sm font-semibold mb-4 ${textPrimary}`}>
            Analytics Rules ({totalRules})
            {failingRules.length > 0 && <span className="ml-2 text-red-400 text-xs font-normal">({failingRules.length} with failures)</span>}
          </h3>
          <div className="overflow-x-auto max-h-[500px] overflow-y-auto">
            <table className="w-full text-sm">
              <thead className="sticky top-0">
                <tr className={`border-b ${darkMode ? 'border-gray-700 bg-gray-800' : 'border-gray-200 bg-white'}`}>
                  <th className={`text-left py-2 px-3 font-medium ${textSecondary} w-6`}></th>
                  <th className={`text-left py-2 px-3 font-medium ${textSecondary}`}>Rule Name</th>
                  <th className={`text-left py-2 px-3 font-medium ${textSecondary}`}>Status</th>
                  <th className={`text-right py-2 px-3 font-medium ${textSecondary}`}>Health %</th>
                  <th className={`text-right py-2 px-3 font-medium ${textSecondary}`}>Runs</th>
                  <th className={`text-right py-2 px-3 font-medium ${textSecondary}`}>Failures</th>
                  <th className={`text-left py-2 px-3 font-medium ${textSecondary}`}>Last Run</th>
                </tr>
              </thead>
              <tbody>
                {ruleHealthData.map((row, i) => {
                  const healthPct = parseFloat(row.HealthPct) || 0;
                  const failures = parseInt(row.FailureCount) || 0;
                  const statusColor = healthPct >= 95 ? STATUS_COLORS.Healthy : healthPct >= 50 ? STATUS_COLORS.Warning : STATUS_COLORS.Critical;
                  const isExpanded = expandedRule === row.RuleName;
                  const ruleFailures = failuresByRule[row.RuleName] || [];
                  const hasFailures = failures > 0 && ruleFailures.length > 0;
                  return (
                    <React.Fragment key={i}>
                      <tr
                        className={`border-b ${darkMode ? 'border-gray-700/50' : 'border-gray-100'} ${hasFailures ? 'cursor-pointer hover:bg-white/5' : ''}`}
                        onClick={() => hasFailures && setExpandedRule(isExpanded ? null : row.RuleName)}
                      >
                        <td className={`py-2 px-3 ${textMuted}`}>
                          {hasFailures && (
                            <span className={`text-xs transition-transform inline-block ${isExpanded ? 'rotate-90' : ''}`}>&#9654;</span>
                          )}
                        </td>
                        <td className={`py-2 px-3 font-medium ${textPrimary}`}>{row.RuleName}</td>
                        <td className="py-2 px-3">
                          <span className="flex items-center gap-2">
                            <span className="w-2 h-2 rounded-full" style={{ backgroundColor: statusColor }} />
                            <span className={textSecondary}>{row.LastStatus || 'Unknown'}</span>
                          </span>
                        </td>
                        <td className={`py-2 px-3 text-right font-medium`} style={{ color: statusColor }}>{healthPct}%</td>
                        <td className={`py-2 px-3 text-right ${textSecondary}`}>{row.TotalRuns}</td>
                        <td className={`py-2 px-3 text-right ${failures > 0 ? 'text-red-400 font-medium' : textMuted}`}>{failures}</td>
                        <td className={`py-2 px-3 ${textMuted}`}>{timeAgo(row.LastRun)}</td>
                      </tr>
                      {isExpanded && ruleFailures.length > 0 && (
                        <tr>
                          <td colSpan={7} className="p-0">
                            <div className={`mx-3 my-2 rounded-lg ${darkMode ? 'bg-gray-800/80 border border-gray-700' : 'bg-gray-50 border border-gray-200'}`}>
                              <div className="px-4 py-3">
                                <h4 className={`text-xs font-semibold mb-3 ${textSecondary}`}>
                                  Recent Failures ({Math.min(ruleFailures.length, 10)} of {ruleFailures.length})
                                </h4>
                                <div className="space-y-2">
                                  {ruleFailures.slice(0, 10).map((f, j) => (
                                    <div key={j} className={`text-xs p-2.5 rounded ${darkMode ? 'bg-gray-900/60' : 'bg-white'}`}>
                                      <div className="flex items-center justify-between mb-1">
                                        <span className={`font-medium ${textMuted}`}>
                                          {new Date(f.TimeGenerated).toLocaleString()}
                                        </span>
                                        {f.IssueCode && (
                                          <span className="px-1.5 py-0.5 rounded bg-red-500/10 text-red-400 font-mono text-[10px]">
                                            {f.IssueCode}
                                          </span>
                                        )}
                                      </div>
                                      <p className={`${textSecondary} leading-relaxed`}>{f.Description}</p>
                                      {f.IssueDetail && f.IssueDetail !== f.Description && (
                                        <p className={`${textMuted} mt-1 leading-relaxed`}>{f.IssueDetail}</p>
                                      )}
                                      {f.Reason && f.Reason !== f.Description && (
                                        <p className={`${textMuted} mt-1 italic leading-relaxed`}>{f.Reason}</p>
                                      )}
                                    </div>
                                  ))}
                                </div>
                              </div>
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
          {failingRules.length > 0 && (
            <div className={`mt-4 p-3 rounded-lg ${darkMode ? 'bg-red-500/10 border border-red-500/20' : 'bg-red-50 border border-red-200'}`}>
              <p className="text-red-400 text-xs font-medium">Failing rules may not be generating alerts. Review and fix these rules in Sentinel to avoid missed detections. Click a row to see failure details.</p>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

// ── Data Freshness Tab ───────────────────────────────────────────────────────

// Tables that sync on a batch/periodic schedule (not real-time streaming)
const BATCH_TABLES = new Set([
  'IntuneDevices', 'IntuneOperationalLogs',
  'UserPeerAnalytics', 'BehaviorAnalytics', 'Anomalies', 'IdentityInfo',
  'AADRiskyUsers', 'AADUserRiskEvents',
  'Watchlist', 'SentinelAudit', 'SentinelHealth',
  'ThreatIntelligenceIndicator',
  'SecurityRecommendation', 'SecurityBaseline', 'SecurityBaselineSummary',
  'Update', 'UpdateSummary',
  'InsightsMetrics', 'ConfigurationData',
  'SqlVulnerabilityAssessmentScanStatus', 'SqlVulnerabilityAssessmentResult',
  'AddonAzureBackupJobs', 'AddonAzureBackupPolicy', 'AddonAzureBackupStorage',
  'CoreAzureBackup', 'AzureBackupOperations',
  'Usage',
]);

// Tables that only log on-demand (event-driven, may be quiet for long periods)
const EVENT_DRIVEN_TABLES = new Set([
  'OfficeActivity', 'AuditLogs', 'MicrosoftGraphActivityLogs',
  'AzureActivity', 'AzureDiagnostics',
  'SecurityAlert', 'SecurityIncident',
  'SentinelAudit',
  'LAQueryLogs',
  'EmailPostDeliveryEvents', 'EmailEvents', 'EmailUrlInfo', 'EmailAttachmentInfo',
  'AlertEvidence', 'AlertInfo',
  'CloudAppEvents',
]);

function getTableType(dataType) {
  if (BATCH_TABLES.has(dataType)) return { label: 'Batch', tip: 'Syncs periodically (hours/daily). Aging status is expected.' };
  if (EVENT_DRIVEN_TABLES.has(dataType)) return { label: 'Event', tip: 'Logs only when events occur. Gaps are normal during quiet periods.' };
  return { label: 'Stream', tip: 'Real-time streaming table. Should stay Fresh.' };
}

function FreshnessTab({ freshnessData, darkMode, cardClass, textPrimary, textSecondary, textMuted }) {
  if (!freshnessData) {
    return <EmptyState message="Click 'Load Data' to check data freshness across all tables." darkMode={darkMode} />;
  }

  const fresh = freshnessData.filter(r => r.FreshnessStatus === 'Fresh');
  const aging = freshnessData.filter(r => r.FreshnessStatus === 'Aging');
  const stale = freshnessData.filter(r => r.FreshnessStatus === 'Stale');
  const critical = freshnessData.filter(r => r.FreshnessStatus === 'Critical');

  const FRESHNESS_COLORS = { Fresh: '#10b981', Aging: '#3b82f6', Stale: '#f59e0b', Critical: '#ef4444' };

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
        <KpiCard label="Total Tables" value={freshnessData.length} darkMode={darkMode} cardClass={cardClass} textPrimary={textPrimary} textSecondary={textSecondary} />
        <KpiCard label="Fresh (<1h)" value={fresh.length} color="#10b981" darkMode={darkMode} cardClass={cardClass} textPrimary={textPrimary} textSecondary={textSecondary} />
        <KpiCard label="Aging (1-24h)" value={aging.length} color="#3b82f6" darkMode={darkMode} cardClass={cardClass} textPrimary={textPrimary} textSecondary={textSecondary} />
        <KpiCard label="Stale (1-3d)" value={stale.length} color="#f59e0b" darkMode={darkMode} cardClass={cardClass} textPrimary={textPrimary} textSecondary={textSecondary} />
        <KpiCard label="Critical (>3d)" value={critical.length} color={critical.length > 0 ? '#ef4444' : '#10b981'} darkMode={darkMode} cardClass={cardClass} textPrimary={textPrimary} textSecondary={textSecondary} />
      </div>

      {freshnessData.length === 0 ? (
        <div className={`${cardClass} p-8 text-center`}>
          <p className={`font-medium ${textPrimary}`}>No Usage Data</p>
          <p className={`text-sm mt-1 ${textSecondary}`}>No billable tables found in this workspace.</p>
        </div>
      ) : (
        <div className={`${cardClass} p-5`}>
          <h3 className={`text-sm font-semibold mb-4 ${textPrimary}`}>Table Freshness ({freshnessData.length} tables)</h3>
          <div className="overflow-x-auto max-h-[500px] overflow-y-auto">
            <table className="w-full text-sm">
              <thead className="sticky top-0">
                <tr className={`border-b ${darkMode ? 'border-gray-700 bg-gray-800' : 'border-gray-200 bg-white'}`}>
                  <th className={`text-left py-2 px-3 font-medium ${textSecondary}`}>Data Type</th>
                  <th className={`text-left py-2 px-3 font-medium ${textSecondary}`}>Type</th>
                  <th className={`text-left py-2 px-3 font-medium ${textSecondary}`}>Status</th>
                  <th className={`text-left py-2 px-3 font-medium ${textSecondary}`}>Last Log</th>
                  <th className={`text-right py-2 px-3 font-medium ${textSecondary}`}>Time Since</th>
                  <th className={`text-right py-2 px-3 font-medium ${textSecondary}`}>Avg Daily Vol</th>
                  <th className={`text-right py-2 px-3 font-medium ${textSecondary}`}>Active Days</th>
                </tr>
              </thead>
              <tbody>
                {freshnessData.map((row, i) => {
                  const statusColor = FRESHNESS_COLORS[row.FreshnessStatus] || '#6b7280';
                  const mins = parseInt(row.MinutesSinceLastLog) || 0;
                  const timeSince = mins === 0 ? '<1m' : mins < 60 ? `${mins}m` : mins < 1440 ? `${Math.floor(mins/60)}h ${mins%60}m` : `${Math.floor(mins/1440)}d ${Math.floor((mins%1440)/60)}h`;
                  const tableType = getTableType(row.DataType);
                  const typeColors = { Stream: '#3b82f6', Batch: '#8b5cf6', Event: '#f59e0b' };
                  return (
                    <tr key={i} className={`border-b ${darkMode ? 'border-gray-700/50' : 'border-gray-100'}`}>
                      <td className={`py-2 px-3 font-medium ${textPrimary}`}>{row.DataType}</td>
                      <td className="py-2 px-3" title={tableType.tip}>
                        <span className="inline-flex items-center px-1.5 py-0.5 rounded text-xs font-medium"
                          style={{ backgroundColor: `${typeColors[tableType.label]}15`, color: typeColors[tableType.label] }}>
                          {tableType.label}
                        </span>
                      </td>
                      <td className="py-2 px-3">
                        <span className={`inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full text-xs font-medium`}
                          style={{ backgroundColor: `${statusColor}20`, color: statusColor }}>
                          <span className="w-1.5 h-1.5 rounded-full" style={{ backgroundColor: statusColor }} />
                          {row.FreshnessStatus}
                        </span>
                      </td>
                      <td className={`py-2 px-3 ${textMuted}`}>{new Date(row.LastLog).toLocaleString()}</td>
                      <td className={`py-2 px-3 text-right font-mono text-xs`} style={{ color: statusColor }}>{timeSince}</td>
                      <td className={`py-2 px-3 text-right ${textSecondary}`}>{formatBytes(parseFloat(row.AvgDailyGB) || 0)}</td>
                      <td className={`py-2 px-3 text-right ${textSecondary}`}>{row.DaysWithData}/30</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
          {(() => {
            const staleStreaming = [...stale, ...critical].filter(r => getTableType(r.DataType).label === 'Stream');
            const staleBatch = [...stale, ...critical].filter(r => getTableType(r.DataType).label !== 'Stream');
            return (
              <>
                {staleStreaming.length > 0 && (
                  <div className={`mt-4 p-3 rounded-lg ${darkMode ? 'bg-red-500/10 border border-red-500/20' : 'bg-red-50 border border-red-200'}`}>
                    <p className="text-red-400 text-xs font-medium">
                      {staleStreaming.length} streaming table(s) have not received data recently: {staleStreaming.map(r => r.DataType).join(', ')}. This may indicate a silent connector failure.
                    </p>
                  </div>
                )}
                {staleBatch.length > 0 && (
                  <div className={`mt-4 p-3 rounded-lg ${darkMode ? 'bg-gray-500/10 border border-gray-500/20' : 'bg-gray-50 border border-gray-200'}`}>
                    <p className={`text-xs font-medium ${textMuted}`}>
                      {staleBatch.length} batch/event-driven table(s) are stale — this is often expected for periodic or on-demand sources.
                    </p>
                  </div>
                )}
              </>
            );
          })()}
        </div>
      )}
    </div>
  );
}

// ── Latency Tab ──────────────────────────────────────────────────────────────

function LatencyTab({ latencyData, latencyTrend, darkMode, cardClass, textPrimary, textSecondary, textMuted }) {
  if (!latencyData) {
    return <EmptyState message="Click 'Load Data' to measure ingestion latency." darkMode={darkMode} />;
  }

  const highLatency = latencyData.filter(r => parseFloat(r.AvgLatencyMin) > 5); // > 5 min avg
  const overallAvg = latencyData.length > 0
    ? (latencyData.reduce((sum, r) => sum + (parseFloat(r.AvgLatencyMin) || 0), 0) / latencyData.length)
    : 0;
  const worstTable = latencyData[0];

  const trendChartData = (latencyTrend || []).map(r => ({
    time: new Date(r.TimeGenerated).toLocaleString(undefined, { month: 'short', day: 'numeric', hour: '2-digit' }),
    avg: Math.round(parseFloat(r.AvgLatencyMin) || 0),
    p95: Math.round(parseFloat(r.P95Latency) || 0),
  }));

  function formatLatency(minutes) {
    if (minutes == null || isNaN(minutes)) return '—';
    minutes = Math.round(minutes);
    if (minutes < 1) return '<1m';
    if (minutes < 60) return `${minutes}m`;
    return `${Math.floor(minutes/60)}h ${minutes%60}m`;
  }

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <KpiCard label="Tables Measured" value={latencyData.length} darkMode={darkMode} cardClass={cardClass} textPrimary={textPrimary} textSecondary={textSecondary} />
        <KpiCard label="Overall Avg Latency" value={formatLatency(overallAvg)} color={overallAvg > 5 ? '#f59e0b' : '#10b981'} darkMode={darkMode} cardClass={cardClass} textPrimary={textPrimary} textSecondary={textSecondary} />
        <KpiCard label="High Latency Tables" value={highLatency.length} color={highLatency.length > 0 ? '#ef4444' : '#10b981'} subtitle=">5 min avg" darkMode={darkMode} cardClass={cardClass} textPrimary={textPrimary} textSecondary={textSecondary} />
        <KpiCard label="Worst Table" value={worstTable?.DataType || '—'} subtitle={worstTable ? formatLatency(parseFloat(worstTable.AvgLatencyMin)) : ''} darkMode={darkMode} cardClass={cardClass} textPrimary={textPrimary} textSecondary={textSecondary} />
      </div>

      {/* Latency trend chart */}
      {trendChartData.length > 0 && (
        <div className={`${cardClass} p-5`}>
          <h3 className={`text-sm font-semibold mb-4 ${textPrimary}`}>Ingestion Latency Trend (All Tables)</h3>
          <ResponsiveContainer width="100%" height={220}>
            <LineChart data={trendChartData}>
              <CartesianGrid strokeDasharray="3 3" stroke={darkMode ? '#374151' : '#e5e7eb'} />
              <XAxis dataKey="time" tick={{ fontSize: 10, fill: darkMode ? '#9ca3af' : '#6b7280' }} interval="preserveStartEnd" />
              <YAxis tick={{ fontSize: 11, fill: darkMode ? '#9ca3af' : '#6b7280' }} tickFormatter={v => `${v}m`} />
              <Tooltip contentStyle={{ backgroundColor: darkMode ? '#1f2937' : '#fff', border: `1px solid ${darkMode ? '#374151' : '#e5e7eb'}`, borderRadius: '8px' }}
                formatter={(val) => [`${val}m`, undefined]} />
              <Legend wrapperStyle={{ fontSize: 11 }} />
              <Line type="monotone" dataKey="avg" stroke="#3b82f6" strokeWidth={2} dot={false} name="Average" />
              <Line type="monotone" dataKey="p95" stroke="#f59e0b" strokeWidth={1.5} dot={false} name="P95" strokeDasharray="4 2" />
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}

      {latencyData.length === 0 ? (
        <div className={`${cardClass} p-8 text-center`}>
          <p className={`font-medium ${textPrimary}`}>No Latency Data</p>
          <p className={`text-sm mt-1 ${textSecondary}`}>No latency data found in the Usage table for this workspace and time range.</p>
        </div>
      ) : (
        <div className={`${cardClass} p-5`}>
          <h3 className={`text-sm font-semibold mb-4 ${textPrimary}`}>Latency by Table</h3>
          <div className="overflow-x-auto max-h-[400px] overflow-y-auto">
            <table className="w-full text-sm">
              <thead className="sticky top-0">
                <tr className={`border-b ${darkMode ? 'border-gray-700 bg-gray-800' : 'border-gray-200 bg-white'}`}>
                  <th className={`text-left py-2 px-3 font-medium ${textSecondary}`}>Table</th>
                  <th className={`text-right py-2 px-3 font-medium ${textSecondary}`}>Avg</th>
                  <th className={`text-right py-2 px-3 font-medium ${textSecondary}`}>P50</th>
                  <th className={`text-right py-2 px-3 font-medium ${textSecondary}`}>P95</th>
                  <th className={`text-right py-2 px-3 font-medium ${textSecondary}`}>P99</th>
                  <th className={`text-right py-2 px-3 font-medium ${textSecondary}`}>Max</th>
                  <th className={`text-right py-2 px-3 font-medium ${textSecondary}`}>Samples</th>
                </tr>
              </thead>
              <tbody>
                {latencyData.map((row, i) => {
                  const avg = parseFloat(row.AvgLatencyMin) || 0;
                  const color = avg > 10 ? '#ef4444' : avg > 5 ? '#f59e0b' : avg > 1 ? '#3b82f6' : '#10b981';
                  return (
                    <tr key={i} className={`border-b ${darkMode ? 'border-gray-700/50' : 'border-gray-100'}`}>
                      <td className={`py-2 px-3 font-medium ${textPrimary}`}>{row.DataType}</td>
                      <td className={`py-2 px-3 text-right font-medium`} style={{ color }}>{formatLatency(avg)}</td>
                      <td className={`py-2 px-3 text-right ${textSecondary}`}>{formatLatency(parseFloat(row.P50Latency))}</td>
                      <td className={`py-2 px-3 text-right ${textSecondary}`}>{formatLatency(parseFloat(row.P95Latency))}</td>
                      <td className={`py-2 px-3 text-right ${textSecondary}`}>{formatLatency(parseFloat(row.P99Latency))}</td>
                      <td className={`py-2 px-3 text-right ${textMuted}`}>{formatLatency(parseFloat(row.MaxLatency))}</td>
                      <td className={`py-2 px-3 text-right ${textMuted}`}>{formatNumber(parseInt(row.SampleCount))}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
          {highLatency.length > 0 && (
            <div className={`mt-4 p-3 rounded-lg ${darkMode ? 'bg-yellow-500/10 border border-yellow-500/20' : 'bg-yellow-50 border border-yellow-200'}`}>
              <p className="text-yellow-400 text-xs font-medium">
                {highLatency.length} table(s) have average ingestion latency over 5 minutes. High latency means events are being seen late, directly impacting detection and response times.
              </p>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

// ── Throttling Tab ───────────────────────────────────────────────────────────

function ThrottlingTab({ throttlingData, throttlingTrend, darkMode, cardClass, textPrimary, textSecondary, textMuted }) {
  if (!throttlingData) {
    return <EmptyState message="Click 'Load Data' to check for ingestion rate limits and throttling." darkMode={darkMode} />;
  }

  const warnings = throttlingData.filter(r => r.Level === 'Warning');
  const errors = throttlingData.filter(r => r.Level === 'Error');
  const totalEvents = throttlingData.reduce((sum, r) => sum + (parseInt(r.EventCount) || 0), 0);

  const trendChartData = (throttlingTrend || []).map(r => ({
    time: new Date(r.TimeGenerated).toLocaleString(undefined, { month: 'short', day: 'numeric', hour: '2-digit' }),
    warnings: parseInt(r.Warnings) || 0,
    errors: parseInt(r.Errors) || 0,
  }));

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <KpiCard label="Issue Types" value={throttlingData.length} darkMode={darkMode} cardClass={cardClass} textPrimary={textPrimary} textSecondary={textSecondary} />
        <KpiCard label="Total Events" value={formatNumber(totalEvents)} color={totalEvents > 0 ? '#f59e0b' : '#10b981'} darkMode={darkMode} cardClass={cardClass} textPrimary={textPrimary} textSecondary={textSecondary} />
        <KpiCard label="Warnings" value={warnings.length} color={warnings.length > 0 ? '#f59e0b' : '#10b981'} darkMode={darkMode} cardClass={cardClass} textPrimary={textPrimary} textSecondary={textSecondary} />
        <KpiCard label="Errors" value={errors.length} color={errors.length > 0 ? '#ef4444' : '#10b981'} darkMode={darkMode} cardClass={cardClass} textPrimary={textPrimary} textSecondary={textSecondary} />
      </div>

      {/* Trend chart */}
      {trendChartData.length > 0 && (
        <div className={`${cardClass} p-5`}>
          <h3 className={`text-sm font-semibold mb-4 ${textPrimary}`}>Throttling Events Over Time</h3>
          <ResponsiveContainer width="100%" height={200}>
            <BarChart data={trendChartData}>
              <CartesianGrid strokeDasharray="3 3" stroke={darkMode ? '#374151' : '#e5e7eb'} />
              <XAxis dataKey="time" tick={{ fontSize: 10, fill: darkMode ? '#9ca3af' : '#6b7280' }} interval="preserveStartEnd" />
              <YAxis tick={{ fontSize: 11, fill: darkMode ? '#9ca3af' : '#6b7280' }} allowDecimals={false} />
              <Tooltip contentStyle={{ backgroundColor: darkMode ? '#1f2937' : '#fff', border: `1px solid ${darkMode ? '#374151' : '#e5e7eb'}`, borderRadius: '8px' }} />
              <Legend wrapperStyle={{ fontSize: 11 }} />
              <Bar dataKey="warnings" fill="#f59e0b" stackId="a" name="Warnings" />
              <Bar dataKey="errors" fill="#ef4444" stackId="a" name="Errors" />
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}

      {throttlingData.length === 0 ? (
        <div className={`${cardClass} p-8 text-center`}>
          <div className="text-4xl mb-3">&#10003;</div>
          <p className={`font-medium ${textPrimary}`}>No Throttling Issues</p>
          <p className={`text-sm mt-1 ${textSecondary}`}>No ingestion warnings or errors found in this time range. Data is flowing without rate limit issues.</p>
        </div>
      ) : (
        <div className={`${cardClass} p-5`}>
          <h3 className={`text-sm font-semibold mb-4 ${textPrimary}`}>Ingestion Issues ({throttlingData.length})</h3>
          <div className="overflow-x-auto max-h-[400px] overflow-y-auto">
            <table className="w-full text-sm">
              <thead className="sticky top-0">
                <tr className={`border-b ${darkMode ? 'border-gray-700 bg-gray-800' : 'border-gray-200 bg-white'}`}>
                  <th className={`text-left py-2 px-3 font-medium ${textSecondary}`}>Level</th>
                  <th className={`text-left py-2 px-3 font-medium ${textSecondary}`}>Detail</th>
                  <th className={`text-left py-2 px-3 font-medium ${textSecondary}`}>Solution</th>
                  <th className={`text-right py-2 px-3 font-medium ${textSecondary}`}>Count</th>
                  <th className={`text-left py-2 px-3 font-medium ${textSecondary}`}>Last Seen</th>
                </tr>
              </thead>
              <tbody>
                {throttlingData.map((row, i) => {
                  const isError = row.Level === 'Error';
                  return (
                    <tr key={i} className={`border-b ${darkMode ? 'border-gray-700/50' : 'border-gray-100'}`}>
                      <td className="py-2 px-3">
                        <span className={`px-2 py-0.5 rounded-full text-xs font-medium ${
                          isError ? 'bg-red-500/10 text-red-400' : 'bg-yellow-500/10 text-yellow-400'
                        }`}>
                          {row.Level}
                        </span>
                      </td>
                      <td className={`py-2 px-3 ${textPrimary} max-w-md`}>
                        <p className="truncate" title={row.Detail}>{row.Detail || '—'}</p>
                      </td>
                      <td className={`py-2 px-3 ${textSecondary}`}>{row.Solution || '—'}</td>
                      <td className={`py-2 px-3 text-right font-medium ${isError ? 'text-red-400' : 'text-yellow-400'}`}>{row.EventCount}</td>
                      <td className={`py-2 px-3 ${textMuted}`}>{timeAgo(row.LastOccurrence)}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
          <div className={`mt-4 p-3 rounded-lg ${darkMode ? 'bg-red-500/10 border border-red-500/20' : 'bg-red-50 border border-red-200'}`}>
            <p className="text-red-400 text-xs font-medium">
              Ingestion throttling means data may be dropped or delayed. Consider increasing the workspace daily cap or optimizing high-volume data sources.
            </p>
          </div>
        </div>
      )}
    </div>
  );
}

// ── Shared Components ────────────────────────────────────────────────────────

function EmptyState({ message, darkMode }) {
  return (
    <div className={`text-center py-16 ${darkMode ? 'text-gray-500' : 'text-gray-400'}`}>
      <div className="text-4xl mb-3">📊</div>
      <p>{message}</p>
    </div>
  );
}
