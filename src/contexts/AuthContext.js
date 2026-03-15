// AuthContext - Provides authentication state and token acquisition throughout the app
import React, { createContext, useContext, useState, useCallback } from 'react';
import { useMsal, useIsAuthenticated } from '@azure/msal-react';
import { InteractionRequiredAuthError } from '@azure/msal-browser';
import { armScopes, logAnalyticsScopes, loginRequest } from '../authConfig';

const AuthContext = createContext(null);

// Default values when auth is not available
const defaultAuthValue = {
  isAuthenticated: false,
  isLoading: false,
  isMsalAvailable: false,
  account: null,
  login: async () => { throw new Error('Auth not configured'); },
  logout: async () => { throw new Error('Auth not configured'); },
  acquireToken: async () => { throw new Error('Auth not configured'); },
  getArmToken: async () => { throw new Error('Auth not configured'); },
  getLogAnalyticsToken: async () => { throw new Error('Auth not configured'); },
  fetchFromArm: async () => { throw new Error('Auth not configured'); },
  fetchFromLogAnalytics: async () => { throw new Error('Auth not configured'); },
  getSentinelWorkspaces: async () => { throw new Error('Auth not configured'); },
  getSentinelIncident: async () => { throw new Error('Auth not configured'); },
  getIncidentLogs: async () => { throw new Error('Auth not configured'); },
};

export function AuthProvider({ children }) {
  const { instance, accounts } = useMsal();
  const isAuthenticated = useIsAuthenticated();
  const [isLoading, setIsLoading] = useState(false);

  // Get the active account
  const account = accounts[0] || null;

  // Login with popup
  const login = useCallback(async () => {
    try {
      setIsLoading(true);
      const response = await instance.loginPopup(loginRequest);
      if (response?.account) {
        instance.setActiveAccount(response.account);
      }
      return response;
    } catch (error) {
      console.error('Login failed:', error);
      throw error;
    } finally {
      setIsLoading(false);
    }
  }, [instance]);

  // Logout
  const logout = useCallback(async () => {
    try {
      await instance.logoutPopup();
    } catch (error) {
      console.error('Logout failed:', error);
      throw error;
    }
  }, [instance]);

  // Acquire token silently, falling back to popup if needed
  const acquireToken = useCallback(async (scopes) => {
    if (!account) {
      throw new Error('No authenticated account found');
    }

    const request = {
      scopes: scopes.scopes || scopes,
      account,
    };

    try {
      const response = await instance.acquireTokenSilent(request);
      return response.accessToken;
    } catch (error) {
      if (error instanceof InteractionRequiredAuthError) {
        // Token expired or consent needed - use popup
        try {
          const response = await instance.acquireTokenPopup(request);
          return response.accessToken;
        } catch (popupError) {
          console.error('Popup token acquisition failed:', popupError);
          throw popupError;
        }
      }
      throw error;
    }
  }, [instance, account]);

  // Get token for Azure Resource Manager API
  const getArmToken = useCallback(async () => {
    return acquireToken(armScopes);
  }, [acquireToken]);

  // Get token for Log Analytics API
  const getLogAnalyticsToken = useCallback(async () => {
    return acquireToken(logAnalyticsScopes);
  }, [acquireToken]);

  // Fetch from Azure Resource Manager with auth
  const fetchFromArm = useCallback(async (url, options = {}) => {
    setIsLoading(true);
    try {
      const token = await getArmToken();
      const response = await fetch(url, {
        ...options,
        headers: {
          ...options.headers,
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json',
        },
      });

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`ARM API error ${response.status}: ${errorText}`);
      }

      return response.json();
    } finally {
      setIsLoading(false);
    }
  }, [getArmToken]);

  // Silent fetch - returns null on error instead of throwing (for expected failures)
  const fetchFromArmSilent = useCallback(async (url) => {
    try {
      const token = await getArmToken();
      const response = await fetch(url, {
        headers: {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json',
        },
      });
      if (!response.ok) return null;
      return response.json();
    } catch {
      return null;
    }
  }, [getArmToken]);

  // Fetch from Log Analytics with auth
  const fetchFromLogAnalytics = useCallback(async (workspaceId, query) => {
    setIsLoading(true);
    try {
      const token = await getLogAnalyticsToken();
      const response = await fetch(
        `https://api.loganalytics.io/v1/workspaces/${workspaceId}/query`,
        {
          method: 'POST',
          headers: {
            'Authorization': `Bearer ${token}`,
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({ query }),
        }
      );

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`Log Analytics API error ${response.status}: ${errorText}`);
      }

      return response.json();
    } finally {
      setIsLoading(false);
    }
  }, [getLogAnalyticsToken]);

  // Get list of Sentinel workspaces using Azure Resource Graph (much faster than checking each workspace)
  const getSentinelWorkspaces = useCallback(async (onProgress) => {
    const CACHE_KEY = 'sentinel_workspaces_cache';
    const CACHE_DURATION = 5 * 60 * 1000; // 5 minutes

    // Check cache first
    try {
      const cached = sessionStorage.getItem(CACHE_KEY);
      if (cached) {
        const { workspaces, timestamp } = JSON.parse(cached);
        if (Date.now() - timestamp < CACHE_DURATION) {
          return workspaces;
        }
      }
    } catch {
      // Cache read failed, continue with fetch
    }

    // Get subscriptions for the query
    const subsResponse = await fetchFromArm(
      'https://management.azure.com/subscriptions?api-version=2022-12-01'
    );
    const subscriptions = subsResponse.value || [];
    const subscriptionIds = subscriptions.map(s => s.subscriptionId);

    // Build subscription lookup for display names and tenant IDs
    const subLookup = {};
    const subTenantLookup = {};
    subscriptions.forEach(s => {
      subLookup[s.subscriptionId] = s.displayName;
      if (s.tenantId) subTenantLookup[s.subscriptionId] = s.tenantId;
    });

    // Use Azure Resource Graph to find all Sentinel-enabled workspaces in one query
    const resourceGraphQuery = {
      query: `
        resources
        | where type == "microsoft.operationsmanagement/solutions"
        | where name startswith "SecurityInsights("
        | extend workspaceId = tolower(tostring(properties.workspaceResourceId))
        | project workspaceId, subscriptionId
      `,
      subscriptions: subscriptionIds,
    };

    const graphResponse = await fetchFromArm(
      'https://management.azure.com/providers/Microsoft.ResourceGraph/resources?api-version=2022-10-01',
      {
        method: 'POST',
        body: JSON.stringify(resourceGraphQuery),
      }
    );

    const sentinelWorkspaceIds = (graphResponse.data || []).map(row => row.workspaceId);

    if (sentinelWorkspaceIds.length === 0) {
      return [];
    }

    // Get workspace details for each Sentinel workspace
    const workspacePromises = sentinelWorkspaceIds.map(async (wsId) => {
      const wsResponse = await fetchFromArmSilent(
        `https://management.azure.com${wsId}?api-version=2022-10-01`
      );
      if (wsResponse) {
        const subscriptionId = wsId.split('/subscriptions/')[1]?.split('/')[0];
        return {
          id: wsResponse.id,
          name: wsResponse.name,
          customerId: wsResponse.properties?.customerId,
          location: wsResponse.location,
          subscriptionId,
          subscriptionName: subLookup[subscriptionId] || subscriptionId,
          tenantId: subTenantLookup[subscriptionId] || null,
          resourceGroup: wsResponse.id.split('/resourceGroups/')[1]?.split('/')[0],
        };
      }
      return null;
    });

    const results = await Promise.all(workspacePromises);
    const sentinelWorkspaces = results.filter(Boolean);

    // Report progress
    if (onProgress) {
      onProgress(sentinelWorkspaces);
    }

    // Cache the results
    try {
      sessionStorage.setItem(CACHE_KEY, JSON.stringify({
        workspaces: sentinelWorkspaces,
        timestamp: Date.now(),
      }));
    } catch {
      // Cache write failed, ignore
    }

    return sentinelWorkspaces;
  }, [fetchFromArm, fetchFromArmSilent]);

  // Get incident details from Sentinel
  const getSentinelIncident = useCallback(async (workspaceResourceId, incidentNumber) => {
    // Incident numbers in Sentinel are different from resource IDs
    // We need to query by incident number to find the actual resource ID (GUID)
    const normalizedNumber = incidentNumber.toString().trim();

    // Query incidents filtering by incident number
    const incidentsResponse = await fetchFromArm(
      `https://management.azure.com${workspaceResourceId}/providers/Microsoft.SecurityInsights/incidents?api-version=2023-11-01&$filter=properties/incidentNumber eq ${normalizedNumber}`
    );

    const incidents = incidentsResponse.value || [];
    if (incidents.length === 0) {
      throw new Error(`Incident #${normalizedNumber} not found in this workspace`);
    }

    const incident = incidents[0];
    const incidentName = incident.name; // This is the GUID

    // Get alerts related to this incident
    let alerts = [];
    try {
      const alertsResponse = await fetchFromArm(
        `https://management.azure.com${workspaceResourceId}/providers/Microsoft.SecurityInsights/incidents/${incidentName}/alerts?api-version=2023-11-01`
      );
      alerts = alertsResponse.value || [];
    } catch (err) {
      console.warn('Failed to fetch incident alerts:', err);
    }

    // Get entities related to this incident
    let entities = [];
    try {
      const entitiesResponse = await fetchFromArm(
        `https://management.azure.com${workspaceResourceId}/providers/Microsoft.SecurityInsights/incidents/${incidentName}/entities?api-version=2023-11-01`
      );
      entities = entitiesResponse.entities || [];
    } catch (err) {
      console.warn('Failed to fetch incident entities:', err);
    }

    return {
      incident,
      alerts,
      entities,
    };
  }, [fetchFromArm]);

  // Run a Log Analytics query for incident-related logs
  const getIncidentLogs = useCallback(async (workspaceCustomerId, incidentId) => {
    // Validate incidentId to prevent KQL injection (allow only alphanumeric, hyphens, underscores)
    const sanitizedId = String(incidentId).replace(/[^a-zA-Z0-9\-_]/g, '');
    if (!sanitizedId) {
      throw new Error('Invalid incident ID');
    }

    // Query SecurityIncident and related tables
    const query = `
      // Get incident details
      SecurityIncident
      | where IncidentNumber == ${sanitizedId} or IncidentName contains "${sanitizedId}"
      | take 1
      | project IncidentNumber, Title, Description, Severity, Status, Classification,
                CreatedTime, LastModifiedTime, Owner, Labels, AlertIds

      // Union with related alerts
      | union (
        SecurityAlert
        | where SystemAlertId in (
          SecurityIncident
          | where IncidentNumber == ${sanitizedId} or IncidentName contains "${sanitizedId}"
          | mv-expand AlertIds
          | project tostring(AlertIds)
        )
        | project AlertName = AlertName, AlertSeverity = AlertSeverity,
                  Description, ProviderName, TimeGenerated, ExtendedProperties,
                  Entities, Tactics, Techniques
      )
    `;

    return fetchFromLogAnalytics(workspaceCustomerId, query);
  }, [fetchFromLogAnalytics]);

  const value = {
    isAuthenticated,
    isLoading,
    isMsalAvailable: true,
    account,
    login,
    logout,
    acquireToken,
    getArmToken,
    getLogAnalyticsToken,
    fetchFromArm,
    fetchFromLogAnalytics,
    getSentinelWorkspaces,
    getSentinelIncident,
    getIncidentLogs,
  };

  return (
    <AuthContext.Provider value={value}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  const context = useContext(AuthContext);
  // Return default values if context not available (no AuthProvider)
  if (!context) {
    return defaultAuthValue;
  }
  return context;
}

export default AuthContext;
