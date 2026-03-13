import React, { useState, useCallback, useMemo, useEffect, useRef } from 'react';
import { motion, AnimatePresence } from 'framer-motion';

// ── Constants ────────────────────────────────────────────────────────────────

const API_BASE = '/api/exposure';

const CLIENTS = [
  'AppalachianMountainClub', 'BecketLee', 'CarolinaEastern', 'CENTA',
  'CentralNewMexicoCC', 'CHAMBERS', 'DavidsonInc', 'EdisonPartners',
  'GenesisHC', 'HighFalls', 'Humacyte', 'Imugene', 'InternationalDrilling',
  'MedicalTeamsInternational', 'mssentinelphrma', 'NAOB', 'PennMutual',
  'Proforce', 'PutnamNorthernWestchester', 'Soltesz', 'Spang', 'Stevedoring',
  'TandemInvestmentAdvisors', 'Tillamook', 'TridentConstruction',
];

const STATUSES = ['Open', 'In Progress', 'Closed', 'Remediated'];

const SEED_ANALYSTS = ['Edward Blackshear', 'Derek MacDonald', 'Darkenstz Corneille'];

const STATUS_STYLES = {
  'Open': { bg: 'bg-red-500/15', text: 'text-red-400', border: 'border-red-500/30' },
  'In Progress': { bg: 'bg-yellow-500/15', text: 'text-yellow-400', border: 'border-yellow-500/30' },
  'Closed': { bg: 'bg-green-500/15', text: 'text-green-400', border: 'border-green-500/30' },
  'Remediated': { bg: 'bg-purple-500/15', text: 'text-purple-400', border: 'border-purple-500/30' },
};

const EMPTY_FORM = {
  client: '', ticket: '', date: '', status: 'Open', initiative: '',
  scope: '', scoreBefore: '', scoreAfter: '', findings: '', actions: '', notes: '', analyst: '',
};

// ── Helpers ──────────────────────────────────────────────────────────────────

function todayStr() {
  return new Date().toISOString().split('T')[0];
}

function StatusBadge({ status }) {
  const s = STATUS_STYLES[status] || STATUS_STYLES['Open'];
  return (
    <span className={`inline-block px-2.5 py-0.5 rounded-full text-xs font-semibold border ${s.bg} ${s.text} ${s.border}`}>
      {status}
    </span>
  );
}

function ScoreDisplay({ before, after }) {
  if (!before && !after) return <span className="text-gray-500">—</span>;
  if (before && !after) return <span className="font-mono font-bold text-sm">{before}</span>;
  if (!before && after) return <span className="font-mono font-bold text-sm">{after}</span>;
  const b = Number(before), a = Number(after);
  const improved = a < b;
  const colorClass = improved ? 'text-green-400' : a > b ? 'text-red-400' : 'text-gray-500';
  const arrow = improved ? '↓' : a > b ? '↑' : '=';
  return (
    <span className="font-mono font-bold text-sm whitespace-nowrap">
      {b} <span className={colorClass}>{arrow}</span> {a}
    </span>
  );
}

// ── Main Component ───────────────────────────────────────────────────────────

export default function ExposureManagement({ darkMode }) {
  // Data state
  const [entries, setEntries] = useState([]);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);

  // Form state
  const [form, setForm] = useState({ ...EMPTY_FORM, date: todayStr() });
  const [editingId, setEditingId] = useState(null);

  // Table state
  const [expandedId, setExpandedId] = useState(null);

  // Filter state
  const [filterClient, setFilterClient] = useState('');
  const [filterStatus, setFilterStatus] = useState('');
  const [filterMonth, setFilterMonth] = useState('');
  const [filterSearch, setFilterSearch] = useState('');

  // Toast state
  const [toast, setToast] = useState(null);
  const toastTimer = useRef(null);

  const showToast = useCallback((message, type = 'success') => {
    if (toastTimer.current) clearTimeout(toastTimer.current);
    setToast({ message, type });
    toastTimer.current = setTimeout(() => setToast(null), 3500);
  }, []);

  // ── API Calls ───────────────────────────────────────────────────────────

  const loadEntries = useCallback(async () => {
    setLoading(true);
    try {
      const res = await fetch(API_BASE);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      setEntries(data.entries || []);
    } catch (e) {
      setEntries([]);
      showToast('Failed to load entries: ' + e.message, 'error');
    } finally {
      setLoading(false);
    }
  }, [showToast]);

  useEffect(() => {
    loadEntries();
  }, [loadEntries]);

  // ── Form Handlers ───────────────────────────────────────────────────────

  const updateField = useCallback((field, value) => {
    setForm(prev => ({ ...prev, [field]: value }));
  }, []);

  const clearForm = useCallback(() => {
    setForm({ ...EMPTY_FORM, date: todayStr() });
    setEditingId(null);
  }, []);

  const enterEditMode = useCallback((entry) => {
    setEditingId(entry.id);
    setForm({
      client: entry.client || '',
      ticket: entry.ticket === '-' ? '' : (entry.ticket || ''),
      date: entry.date || '',
      status: entry.status || 'Open',
      initiative: entry.initiative || '',
      scope: entry.scope || '',
      scoreBefore: entry.scoreBefore || '',
      scoreAfter: entry.scoreAfter || '',
      findings: entry.findings || '',
      actions: entry.actions || '',
      notes: entry.notes || '',
      analyst: entry.analyst || '',
    });
    window.scrollTo({ top: 0, behavior: 'smooth' });
  }, []);

  const handleAdd = useCallback(async () => {
    if (!form.analyst) { showToast('Select an analyst.', 'error'); return; }
    if (!form.client) { showToast('Select a client.', 'error'); return; }
    if (!form.date) { showToast('Enter a date.', 'error'); return; }
    setSaving(true);
    try {
      const res = await fetch(API_BASE, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ...form, ticket: form.ticket.trim() || '-' }),
      });
      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error(err.error || `HTTP ${res.status}`);
      }
      const data = await res.json();
      setEntries(prev => [data.entry, ...prev]);
      clearForm();
      showToast('Entry saved.', 'success');
    } catch (e) {
      showToast('Save failed: ' + e.message, 'error');
    } finally {
      setSaving(false);
    }
  }, [form, clearForm, showToast]);

  const handleSaveEdit = useCallback(async () => {
    if (!form.analyst) { showToast('Select an analyst.', 'error'); return; }
    if (!form.client) { showToast('Select a client.', 'error'); return; }
    if (!form.date) { showToast('Enter a date.', 'error'); return; }
    setSaving(true);
    try {
      const res = await fetch(`${API_BASE}/${editingId}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ...form, ticket: form.ticket.trim() || '-' }),
      });
      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error(err.error || `HTTP ${res.status}`);
      }
      const data = await res.json();
      setEntries(prev => prev.map(e => e.id === editingId ? data.entry : e));
      clearForm();
      showToast('Entry updated.', 'success');
    } catch (e) {
      showToast('Update failed: ' + e.message, 'error');
    } finally {
      setSaving(false);
    }
  }, [form, editingId, clearForm, showToast]);

  const handleDelete = useCallback(async (id) => {
    if (!window.confirm('Delete this entry?')) return;
    if (editingId === id) clearForm();
    setSaving(true);
    try {
      const res = await fetch(`${API_BASE}/${id}`, { method: 'DELETE' });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      setEntries(prev => prev.filter(e => e.id !== id));
      if (expandedId === id) setExpandedId(null);
      showToast('Entry deleted.', 'success');
    } catch (e) {
      showToast('Delete failed: ' + e.message, 'error');
    } finally {
      setSaving(false);
    }
  }, [editingId, expandedId, clearForm, showToast]);

  // ── Filtering ───────────────────────────────────────────────────────────

  const filtered = useMemo(() => {
    const q = filterSearch.toLowerCase();
    return entries
      .filter(e => {
        if (filterClient && e.client !== filterClient) return false;
        if (filterStatus && e.status !== filterStatus) return false;
        if (filterMonth && !e.date.startsWith(filterMonth)) return false;
        if (q && ![e.ticket, e.findings, e.actions, e.scope, e.notes, e.client, e.initiative, e.analyst]
          .join(' ').toLowerCase().includes(q)) return false;
        return true;
      })
      .sort((a, b) => new Date(b.date) - new Date(a.date));
  }, [entries, filterClient, filterStatus, filterMonth, filterSearch]);

  // ── Stats ───────────────────────────────────────────────────────────────

  const stats = useMemo(() => ({
    total: entries.length,
    clients: new Set(entries.map(e => e.client)).size,
    open: entries.filter(e => e.status === 'Open' || e.status === 'In Progress').length,
  }), [entries]);

  // Derive unique analyst names from existing entries for autocomplete
  const knownAnalysts = useMemo(() =>
    [...new Set([...SEED_ANALYSTS, ...entries.map(e => e.analyst).filter(Boolean)])].sort(),
  [entries]);

  // ── CSV Export ──────────────────────────────────────────────────────────

  const exportCSV = useCallback(() => {
    if (!filtered.length) { showToast('No entries to export.', 'error'); return; }
    const headers = ['Date', 'Analyst', 'Client', 'Ticket', 'Status', 'Score Before', 'Score After',
      'Initiative', 'Scope', 'Recommendations Reviewed', 'Changes Made', 'Notes'];
    const rows = filtered.map(e =>
      [e.date, e.analyst, e.client, e.ticket, e.status, e.scoreBefore, e.scoreAfter,
        e.initiative, e.scope, e.findings, e.actions, e.notes]
        .map(v => `"${String(v || '').replace(/"/g, '""')}"`)
        .join(',')
    );
    const csv = [headers.join(','), ...rows].join('\n');
    const a = document.createElement('a');
    a.href = URL.createObjectURL(new Blob([csv], { type: 'text/csv' }));
    a.download = `ExposureManagement_${todayStr()}.csv`;
    a.click();
    showToast('CSV exported.', 'success');
  }, [filtered, showToast]);

  // ── Render ──────────────────────────────────────────────────────────────

  const inputClass = `w-full px-3 py-2.5 rounded-md border text-sm outline-none transition-colors ${
    darkMode
      ? 'bg-gray-700/50 border-gray-600 text-white placeholder-gray-500 focus:border-blue-500 focus:ring-1 focus:ring-blue-500/20'
      : 'bg-white border-gray-300 text-gray-900 placeholder-gray-400 focus:border-blue-500 focus:ring-1 focus:ring-blue-500/20'
  }`;

  const labelClass = `block text-xs font-semibold uppercase tracking-wider mb-1.5 ${
    darkMode ? 'text-gray-400' : 'text-gray-500'
  }`;

  return (
    <div className="space-y-6">
      {/* Stats Bar */}
      <div className="flex items-center justify-between flex-wrap gap-4">
        <div>
          <h1 className={`text-2xl font-bold ${darkMode ? 'text-white' : 'text-gray-900'}`}>
            Exposure Management Tracker
          </h1>
          <p className={`text-sm mt-1 ${darkMode ? 'text-gray-400' : 'text-gray-600'}`}>
            ThreatHunter MSSP &middot; Cadence Tracker
          </p>
        </div>
        <div className="flex gap-6">
          {[
            { label: 'Entries', value: stats.total, color: 'text-blue-400' },
            { label: 'Clients', value: stats.clients, color: 'text-blue-400' },
            { label: 'Open', value: stats.open, color: stats.open > 0 ? 'text-red-400' : 'text-green-400' },
          ].map(s => (
            <div key={s.label} className="text-right">
              <div className={`font-mono text-lg font-bold ${s.color}`}>{s.value}</div>
              <div className={`text-xs uppercase tracking-wider ${darkMode ? 'text-gray-500' : 'text-gray-400'}`}>{s.label}</div>
            </div>
          ))}
        </div>
      </div>

      {/* Loading Indicator */}
      {loading && (
        <div className="flex items-center justify-center py-8">
          <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500" />
          <span className={`ml-3 text-sm ${darkMode ? 'text-gray-400' : 'text-gray-600'}`}>Loading entries...</span>
        </div>
      )}

      {/* Form */}
      {!loading && (
        <div className={`rounded-xl border p-6 relative overflow-hidden ${
          editingId
            ? darkMode ? 'bg-gray-800 border-yellow-500/50' : 'bg-white border-yellow-400'
            : darkMode ? 'bg-gray-800 border-gray-700' : 'bg-white border-gray-200'
        }`}>
          {/* Top accent bar */}
          <div className={`absolute top-0 left-0 right-0 h-0.5 ${
            editingId
              ? 'bg-gradient-to-r from-yellow-500 to-blue-500'
              : 'bg-gradient-to-r from-blue-500 to-purple-500'
          }`} />

          {/* Edit banner */}
          <AnimatePresence>
            {editingId && (
              <motion.div
                initial={{ opacity: 0, height: 0 }}
                animate={{ opacity: 1, height: 'auto' }}
                exit={{ opacity: 0, height: 0 }}
                className="mb-4"
              >
                <div className="bg-yellow-500/10 border border-yellow-500/30 rounded-md px-3 py-2 text-xs font-mono text-yellow-400">
                  Editing existing entry. Make your changes and click Save Changes.
                </div>
              </motion.div>
            )}
          </AnimatePresence>

          <h2 className={`text-xs font-bold uppercase tracking-widest mb-4 ${darkMode ? 'text-blue-400' : 'text-blue-600'}`}>
            {editingId ? 'Edit Entry' : 'Log New Entry'}
          </h2>

          {/* Row 1: Analyst, Client, Ticket, Date */}
          <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-4">
            <div>
              <label className={labelClass}>Analyst</label>
              <input type="text" list="analyst-suggestions" value={form.analyst}
                onChange={e => updateField('analyst', e.target.value)}
                placeholder="Type your name..." className={inputClass} />
              <datalist id="analyst-suggestions">
                {knownAnalysts.map(a => <option key={a} value={a} />)}
              </datalist>
            </div>
            <div>
              <label className={labelClass}>Client</label>
              <select value={form.client} onChange={e => updateField('client', e.target.value)} className={inputClass}>
                <option value="">Select client...</option>
                {CLIENTS.map(c => <option key={c} value={c}>{c}</option>)}
              </select>
            </div>
            <div>
              <label className={labelClass}>Ticket Number <span className="opacity-50 font-normal">(optional)</span></label>
              <input type="text" value={form.ticket} onChange={e => updateField('ticket', e.target.value)}
                placeholder="e.g. CW-10492 or #673104" className={inputClass} />
            </div>
            <div>
              <label className={labelClass}>Review Date</label>
              <input type="date" value={form.date} onChange={e => updateField('date', e.target.value)} className={inputClass} />
            </div>
          </div>

          {/* Row 2: Status, Initiative, Scope */}
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">
            <div>
              <label className={labelClass}>Status</label>
              <select value={form.status} onChange={e => updateField('status', e.target.value)} className={inputClass}>
                {STATUSES.map(s => <option key={s} value={s}>{s}</option>)}
              </select>
            </div>
            <div>
              <label className={labelClass}>Initiative / Recommendation <span className="opacity-50 font-normal">(short name)</span></label>
              <input type="text" value={form.initiative} onChange={e => updateField('initiative', e.target.value)}
                placeholder="e.g. Quarantine impersonated message senders" className={inputClass} />
            </div>
            <div>
              <label className={labelClass}>Scope / Area <span className="opacity-50 font-normal">(optional)</span></label>
              <input type="text" value={form.scope} onChange={e => updateField('scope', e.target.value)}
                placeholder="e.g. Email, Identity, Endpoints" className={inputClass} />
            </div>
          </div>

          {/* Row 3: Scores + Notes */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-4">
            <div>
              <label className={labelClass}>Score Before <span className="opacity-50 font-normal">(optional)</span></label>
              <input type="number" min="0" max="100" value={form.scoreBefore}
                onChange={e => updateField('scoreBefore', e.target.value)} placeholder="e.g. 74" className={inputClass} />
            </div>
            <div>
              <label className={labelClass}>Score After <span className="opacity-50 font-normal">(optional)</span></label>
              <input type="number" min="0" max="100" value={form.scoreAfter}
                onChange={e => updateField('scoreAfter', e.target.value)} placeholder="e.g. 61" className={inputClass} />
            </div>
            <div className="col-span-2">
              <label className={labelClass}>Notes <span className="opacity-50 font-normal">(optional)</span></label>
              <input type="text" value={form.notes} onChange={e => updateField('notes', e.target.value)}
                placeholder="Follow-up items, next steps, client context..." className={inputClass} />
            </div>
          </div>

          {/* Row 4: Findings + Actions */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-4">
            <div>
              <label className={labelClass}>Recommendations Reviewed</label>
              <textarea value={form.findings} onChange={e => updateField('findings', e.target.value)}
                placeholder="What recommendations did you review in the Initiatives tab? What exposure risk was identified?"
                rows={3} className={`${inputClass} font-mono text-xs leading-relaxed resize-vertical`} />
            </div>
            <div>
              <label className={labelClass}>Changes Made</label>
              <textarea value={form.actions} onChange={e => updateField('actions', e.target.value)}
                placeholder="What did you configure, remediate, or recommend to the client? Be specific about what was changed."
                rows={3} className={`${inputClass} font-mono text-xs leading-relaxed resize-vertical`} />
            </div>
          </div>

          {/* Form Actions */}
          <div className="flex items-center gap-3 flex-wrap">
            {editingId ? (
              <>
                <button onClick={handleSaveEdit} disabled={saving}
                  className="px-5 py-2.5 bg-yellow-500 hover:bg-yellow-400 text-black font-bold text-sm rounded-md transition-colors disabled:opacity-50">
                  {saving ? 'Saving...' : 'Save Changes'}
                </button>
                <button onClick={clearForm}
                  className={`px-5 py-2.5 rounded-md border text-sm font-semibold transition-colors ${
                    darkMode ? 'border-gray-600 text-gray-300 hover:border-gray-500' : 'border-gray-300 text-gray-600 hover:border-gray-400'
                  }`}>
                  Cancel Edit
                </button>
              </>
            ) : (
              <>
                <button onClick={handleAdd} disabled={saving}
                  className="px-5 py-2.5 bg-blue-500 hover:bg-blue-400 text-white font-bold text-sm rounded-md transition-colors disabled:opacity-50">
                  {saving ? 'Saving...' : '+ Add Entry'}
                </button>
                <button onClick={clearForm}
                  className={`px-5 py-2.5 rounded-md border text-sm font-semibold transition-colors ${
                    darkMode ? 'border-gray-600 text-gray-300 hover:border-gray-500' : 'border-gray-300 text-gray-600 hover:border-gray-400'
                  }`}>
                  Clear
                </button>
              </>
            )}
          </div>
        </div>
      )}

      {/* Activity Log Header */}
      {!loading && (
        <div className="flex items-center justify-between flex-wrap gap-3">
          <h2 className={`text-xs font-bold uppercase tracking-widest ${darkMode ? 'text-blue-400' : 'text-blue-600'}`}>
            Activity Log
          </h2>
          <div className="flex items-center gap-4">
            <span className={`font-mono text-xs ${darkMode ? 'text-gray-500' : 'text-gray-400'}`}>
              <span className={darkMode ? 'text-blue-400' : 'text-blue-600'}>{filtered.length}</span> records
            </span>
            <button onClick={exportCSV}
              className={`px-4 py-2 rounded-md border text-xs font-semibold transition-colors ${
                darkMode ? 'border-gray-600 text-gray-300 hover:border-blue-500 hover:text-blue-400' : 'border-gray-300 text-gray-600 hover:border-blue-500 hover:text-blue-600'
              }`}>
              Export CSV
            </button>
          </div>
        </div>
      )}

      {/* Filters */}
      {!loading && (
        <div className="flex flex-wrap gap-3">
          <select value={filterClient} onChange={e => setFilterClient(e.target.value)}
            className={`${inputClass} flex-1 min-w-[140px]`}>
            <option value="">All Clients</option>
            {CLIENTS.map(c => <option key={c} value={c}>{c}</option>)}
          </select>
          <select value={filterStatus} onChange={e => setFilterStatus(e.target.value)}
            className={`${inputClass} flex-1 min-w-[140px]`}>
            <option value="">All Statuses</option>
            {STATUSES.map(s => <option key={s} value={s}>{s}</option>)}
          </select>
          <input type="month" value={filterMonth} onChange={e => setFilterMonth(e.target.value)}
            className={`${inputClass} flex-1 min-w-[140px]`} title="Filter by month" />
          <input type="text" value={filterSearch} onChange={e => setFilterSearch(e.target.value)}
            placeholder="Search ticket, client, initiative..." className={`${inputClass} flex-[2] min-w-[200px]`} />
        </div>
      )}

      {/* Table */}
      {!loading && (
        <div className={`rounded-xl border overflow-hidden ${darkMode ? 'bg-gray-800 border-gray-700' : 'bg-white border-gray-200'}`}>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className={darkMode ? 'bg-gray-700/50' : 'bg-gray-50'}>
                  {['', 'Date', 'Analyst', 'Client', 'Ticket', 'Status', 'Score', 'Initiative / Recommendation', 'Scope', 'Changes Made (preview)'].map(h => (
                    <th key={h} className={`px-4 py-3 text-left text-xs font-semibold uppercase tracking-wider whitespace-nowrap ${
                      darkMode ? 'text-gray-400' : 'text-gray-500'
                    }`}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {filtered.length === 0 ? (
                  <tr>
                    <td colSpan={10} className="text-center py-16">
                      <div className="text-4xl mb-3">{entries.length === 0 ? '📋' : '🔍'}</div>
                      <p className={`text-sm ${darkMode ? 'text-gray-500' : 'text-gray-400'}`}>
                        {entries.length === 0
                          ? 'No entries yet. Start logging your exposure management reviews above.'
                          : 'No entries match the current filters.'}
                      </p>
                    </td>
                  </tr>
                ) : (
                  filtered.map(entry => (
                    <React.Fragment key={entry.id}>
                      {/* Summary Row */}
                      <tr
                        onClick={() => setExpandedId(expandedId === entry.id ? null : entry.id)}
                        className={`cursor-pointer transition-colors ${
                          editingId === entry.id
                            ? darkMode ? 'bg-yellow-500/5' : 'bg-yellow-50'
                            : expandedId === entry.id
                              ? darkMode ? 'bg-blue-500/5' : 'bg-blue-50'
                              : darkMode ? 'hover:bg-gray-700/30' : 'hover:bg-gray-50'
                        }`}
                      >
                        <td className="px-4 py-3">
                          <span className={`inline-block text-[10px] transition-transform ${
                            expandedId === entry.id ? 'rotate-90' : ''
                          } ${darkMode ? 'text-gray-500' : 'text-gray-400'}`}>&#9654;</span>
                        </td>
                        <td className={`px-4 py-3 font-mono text-xs whitespace-nowrap ${darkMode ? 'text-gray-400' : 'text-gray-500'}`}>
                          {entry.date}
                        </td>
                        <td className={`px-4 py-3 text-xs font-semibold whitespace-nowrap ${darkMode ? 'text-gray-300' : 'text-gray-700'}`}>
                          {entry.analyst || <span className="text-gray-500">—</span>}
                        </td>
                        <td className={`px-4 py-3 font-bold text-sm whitespace-nowrap ${darkMode ? 'text-white' : 'text-gray-900'}`}>
                          {entry.client}
                        </td>
                        <td className="px-4 py-3">
                          <span className="font-mono text-xs text-blue-400 font-semibold">{entry.ticket}</span>
                        </td>
                        <td className="px-4 py-3">
                          <StatusBadge status={entry.status} />
                        </td>
                        <td className="px-4 py-3">
                          <ScoreDisplay before={entry.scoreBefore} after={entry.scoreAfter} />
                        </td>
                        <td className={`px-4 py-3 text-xs max-w-[280px] truncate ${darkMode ? 'text-gray-300' : 'text-gray-700'}`}>
                          {entry.initiative || <span className="text-gray-500">—</span>}
                        </td>
                        <td className={`px-4 py-3 font-mono text-xs max-w-[160px] truncate ${darkMode ? 'text-gray-500' : 'text-gray-400'}`}>
                          {entry.scope || '—'}
                        </td>
                        <td className={`px-4 py-3 font-mono text-xs max-w-[160px] truncate ${darkMode ? 'text-gray-500' : 'text-gray-400'}`}>
                          {(entry.actions || entry.findings || '').substring(0, 80) || '—'}
                        </td>
                      </tr>

                      {/* Expandable Detail Row */}
                      <AnimatePresence>
                        {expandedId === entry.id && (
                          <tr>
                            <td colSpan={10} className="p-0">
                              <motion.div
                                initial={{ opacity: 0, height: 0 }}
                                animate={{ opacity: 1, height: 'auto' }}
                                exit={{ opacity: 0, height: 0 }}
                                transition={{ duration: 0.2 }}
                                className="overflow-hidden"
                              >
                                <div className={`px-6 py-5 ${darkMode ? 'bg-gray-900/60 border-t border-gray-700/50' : 'bg-gray-50 border-t border-gray-200'}`}>
                                  {/* Meta row */}
                                  <div className={`flex flex-wrap gap-8 mb-4 pb-4 border-b ${darkMode ? 'border-gray-700/50' : 'border-gray-200'}`}>
                                    {[
                                      { label: 'Analyst', value: entry.analyst || '—' },
                                      { label: 'Client', value: entry.client },
                                      { label: 'Date', value: entry.date, mono: true },
                                      { label: 'Ticket', value: entry.ticket, mono: true },
                                      { label: 'Score', value: null, custom: <ScoreDisplay before={entry.scoreBefore} after={entry.scoreAfter} /> },
                                      ...(entry.scope ? [{ label: 'Scope', value: entry.scope }] : []),
                                      ...(entry.notes ? [{ label: 'Notes', value: entry.notes }] : []),
                                    ].map(m => (
                                      <div key={m.label}>
                                        <div className={`text-[9px] font-semibold uppercase tracking-widest mb-1 ${darkMode ? 'text-gray-500' : 'text-gray-400'}`}>{m.label}</div>
                                        {m.custom || (
                                          <div className={`text-sm font-semibold ${m.mono ? 'font-mono text-blue-400' : darkMode ? 'text-white' : 'text-gray-900'}`}>
                                            {m.value}
                                          </div>
                                        )}
                                      </div>
                                    ))}
                                    <div>
                                      <div className={`text-[9px] font-semibold uppercase tracking-widest mb-1 ${darkMode ? 'text-gray-500' : 'text-gray-400'}`}>Status</div>
                                      <StatusBadge status={entry.status} />
                                    </div>
                                  </div>

                                  {/* Initiative */}
                                  {entry.initiative && (
                                    <div className="mb-4">
                                      <div className={`text-[9px] font-semibold uppercase tracking-widest mb-1.5 ${darkMode ? 'text-gray-500' : 'text-gray-400'}`}>
                                        Initiative / Recommendation
                                      </div>
                                      <div className="text-sm font-semibold text-blue-400">{entry.initiative}</div>
                                    </div>
                                  )}

                                  {/* Findings + Actions */}
                                  <div className="grid grid-cols-1 md:grid-cols-2 gap-5 mb-4">
                                    <div>
                                      <div className={`text-[9px] font-semibold uppercase tracking-widest mb-1.5 ${darkMode ? 'text-gray-500' : 'text-gray-400'}`}>
                                        Recommendations Reviewed
                                      </div>
                                      <div className={`text-xs font-mono leading-relaxed whitespace-pre-wrap break-words ${
                                        entry.findings ? (darkMode ? 'text-gray-300' : 'text-gray-700') : 'text-gray-500 italic'
                                      }`}>
                                        {entry.findings || 'No details recorded'}
                                      </div>
                                    </div>
                                    <div>
                                      <div className={`text-[9px] font-semibold uppercase tracking-widest mb-1.5 ${darkMode ? 'text-gray-500' : 'text-gray-400'}`}>
                                        Changes Made
                                      </div>
                                      <div className={`text-xs font-mono leading-relaxed whitespace-pre-wrap break-words ${
                                        entry.actions ? (darkMode ? 'text-gray-300' : 'text-gray-700') : 'text-gray-500 italic'
                                      }`}>
                                        {entry.actions || 'No details recorded'}
                                      </div>
                                    </div>
                                  </div>

                                  {/* Actions */}
                                  <div className="flex gap-2">
                                    <button
                                      onClick={(e) => { e.stopPropagation(); enterEditMode(entry); }}
                                      className="px-3 py-1.5 text-xs font-semibold rounded border border-blue-500/30 text-blue-400 hover:bg-blue-500/10 transition-colors"
                                    >
                                      Edit Entry
                                    </button>
                                    <button
                                      onClick={(e) => { e.stopPropagation(); handleDelete(entry.id); }}
                                      className="px-3 py-1.5 text-xs font-semibold rounded border border-red-500/30 text-red-400 hover:bg-red-500/10 transition-colors"
                                    >
                                      Delete Entry
                                    </button>
                                  </div>
                                </div>
                              </motion.div>
                            </td>
                          </tr>
                        )}
                      </AnimatePresence>
                    </React.Fragment>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Toast */}
      <AnimatePresence>
        {toast && (
          <motion.div
            initial={{ opacity: 0, y: 10 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: 10 }}
            className={`fixed bottom-8 right-8 z-50 px-5 py-3 rounded-lg font-bold text-sm font-mono shadow-lg ${
              toast.type === 'success' ? 'bg-green-500 text-black'
                : toast.type === 'error' ? 'bg-red-500 text-white'
                  : darkMode ? 'bg-gray-700 text-blue-400 border border-gray-600' : 'bg-gray-100 text-blue-600 border border-gray-300'
            }`}
          >
            {toast.message}
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}
