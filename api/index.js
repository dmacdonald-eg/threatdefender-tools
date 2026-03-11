// Entry point for Azure Functions
// This file imports all function definitions
// Wrap requires in try-catch to debug loading errors

console.log('========================================');
console.log('Starting Azure Functions initialization');
console.log('Node version:', process.version);
console.log('========================================');
try {
  require('./AlertTriage');
  console.log('✓ AlertTriage loaded');
} catch (e) {
  console.error('✗ AlertTriage failed:', e.message);
}

try {
  require('./KQLAnalyzer');
  console.log('✓ KQLAnalyzer loaded');
} catch (e) {
  console.error('✗ KQLAnalyzer failed:', e.message);
}

try {
  require('./KQLAnalyzerTest');
  console.log('✓ KQLAnalyzerTest loaded');
} catch (e) {
  console.error('✗ KQLAnalyzerTest failed:', e.message);
}

try {
  require('./KQLAnalyzerSimple');
  console.log('✓ KQLAnalyzerSimple loaded');
} catch (e) {
  console.error('✗ KQLAnalyzerSimple failed:', e.message);
}

try {
  require('./OpenAIDirectTest');
  console.log('✓ OpenAIDirectTest loaded');
} catch (e) {
  console.error('✗ OpenAIDirectTest failed:', e.message);
}

try {
  require('./OpenAISafeTest');
  console.log('✓ OpenAISafeTest loaded');
} catch (e) {
  console.error('✗ OpenAISafeTest failed:', e.message);
}

try {
  require('./ThreatIntelLookup');
  console.log('✓ ThreatIntelLookup loaded');
} catch (e) {
  console.error('✗ ThreatIntelLookup failed:', e.message);
}

try {
  require('./EmailPosture');
  console.log('✓ EmailPosture loaded');
} catch (e) {
  console.error('✗ EmailPosture failed:', e.message);
}

try {
  require('./EmailHeaderAnalyzer');
  console.log('✓ EmailHeaderAnalyzer loaded');
} catch (e) {
  console.error('✗ EmailHeaderAnalyzer failed:', e.message);
}

try {
  require('./HybridAnalysisLookup');
  console.log('✓ HybridAnalysisLookup loaded');
} catch (e) {
  console.error('✗ HybridAnalysisLookup failed:', e.message);
}

try {
  require('./LoadingDiagnostic');
  console.log('✓ LoadingDiagnostic loaded');
} catch (e) {
  console.error('✗ LoadingDiagnostic failed:', e.message);
}

// DISABLED: Old multi-handler version causes route registration conflicts in Azure Functions v4
// try {
//   require('./PromptsAPI');
//   console.log('✓ PromptsAPI loaded');
// } catch (e) {
//   console.error('✗ PromptsAPI failed:', e.message, e.stack);
// }

// NEW: REST API version (bypasses @azure/data-tables crypto issues)
// Uses direct Azure Table Storage REST API with axios
try {
  require('./PromptsAPI-REST');
  console.log('✓ PromptsAPI-REST loaded');
} catch (e) {
  console.error('✗ PromptsAPI-REST failed:', e.message, e.stack);
}

// Load PromptRunAPI after PromptsAPI-Unified
// PromptRunAPI handles /api/prompts/{id}/run which is caught by the wildcard
// but that's OK because PromptRunAPI explicitly checks the path
try {
  require('./PromptRunAPI');
  console.log('✓ PromptRunAPI loaded');
} catch (e) {
  console.error('✗ PromptRunAPI failed:', e.message);
}

try {
  require('./test-simple');
  console.log('✓ test-simple loaded');
} catch (e) {
  console.error('✗ test-simple failed:', e.message);
}

try {
  require('./DiagnosticEndpoint');
  console.log('✓ DiagnosticEndpoint loaded');
} catch (e) {
  console.error('✗ DiagnosticEndpoint failed:', e.message, e.stack);
}

try {
  require('./RouteDiagnostic');
  console.log('✓ RouteDiagnostic loaded');
} catch (e) {
  console.error('✗ RouteDiagnostic failed:', e.message, e.stack);
}

try {
  require('./TableStorageDiagnostic');
  console.log('✓ TableStorageDiagnostic loaded');
} catch (e) {
  console.error('✗ TableStorageDiagnostic failed:', e.message, e.stack);
}

try {
  require('./TriageSession');
  console.log('✓ TriageSession loaded');
} catch (e) {
  console.error('✗ TriageSession failed:', e.message, e.stack);
}

try {
  require('./ConnectWise');
  console.log('✓ ConnectWise loaded');
} catch (e) {
  console.error('✗ ConnectWise failed:', e.message, e.stack);
}

try {
  require('./GenerateDetermination');
  console.log('✓ GenerateDetermination loaded');
} catch (e) {
  console.error('✗ GenerateDetermination failed:', e.message, e.stack);
}

try {
  require('./ExposureManagementAPI');
  console.log('✓ ExposureManagementAPI loaded');
} catch (e) {
  console.error('✗ ExposureManagementAPI failed:', e.message, e.stack);
}

console.log('========================================');
console.log('All Azure Functions modules loaded');
console.log('Total modules in cache:', Object.keys(require.cache).length);
console.log('========================================');
