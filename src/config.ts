const required = (name: string): string => {
  const val = process.env[name];
  if (!val) throw new Error(`Missing required env var: ${name}`);
  return val;
};

export const config = {
  redis: {
    url: required('REDIS_URL'),
  },
  supabase: {
    url: required('SUPABASE_URL'),
    serviceRoleKey: required('SUPABASE_SERVICE_ROLE_KEY'),
    anonKey: required('SUPABASE_ANON_KEY'),
  },
  unipile: {
    // Not `required()` — the orchestrator service uses the same config
    // module but never calls Unipile. If a Unipile request happens at
    // runtime without a key, unipile-fetch.ts will fail HTTP with 401
    // (or short-circuit if DRY_RUN is set). The worker's Railway env
    // still sets this in practice.
    apiKey: process.env.UNIPILE_API_KEY || '',
    dsn: process.env.UNIPILE_DSN || '',
    get apiUrl() {
      return this.dsn.startsWith('http') ? this.dsn : `https://${this.dsn}`;
    },
  },
  port: parseInt(process.env.PORT || '3000', 10),

  // When true, any non-GET Unipile request is short-circuited to a fake success
  // response. Reads still hit Unipile so connection checks and profile lookups
  // work normally; only side-effecting calls (invites, messages, emails, etc.)
  // are blocked. Used in staging during orchestrator shadow + canary phases.
  dryRun: process.env.DRY_RUN === 'true',

  // Scanning interval (ms)
  scanIntervalMs: 15_000,

  // How many executions to scan per cycle
  scanLimit: 5_000,

  // Worker concurrency and rate limit
  workerConcurrency: 30,
  workerRateLimit: 120,

  // LinkedIn pacing — matches Edge Function scheduler
  linkedinInterSendDelayMs: 8_000,
  linkedinJitterMs: 7_000,

  // Email pacing
  emailBatchSize: 5,
  emailInterSendDelayMs: 2_000,
  emailJitterMs: 2_000,

  // Recovery interval
  recoveryIntervalMs: 5 * 60_000,

  // Horizontal scaling: which partition this worker handles (0-based)
  partition: parseInt(process.env.WORKER_PARTITION || '0', 10),
  partitionCount: parseInt(process.env.WORKER_PARTITION_COUNT || '1', 10),
};
