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
    apiKey: required('UNIPILE_API_KEY'),
    dsn: process.env.UNIPILE_DSN || '',
    get apiUrl() {
      return this.dsn.startsWith('http') ? this.dsn : `https://${this.dsn}`;
    },
  },
  port: parseInt(process.env.PORT || '3000', 10),

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
