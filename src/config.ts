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
  port: parseInt(process.env.PORT || '3000', 10),

  // Scanning interval (ms)
  scanIntervalMs: 60_000,

  // LinkedIn pacing — matches Edge Function scheduler
  linkedinInterSendDelayMs: 8_000,
  linkedinJitterMs: 7_000,

  // Email pacing
  emailBatchSize: 5,
  emailInterSendDelayMs: 2_000,
  emailJitterMs: 2_000,

  // Recovery interval
  recoveryIntervalMs: 5 * 60_000,
};
