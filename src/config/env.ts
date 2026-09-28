export interface AppEnv {
  port: number;
  databaseUrl: string;
  redisUrl: string;
  kafkaBrokers: string[];
  partnerApiBaseUrl: string;
  reconciliationIntervalMs: number;
}

export function loadEnv(): AppEnv {
  const rawPort = process.env.PORT;
  const port = rawPort === undefined ? 3000 : Number(rawPort);
  if (Number.isNaN(port)) {
    throw new Error(`Invalid PORT environment variable: "${rawPort}" is not a number`);
  }

  const rawInterval = process.env.RECONCILIATION_INTERVAL_MS;
  const reconciliationIntervalMs = rawInterval === undefined ? 60 * 60 * 1000 : Number(rawInterval);
  if (Number.isNaN(reconciliationIntervalMs)) {
    throw new Error(`Invalid RECONCILIATION_INTERVAL_MS environment variable: "${rawInterval}" is not a number`);
  }

  return {
    port,
    databaseUrl: process.env.DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5432/webhook_ingestion',
    redisUrl: process.env.REDIS_URL ?? 'redis://localhost:6379',
    kafkaBrokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(','),
    partnerApiBaseUrl: process.env.PARTNER_API_BASE_URL ?? 'http://localhost:4002',
    reconciliationIntervalMs,
  };
}
