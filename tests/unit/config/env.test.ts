import { describe, it, expect, afterEach } from 'vitest';
import { loadEnv } from '../../../src/config/env';

const originalEnv = { ...process.env };

afterEach(() => {
  process.env = { ...originalEnv };
});

describe('loadEnv', () => {
  it('returns sensible defaults when nothing is set', () => {
    delete process.env.PORT;
    delete process.env.DATABASE_URL;
    delete process.env.REDIS_URL;
    delete process.env.KAFKA_BROKERS;
    delete process.env.PARTNER_API_BASE_URL;
    delete process.env.RECONCILIATION_INTERVAL_MS;

    const env = loadEnv();

    expect(env).toEqual({
      port: 3000,
      databaseUrl: 'postgres://postgres:postgres@localhost:5432/webhook_ingestion',
      redisUrl: 'redis://localhost:6379',
      kafkaBrokers: ['localhost:9092'],
      partnerApiBaseUrl: 'http://localhost:4002',
      reconciliationIntervalMs: 60 * 60 * 1000,
    });
  });

  it('reads every variable from the environment when set, splitting KAFKA_BROKERS on commas', () => {
    process.env.PORT = '5050';
    process.env.DATABASE_URL = 'postgres://u:p@db:5432/app';
    process.env.REDIS_URL = 'redis://cache:6379';
    process.env.KAFKA_BROKERS = 'broker1:9092,broker2:9092';
    process.env.PARTNER_API_BASE_URL = 'http://partner:4002';
    process.env.RECONCILIATION_INTERVAL_MS = '1000';

    const env = loadEnv();

    expect(env).toEqual({
      port: 5050,
      databaseUrl: 'postgres://u:p@db:5432/app',
      redisUrl: 'redis://cache:6379',
      kafkaBrokers: ['broker1:9092', 'broker2:9092'],
      partnerApiBaseUrl: 'http://partner:4002',
      reconciliationIntervalMs: 1000,
    });
  });

  it('throws when PORT is not a number', () => {
    process.env.PORT = 'abc';

    expect(() => loadEnv()).toThrow('Invalid PORT environment variable: "abc" is not a number');
  });

  it('throws when RECONCILIATION_INTERVAL_MS is not a number', () => {
    process.env.RECONCILIATION_INTERVAL_MS = 'abc';

    expect(() => loadEnv()).toThrow(
      'Invalid RECONCILIATION_INTERVAL_MS environment variable: "abc" is not a number',
    );
  });
});
