import { readdirSync, readFileSync } from 'node:fs';
import path from 'node:path';
import { Pool } from 'pg';

async function migrate(): Promise<void> {
  const databaseUrl = process.env.DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5432/webhook_ingestion';
  const pool = new Pool({ connectionString: databaseUrl });

  await pool.query(`
    CREATE TABLE IF NOT EXISTS schema_migrations (
      filename TEXT PRIMARY KEY,
      applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `);

  const migrationsDir = path.join(__dirname, 'migrations');
  const files = readdirSync(migrationsDir)
    .filter((file) => file.endsWith('.sql'))
    .sort();

  for (const file of files) {
    const alreadyApplied = await pool.query('SELECT 1 FROM schema_migrations WHERE filename = $1', [file]);
    if (alreadyApplied.rows.length > 0) {
      continue;
    }

    const sql = readFileSync(path.join(migrationsDir, file), 'utf-8');
    await pool.query(sql);
    await pool.query('INSERT INTO schema_migrations (filename) VALUES ($1)', [file]);
    console.log(`applied migration ${file}`);
  }

  await pool.end();
}

migrate().catch((error) => {
  console.error('migration failed', error);
  process.exit(1);
});
