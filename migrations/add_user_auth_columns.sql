-- Run once for databases created before JWT auth --
ALTER TABLE "User" ADD COLUMN IF NOT EXISTS password_hash VARCHAR(255);
ALTER TABLE "User" ADD COLUMN IF NOT EXISTS email VARCHAR(255);
