-- Schema v25: cron jobs can source their content from a local script.
-- ``script`` holds an absolute command line (path + optional argv) that must
-- resolve inside OCTOP_CRON_SCRIPT_ROOTS; ``script_timeout_s`` overrides the
-- 300s default. Both are nullable, so existing rows keep their behaviour and
-- no backfill is required.

ALTER TABLE cron_jobs ADD COLUMN script TEXT;
ALTER TABLE cron_jobs ADD COLUMN script_timeout_s INTEGER;

UPDATE _schema_version SET version = 25;
