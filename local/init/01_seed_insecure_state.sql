-- 01_seed_insecure_state.sql — local harness only.
-- DEVELOPMENT SIGNAL ONLY.
--
-- Deliberately creates a WEAK state so the assessment scripts have something to
-- detect and the hardening scripts have something to remediate. Never run outside
-- the local throwaway container.

ALTER SESSION SET CONTAINER = FREEPDB1;

-- Weak profile: unlimited failed logins + no password expiry (STIG findings).
CREATE PROFILE weak_profile LIMIT
  FAILED_LOGIN_ATTEMPTS UNLIMITED
  PASSWORD_LIFE_TIME UNLIMITED;

-- A user on the weak profile.
CREATE USER seed_weak IDENTIFIED BY "devpw_ChangeMe1" PROFILE weak_profile;
GRANT CREATE SESSION TO seed_weak;

-- Over-privileged grant to PUBLIC (classic STIG finding; detect-first, never
-- auto-revoked by hardening without an explicit allowlist).
-- (Left as a comment: granting to PUBLIC on a real system is dangerous; the
-- assessment detects existing PUBLIC grants rather than us creating one here.)
