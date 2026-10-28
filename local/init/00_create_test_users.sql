-- 00_create_test_users.sql — local harness only.
-- DEVELOPMENT SIGNAL ONLY.
--
-- Creates a NON-SYS privileged application user that mirrors the RDS master-user
-- privilege model (RDS does not grant SYS/SYSDBA). Hardening scripts are developed
-- and tested as this user so RDS-only permission failures surface locally.
--
-- gvenzl/oracle-free already creates APPUSER via APP_USER env; this adds the
-- privileged-but-not-SYS role set an RDS master user typically has.

ALTER SESSION SET CONTAINER = FREEPDB1;

-- Grant the RDS-master-like privilege set (NOT SYSDBA).
GRANT CREATE SESSION TO APPUSER;
GRANT CREATE USER, ALTER USER, DROP USER TO APPUSER;
GRANT CREATE ROLE, GRANT ANY ROLE TO APPUSER;
GRANT CREATE PROFILE, ALTER PROFILE, DROP PROFILE TO APPUSER;
GRANT SELECT ON SYS.DBA_USERS TO APPUSER;
GRANT SELECT ON SYS.DBA_PROFILES TO APPUSER;
GRANT SELECT ON SYS.DBA_ROLE_PRIVS TO APPUSER;
GRANT SELECT ON SYS.DBA_SYS_PRIVS TO APPUSER;
GRANT SELECT ON SYS.DBA_TAB_PRIVS TO APPUSER;
GRANT AUDIT_ADMIN TO APPUSER;
