# ADR-0004 - Use unique PostgreSQL credentials for each service binding

- **Status:** Proposed
- **Date:** 2026-10-15
- **Related:** [#12](https://github.com/cloud-gov/aws-broker/issues/12), [ADR-0003](ADR-0003-sql-hardening-platform-vs-customer-responsibility.md)
- **NIST controls:** AC-3, AC-6, AU-2, AU-3, CM-3, SA-8, SC-8, SC-13, SI-10

## Context

The RDS PostgreSQL binding path currently returns the provisioned RDS master
username and password to every application binding. All applications therefore
share the same database identity, a compromised credential cannot be revoked
without affecting every consumer, and the broker cannot safely distinguish an
OSB binding retry from a new binding.

The current credential is intentionally powerful: it is the RDS master login
created for the instance, including its privileges obtained from the RDS master
role and any customer-granted privileges. Replacing it with a narrowly granted
role by default would be a potentially breaking behavior change for existing
Cloud.gov consumers, including consumers that create schemas, roles, or other
database objects through ordinary application or migration tooling.

ADR-0003 prohibits general broker-managed SQL execution against tenant
databases. This decision creates one narrow exception for broker-provisioned
PostgreSQL instances: lifecycle management of generated binding logins. The
broker may not inspect or modify customer rows or execute application SQL.

## Decision Drivers

- Each OSB binding needs a distinct, encrypted-at-rest, independently revocable
  login credential.
- The default binding capability must preserve current PostgreSQL consumer
  behavior rather than silently reducing privileges.
- Bind and unbind requests must be safely retryable.
- The broker must use verified TLS and a configured RDS CA bundle for this
  narrow data-plane administrative operation.
- The change must not alter MySQL, Oracle, or other non-PostgreSQL RDS binding
  behavior.

## Considered Options

1. Continue returning the RDS master credential. Rejected because bindings do
   not have independent identity or revocation boundaries.
2. Create a restricted PostgreSQL role by default. Rejected for this release
   because it silently breaks consumers that depend on the current master-role
   capability set.
3. Create one generated login per binding and grant it membership in the RDS
   instance master role. Chosen because it preserves the current master-role
   privileges and customer-granted role privileges as closely as PostgreSQL RDS
   permits while giving each binding a unique password and revocation point.
4. Use IAM database authentication. Rejected because it changes client
   connection behavior and is outside the smallest safe compatibility change.

## Decision Outcome

For a PostgreSQL instance provisioned by this broker, each OSB binding receives
a generated `LOGIN INHERIT` PostgreSQL role and a unique password. The broker
grants the generated login membership in that instance's stored master role.
The generated role therefore inherits the master role's object privileges and
role-derived permissions, and can use `SET ROLE` where PostgreSQL permits it.

The compatibility profile is explicit:

- The generated role has `LOGIN`, `INHERIT`, and each delegable role attribute
  (`CREATEDB`, `CREATEROLE`, `REPLICATION`, and `BYPASSRLS`) read from the
  stored master role at binding time.
- It is a member of the stored RDS master role.
- The broker does not add a schema, database, table, role, or public-schema
  grant beyond what membership in the current master role provides.
- The broker does not claim that role attributes which PostgreSQL does not
  inherit, or AWS RDS does not permit a master user to delegate, are preserved.

Before production rollout, operators must validate against the supported RDS
PostgreSQL versions that the master role can grant this membership and that a
generated role has the same required effective capabilities as an existing
master-credential binding, including customer-required schema creation and role
creation behavior. A failure of this compatibility check blocks rollout; it
must not be replaced by a narrower default grant set.

The broker persists the binding ID, generated login, encrypted password and
salt, and provisioning state before creating the database role. A failed Bind
retains this state for retry. A successful retry returns the same credential.
Unbind removes the generated role before deleting its record. Instance deletion
removes all generated binding roles before deleting the RDS instance. Missing
binding records and missing generated roles are idempotently successful.

The existing `rotate_credentials` update parameter continues to rotate only the
RDS master password. This decision does not expose binding-password rotation.
The persisted binding model reserves the necessary lifecycle seam; a future
binding-parameter contract must make rotation retry-safe without conflating it
with the master credential.

The broker connects as the stored RDS master login only to create, reconcile,
grant membership to, and remove generated binding roles. The connection uses a
mounted, configured RDS CA bundle with `sslmode=verify-full`, hostname
verification, and bounded connection/query timeouts. A missing or unreadable CA
bundle, unavailable endpoint, or TLS failure fails the lifecycle operation
closed. Credentials and DSNs must not be logged or included in job arguments.

## Follow-up: restricted profiles

Issue #12 also tracks opt-in constrained credentials. That work is explicitly
out of scope for this decision's implementation PR and will be delivered in a
separate follow-up PR. It will add strict bind parameters and persisted scope
comparison for:

- an `application` profile limited to a caller-requested PostgreSQL schema;
  it cannot grant permissions to other users; and
- a `migration` profile with reviewed, declarative-schema-management
  capabilities within a caller-requested PostgreSQL schema, including creating,
  altering, and dropping its objects and establishing bounded ownership/default
  privileges, but no unrestricted database-administrator capability.

The follow-up parameter contract will be versioned and accept a caller-supplied
schema name only for these opt-in profiles. It will allowlist PostgreSQL
identifiers, reject reserved and system schema names, reject unsafe quoting
cases, and reject arbitrary roles, grants, SQL, or privilege fields. The
normalized schema and profile are persisted with the binding. An exact retry
returns the existing credential; a scope mismatch is a conflict and never
changes or widens access.

The broker will create the requested schema if absent, then reconcile ownership
and grants only when the schema is already owned and claimed by that same
binding. It will reject a schema claimed by another binding, or an existing
schema with incompatible owner or privileges; it must never silently take over
or broaden an existing schema. Schema sharing across bindings is unsupported
until a separate safe ownership model is designed and approved.

Those profiles must reject malformed, unknown, or privilege-expanding fields
and must never change the compatibility profile when parameters are omitted.
Omitted parameters retain this decision's compatibility profile and do not
implicitly create a schema.

## Consequences

### Positive

- Every new PostgreSQL binding receives a unique credential and can be revoked
  independently.
- Bind retries return persisted credential material rather than creating more
  roles.
- Default PostgreSQL binding capability remains aligned with current consumers.
- The broker stores no new plaintext credential material.

### Negative and operational limits

- The broker gains a narrow PostgreSQL data-plane administrative capability and
  requires network reachability, TLS trust material, and a master role capable
  of delegating membership.
- PostgreSQL/RDS may reject delegation of part of the master role's capability
  set. This is a rollout blocker, not an invitation to silently narrow default
  permissions.
- A generated member role can inherit future privileges granted to the master
  role. That matches the current shared-master behavior but does not provide
  least privilege by default.
- Cleanup may fail if a generated binding role owns objects. The broker must
  preserve binding state and fail deletion or unbind for retry rather than
  orphaning the role.

## Rollout and Rollback

Rollout requires the CA bundle on all broker and worker processes, network
access to PostgreSQL endpoints, and live non-production validation of bind,
retry, unbind, instance deletion, and the compatibility capabilities above.

The database migration is additive. Existing bindings cannot be retroactively
associated with an OSB binding ID and continue using their previously issued
credential until they are rebound or otherwise migrated through an approved
operator procedure.

Rollback must not resume issuing the RDS master credential to new PostgreSQL
bindings. Existing generated roles remain valid until unbound or cleaned by a
cleanup-capable broker release. Operators must retain a cleanup-capable release
until those bindings are drained; dropping the binding state table or rolling
back cleanup capability risks orphaned database roles.

The restricted-profile follow-up must document Terraform/service-binding
examples with parameter values only, operator prerequisites, schema-conflict
behavior, and equivalent rollback constraints. It must not include credentials
in examples or logs.
