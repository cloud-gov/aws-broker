# ADR-0003 — SQL-statement hardening: platform (broker) vs. customer responsibility

- **Status:** Accepted
- **Date:** 2026-07-24 (accepted 2026-09-23)
- **Related:** [ADR-0001](ADR-0001-oracle-19c-stig-hardened-in-aws-broker.md) (STIG-hardened Oracle 19c in aws-broker; the broker provisions, the overlay validates), [#557](https://github.com/cloud-gov/aws-broker/issues/557) (the design decision this ADR resolves), [#558](https://github.com/cloud-gov/aws-broker/issues/558) (live GovCloud RDS proof), [#541](https://github.com/cloud-gov/aws-broker/issues/541) (TCPS 2484 / deny 1521)
- **Deciders:** Peter Burkholder, William Zujkowski, Mark Boyd, Sean

## Context

- The Oracle 19c STIG hardening splits into two mechanically different layers:
  1. Parameter/option/network hardening (audit config, sqlnet/TLS, listener, TDE) — applied via RDS PARAMETER GROUPS and OPTION GROUPS at the control plane. This is the broker's own “born-hardened” baseline (`services/rds/oracle_tls.go`, `option_group.go`, and the Oracle parameter-group baseline); the overlay notes these are “platform/option-group managed on RDS — NOT SQL-checkable.”
  2. SQL-statement hardening — the parts that require issuing SQL as the master user (DEFAULT profile limits, locking default accounts, unified audit policies, etc.). Scripts live in the sibling repo `cg-oracle-database-19c-stig-overlay/hardening/sql/`.

- This ADR covers only layer 2. Layer 1 is already decided (see ADR-0001).

- The design question: is issuing the hardening SQL a PLATFORM responsibility the broker performs automatically (post-provision, before reporting the instance ready), or a CUSTOMER responsibility the tenant runs against their master user?

- Constraints that bear on it:
  - The broker today opens NO client SQL connection to tenant DBs; it only calls the AWS RDS control plane. Broker-run SQL hardening is net-new capability (Oracle driver, TLS/TCPS path, endpoint fetch, master-cred decrypt).
  - RDS grants a master user, not SYS/SYSDBA — bounding what SQL hardening is even possible.
  - Compliance posture on delivery differs by option:
    - Option 1 (broker owns it): the broker must apply the SQL and pass before marking the instance ready — failing closed rather than handing over a partially hardened DB.
    - Option 2 (customer owns it): the instance ships with layer-1 hardening only; the layer-2 SQL state is, by design, the tenant's to apply — an accepted, documented posture (see Compliance / boundary), not a finding.
  - Cloud.gov's self-service RDS model (ADR-0001 alignment) — how much does the platform do FOR the tenant vs. document FOR the tenant?

## Decision Drivers

These drivers summarize Peter's stated rationale from synchronous and other
out-of-band discussions with the deciders.

- The hardening we do at the AWS API layer cannot be modified by the customer, so
  there's no drift unless an operator intervenes. We continue to “own” that layer
  (security groups, option groups, parameters, updates).
- Any hardening applied via SQL could be undone by a customer with their own SQL,
  so leaving all SQL hardening (and any later drift) in the customer's hands makes
  for clear boundaries.
- Broker-managed SQL connections would vastly expand this project's scope — new
  complexities like managing and rotating connection credentials after hardening.
- Oracle is a small fraction of the Cloud.gov user base, so the engineering
  investment should be bounded accordingly. _(Build-effort sizing factor only; the
  compliance bar is identical regardless of user count.)_

The decision therefore is: **maintain the SQL hardening code that works and
provide customers with instructions on how to run it.** Whether a customer runs
it is a customer responsibility.

## Considered Options

The following option framing was drafted with AI assistance and reviewed against
the deciders' stated responsibility-boundary rationale.

1. **Platform responsibility — broker runs the SQL hardening post-provision.**
   The broker connects to the new instance after it is AWS-`available` but before
   transitioning to `InstanceReady` (the `create_worker.go` seam between
   `waitForDbReady` and the `InstanceReady` write), runs the hardening SQL
   idempotently, and fails closed on error.
2. **Customer responsibility — broker documents the hardening; tenant applies it.**
   The broker delivers the born-hardened param/option baseline (layer 1) only; the
   SQL-statement hardening is platform-maintained and published (overlay + docs)
   for the tenant to run against their master user. The platform validates the SQL
   works against smoke-test instances but never runs it against tenant DBs.
3. **Hybrid — broker applies a mandatory SQL subset, documents the rest.** Rejected:
   still requires the full broker→tenant SQL-connection capability and
   credential-rotation machinery of option 1 (so it does not save the scope cost),
   while muddying the clean ownership boundary of option 2.

## Decision Outcome

**Customer responsibility.** The broker delivers the born-hardened param/option
baseline (layer 1) only; the SQL-statement hardening (layer 2) is
platform-maintained and published for the tenant to apply against their master
user. The platform validates the SQL works but does not run it against tenant
instances.

## Validation (CI smoke test)

The platform must prove the maintained SQL hardening actually works against a
real brokered instance (per ADR-0001, this is the pipeline proof, not the tenant
compliance guarantee). This is being added to `aws-broker` CI as a STIG-validation
step running the `cg-oracle-database-19c-stig-overlay` CINC runner against a
broker-provisioned Oracle instance (tracked as new CI issues; see
[#558](https://github.com/cloud-gov/aws-broker/issues/558)). Flow:

1. Instantiate a smoke-test instance (`cf create-service aws-rds medium-oracle-se2 ...`).
2. Create a Cloud.gov app and bind the instance to it.
3. Run compliance validation via the overlay CINC runner reaching the instance
   through the bound app.
4. First pass: platform-level (layer-1) hardening PASSES; the SQL-layer (layer-2)
   checks are expected to fail/skip (the customer hardening is not yet applied).
5. Apply the overlay `hardening/sql/` scripts against the instance (as the master
   user — the same procedure a customer would follow), using SQLcl bundled into
   the same runner image (see overlay [#95](https://github.com/cloud-gov/cg-oracle-database-19c-stig-overlay/issues/95)).
6. Re-run validation: all customer-remediable checks — including the SQL-layer
   controls — PASS. (Non-customer-remediable findings, e.g. the RDSADMIN profile,
   and local-inconclusive controls are dispositioned in the CI gating baseline;
   see overlay [#77](https://github.com/cloud-gov/cg-oracle-database-19c-stig-overlay/issues/77) / [#21](https://github.com/cloud-gov/cg-oracle-database-19c-stig-overlay/issues/21) / [#79](https://github.com/cloud-gov/cg-oracle-database-19c-stig-overlay/issues/79).)
7. Tear everything down (self-clean; see aws-broker #544).

> This proves the published customer procedure is known-good end to end: a
> broker-provisioned instance, hardened only by the documented `hardening/sql/`
> scripts, passes the overlay STIG validation. It does NOT change the
> responsibility boundary — the broker still never runs SQL against a tenant DB.

> **Validation transport.** The candidate transport is to run a single hardened
> runner app *on* a bound Cloud.gov app and drive it via `cf ssh`. That one image
> carries both the CINC-Auditor scanner and Oracle SQLcl (overlay [#95](https://github.com/cloud-gov/cg-oracle-database-19c-stig-overlay/issues/95)), so the
> same app both runs the scan and applies the `hardening/sql/` scripts between
> passes — avoiding a second buildpack app just to carry a SQL client. It reads
> coordinates from `VCAP_SERVICES` and connects over TCPS 2484 with `verify-ca`.
> This replaces the earlier `cinc-auditor -t ssh://…` approach, which failed on
> CF SSH-algorithm incompatibility. Proving this out in CI is an implementation
> detail tracked in the new CI issues, not part of this responsibility decision.

## Consequences

### Positive

- **Clear, tamper-resistant ownership boundary.** The platform owns the
  AWS-API-layer hardening it *can* durably enforce (security groups, option/
  parameter groups, updates); the in-DB SQL layer, which a customer can undo with
  their own SQL, stays in the customer's hands. Responsibility follows
  enforceability.
- **Avoids a large, security-sensitive scope expansion.** No broker-managed SQL
  connections, and no credential-management/rotation machinery to build.
- **No new broker attack surface.** The broker continues to touch only the AWS
  control plane and never opens a data-plane connection to a tenant DB, so the
  authorization boundary is unchanged.
- **Investment stays proportionate.** Keeping the SQL layer as maintained scripts
  plus instructions bounds the engineering effort for a small user segment.
- **Platform still ships and validates working SQL** (see Validation), so
  customers get a known-good, tested procedure rather than authoring hardening
  themselves.

### Negative

- **Post-delivery SQL-layer drift is a customer-owned risk.** The broker neither
  detects nor remediates changes a tenant makes to the SQL-layer hardening. The
  customer owns applying it *and keeping it applied*; the platform provides no
  drift detection for these controls, so an instance may silently fall out of the
  hardened state.

### Compliance / boundary

- **These SQL-statement STIG controls are a customer / shared responsibility.**
  The controls enforced by the layer-2 scripts — DEFAULT profile limits, locking/
  expiring default accounts, enabling unified audit policies, etc. — are **not**
  applied by the broker. They are the tenant's responsibility to apply (and keep
  applied) against their master user, using the platform-maintained SQL and
  instructions.
- **Requires ISSO acceptance.** This split MUST be reviewed and accepted by the
  ISSO before the ATO relies on it. Delivery of a SQL-un-hardened instance is a
  known posture, accepted on the basis that (a) the platform cannot durably
  enforce in-DB settings the customer can reverse, and (b) the AWS-API-layer
  hardening the broker *does* own is tamper-resistant by the customer.
- **Must be recorded in the customer-responsibility matrix / SSP.** Each affected
  STIG control ID MUST be documented as customer- (or shared-) responsibility in
  the CRM and reflected in the SSP control-origination fields, so the transfer is
  auditable and the tenant is on notice. The durable follow-up is tracked in
  [#587](https://github.com/cloud-gov/aws-broker/issues/587).

## Validation transport implementation

The chosen path is to run a single hardened runner app — carrying both
CINC-Auditor and SQLcl (overlay [#95](https://github.com/cloud-gov/cg-oracle-database-19c-stig-overlay/issues/95)) — *on* a bound Cloud.gov app driven via
`cf ssh` (TCPS 2484, `verify-ca`), replacing the failed
`cinc-auditor -t ssh://…` approach. Proving it out end to end in `aws-broker` CI
is tracked by the new CI STIG-validation issues (anchored to
[#558](https://github.com/cloud-gov/aws-broker/issues/558)); it is
dependency-blocked on a pullable overlay runner image in the boundary (overlay
[#84](https://github.com/cloud-gov/cg-oracle-database-19c-stig-overlay/issues/84)/[#89](https://github.com/cloud-gov/cg-oracle-database-19c-stig-overlay/issues/89)/[#93](https://github.com/cloud-gov/cg-oracle-database-19c-stig-overlay/issues/93)/[#85](https://github.com/cloud-gov/cg-oracle-database-19c-stig-overlay/issues/85)/[#95](https://github.com/cloud-gov/cg-oracle-database-19c-stig-overlay/issues/95)).
