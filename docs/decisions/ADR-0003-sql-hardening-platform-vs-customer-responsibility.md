# ADR-0003 — SQL-statement hardening: platform (broker) vs. customer responsibility

- **Status:** Accepted
- **Date:** 2026-07-24 (accepted 2026-09-23)
- **Related:** [ADR-0001](ADR-0001-oracle-19c-stig-hardened-in-aws-broker.md) (STIG-hardened Oracle 19c in aws-broker; the broker provisions, the overlay validates), [#557](https://github.com/cloud-gov/aws-broker/issues/557) (the design decision this ADR resolves), [#558](https://github.com/cloud-gov/aws-broker/issues/558) (live GovCloud RDS proof), [#541](https://github.com/cloud-gov/aws-broker/issues/541) (TCPS 2484 / deny 1521)
- **Deciders:** Peter Burkholder, William Zujkowski, Mark Boyd, Sean

## Context (Agent written)

- The Oracle 19c STIG hardening splits into two mechanically different layers:
  1. Parameter/option/network hardening (audit config, sqlnet/TLS, listener, TDE) — applied via RDS PARAMETER GROUPS and OPTION GROUPS at the control plane. This is the broker's own “born-hardened” baseline (`services/rds/oracle_tls.go`, `option_group.go`, and the Oracle parameter-group baseline). The overlay itself notes these settings are “platform/option-group managed on RDS — NOT SQL-checkable.”
  2. SQL-statement hardening — the parts that require issuing SQL as the master user (DEFAULT profile limits, locking default accounts, unified audit policies, etc.). Scripts live in the sibling repo `cg-oracle-database-19c-stig-overlay/hardening/sql/`.

- THIS ADR is only about layer 2 (the SQL-statement hardening). Layer 1 is already decided (born-hardened via param/option groups; see ADR-0001).

- The design question: is issuing that hardening SQL a PLATFORM responsibility the broker performs automatically (post-provision, before reporting the instance ready), or a CUSTOMER responsibility documented and left to the tenant to run against their master user?

- Constraints that bear on it:
  - The broker today opens NO client SQL connection to tenant DBs; it only calls the AWS RDS control plane. Broker-run SQL hardening is net-new capability (needs an Oracle driver, TLS/TCPS path, endpoint fetch, master-cred decrypt).
  - RDS grants a master user, not SYS/SYSDBA — bounds what hardening is even possible via SQL.
  - Compliance posture: the state in which a newly provisioned instance reaches the tenant differs by option.
    - If the broker owns the SQL hardening (Option 1), it would have to apply it and pass before marking the instance ready — and fail closed if it could not, rather than hand over a partially hardened DB.
    - If the customer owns it (Option 2), the instance is delivered with layer-1 hardening only and the layer-2 SQL state is, by design, the tenant's to apply — an accepted, documented posture (see Compliance / boundary), not a finding.
  - Cloud.gov's self-service RDS model (ADR-0001 alignment) — how much does the platform do FOR the tenant vs. document FOR the tenant?

## Decision Drivers (Peter)

- The hardening we do at the AWS API layer cannot be modified by the customer, so
  there's no opportunity for drift unless an operator intervenes.
- Any hardening we apply via SQL could be undone by a customer with their own SQL,
  so letting all SQL hardening (and any subsequent drift) remain in the customer's
  hands makes for clear boundaries.
- We still “own” the hardening at the AWS API layer (security groups, option
  groups, parameters, updates, etc.).
- The work required to support broker-managed SQL connections would vastly expand
  the scope of this project, with all the accompanying downsides — including new
  complexities like managing connection credentials and rotating them after the
  hardening is done.
- Oracle users represent a small fraction of the Cloud.gov user base, so the
  engineering investment in supporting them should be bounded accordingly. _(This
  is a build-effort sizing factor only; the compliance bar is identical
  regardless of user count.)_

The decision therefore is: **maintain the SQL hardening code that works and
provide customers with instructions on how to run it.** Whether a customer chooses
to run it is a customer responsibility.

## Considered Options (agent written)

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
compliance guarantee). This validation is being added to `aws-broker` CI as a
STIG-validation step that runs the `cg-oracle-database-19c-stig-overlay` CINC
runner against a broker-provisioned Oracle instance (tracked as new CI issues;
see [#558](https://github.com/cloud-gov/aws-broker/issues/558)). Flow:

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
  AWS-API-layer hardening it *can* durably enforce (security groups, option
  groups, parameters, updates) — which “cannot be modified by the customer, so
  there’s no opportunity for drift unless an operator intervenes.” The in-DB SQL
  layer, which “could be undone by a customer with their own SQL,” stays entirely
  in the customer's hands. Responsibility follows enforceability.
- **Avoids a large, security-sensitive scope expansion.** Broker-managed SQL
  connections “would vastly expand the scope of this project,” including net-new
  capability (Oracle driver, TLS/TCPS path, endpoint fetch) and “new complexities
  like how to manage connection credentials, and then rotate them after the
  hardening is done.” Customer-responsibility avoids all of it.
- **No new broker attack surface.** The broker continues to touch only the AWS
  control plane and never opens a data-plane connection to a tenant DB, so the
  authorization boundary is unchanged.
- **Investment stays proportionate to build effort.** Oracle is a small slice of
  the user base; keeping the SQL layer as maintained-scripts-plus-instructions
  bounds the engineering investment. _(Sizing factor only — the compliance bar is
  identical regardless of user count; see Compliance / boundary.)_
- **Platform still ships and validates working SQL.** We maintain the hardening
  SQL and prove it works against smoke-test instances (see Validation), so
  customers are handed a known-good, tested procedure rather than being left to
  author hardening themselves.

### Negative

- **Post-delivery SQL-layer drift is a customer-owned risk.** The broker neither
  detects nor remediates changes a tenant makes to the SQL-layer hardening after
  delivery. Because the customer owns applying this hardening (see Compliance /
  boundary), they also own **keeping it applied** — including monitoring for and
  correcting any drift. The platform provides no drift detection for these
  controls; an instance may silently fall out of the hardened state, and that
  residual risk rests with the customer.

### Compliance / boundary

- **These SQL-statement STIG controls are a customer / shared responsibility.**
  The controls enforced by the layer-2 scripts — DEFAULT profile limits, locking/
  expiring default accounts, enabling unified audit policies, etc. — are **not**
  applied by the broker. They are the tenant's responsibility to apply (and keep
  applied) against their master user, using the platform-maintained SQL and
  instructions.
- **Requires ISSO acceptance.** This shared-responsibility split MUST be reviewed
  and accepted by the ISSO before it is relied upon for the ATO. Delivery of a
  SQL-un-hardened instance is a known posture, accepted here on the basis that
  (a) the platform cannot durably enforce in-DB settings the customer can reverse
  with their own SQL, and (b) the AWS-API-layer hardening the broker *does* own
  (security groups, option/parameter groups, updates) is tamper-resistant by the
  customer.
- **Must be recorded in the customer-responsibility matrix / SSP.** Each affected
  STIG control ID MUST be documented as customer- (or shared-) responsibility in
  the system's Customer Responsibility Matrix (CRM) and reflected in the SSP
  control-origination fields, so the responsibility transfer is auditable and the
  tenant is on notice. _<!-- TODO: enumerate the specific STIG rule IDs from the
  hardening/sql/ scripts and map each to its 800-53 control in the CRM. -->_

## Open questions

- **Validation transport (implementation detail, not this decision).** The chosen
  path is to run a single hardened runner app — carrying both CINC-Auditor and
  SQLcl (overlay [#95](https://github.com/cloud-gov/cg-oracle-database-19c-stig-overlay/issues/95)) — *on* a bound Cloud.gov app driven via `cf ssh` (TCPS 2484,
  `verify-ca`), replacing the failed `cinc-auditor -t ssh://…` approach. Proving
  it out end to end in `aws-broker` CI is tracked by the new CI STIG-validation
  issues (anchored to [#558](https://github.com/cloud-gov/aws-broker/issues/558));
  it is dependency-blocked on a pullable overlay runner image in the boundary
  (overlay [#84](https://github.com/cloud-gov/cg-oracle-database-19c-stig-overlay/issues/84)/[#89](https://github.com/cloud-gov/cg-oracle-database-19c-stig-overlay/issues/89)/[#93](https://github.com/cloud-gov/cg-oracle-database-19c-stig-overlay/issues/93)/[#85](https://github.com/cloud-gov/cg-oracle-database-19c-stig-overlay/issues/85)/[#95](https://github.com/cloud-gov/cg-oracle-database-19c-stig-overlay/issues/95)).
- **STIG rule ID → 800-53 mapping** for the CRM/SSP entries above.
  _<!-- TODO. -->_
