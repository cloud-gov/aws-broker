# Oracle 19c — licensing (BYOL) and how a customer entitles their database

> Epic [#519](https://github.com/cloud-gov/aws-broker/issues/519), gate
> [#536](https://github.com/cloud-gov/aws-broker/issues/536). This is a
> **must-solve-before-GA** operational item: without a working licensing path the
> offering does not actually work for a customer.

## Why this matters

The `oracle-19c-dev` plan uses **`licenseModel: bring-your-own-license` (BYOL)**
([ADR-0004](../decisions/ADR-0004-rds-oracle-standard-first.md)). With BYOL, **AWS
does not supply the Oracle license** — the customer (or GSA/cloud.gov centrally)
must hold a valid Oracle Database Enterprise Edition license with the appropriate
options, and be able to demonstrate entitlement for the vCPUs the RDS instance
consumes. If we broker an Oracle instance with no license behind it, the tenant is
out of compliance with Oracle's terms the moment it starts.

> **Contrast with the stalled [PR #500](https://github.com/cloud-gov/aws-broker/issues/500):**
> Peter's draft used `oracle-se2` + **`license-included`**, where AWS bundles the
> license into the hourly cost (~$118/mo in his plan). That sidesteps the
> entitlement problem but locks us to SE2 and AWS's license terms. We chose EE+BYOL
> for the STIG/GovCloud posture and are keeping it (decision re-affirmed), which
> means we **own** the licensing workflow described here. If the licensing path
> proves impractical, switching to `license-included` (SE2) is the documented
> fallback — see ADR-0004.

## The licensing model (BYOL on RDS for Oracle)

- Oracle EE on RDS BYOL is licensed per **vCPU** (Oracle counts vCPUs with a core
  factor). The plan's `instanceClass` (`db.t3.medium` for dev) determines the vCPU
  count that must be covered.
- License Included is **not offered for EE** on RDS — EE is BYOL-only — so the
  edition choice and the license model are coupled.
- The license is **not** something the broker can provision or verify against
  Oracle; it is a contractual entitlement held outside AWS.

## Required workflow (proposed — needs platform-team decision, #536)

Because Oracle is self-service, but licensing is a contractual/organizational fact
the broker cannot introspect, the entitlement gate must live **outside** the
provision call. Options, in order of preference:

1. **Org-level entitlement + `ENABLE_ORACLE` gate (recommended).**
   - GSA/cloud.gov holds (or the tenant agency provides proof of) an Oracle EE
     BYOL entitlement sized for the dev/prod fleet.
   - The platform team only sets `ENABLE_ORACLE` (and adds `oracle-19c-dev` to the
     enabled plans / marketplace visibility) for **orgs/spaces that have provided
     license evidence**, tracked in the same place as other service-enablement
     requests.
   - Net effect: a tenant can only `cf create-service … oracle-19c-dev` if an
     operator has already confirmed licensing for their org. The existing
     `ENABLE_ORACLE` feature flag ([#534](https://github.com/cloud-gov/aws-broker/issues/534))
     is the enforcement point; licensing is the human precondition for flipping it.

2. **Provision-time acknowledgement parameter.** Require
   `-c '{"license_acknowledged": true, "license_ref": "<contract/PO id>"}'` on
   create; the broker records it (audit trail) and refuses without it. Weaker
   (self-attestation), but captures intent + an auditable reference. Could layer on
   top of option 1.

3. **Central shared license (License Manager).** Track the Oracle BYOL entitlement
   in **AWS License Manager** and associate RDS usage with it, so consumption is
   measured against a known entitlement. Best long-term posture; most setup.

## What the customer must be told (docs / catalog)

- The plan is BYOL: **you must have an Oracle EE license covering this instance's
  vCPUs**; cloud.gov does not provide it.
- How to request Oracle enablement for your org (the license-evidence step).
- The instance size → vCPU → license quantity relationship, so they can size their
  entitlement.
- That deprovisioning stops license consumption for that instance.

## Status / open items ([#536](https://github.com/cloud-gov/aws-broker/issues/536))

- [ ] Platform team decides which workflow (1/2/3 above) is authoritative.
- [ ] Enablement runbook: what license evidence is required before `ENABLE_ORACLE`
      + plan visibility are turned on for an org.
- [ ] Customer-facing catalog copy + this page linked from `service-plans.md`.
- [ ] If BYOL proves impractical for GovCloud, execute the ADR-0004 fallback to
      `license-included` (SE2).
