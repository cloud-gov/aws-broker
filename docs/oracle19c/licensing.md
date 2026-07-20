# Oracle 19c — licensing (BYOL): customer responsibilities & instructions

> Epic [#519](https://github.com/cloud-gov/aws-broker/issues/519).
>
> **cloud.gov does not manage, supply, verify, or enforce Oracle licensing.**
> Oracle Database is licensed by the customer directly with Oracle. This page tells
> a customer what they are responsible for and how to bring their license to a
> brokered Oracle instance. It is **not** a gate the broker enforces.

## Responsibility model

| Responsibility | Owner |
|---|---|
| Holding a valid Oracle Database license (edition + options + quantity) | **Customer** |
| Ensuring the license covers the instance's vCPUs | **Customer** |
| Compliance with Oracle's licensing terms | **Customer** |
| Providing the RDS Oracle service (provision/bind/hardening) | cloud.gov / this broker |
| Managing or auditing customer Oracle licenses | **No one at cloud.gov** — out of scope |

The plan is **`bring-your-own-license` (BYOL)** ([ADR-0004](../decisions/ADR-0004-rds-oracle-standard-first.md)):
AWS does not include an Oracle license in the price, and the broker cannot see,
validate, or track your entitlement. Provisioning an Oracle instance does **not**
constitute a license — you must already hold one.

## What you need before you provision

Amazon RDS for Oracle **Enterprise Edition** is offered **BYOL only** (License
Included is not available for EE). To use `oracle-19c-dev` you must have:

- An **Oracle Database Enterprise Edition** license, with any options your workload
  uses, sufficient to cover the instance's vCPUs.
- vCPU sizing: the plan's `instanceClass` (dev: `db.t3.medium`) sets the vCPU count
  your license must cover. Oracle applies its core-factor table to cloud vCPUs —
  confirm the exact quantity with your Oracle licensing contact.

> Not sure you can meet EE/BYOL? Tell us — we can discuss the `license-included`
> (Standard Edition 2) alternative documented in ADR-0004. That is a product
> decision, not something the broker toggles per request.

## How to apply your license (BYOL on RDS)

BYOL for RDS Oracle is a **contractual** arrangement between you and Oracle — there
is no key to paste into RDS and nothing the broker installs. Steps:

1. **Confirm entitlement with Oracle.** Ensure your existing Oracle agreement
   permits deploying on Amazon RDS (authorized cloud environment) for the vCPU
   count of your chosen plan.
2. **(Optional) Record it in AWS License Manager.** If your org uses AWS License
   Manager, create a self-managed license for your Oracle entitlement and a rule to
   track RDS consumption against it. This is for **your** visibility/compliance; the
   broker neither requires nor reads it.
3. **Provision normally.** `cf create-service aws-rds oracle-19c-dev my-oracle`.
   No license parameter is required or accepted — provisioning assumes you are
   licensed.
4. **Keep records.** Retain proof of entitlement for your own audits; cloud.gov
   does not store it.

## Why the broker still has an `ENABLE_ORACLE` switch (not a license gate)

Oracle provisioning is disabled unless the operator sets `ENABLE_ORACLE`. **This is
a staged-rollout switch for a new offering, not a licensing checkpoint** (and not a
security/boundary control — Oracle uses the same credential model as the
postgres/mysql plans). cloud.gov does not condition Oracle availability on license
evidence and does not collect it. Once the offering is enabled, provisioning is
self-service; licensing remains entirely your responsibility.

## What to tell us vs. what to handle yourself

| You handle (with Oracle) | We can help with |
|---|---|
| Buying / sizing / renewing the Oracle license | Choosing a plan / instance size (which sets the vCPU count you must license) |
| Compliance with Oracle's terms | The EE/BYOL vs SE2/license-included tradeoff (ADR-0004) |
| Your own License Manager tracking | Provisioning, binding, connecting, hardening, STIG validation |

## References

- [ADR-0004](../decisions/ADR-0004-rds-oracle-standard-first.md) — RDS-standard-first; EE + BYOL, SE2/license-included fallback
- [service-plans.md](service-plans.md) — the `oracle-19c-dev` plan
- AWS: *Licensing Amazon RDS for Oracle* (BYOL vs License Included) — vendor docs
- Oracle: *Licensing Oracle Software in the Cloud Computing Environment* — vendor policy
