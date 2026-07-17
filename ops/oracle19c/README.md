# Oracle 19c — operator runbooks

> Epic [#519](https://github.com/cloud-gov/aws-broker/issues/519), WS16.
> Operational guidance for Cloud.gov staff. Dev/test only until the gated live
> RDS proof (WS15).

| Runbook | Topic |
|---------|-------|
| [credential-rotation.md](credential-rotation.md) | rotate the Oracle master credential + rebind |
| parameter-group-change | change a hardened parameter (pending-reboot handling) |
| option-group-change | change option-group options (major-version rebuild) |
| reboot-required-changes | which changes need a reboot (static params) |
| audit-log-review | reviewing CloudWatch `audit`/`alert`/`listener` exports |
| backup-restore | snapshot/restore expectations |
| patching | RDS-managed minor/major version + maintenance window |
| deprovisioning | `cf delete-service`; parameter/option-group cleanup |
| incident-response | credential compromise, unexpected findings |

## Cross-cutting notes

- **Reboot-required changes:** `audit_trail`, `audit_sys_operations`,
  `remote_login_passwordfile`, `sql92_security` are static (pending-reboot). The
  broker surfaces pending-reboot state; a maintenance reboot applies them.
- **Parameter/option-group cleanup** on deprovision is handled by the existing
  `cg-aws-broker-` prefix sweep.
- **Patching:** managed by RDS; the plan pins the 19c version and documents the
  auto-minor-version-upgrade policy. Host OS patching is AWS-inherited.
- **Audit-log review:** the STIG audit posture depends on CloudWatch log-group
  retention — an operational policy decision flagged for manual review in the
  overlay's `control-layers.yml`.

> Only `credential-rotation.md` is fully drafted in this first PR; the rest are
> stubs to be completed alongside the live dev proof (WS15).
