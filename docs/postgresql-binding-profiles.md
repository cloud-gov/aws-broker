# PostgreSQL Binding Profiles Runbook

## Profiles

Omitted parameters retain the PR A compatibility credential and create no schema
resource. Restricted profiles use a lowercase schema identifier and are bound
in this order: `owner`, optional `migration`, then `application`.

```hcl
resource "cloudfoundry_service_key" "orders_owner" {
  name             = "orders-owner"
  service_instance = cloudfoundry_service_instance.orders.id
  parameters = jsonencode({ version = 1, profile = "owner", schema = "orders" })
}

resource "cloudfoundry_service_key" "orders_migration" {
  name             = "orders-migration"
  service_instance = cloudfoundry_service_instance.orders.id
  parameters = jsonencode({ version = 1, profile = "migration", schema = "orders" })
}

resource "cloudfoundry_service_key" "orders_application" {
  name             = "orders-application"
  service_instance = cloudfoundry_service_instance.orders.id
  parameters = jsonencode({ version = 1, profile = "application", schema = "orders" })
}
```

Use `on_owner_unbind = "keep"` when the broker-created schema must outlive the
owner binding. This choice is immutable. An adopted schema must use `keep`:

```hcl
resource "cloudfoundry_service_key" "orders_adopted_owner" {
  name             = "orders-adopted-owner"
  service_instance = cloudfoundry_service_instance.orders.id
  parameters = jsonencode({
    version         = 1
    profile         = "owner"
    schema          = "orders"
    adopt_external  = true
    on_owner_unbind = "keep"
  })
}
```

For CloudFormation-style service-binding automation, pass the same JSON without
credentials. The broker returns only standard connection fields; profile, schema,
provenance, and unbind policy are broker state and are intentionally not exposed
in application binding metadata:

```yaml
Parameters:
  BindingParameters:
    Type: String
    Default: '{"version":1,"profile":"application","schema":"orders"}'
Resources:
  OrdersApplicationBinding:
    Type: CloudFoundry::ServiceBinding
    Properties:
      ServiceInstance: orders
      Parameters: !Ref BindingParameters
```

```shell
cf create-service-key orders orders-owner -c '{"version":1,"profile":"owner","schema":"orders"}'
cf create-service-key orders orders-migration -c '{"version":1,"profile":"migration","schema":"orders"}'
cf create-service-key orders orders-application -c '{"version":1,"profile":"application","schema":"orders"}'
```

Credentials are intentionally omitted from these examples.

## Resource Lifecycle

| State | Valid action | Result |
| --- | --- | --- |
| No resource | Owner bind | Create absent schema or explicitly adopt it |
| Provisioning | Exact owner retry | Resume the same lifecycle only |
| Ready | One migration and application binds | Attach restricted principals by schema name |
| Ready | Second owner or second migration | Conflict; no replacement role or schema change |
| Deleting | Cleanup retry | Resume cleanup only |

Each `(instance_id, schema_name)` has exactly one owner binding. Migration and
application binds resolve that ready owner resource by schema name. They never
become the schema owner. Application-first or migration-first requests conflict.

## Ownership And Adoption

An owner bind creates an absent schema under its generated owner role. The owner
credential is an OSB credential but is a privileged schema-owner credential, not
an application runtime credential.

To adopt an out-of-band schema, the request must explicitly contain
`"adopt_external": true`. Adoption is allowed only when the schema is presently
owned by the RDS master role and passes the broker's approved baseline checks.
The broker then transfers schema ownership to the generated owner role and
records `adopted_external` provenance. It never silently adopts any schema.

The adoption baseline is deliberately narrow: the schema must be empty and have
only PostgreSQL's default schema ACL. The broker checks PostgreSQL's namespace
dependency catalog, so tables, sequences, functions, types, collations,
conversions, and any other schema-owned object block adoption. Such schemas must
be managed outside this profile; the broker will reject rather than inspect,
rewrite, or broaden them.

The owner policy is immutable in the durable resource record. Broker-created
resources default to `drop`; an explicit `keep` and all `adopted_external`
resources are never dropped by owner unbind or instance deletion. Manual,
external, or unclaimed schemas are never dropped.

## Privilege Boundary

The owner owns the schema but has no database-wide role, database creation,
replication, or bypass-RLS powers.

The single migration attachment receives schema `USAGE, CREATE`. PostgreSQL
therefore makes it owner of objects it creates: it can alter/drop and control
ACLs for its own objects, but cannot alter/drop objects owned by the owner or
external roles. It has no schema ownership, master membership, or role-admin
power.

Application attachments receive only schema `USAGE`, table `SELECT`, `INSERT`,
`UPDATE`, and `DELETE`, sequence `USAGE` and `SELECT`, function `EXECUTE`, and
type `USAGE`. They receive no schema `CREATE`, ownership, role management, or
grant option.

PostgreSQL default privileges are creator-specific. The broker therefore opens
a separately TLS-verified `verify-full`, timeout-bounded connection as the
generated owner and, when present, migration role to establish that role's own
future-object grants for each application role. It never grants master-role
membership to migration or uses broad role membership to configure defaults.
Failure to establish those defaults fails the binding lifecycle closed.

## Unbind And Deletion

1. Application unbind revokes its existing/default privileges, then removes the
   application credential.
2. Migration unbind conflicts while applications remain; after they are removed
   it removes only the migration credential and does not drop the schema.
3. Owner unbind conflicts while any migration/application attachment remains.
   After attachments are removed, it drops only a validated `broker_created`
   schema whose immutable policy is `drop`. This is destructive, not credential
   rotation. For `keep` and adopted resources, the broker disables the generated
   owner login and retains the durable schema claim and owner binding state; it
   never drops or reassigns the schema. Only the exact owner binding retry may
   reactivate that principal.
4. Adopted external schemas are never dropped. Instance deletion applies the
   same application, migration, owner order and preserves adopted schemas.

Retries must use the same binding ID and exact parameters. Profile, schema,
adoption, ownership, or baseline drift conflicts rather than widening access or
adopting state.
