module "snapshot_bucket" {
  source               = "./s3_module"
  stack_description    = var.stack_description
  s3_bucket_suffix     = "aws-broker-snapshot-storage"
  base_stack           = var.base_stack
  access_role_arn      = var.platform_access_role_arn
  s3_expire_after_days = var.snapshot_expiration
}