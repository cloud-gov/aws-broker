module "snapshot_bucket"{
    source = "./s3_module"
    stack_description = var.stack_description
    s3_bucket_prefix = var.s3_snapshots_bucket_name_prefix
}