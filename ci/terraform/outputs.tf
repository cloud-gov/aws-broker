output "rds_internal_rds_host" {
  value = module.rds_internal.rds_host
}

output "rds_internal_rds_port" {
  value = module.rds_internal.rds_port
}

output "s3_snapshots_bucket_id"{
  value = module.snapshot_bucket.s3_bucket_id
}
