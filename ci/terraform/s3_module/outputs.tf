
output "s3_bucket_id" {
  description = "The name of the bucket."
  value       = module.aws_s3_bucket.s3_bucket_id
}