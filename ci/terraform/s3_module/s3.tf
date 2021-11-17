module "aws_s3_bucket"{
  source = "terraform-aws-modules/s3-bucket/aws"
  version = "2.8.0"
  bucket_prefix = var.s3_bucket_prefix
  acl    = var.s3_acl
  
  versioning = {
    enabled = var.s3_versioning_enabled
  }

  lifecycle_rule = [
    {
        id  = "14day-Retention"
        enabled = var.s3_retention_policy_enabled
        expiration = {
          days = var.s3_expire_after_days
        }
        noncurrent_version_expiration = {
          days = var.s3_delete_after_expired_days
        }
    }
  ]

  server_side_encryption_configuration = {
      rule = {
          apply_server_side_encryption_by_default = {
            sse_algorithm = "AES256"
          }
      }
  }

  block_public_acls       = var.s3_block_public_acls
  block_public_policy     = var.s3_block_public_policy
  ignore_public_acls      = var.s3_ignore_public_acls
  restrict_public_buckets = var.s3_restrict_public_buckets

  tags= {
    Name = var.stack_description
  }

}



