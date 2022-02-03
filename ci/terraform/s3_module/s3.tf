
resource "random_string" "suffix"{
  length = 8
  special = false
  number = true
  lower = true
}

locals{
  bucket_name = "${var.base_stack}-${var.s3_bucket-suffix}-${random_string.suffix}"
}

data "aws_iam_policy_document" "bucket_policy" {
  statement {
    principals {
      type        = "AWS"
      identifiers = [var.access_role_arn]
    }

    actions = [
      "s3:ListBucket",
      "s3:PubObject",
      "s3:GetObject"
    ]

    resources = [
      "arn:aws:s3:::${local.bucket_name}",
      "arn:aws:s3:::${local.bucket_name}/*",
    ]
  }
}

module "aws_s3_bucket"{
  source = "terraform-aws-modules/s3-bucket/aws"
  version = "2.8.0"
  bucket = locals.bucket_name
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

  attach_policy = true
  policy = data.aws_iam_policy_document.bucket_policy.json

}



