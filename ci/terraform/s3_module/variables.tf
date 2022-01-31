variable "stack_description"{
}

variable "s3_bucket_prefix"{
}

variable "s3_acl" {
  default = "private"
}

variable "s3_versioning_enabled" {
  default = true
}

variable "s3_retention_policy_enabled" {
  default = true
}

variable "s3_delete_after_expired_days"{
    default = 1
}

variable "s3_expire_after_days" {
    default = 14
}

variable "s3_block_public_acls" {
    default = true
}

variable "s3_block_public_policy" {
    default = true
}

variable "s3_ignore_public_acls" {
    default = true
}

variable "s3_restrict_public_buckets" {
    default = true
}