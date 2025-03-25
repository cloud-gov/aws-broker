variable "base_stack" {
}

variable "stack_description" {
}

variable "remote_state_bucket" {
}

variable "aws_deploy_region" {

}

variable "aws_deploy_role_arn" {

}

variable "platform_access_role_arn" {

}

variable "snapshot_expiration" {
  default = 14
}

variable "rds_internal_instance_type" {
}

variable "rds_internal_db_size" {
}

variable "rds_internal_db_name" {
}

variable "rds_internal_db_engine" {
}

variable "rds_internal_db_engine_version" {
}

variable "rds_internal_db_parameter_group_family" {
}

variable "rds_internal_multi_az" {
}

variable "rds_internal_username" {
}

variable "rds_internal_password" {
}

variable "rds_internal_apply_immediately" {
}

variable "rds_internal_allow_major_version_upgrade" {
}

variable "rds_force_ssl" {
  description = "Sets the value of rds.force_ssl in the parameter group created in the rds_module, 0=false, 1=true"
  type        = number
  default     = 1
}