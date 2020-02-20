variable "base_stack" {}
variable "stack_description" {}
variable "remote_state_bucket" {}

variable "rds_internal_instance_type" {}
variable "rds_internal_db_size" {}
variable "rds_internal_db_name" {}
variable "rds_internal_db_engine" {}
variable "rds_internal_db_engine_version" {}
variable "rds_internal_db_parameter_group_family" {}
variable "rds_internal_username" {}
variable "rds_internal_password" {}
variable "rds_internal_apply_immediately" {}
variable "rds_internal_allow_major_version_upgrade" {}

variable "rds_shared_mysql_instance_type" {}
variable "rds_shared_mysql_db_size" {}
variable "rds_shared_mysql_db_name" {}
variable "rds_shared_mysql_db_engine" {}
variable "rds_shared_mysql_db_engine_version" {}
variable "rds_shared_mysql_username" {}
variable "rds_shared_mysql_password" {}

variable "rds_shared_postgres_instance_type" {}
variable "rds_shared_postgres_db_size" {}
variable "rds_shared_postgres_db_name" {}
variable "rds_shared_postgres_db_engine" {}
variable "rds_shared_postgres_db_parameter_group_family" {}
variable "rds_shared_postgres_db_engine_version" {}
variable "rds_shared_postgres_username" {}
variable "rds_shared_postgres_password" {}
variable "rds_shared_postgres_apply_immediately" {}
variable "rds_shared_postgres_allow_major_version_upgrade" {}
