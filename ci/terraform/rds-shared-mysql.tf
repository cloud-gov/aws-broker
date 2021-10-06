module "rds_shared_mysql" {
  source            = "./rds_module"
  stack_description = var.stack_description
  rds_subnet_group  = data.terraform_remote_state.vpc.outputs.rds_subnet_group

  /* TODO: Use database instance type from config */
  rds_security_groups = [data.terraform_remote_state.vpc.outputs.rds_mysql_security_group]

  rds_instance_type             = var.rds_shared_mysql_instance_type
  rds_db_size                   = var.rds_shared_mysql_db_size
  rds_db_name                   = var.rds_shared_mysql_db_name
  rds_final_snapshot_identifier = "${var.base_stack}-${replace(var.rds_shared_mysql_db_name, "_", "-")}"
  rds_db_engine                 = var.rds_shared_mysql_db_engine
  rds_db_engine_version         = var.rds_shared_mysql_db_engine_version
  rds_multi_az                  = var.rds_shared_mysql_multi_az
  rds_username                  = var.rds_shared_mysql_username
  rds_password                  = var.rds_shared_mysql_password
  rds_parameter_group_family    = var.rds_shared_mysql_db_parameter_group_family
  apply_immediately             = var.rds_shared_mysql_apply_immediately
  allow_major_version_upgrade   = var.rds_shared_mysql_allow_major_version_upgrade
}