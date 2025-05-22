module "rds_internal" {
  source            = "./rds_module"
  stack_description = var.stack_description
  rds_subnet_group  = data.terraform_remote_state.vpc.outputs.rds_subnet_group

  /* TODO: Use database instance type from config */
  rds_security_groups = [data.terraform_remote_state.vpc.outputs.rds_postgres_security_group]

  rds_instance_type             = var.rds_internal_instance_type
  rds_db_size                   = var.rds_internal_db_size
  rds_db_name                   = var.rds_internal_db_name
  rds_final_snapshot_identifier = "${var.base_stack}-${replace(var.rds_internal_db_name, "_", "-")}"
  rds_db_engine                 = var.rds_internal_db_engine
  rds_db_engine_version         = var.rds_internal_db_engine_version
  rds_multi_az                  = var.rds_internal_multi_az
  rds_username                  = var.rds_internal_username
  rds_password                  = var.rds_internal_password
  rds_parameter_group_family    = var.rds_internal_db_parameter_group_family
  apply_immediately             = var.rds_internal_apply_immediately
  allow_major_version_upgrade   = var.rds_internal_allow_major_version_upgrade
  rds_force_ssl                 = var.rds_force_ssl

  rds_add_pgaudit_to_shared_preload_libraries = var.rds_add_pgaudit_to_shared_preload_libraries
  rds_add_pgaudit_log_parameter               = var.rds_add_pgaudit_log_parameter
  rds_shared_preload_libraries                = var.rds_shared_preload_libraries
  rds_pgaudit_log_values                      = var.rds_pgaudit_log_values

}
