module "rds_shared_postgres" {
    source = "git::https://github.com/18F/cg-provision//terraform/modules/rds"
    stack_description = "${var.stack_description}"
    rds_subnet_group = "${data.terraform_remote_state.vpc.rds_subnet_group}"
    /* TODO: Use database instance type from config */
    rds_security_groups = ["${data.terraform_remote_state.vpc.rds_postgres_security_group}"]

    rds_instance_type = "${var.rds_shared_postgres_instance_type}"
    rds_db_size = "${var.rds_shared_postgres_db_size}"
    rds_db_name = "${var.rds_shared_postgres_db_name}"
    rds_db_engine = "${var.rds_shared_postgres_db_engine}"
    rds_db_engine_version = "${var.rds_shared_postgres_db_engine_version}"
    rds_username = "${var.rds_shared_postgres_username}"
    rds_password = "${var.rds_shared_postgres_password}"
    rds_parameter_group_family = "postgres9.4"
}
