output "rds_shared_postgres_rds_host" {
  value = module.rds_shared_postgres.aws_db_instance.rds_database.address
}
