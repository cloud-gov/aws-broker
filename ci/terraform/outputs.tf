output "rds_internal_rds_host" {
  value = module.rds_internal.rds_host
}

output "rds_internal_rds_port" {
  value = module.rds_internal.rds_port
}

output "rds_shared_postgres_rds_host" {
  value = module.rds_shared_postgres.rds_host
}

output "rds_shared_postgres_rds_port" {
  value = module.rds_shared_postgres.rds_port
}

output "rds_shared_mysql_rds_host" {
  value = module.rds_shared_mysql.rds_host
}

output "rds_shared_mysql_rds_port" {
  value = module.rds_shared_mysql.rds_port
}