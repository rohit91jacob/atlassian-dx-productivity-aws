output "raw_bucket" {
  description = "S3 bucket holding the raw zone (raw/) and data-quality results (ops/)."
  value       = module.data_lake.raw_bucket
}

output "curated_bucket" {
  description = "S3 bucket holding the Iceberg warehouse (warehouse/) and dbt tables (dbt/)."
  value       = module.data_lake.curated_bucket
}

output "state_table" {
  description = "DynamoDB table with watermarks and processed-object markers."
  value       = module.state_table.name
}

output "extract_function" {
  description = "Extractor Lambda function name."
  value       = module.extract.function_name
}

output "curate_function" {
  description = "Curate Lambda function name."
  value       = module.curate.function_name
}

output "curate_repository_url" {
  description = "ECR repository for the curate image."
  value       = aws_ecr_repository.curate.repository_url
}

output "github_token_secret_arn" {
  description = "Secrets Manager secret to put the GitHub token in."
  value       = aws_secretsmanager_secret.github_token.arn
}

output "athena_workgroup" {
  description = "Athena workgroup for dbt (DX_ATHENA_WORKGROUP)."
  value       = module.analytics.workgroup_name
}

output "glue_curated_database" {
  description = "Glue database with the curated Iceberg tables (dbt var athena_curated_database)."
  value       = local.glue_curated_database
}

output "deploy_role_arn" {
  description = "Role GitHub Actions assumes through OIDC (set as the AWS_DEPLOY_ROLE_ARN repository variable)."
  value       = one(module.github_oidc[*].role_arn)
}
