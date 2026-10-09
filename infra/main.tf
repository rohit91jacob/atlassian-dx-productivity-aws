data "aws_caller_identity" "current" {}
data "aws_partition" "current" {}

locals {
  name       = "${var.project}-${var.environment}"
  account_id = data.aws_caller_identity.current.account_id
  partition  = data.aws_partition.current.partition

  tags = {
    Project     = "atlassian-dx-productivity-aws"
    Environment = var.environment
    Owner       = var.owner
    ManagedBy   = "terraform"
    DataClass   = "public-source-pseudonymised"
  }

  glue_curated_database   = "${var.project}_${var.environment}_curated"
  glue_analytics_database = "${var.project}_${var.environment}_analytics"

  common_env = {
    DX_BACKEND           = "aws"
    DX_RAW_BUCKET        = module.data_lake.raw_bucket
    DX_STATE_TABLE       = module.state_table.name
    DX_JIRA_PROJECTS     = join(",", var.jira_projects)
    DX_GITHUB_REPOS      = join(",", var.github_repos)
    DX_LOOKBACK_DAYS     = tostring(var.lookback_days)
    DX_LOG_FORMAT        = "json"
    DX_ICEBERG_CATALOG   = "glue"
    DX_ICEBERG_WAREHOUSE = "s3://${module.data_lake.curated_bucket}/warehouse"
    DX_ICEBERG_NAMESPACE = local.glue_curated_database
  }
}

module "kms" {
  source     = "./modules/kms"
  name       = local.name
  account_id = local.account_id
  partition  = local.partition
  region     = var.region
}

module "data_lake" {
  source              = "./modules/data_lake"
  name                = local.name
  account_id          = local.account_id
  kms_key_arn         = module.kms.key_arn
  raw_transition_days = var.raw_transition_days
}

module "state_table" {
  source      = "./modules/state_table"
  name        = "${local.name}-state"
  kms_key_arn = module.kms.key_arn
}

module "monitoring" {
  source             = "./modules/monitoring"
  name               = local.name
  kms_key_arn        = module.kms.key_arn
  alert_email        = var.alert_email
  monthly_budget_usd = var.monthly_budget_usd
  function_names     = [module.extract.function_name, module.curate.function_name]
}

# ------------------------------------------------------------------------- secrets
resource "aws_secretsmanager_secret" "github_token" {
  #checkov:skip=CKV2_AWS_57:A GitHub fine-grained token cannot be rotated by a Lambda; it is rotated in GitHub and updated here (runbook).
  name                    = "${local.name}/github-token"
  description             = "Read-only GitHub token for the extractor (public repos only). Value set out of band."
  kms_key_id              = module.kms.key_arn
  recovery_window_in_days = 7
}

# ------------------------------------------------------------------------- extract (zip Lambda)
module "extract" {
  source        = "./modules/lambda_function"
  name          = "${local.name}-extract"
  description   = "Incremental Jira + GitHub extractor -> S3 raw zone, watermarks in DynamoDB"
  package_type  = "Zip"
  zip_path      = var.extract_lambda_zip
  handler       = "dx_productivity.lambdas.handlers.extract_handler"
  timeout       = 900
  memory_size   = 512
  kms_key_arn   = module.kms.key_arn
  log_retention = var.log_retention_days
  environment = merge(local.common_env, {
    DX_GITHUB_TOKEN_SECRET_ARN = aws_secretsmanager_secret.github_token.arn
    DX_GITHUB_MAX_REQUESTS     = tostring(var.github_max_requests)
  })
  policy_json = data.aws_iam_policy_document.extract.json
}

data "aws_iam_policy_document" "extract" {
  statement {
    sid       = "WriteRawZone"
    actions   = ["s3:PutObject"]
    resources = ["${module.data_lake.raw_bucket_arn}/raw/*"]
  }
  statement {
    sid       = "Watermarks"
    actions   = ["dynamodb:GetItem", "dynamodb:PutItem"]
    resources = [module.state_table.arn]
  }
  statement {
    sid       = "GitHubToken"
    actions   = ["secretsmanager:GetSecretValue"]
    resources = [aws_secretsmanager_secret.github_token.arn]
  }
  statement {
    sid       = "UseDataKey"
    actions   = ["kms:Decrypt", "kms:GenerateDataKey"]
    resources = [module.kms.key_arn]
  }
}

# ------------------------------------------------------------------------- curate (image Lambda)
resource "aws_ecr_repository" "curate" {
  name                 = "${local.name}-curate"
  image_tag_mutability = "IMMUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  encryption_configuration {
    encryption_type = "KMS"
    kms_key         = module.kms.key_arn
  }
}

resource "aws_ecr_lifecycle_policy" "curate" {
  repository = aws_ecr_repository.curate.name
  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Keep the 10 most recent images"
      selection    = { tagStatus = "any", countType = "imageCountMoreThan", countNumber = 10 }
      action       = { type = "expire" }
    }]
  })
}

module "curate" {
  source        = "./modules/lambda_function"
  name          = "${local.name}-curate"
  description   = "Raw -> curated Iceberg (Glue catalog) with raw + curated data-quality gates"
  package_type  = "Image"
  image_uri     = "${aws_ecr_repository.curate.repository_url}:${var.curate_image_tag}"
  timeout       = 900
  memory_size   = 3008
  kms_key_arn   = module.kms.key_arn
  log_retention = var.log_retention_days
  environment   = local.common_env
  policy_json   = data.aws_iam_policy_document.curate.json
}

data "aws_iam_policy_document" "curate" {
  statement {
    sid       = "ReadRawZone"
    actions   = ["s3:GetObject"]
    resources = ["${module.data_lake.raw_bucket_arn}/raw/*"]
  }
  statement {
    sid       = "ListZones"
    actions   = ["s3:ListBucket"]
    resources = [module.data_lake.raw_bucket_arn, module.data_lake.curated_bucket_arn]
  }
  statement {
    sid       = "WriteDataQualityResults"
    actions   = ["s3:PutObject", "s3:GetObject"]
    resources = ["${module.data_lake.raw_bucket_arn}/ops/*"]
  }
  statement {
    sid       = "IcebergWarehouse"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["${module.data_lake.curated_bucket_arn}/warehouse/*"]
  }
  statement {
    sid       = "ProcessedMarkers"
    actions   = ["dynamodb:GetItem", "dynamodb:BatchGetItem", "dynamodb:BatchWriteItem"]
    resources = [module.state_table.arn]
  }
  statement {
    sid = "GlueCatalog"
    actions = [
      "glue:GetDatabase", "glue:CreateDatabase", "glue:GetTable", "glue:GetTables",
      "glue:CreateTable", "glue:UpdateTable",
    ]
    resources = [
      "arn:${local.partition}:glue:${var.region}:${local.account_id}:catalog",
      module.analytics.curated_database_arn,
      "arn:${local.partition}:glue:${var.region}:${local.account_id}:table/${local.glue_curated_database}/*",
    ]
  }
  statement {
    sid       = "UseDataKey"
    actions   = ["kms:Decrypt", "kms:GenerateDataKey"]
    resources = [module.kms.key_arn]
  }
}

# ------------------------------------------------------------------------- analytics
module "analytics" {
  source                 = "./modules/analytics"
  name                   = local.name
  curated_database       = local.glue_curated_database
  analytics_database     = local.glue_analytics_database
  results_bucket         = module.data_lake.athena_results_bucket
  kms_key_arn            = module.kms.key_arn
  bytes_scanned_cutoff   = var.athena_bytes_scanned_cutoff
  curated_bucket         = module.data_lake.curated_bucket
  enable_catalog_encrypt = true
}

# ------------------------------------------------------------------------- schedules
module "scheduler" {
  source      = "./modules/scheduler"
  name        = local.name
  timezone    = var.schedule_timezone
  kms_key_arn = module.kms.key_arn
  schedules = {
    jira = {
      expression = var.extract_schedule
      target_arn = module.extract.function_arn
      input      = jsonencode({ sources = ["jira"] })
    }
    github = {
      expression = var.extract_schedule
      target_arn = module.extract.function_arn
      input      = jsonencode({ sources = ["github"] })
    }
    curate = {
      expression = var.curate_schedule
      target_arn = module.curate.function_arn
      input      = jsonencode({})
    }
  }
}

# ------------------------------------------------------------------------- CI/CD (GitHub OIDC)
module "github_oidc" {
  count                = var.github_oidc_repository == "" ? 0 : 1
  source               = "./modules/github_oidc"
  name                 = local.name
  repository           = var.github_oidc_repository
  create_provider      = var.create_github_oidc_provider
  account_id           = local.account_id
  partition            = local.partition
  region               = var.region
  function_arns        = [module.extract.function_arn, module.curate.function_arn]
  ecr_repository_arn   = aws_ecr_repository.curate.arn
  athena_workgroup_arn = module.analytics.workgroup_arn
  bucket_arns          = [module.data_lake.curated_bucket_arn, module.data_lake.athena_results_bucket_arn]
  glue_database_names  = [local.glue_curated_database, local.glue_analytics_database]
  kms_key_arn          = module.kms.key_arn
}
