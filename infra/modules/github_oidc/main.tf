terraform {
  required_version = ">= 1.9.0, < 2.0.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.68"
    }
  }
}

variable "name" {
  description = "Resource-name prefix."
  type        = string
}

variable "repository" {
  description = "owner/repo allowed to assume the role."
  type        = string
}

variable "create_provider" {
  description = "Create the GitHub OIDC provider (one per account)."
  type        = bool
}

variable "account_id" {
  description = "AWS account id."
  type        = string
}

variable "partition" {
  description = "AWS partition."
  type        = string
}

variable "region" {
  description = "AWS region."
  type        = string
}

variable "function_arns" {
  description = "Lambda functions the deploy job may update and invoke."
  type        = list(string)
}

variable "ecr_repository_arn" {
  description = "ECR repository the deploy job pushes the curate image to."
  type        = string
}

variable "athena_workgroup_arn" {
  description = "Athena workgroup dbt runs in."
  type        = string
}

variable "bucket_arns" {
  description = "Buckets dbt-athena reads and writes (curated, Athena results)."
  type        = list(string)
}

variable "glue_database_names" {
  description = "Glue databases dbt reads (curated) and writes (analytics, dx_*)."
  type        = list(string)
}

variable "kms_key_arn" {
  description = "Data key."
  type        = string
}

resource "aws_iam_openid_connect_provider" "github" {
  count          = var.create_provider ? 1 : 0
  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]
}

locals {
  provider_arn = var.create_provider ? aws_iam_openid_connect_provider.github[0].arn : "arn:${var.partition}:iam::${var.account_id}:oidc-provider/token.actions.githubusercontent.com"
}

# Only workflow runs on main of this one repository (or its `aws` environment) can assume it.
data "aws_iam_policy_document" "trust" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]
    principals {
      type        = "Federated"
      identifiers = [local.provider_arn]
    }
    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }
    condition {
      test     = "StringLike"
      variable = "token.actions.githubusercontent.com:sub"
      values = [
        "repo:${var.repository}:ref:refs/heads/main",
        "repo:${var.repository}:environment:aws",
      ]
    }
  }
}

resource "aws_iam_role" "deploy" {
  name                 = "${var.name}-github-deploy"
  assume_role_policy   = data.aws_iam_policy_document.trust.json
  max_session_duration = 3600
}

# What the deploy workflow does: ship new Lambda code, push the curate image, run dbt on Athena.
# Infrastructure changes (terraform apply) are made by an operator, not by this role.
data "aws_iam_policy_document" "deploy" {
  statement {
    sid       = "ShipLambdaCode"
    actions   = ["lambda:UpdateFunctionCode", "lambda:GetFunction", "lambda:InvokeFunction"]
    resources = var.function_arns
  }
  statement {
    sid = "PushCurateImage"
    actions = [
      "ecr:BatchCheckLayerAvailability", "ecr:CompleteLayerUpload", "ecr:InitiateLayerUpload",
      "ecr:PutImage", "ecr:UploadLayerPart", "ecr:BatchGetImage", "ecr:DescribeImages",
    ]
    resources = [var.ecr_repository_arn]
  }
  statement {
    sid       = "EcrLogin"
    actions   = ["ecr:GetAuthorizationToken"]
    resources = ["*"] # GetAuthorizationToken has no resource-level permissions
  }
  statement {
    sid = "RunAthena"
    actions = [
      "athena:StartQueryExecution", "athena:GetQueryExecution", "athena:GetQueryResults",
      "athena:StopQueryExecution", "athena:GetWorkGroup",
    ]
    resources = [var.athena_workgroup_arn]
  }
  statement {
    sid = "GlueForDbt"
    actions = [
      "glue:GetDatabase", "glue:GetDatabases", "glue:CreateDatabase", "glue:GetTable", "glue:GetTables",
      "glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable", "glue:GetPartitions", "glue:BatchGetPartition",
    ]
    resources = concat(
      ["arn:${var.partition}:glue:${var.region}:${var.account_id}:catalog"],
      [for db in var.glue_database_names : "arn:${var.partition}:glue:${var.region}:${var.account_id}:database/${db}"],
      [for db in var.glue_database_names : "arn:${var.partition}:glue:${var.region}:${var.account_id}:table/${db}/*"],
      ["arn:${var.partition}:glue:${var.region}:${var.account_id}:database/dx_*",
      "arn:${var.partition}:glue:${var.region}:${var.account_id}:table/dx_*/*"],
    )
  }
  statement {
    sid       = "DataForDbt"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket", "s3:GetBucketLocation"]
    resources = concat(var.bucket_arns, [for b in var.bucket_arns : "${b}/*"])
  }
  statement {
    sid       = "DataKey"
    actions   = ["kms:Decrypt", "kms:GenerateDataKey"]
    resources = [var.kms_key_arn]
  }
}

resource "aws_iam_role_policy" "deploy" {
  name   = "deploy"
  role   = aws_iam_role.deploy.id
  policy = data.aws_iam_policy_document.deploy.json
}

output "role_arn" {
  description = "Deploy role ARN."
  value       = aws_iam_role.deploy.arn
}
