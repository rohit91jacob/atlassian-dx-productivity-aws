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

# One customer-managed key encrypts every data store of the pipeline (S3, DynamoDB, SQS, SNS,
# CloudWatch Logs, Secrets Manager, ECR, Athena results, Glue catalog). The key policy keeps
# account administration with IAM and lets the regional services that write encrypted logs and
# notifications use it, scoped to this account.
data "aws_iam_policy_document" "key" {
  # In a key policy "*" means "this key" (the policy is attached to exactly one key), and the
  # account-root statement is the AWS-documented default that delegates control to IAM.
  #checkov:skip=CKV_AWS_109:Key policy: resource "*" is this key; root delegation is the AWS default.
  #checkov:skip=CKV_AWS_111:Key policy: resource "*" is this key; writes are limited to the account and scoped services.
  #checkov:skip=CKV_AWS_356:Key policy: resource "*" is this key, not every resource.
  statement {
    sid       = "AccountAdministration"
    actions   = ["kms:*"]
    resources = ["*"]
    principals {
      type        = "AWS"
      identifiers = ["arn:${var.partition}:iam::${var.account_id}:root"]
    }
  }

  statement {
    sid       = "CloudWatchLogs"
    actions   = ["kms:Encrypt", "kms:Decrypt", "kms:ReEncrypt*", "kms:GenerateDataKey*", "kms:DescribeKey"]
    resources = ["*"]
    principals {
      type        = "Service"
      identifiers = ["logs.${var.region}.amazonaws.com"]
    }
    condition {
      test     = "ArnLike"
      variable = "kms:EncryptionContext:aws:logs:arn"
      values   = ["arn:${var.partition}:logs:${var.region}:${var.account_id}:log-group:*"]
    }
  }

  statement {
    sid       = "ServiceNotifications"
    actions   = ["kms:Decrypt", "kms:GenerateDataKey*"]
    resources = ["*"]
    principals {
      type        = "Service"
      identifiers = ["cloudwatch.amazonaws.com", "budgets.amazonaws.com", "scheduler.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [var.account_id]
    }
  }
}

resource "aws_kms_key" "this" {
  description             = "${var.name}: data at rest for the DX analytics pipeline"
  enable_key_rotation     = true
  rotation_period_in_days = 365
  deletion_window_in_days = 30
  policy                  = data.aws_iam_policy_document.key.json
}

resource "aws_kms_alias" "this" {
  name          = "alias/${var.name}"
  target_key_id = aws_kms_key.this.key_id
}

output "key_arn" {
  description = "ARN of the customer-managed key."
  value       = aws_kms_key.this.arn
}
