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

variable "curated_database" {
  description = "Glue database for the curated Iceberg tables (created by pyiceberg's Glue catalog)."
  type        = string
}

variable "analytics_database" {
  description = "Glue database prefix for dbt output on Athena."
  type        = string
}

variable "curated_bucket" {
  description = "Curated bucket name (Iceberg warehouse location)."
  type        = string
}

variable "results_bucket" {
  description = "Athena results bucket name."
  type        = string
}

variable "kms_key_arn" {
  description = "KMS key for query results and the Data Catalog."
  type        = string
}

variable "bytes_scanned_cutoff" {
  description = "Per-query data-scanned limit (bytes): the main Athena cost guardrail."
  type        = number
}

variable "enable_catalog_encrypt" {
  description = "Manage the account-level Glue Data Catalog encryption settings."
  type        = bool
}

resource "aws_glue_catalog_database" "curated" {
  name         = var.curated_database
  description  = "Curated Iceberg tables (pyiceberg writes, Athena reads)"
  location_uri = "s3://${var.curated_bucket}/warehouse/${var.curated_database}.db"
}

resource "aws_glue_catalog_database" "analytics" {
  name        = var.analytics_database
  description = "dbt staging/intermediate/marts models built on Athena"
}

resource "aws_glue_data_catalog_encryption_settings" "this" {
  count = var.enable_catalog_encrypt ? 1 : 0

  data_catalog_encryption_settings {
    connection_password_encryption {
      return_connection_password_encrypted = true
      aws_kms_key_id                       = var.kms_key_arn
    }
    encryption_at_rest {
      catalog_encryption_mode = "SSE-KMS"
      sse_aws_kms_key_id      = var.kms_key_arn
    }
  }
}

resource "aws_athena_workgroup" "this" {
  name          = "${var.name}-analytics"
  description   = "dbt-athena and ad-hoc queries over the DX marts"
  force_destroy = false

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = var.bytes_scanned_cutoff

    engine_version {
      selected_engine_version = "Athena engine version 3"
    }

    result_configuration {
      output_location = "s3://${var.results_bucket}/results/"
      encryption_configuration {
        encryption_option = "SSE_KMS"
        kms_key_arn       = var.kms_key_arn
      }
    }
  }
}

output "workgroup_name" {
  description = "Athena workgroup name."
  value       = aws_athena_workgroup.this.name
}

output "workgroup_arn" {
  description = "Athena workgroup ARN."
  value       = aws_athena_workgroup.this.arn
}

output "curated_database_arn" {
  description = "ARN of the curated Glue database."
  value       = aws_glue_catalog_database.curated.arn
}
