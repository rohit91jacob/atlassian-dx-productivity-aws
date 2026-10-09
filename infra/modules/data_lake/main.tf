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
  description = "AWS account id (bucket names are globally unique, so they include it)."
  type        = string
}

variable "kms_key_arn" {
  description = "KMS key for SSE-KMS."
  type        = string
}

variable "raw_transition_days" {
  description = "Days before raw objects move to STANDARD_IA."
  type        = number
}

locals {
  buckets = {
    raw            = "${var.name}-raw-${var.account_id}"
    curated        = "${var.name}-curated-${var.account_id}"
    athena_results = "${var.name}-athena-results-${var.account_id}"
  }
}

# ---------------------------------------------------------------- access logs
# S3 server access logging only delivers to buckets encrypted with SSE-S3, so this one bucket
# cannot use the KMS key.
resource "aws_s3_bucket" "logs" {
  #checkov:skip=CKV_AWS_145:S3 server access log delivery does not support SSE-KMS target buckets.
  #checkov:skip=CKV_AWS_18:This is the access-log target bucket; logging it into itself would loop.
  #checkov:skip=CKV_AWS_144:Single-region portfolio deployment; raw data can be re-extracted from the public APIs.
  bucket = "${var.name}-access-logs-${var.account_id}"
}

resource "aws_s3_bucket_server_side_encryption_configuration" "logs" {
  bucket = aws_s3_bucket.logs.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "logs" {
  bucket                  = aws_s3_bucket.logs.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "logs" {
  bucket = aws_s3_bucket.logs.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "logs" {
  bucket = aws_s3_bucket.logs.id
  rule {
    id     = "expire-access-logs"
    status = "Enabled"
    filter {}
    expiration {
      days = 365
    }
    noncurrent_version_expiration {
      noncurrent_days = 30
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

resource "aws_s3_bucket_notification" "logs" {
  bucket      = aws_s3_bucket.logs.id
  eventbridge = true
}

data "aws_iam_policy_document" "logs" {
  statement {
    sid       = "S3ServerAccessLogs"
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.logs.arn}/*"]
    principals {
      type        = "Service"
      identifiers = ["logging.s3.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [var.account_id]
    }
  }
  statement {
    sid       = "DenyInsecureTransport"
    effect    = "Deny"
    actions   = ["s3:*"]
    resources = [aws_s3_bucket.logs.arn, "${aws_s3_bucket.logs.arn}/*"]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "logs" {
  bucket = aws_s3_bucket.logs.id
  policy = data.aws_iam_policy_document.logs.json
}

# ---------------------------------------------------------------- data buckets
resource "aws_s3_bucket" "this" {
  #checkov:skip=CKV_AWS_144:Single-region portfolio deployment; raw data can be re-extracted from the public APIs and curated data rebuilt from raw.
  for_each = local.buckets
  bucket   = each.value
}

resource "aws_s3_bucket_ownership_controls" "this" {
  for_each = local.buckets
  bucket   = aws_s3_bucket.this[each.key].id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  for_each = local.buckets
  bucket   = aws_s3_bucket.this[each.key].id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = var.kms_key_arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "this" {
  for_each                = local.buckets
  bucket                  = aws_s3_bucket.this[each.key].id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "this" {
  for_each = local.buckets
  bucket   = aws_s3_bucket.this[each.key].id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_logging" "this" {
  for_each      = local.buckets
  bucket        = aws_s3_bucket.this[each.key].id
  target_bucket = aws_s3_bucket.logs.id
  target_prefix = "${each.key}/"
}

# Raw is the system of record: it ages to cheaper storage but is kept. Athena results are
# scratch. Iceberg manages its own snapshots, so only superseded object versions expire.
resource "aws_s3_bucket_lifecycle_configuration" "this" {
  for_each = local.buckets
  bucket   = aws_s3_bucket.this[each.key].id

  rule {
    id     = "storage-class-and-versions"
    status = "Enabled"
    filter {}

    dynamic "transition" {
      for_each = each.key == "raw" ? [1] : []
      content {
        days          = var.raw_transition_days
        storage_class = "STANDARD_IA"
      }
    }

    dynamic "expiration" {
      for_each = each.key == "athena_results" ? [1] : []
      content {
        days = 7
      }
    }

    noncurrent_version_expiration {
      noncurrent_days = 30
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

# EventBridge notifications: object-created events for the raw zone can trigger curation
# event-driven instead of on a schedule.
resource "aws_s3_bucket_notification" "this" {
  for_each    = local.buckets
  bucket      = aws_s3_bucket.this[each.key].id
  eventbridge = true
}

data "aws_iam_policy_document" "tls_only" {
  for_each = local.buckets
  statement {
    sid       = "DenyInsecureTransport"
    effect    = "Deny"
    actions   = ["s3:*"]
    resources = [aws_s3_bucket.this[each.key].arn, "${aws_s3_bucket.this[each.key].arn}/*"]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "this" {
  for_each = local.buckets
  bucket   = aws_s3_bucket.this[each.key].id
  policy   = data.aws_iam_policy_document.tls_only[each.key].json
}

output "raw_bucket" {
  description = "Raw-zone bucket name."
  value       = aws_s3_bucket.this["raw"].bucket
}

output "raw_bucket_arn" {
  description = "Raw-zone bucket ARN."
  value       = aws_s3_bucket.this["raw"].arn
}

output "curated_bucket" {
  description = "Curated (Iceberg warehouse) bucket name."
  value       = aws_s3_bucket.this["curated"].bucket
}

output "curated_bucket_arn" {
  description = "Curated bucket ARN."
  value       = aws_s3_bucket.this["curated"].arn
}

output "athena_results_bucket" {
  description = "Athena query results bucket name."
  value       = aws_s3_bucket.this["athena_results"].bucket
}

output "athena_results_bucket_arn" {
  description = "Athena query results bucket ARN."
  value       = aws_s3_bucket.this["athena_results"].arn
}
