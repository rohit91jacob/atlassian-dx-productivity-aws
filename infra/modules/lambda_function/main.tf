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
  description = "Function name."
  type        = string
}

variable "description" {
  description = "Function description."
  type        = string
}

variable "package_type" {
  description = "Zip or Image."
  type        = string

  validation {
    condition     = contains(["Zip", "Image"], var.package_type)
    error_message = "package_type must be Zip or Image."
  }
}

variable "zip_path" {
  description = "Deployment package path (Zip only)."
  type        = string
  default     = null
}

variable "handler" {
  description = "Handler (Zip only)."
  type        = string
  default     = null
}

variable "image_uri" {
  description = "Container image URI (Image only)."
  type        = string
  default     = null
}

variable "timeout" {
  description = "Timeout in seconds."
  type        = number
}

variable "memory_size" {
  description = "Memory in MB."
  type        = number
}

variable "environment" {
  description = "Environment variables."
  type        = map(string)
}

variable "kms_key_arn" {
  description = "KMS key for environment variables, logs and the DLQ."
  type        = string
}

variable "log_retention" {
  description = "Log retention in days."
  type        = number
}

variable "policy_json" {
  description = "Least-privilege inline policy for the function's data access."
  type        = string
}

data "aws_iam_policy_document" "assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "this" {
  name               = var.name
  assume_role_policy = data.aws_iam_policy_document.assume.json
}

resource "aws_iam_role_policy" "data" {
  name   = "data-access"
  role   = aws_iam_role.this.id
  policy = var.policy_json
}

data "aws_iam_policy_document" "runtime" {
  statement {
    sid       = "Logs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.this.arn}:*"]
  }
  statement {
    sid       = "DeadLetterQueue"
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.dlq.arn]
  }
}

resource "aws_iam_role_policy" "runtime" {
  name   = "runtime"
  role   = aws_iam_role.this.id
  policy = data.aws_iam_policy_document.runtime.json
}

# X-Ray's PutTraceSegments has no resource-level permissions; the AWS managed policy is the
# narrowest option.
resource "aws_iam_role_policy_attachment" "xray" {
  role       = aws_iam_role.this.name
  policy_arn = "arn:aws:iam::aws:policy/AWSXRayDaemonWriteAccess"
}

resource "aws_cloudwatch_log_group" "this" {
  name              = "/aws/lambda/${var.name}"
  retention_in_days = var.log_retention
  kms_key_id        = var.kms_key_arn
}

resource "aws_sqs_queue" "dlq" {
  name                      = "${var.name}-dlq"
  message_retention_seconds = 1209600
  kms_master_key_id         = var.kms_key_arn
}

resource "aws_lambda_function" "this" {
  #checkov:skip=CKV_AWS_117:The functions call public internet APIs (Jira, GitHub); a VPC would need a NAT gateway (~32 USD/month), above this project's whole budget. No private resources are reachable.
  #checkov:skip=CKV_AWS_272:Code signing applies to zip packages built in CI from a reviewed commit; signing profiles are left for a production account.
  function_name = var.name
  description   = var.description
  role          = aws_iam_role.this.arn
  package_type  = var.package_type
  filename      = var.package_type == "Zip" ? var.zip_path : null
  # try(): validate/plan must work before the package has been built.
  source_code_hash = var.package_type == "Zip" ? try(filebase64sha256(var.zip_path), null) : null
  handler          = var.package_type == "Zip" ? var.handler : null
  runtime          = var.package_type == "Zip" ? "python3.12" : null
  architectures    = ["arm64"]
  image_uri        = var.package_type == "Image" ? var.image_uri : null
  timeout          = var.timeout
  memory_size      = var.memory_size
  kms_key_arn      = var.kms_key_arn
  # One run at a time per function: no overlapping extractions racing on a watermark, and a hard
  # ceiling on spend if a schedule misfires.
  reserved_concurrent_executions = 1

  environment {
    variables = var.environment
  }

  tracing_config {
    mode = "Active"
  }

  dead_letter_config {
    target_arn = aws_sqs_queue.dlq.arn
  }

  logging_config {
    log_format = "Text"
    log_group  = aws_cloudwatch_log_group.this.name
  }

  # Code is shipped by .github/workflows/deploy.yml (update-function-code); Terraform owns the
  # configuration, so later code updates are not drift.
  lifecycle {
    ignore_changes = [image_uri, source_code_hash]
  }
}

resource "aws_lambda_function_event_invoke_config" "this" {
  function_name                = aws_lambda_function.this.function_name
  maximum_retry_attempts       = 1
  maximum_event_age_in_seconds = 3600
}

output "function_name" {
  description = "Function name."
  value       = aws_lambda_function.this.function_name
}

output "function_arn" {
  description = "Function ARN."
  value       = aws_lambda_function.this.arn
}

output "dlq_arn" {
  description = "Dead-letter queue ARN."
  value       = aws_sqs_queue.dlq.arn
}
