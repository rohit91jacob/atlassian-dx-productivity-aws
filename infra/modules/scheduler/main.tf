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

variable "timezone" {
  description = "Schedule time zone."
  type        = string
}

variable "kms_key_arn" {
  description = "KMS key for schedule payloads and the scheduler DLQ."
  type        = string
}

variable "schedules" {
  description = "Schedules keyed by name: cron/rate expression, Lambda target ARN and JSON input."
  type = map(object({
    expression = string
    target_arn = string
    input      = string
  }))
}

resource "aws_scheduler_schedule_group" "this" {
  name = var.name
}

resource "aws_iam_role" "this" {
  name = "${var.name}-scheduler"
  # Trust only schedules in this group (the schedules reference the role, so the trust policy
  # uses the group's ARN pattern rather than each schedule ARN).
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "scheduler.amazonaws.com" }
      Action    = "sts:AssumeRole"
      Condition = {
        ArnLike = { "aws:SourceArn" = "${replace(aws_scheduler_schedule_group.this.arn, ":schedule-group/", ":schedule/")}/*" }
      }
    }]
  })
}

data "aws_iam_policy_document" "invoke" {
  statement {
    sid       = "InvokeTargets"
    actions   = ["lambda:InvokeFunction"]
    resources = distinct([for s in var.schedules : s.target_arn])
  }
  statement {
    sid       = "DeadLetter"
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.dlq.arn]
  }
  statement {
    sid       = "EncryptedPayloads"
    actions   = ["kms:Decrypt", "kms:GenerateDataKey"]
    resources = [var.kms_key_arn]
  }
}

resource "aws_iam_role_policy" "this" {
  name   = "invoke-targets"
  role   = aws_iam_role.this.id
  policy = data.aws_iam_policy_document.invoke.json
}

resource "aws_sqs_queue" "dlq" {
  name                      = "${var.name}-scheduler-dlq"
  message_retention_seconds = 1209600
  kms_master_key_id         = var.kms_key_arn
}

resource "aws_scheduler_schedule" "this" {
  for_each                     = var.schedules
  name                         = "${var.name}-${each.key}"
  group_name                   = aws_scheduler_schedule_group.this.name
  schedule_expression          = each.value.expression
  schedule_expression_timezone = var.timezone
  kms_key_arn                  = var.kms_key_arn

  flexible_time_window {
    mode                      = "FLEXIBLE"
    maximum_window_in_minutes = 10
  }

  target {
    arn      = each.value.target_arn
    role_arn = aws_iam_role.this.arn
    input    = each.value.input

    retry_policy {
      maximum_retry_attempts       = 2
      maximum_event_age_in_seconds = 3600
    }

    dead_letter_config {
      arn = aws_sqs_queue.dlq.arn
    }
  }
}

output "schedule_arns" {
  description = "Schedule ARNs by name."
  value       = { for k, s in aws_scheduler_schedule.this : k => s.arn }
}
