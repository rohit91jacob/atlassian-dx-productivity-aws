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
  description = "Table name."
  type        = string
}

variable "kms_key_arn" {
  description = "KMS key for server-side encryption."
  type        = string
}

# Single-table design, hash key "pk": watermark#<source>#<scope>#<entity>,
# processed#<raw object key>. Optimistic locking uses the numeric "version" attribute in
# conditional writes. On-demand capacity: a few hundred requests per day.
resource "aws_dynamodb_table" "this" {
  name                        = var.name
  billing_mode                = "PAY_PER_REQUEST"
  hash_key                    = "pk"
  deletion_protection_enabled = true

  attribute {
    name = "pk"
    type = "S"
  }

  point_in_time_recovery {
    enabled = true
  }

  server_side_encryption {
    enabled     = true
    kms_key_arn = var.kms_key_arn
  }
}

output "name" {
  description = "Table name."
  value       = aws_dynamodb_table.this.name
}

output "arn" {
  description = "Table ARN."
  value       = aws_dynamodb_table.this.arn
}
