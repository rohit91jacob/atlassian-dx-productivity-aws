variable "region" {
  description = "AWS region for every resource."
  type        = string
  default     = "eu-west-1"
}

variable "environment" {
  description = "Deployment environment name, used in resource names and tags."
  type        = string
  default     = "dev"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{1,15}$", var.environment))
    error_message = "environment must be 2-16 lowercase letters, digits or hyphens."
  }
}

variable "project" {
  description = "Short project name used as a resource-name prefix."
  type        = string
  default     = "dx"
}

variable "owner" {
  description = "Owner tag value (a team or person)."
  type        = string
  default     = "rohit91jacob"
}

variable "jira_projects" {
  description = "Apache Jira project keys to extract."
  type        = list(string)
  default     = ["KAFKA", "SPARK", "FLINK"]
}

variable "github_repos" {
  description = "GitHub repos as owner/repo:default-branch:JIRA_PROJECT."
  type        = list(string)
  default     = ["apache/kafka:trunk:KAFKA", "apache/spark:master:SPARK", "apache/flink:master:FLINK"]
}

variable "extract_schedule" {
  description = "EventBridge Scheduler expression for the extractors (daily: the results site is daily)."
  type        = string
  default     = "cron(15 4 * * ? *)"
}

variable "curate_schedule" {
  description = "Schedule for the curate Lambda; runs after the extractors have finished."
  type        = string
  default     = "cron(45 4 * * ? *)"
}

variable "schedule_timezone" {
  description = "Time zone for the schedules."
  type        = string
  default     = "UTC"
}

variable "extract_lambda_zip" {
  description = "Path to the extractor deployment package built by `make lambda-zip`."
  type        = string
  default     = "../dist/extract_lambda.zip"
}

variable "curate_image_tag" {
  description = "Tag of the curate container image in the ECR repository (pushed by the deploy workflow)."
  type        = string
  default     = "latest-release"
}

variable "github_max_requests" {
  description = "Per-run GitHub request budget (the Lambda uses a personal token from Secrets Manager)."
  type        = number
  default     = 3000
}

variable "lookback_days" {
  description = "History window for the first run (later runs are incremental)."
  type        = number
  default     = 90
}

variable "log_retention_days" {
  description = "CloudWatch Logs retention."
  type        = number
  default     = 365
}

variable "raw_transition_days" {
  description = "Days before raw objects move to STANDARD_IA."
  type        = number
  default     = 30
}

variable "athena_bytes_scanned_cutoff" {
  description = "Per-query scan limit for the Athena workgroup (cost guardrail), in bytes."
  type        = number
  default     = 1073741824
}

variable "monthly_budget_usd" {
  description = "Monthly cost budget; alerts at 80% actual and 100% forecast."
  type        = number
  default     = 10
}

variable "alert_email" {
  description = "E-mail for budget and alarm notifications (empty = no e-mail subscription)."
  type        = string
  default     = ""
}

variable "github_oidc_repository" {
  description = "owner/repo allowed to assume the deploy role through GitHub OIDC (empty = no role)."
  type        = string
  default     = "rohit91jacob/atlassian-dx-productivity-aws"
}

variable "create_github_oidc_provider" {
  description = "Create the account's GitHub OIDC provider (false if it already exists)."
  type        = bool
  default     = true
}
