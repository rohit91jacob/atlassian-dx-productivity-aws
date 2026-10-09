terraform {
  required_version = ">= 1.9.0, < 2.0.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.68"
    }
  }

  # Remote state is configured at init time by the deploy workflow / operator:
  #   terraform init -backend-config=backend.hcl
  # (S3 bucket + key + use_lockfile = true). CI validates with -backend=false.
  backend "s3" {}
}

provider "aws" {
  region = var.region

  default_tags {
    tags = local.tags
  }
}
