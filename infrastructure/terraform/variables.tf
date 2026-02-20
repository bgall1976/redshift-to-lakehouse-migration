variable "project_name" {
  description = "Project name used for resource naming"
  type        = string
  default     = "fintechco"
}

variable "environment" {
  description = "Environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "us-east-1"
}

variable "kms_key_arn" {
  description = "KMS key ARN for S3 encryption"
  type        = string
}

variable "databricks_account_id" {
  description = "Databricks account ID"
  type        = string
}

variable "databricks_workspace_url" {
  description = "Databricks workspace URL"
  type        = string
  default     = ""
}
