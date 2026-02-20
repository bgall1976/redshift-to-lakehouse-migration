# ============================================================
# S3 Bucket Configuration for Lakehouse Storage Layers
# ============================================================
# Implements tiered storage with encryption, versioning, and
# lifecycle policies aligned to GLBA compliance requirements.
# ============================================================

locals {
  layers = ["bronze", "silver", "gold", "sandbox"]
}

resource "aws_s3_bucket" "datalake" {
  for_each = toset(local.layers)

  bucket = "${var.project_name}-datalake-${var.environment}-${each.value}"

  tags = {
    Project     = var.project_name
    Environment = var.environment
    Layer       = each.value
    ManagedBy   = "terraform"
    Compliance  = "GLBA"
  }
}

# Encryption at rest (GLBA requirement)
resource "aws_s3_bucket_server_side_encryption_configuration" "datalake" {
  for_each = toset(local.layers)

  bucket = aws_s3_bucket.datalake[each.value].id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = var.kms_key_arn
    }
    bucket_key_enabled = true
  }
}

# Versioning for disaster recovery
resource "aws_s3_bucket_versioning" "datalake" {
  for_each = toset(local.layers)

  bucket = aws_s3_bucket.datalake[each.value].id

  versioning_configuration {
    status = "Enabled"
  }
}

# Block public access (GLBA requirement)
resource "aws_s3_bucket_public_access_block" "datalake" {
  for_each = toset(local.layers)

  bucket = aws_s3_bucket.datalake[each.value].id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Lifecycle policies for cost optimization
resource "aws_s3_bucket_lifecycle_configuration" "bronze" {
  bucket = aws_s3_bucket.datalake["bronze"].id

  rule {
    id     = "archive-old-bronze-data"
    status = "Enabled"

    transition {
      days          = 90
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 365
      storage_class = "GLACIER"
    }

    # GLBA retention: keep for 7 years minimum
    expiration {
      days = 2555  # ~7 years
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "silver" {
  bucket = aws_s3_bucket.datalake["silver"].id

  rule {
    id     = "archive-old-silver-data"
    status = "Enabled"

    transition {
      days          = 180
      storage_class = "STANDARD_IA"
    }
  }
}

# Enforce TLS in transit (GLBA requirement)
resource "aws_s3_bucket_policy" "enforce_tls" {
  for_each = toset(local.layers)

  bucket = aws_s3_bucket.datalake[each.value].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "EnforceTLS"
        Effect    = "Deny"
        Principal = "*"
        Action    = "s3:*"
        Resource = [
          aws_s3_bucket.datalake[each.value].arn,
          "${aws_s3_bucket.datalake[each.value].arn}/*"
        ]
        Condition = {
          Bool = {
            "aws:SecureTransport" = "false"
          }
        }
      }
    ]
  })
}

# Enable S3 access logging for audit
resource "aws_s3_bucket" "access_logs" {
  bucket = "${var.project_name}-access-logs-${var.environment}"

  tags = {
    Project   = var.project_name
    Purpose   = "S3 access logging for GLBA audit"
    ManagedBy = "terraform"
  }
}

resource "aws_s3_bucket_logging" "datalake" {
  for_each = toset(local.layers)

  bucket        = aws_s3_bucket.datalake[each.value].id
  target_bucket = aws_s3_bucket.access_logs.id
  target_prefix = "s3-access-logs/${each.value}/"
}
