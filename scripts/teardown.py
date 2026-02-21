"""
Teardown: Remove all infrastructure created by this project.

Reads all configuration from environment variables — no credentials in code.

Required env vars:
  DATABRICKS_HOST          - Databricks workspace URL
  DATABRICKS_TOKEN         - Databricks personal access token
  BUNDLE_VAR_warehouse_id  - SQL Warehouse ID
  AWS_DEFAULT_REGION       - AWS region (defaults to us-east-1)
  TF_STATE_BUCKET          - Terraform state S3 bucket name
  KMS_KEY_ID               - KMS key ID to schedule for deletion
  PROJECT_NAME             - Project name prefix (defaults to fintechco)
  ENVIRONMENT              - Environment name (defaults to dev)
"""

import contextlib
import json
import os
import subprocess
import sys
import time

import requests

# ── Configuration from environment ──────────────────────────
HOST = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
TOKEN = os.environ.get("DATABRICKS_TOKEN", "")
WAREHOUSE_ID = os.environ.get("BUNDLE_VAR_warehouse_id", "")  # noqa: SIM112
REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
TF_STATE_BUCKET = os.environ.get("TF_STATE_BUCKET", "")
KMS_KEY_ID = os.environ.get("KMS_KEY_ID", "")
PROJECT = os.environ.get("PROJECT_NAME", "fintechco")
ENV = os.environ.get("ENVIRONMENT", "dev")
CATALOG = os.environ.get("CATALOG_NAME", f"fintech_catalog_{ENV}")

SQL_API = f"{HOST}/api/2.0/sql/statements"
UC_API = f"{HOST}/api/2.1/unity-catalog"
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

ERRORS = []


def check_env():
    """Validate required environment variables."""
    required = ["DATABRICKS_HOST", "DATABRICKS_TOKEN", "BUNDLE_VAR_warehouse_id"]
    missing = [v for v in required if not os.environ.get(v)]
    if missing:
        print(f"ERROR: Missing required env vars: {', '.join(missing)}")
        sys.exit(1)


def run_sql(sql, label=""):
    """Execute SQL via Statement API."""
    r = requests.post(
        SQL_API,
        headers=HEADERS,
        json={"warehouse_id": WAREHOUSE_ID, "statement": sql, "wait_timeout": "50s"},
    )
    resp = r.json()
    state = resp.get("status", {}).get("state", "UNKNOWN")
    sid = resp.get("statement_id", "")
    while state in ("PENDING", "RUNNING"):
        time.sleep(3)
        r2 = requests.get(f"{SQL_API}/{sid}", headers=HEADERS)
        resp = r2.json()
        state = resp.get("status", {}).get("state", "UNKNOWN")
    if state == "SUCCEEDED":
        print(f"  OK: {label}")
        return True
    else:
        err = resp.get("status", {}).get("error", {}).get("message", "Unknown")[:200]
        print(f"  SKIP: {label} -- {err}")
        return False


def run_cmd(cmd, label=""):
    """Run a shell command."""
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, shell=True)
        if result.returncode == 0:
            print(f"  OK: {label}")
            return True
        else:
            msg = (result.stderr or result.stdout or "")[:200]
            print(f"  SKIP: {label} -- {msg}")
            return False
    except Exception as e:
        print(f"  SKIP: {label} -- {e}")
        return False


def uc_delete(path, label=""):
    """Delete a Unity Catalog resource via REST API."""
    r = requests.delete(f"{UC_API}/{path}", headers=HEADERS)
    if r.status_code in (200, 204):
        print(f"  OK: {label}")
        return True
    else:
        msg = r.json().get("message", str(r.status_code))[:200]
        print(f"  SKIP: {label} -- {msg}")
        return False


def teardown_unity_catalog():
    """Drop all tables, schemas, functions, and catalog."""
    print("\n[1/7] UNITY CATALOG TEARDOWN")

    # Drop tables in each schema
    for schema in ["gold", "silver", "bronze", "data_quality"]:
        run_sql(f"USE CATALOG {CATALOG}", "use catalog")
        # List and drop tables
        r = requests.post(
            SQL_API,
            headers=HEADERS,
            json={
                "warehouse_id": WAREHOUSE_ID,
                "statement": f"SHOW TABLES IN {CATALOG}.{schema}",
                "wait_timeout": "50s",
            },
        )
        resp = r.json()
        state = resp.get("status", {}).get("state", "")
        if state == "SUCCEEDED":
            tables = resp.get("result", {}).get("data_array", [])
            for row in tables:
                tbl = row[1] if len(row) > 1 else row[0]
                run_sql(
                    f"DROP TABLE IF EXISTS {CATALOG}.{schema}.{tbl}",
                    f"drop {schema}.{tbl}",
                )

    # Drop functions in gold
    run_sql(f"DROP FUNCTION IF EXISTS {CATALOG}.gold.mask_email", "drop mask_email")
    run_sql(f"DROP FUNCTION IF EXISTS {CATALOG}.gold.mask_name", "drop mask_name")

    # Drop schemas
    for schema in ["gold", "silver", "bronze", "data_quality"]:
        run_sql(
            f"DROP SCHEMA IF EXISTS {CATALOG}.{schema} CASCADE",
            f"drop schema {schema}",
        )

    # Drop catalog
    run_sql(f"DROP CATALOG IF EXISTS {CATALOG} CASCADE", f"drop catalog {CATALOG}")


def teardown_external_locations():
    """Remove external locations and storage credential."""
    print("\n[2/7] EXTERNAL LOCATIONS & STORAGE CREDENTIAL")
    for name in ["fintechco_bronze", "fintechco_silver", "fintechco_gold"]:
        uc_delete(f"external-locations/{name}", f"delete ext location {name}")
    uc_delete("storage-credentials/fintechco_s3_cred", "delete storage credential")


def teardown_databricks_bundle():
    """Destroy the Databricks Asset Bundle."""
    print("\n[3/7] DATABRICKS ASSET BUNDLE")
    run_cmd(
        f"databricks bundle destroy -t {ENV} --auto-approve",
        "bundle destroy",
    )


def teardown_databricks_groups():
    """Remove workspace groups created for this project."""
    print("\n[4/7] DATABRICKS GROUPS")
    for group_name in [
        "data_engineers",
        "bi_analysts",
        "data_scientists",
        "app_services",
        "pii_authorized",
    ]:
        # Find group ID
        r = requests.get(
            f"{HOST}/api/2.0/preview/scim/v2/Groups",
            headers=HEADERS,
            params={"filter": f'displayName eq "{group_name}"'},
        )
        resp = r.json()
        resources = resp.get("Resources", [])
        if resources:
            gid = resources[0].get("id", "")
            r2 = requests.delete(f"{HOST}/api/2.0/preview/scim/v2/Groups/{gid}", headers=HEADERS)
            if r2.status_code in (200, 204):
                print(f"  OK: delete group {group_name}")
            else:
                print(f"  SKIP: delete group {group_name} -- {r2.status_code}")
        else:
            print(f"  SKIP: group {group_name} not found")


def teardown_terraform():
    """Run terraform destroy to remove S3 buckets and related resources."""
    print("\n[5/7] TERRAFORM DESTROY")
    tf_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "infrastructure",
        "terraform",
    )
    if not os.path.isdir(tf_dir):
        print(f"  SKIP: Terraform dir not found: {tf_dir}")
        return

    # Init with backend config
    if TF_STATE_BUCKET:
        run_cmd(
            f'cd "{tf_dir}" && terraform init '
            f'-backend-config="bucket={TF_STATE_BUCKET}" '
            f'-backend-config="key=lakehouse-migration/terraform.tfstate" '
            f'-backend-config="region={REGION}" '
            f"-reconfigure",
            "terraform init",
        )
    else:
        print("  SKIP: TF_STATE_BUCKET not set, skipping terraform destroy")
        return

    # Empty versioned S3 buckets before terraform destroy (required for deletion)
    result = subprocess.run("aws s3 ls", capture_output=True, text=True, shell=True)
    if result.returncode == 0:
        for line in result.stdout.splitlines():
            parts = line.strip().split()
            if len(parts) >= 3:
                bname = parts[-1]
                if bname.startswith(PROJECT):
                    print(f"  Emptying versioned bucket: {bname}")
                    empty_versioned_bucket(bname)
                    run_cmd(f"aws s3 rm s3://{bname} --recursive", f"empty {bname}")

    run_cmd(f'cd "{tf_dir}" && terraform destroy -auto-approve', "terraform destroy")

    # Force-delete any remaining project buckets
    result = subprocess.run("aws s3 ls", capture_output=True, text=True, shell=True)
    if result.returncode == 0:
        for line in result.stdout.splitlines():
            parts = line.strip().split()
            if len(parts) >= 3:
                bname = parts[-1]
                if bname.startswith(PROJECT):
                    empty_versioned_bucket(bname)
                    run_cmd(f"aws s3 rb s3://{bname}", f"force delete {bname}")


def empty_versioned_bucket(bucket_name):
    """Delete all object versions and delete markers from a versioned S3 bucket."""
    import tempfile

    while True:
        result = subprocess.run(
            f"aws s3api list-object-versions --bucket {bucket_name} --max-items 1000 --output json",
            capture_output=True,
            text=True,
            shell=True,
        )
        if result.returncode != 0:
            break
        data = json.loads(result.stdout)
        items = []
        for v in data.get("Versions", []):
            items.append({"Key": v["Key"], "VersionId": v["VersionId"]})
        for m in data.get("DeleteMarkers", []):
            items.append({"Key": m["Key"], "VersionId": m["VersionId"]})
        if not items:
            break
        payload = json.dumps({"Objects": items, "Quiet": True})
        tmp = os.path.join(tempfile.gettempdir(), "_s3del.json")
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(payload)
        subprocess.run(
            f"aws s3api delete-objects --bucket {bucket_name} --delete file://{tmp}",
            capture_output=True,
            shell=True,
        )
        print(f"    deleted {len(items)} versioned objects")
    with contextlib.suppress(OSError):
        os.remove(os.path.join(tempfile.gettempdir(), "_s3del.json"))


def teardown_tf_state_bucket():
    """Remove the Terraform state bucket (created outside of Terraform)."""
    print("\n[6/7] TERRAFORM STATE BUCKET")
    if not TF_STATE_BUCKET:
        print("  SKIP: TF_STATE_BUCKET not set")
        return
    empty_versioned_bucket(TF_STATE_BUCKET)
    run_cmd(f"aws s3 rb s3://{TF_STATE_BUCKET}", "delete state bucket")


def teardown_kms():
    """Schedule KMS key for deletion (minimum 7-day wait)."""
    print("\n[7/7] KMS KEY")
    if not KMS_KEY_ID:
        print("  SKIP: KMS_KEY_ID not set")
        return
    run_cmd(
        f"aws kms schedule-key-deletion --key-id {KMS_KEY_ID} "
        f"--pending-window-in-days 7 --region {REGION}",
        "schedule KMS key deletion (7-day wait)",
    )
    run_cmd(
        f"aws kms delete-alias --alias-name alias/{PROJECT}-datalake-key --region {REGION}",
        "delete KMS alias",
    )


def main():
    print("=" * 60)
    print("TEARDOWN: Removing all project infrastructure")
    print("=" * 60)
    print(f"  Catalog:   {CATALOG}")
    print(f"  Project:   {PROJECT}")
    print(f"  Env:       {ENV}")
    print(f"  Region:    {REGION}")

    check_env()

    teardown_unity_catalog()
    teardown_external_locations()
    teardown_databricks_bundle()
    teardown_databricks_groups()
    teardown_terraform()
    teardown_tf_state_bucket()
    teardown_kms()

    print("\n" + "=" * 60)
    print("TEARDOWN COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
