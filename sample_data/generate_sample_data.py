"""
Generate synthetic insurance data for the migration demo.

Produces realistic CSV files for policies, claims, premiums, and properties
that mirror the data shapes found in a real property insurance company.

Usage:
    python generate_sample_data.py --rows 100000 --output-dir .
    python generate_sample_data.py --rows 1000 --output-dir . --seed 42
"""

import argparse
import csv
import os
import random
from datetime import datetime, timedelta
from typing import Optional

# ──────────────────────────────────────────────────────────────────────
# Reference data
# ──────────────────────────────────────────────────────────────────────

STATES = [
    ("FL", ["Miami", "Tampa", "Orlando", "Jacksonville", "Fort Lauderdale", "Naples"]),
    ("TX", ["Houston", "Dallas", "Austin", "San Antonio", "Fort Worth", "Corpus Christi"]),
    ("LA", ["New Orleans", "Baton Rouge", "Shreveport", "Lafayette", "Lake Charles"]),
    ("SC", ["Charleston", "Columbia", "Myrtle Beach", "Greenville"]),
    ("NC", ["Charlotte", "Raleigh", "Wilmington", "Asheville"]),
    ("GA", ["Atlanta", "Savannah", "Augusta", "Macon"]),
    ("AL", ["Birmingham", "Mobile", "Huntsville", "Montgomery"]),
    ("MS", ["Jackson", "Gulfport", "Biloxi", "Hattiesburg"]),
    ("CA", ["Los Angeles", "San Francisco", "San Diego", "Sacramento"]),
    ("IL", ["Chicago", "Springfield", "Naperville", "Rockford"]),
]

COVERAGE_TYPES = ["HO3", "HO5", "HO6", "DP1", "DP3", "HO4", "FLOOD", "WIND"]
CONSTRUCTION_TYPES = ["FRAME", "MASONRY", "CONCRETE", "STEEL", "WOOD", "MANUFACTURED"]
ROOF_TYPES = ["SHINGLE", "TILE", "METAL", "FLAT", "SLATE", "WOOD_SHAKE"]
OCCUPANCY_TYPES = ["PRIMARY", "SECONDARY", "RENTAL", "VACANT"]
FLOOD_ZONES = ["A", "AE", "V", "VE", "B", "C", "X", "X500"]
WIND_ZONES = ["1", "2", "3", "4", "5"]
CLAIM_TYPES = ["PROPERTY_DAMAGE", "LIABILITY", "THEFT", "WATER_DAMAGE", "WIND_DAMAGE",
               "FIRE", "HAIL", "FLOOD", "MOLD", "OTHER"]
CLAIM_STATUSES = ["OPEN", "UNDER_REVIEW", "APPROVED", "DENIED", "CLOSED", "REOPENED"]
POLICY_STATUSES = ["ACTIVE", "ACTIVE", "ACTIVE", "ACTIVE", "CANCELLED", "EXPIRED", "PENDING"]
PAYMENT_METHODS = ["ACH", "CREDIT_CARD", "CHECK", "WIRE", "ESCROW"]
PAYMENT_STATUSES = ["COMPLETED", "COMPLETED", "COMPLETED", "COMPLETED", "FAILED", "PENDING"]
CHANNELS = ["ONLINE", "AGENT", "REFERRAL", "PARTNER", "DIRECT_MAIL"]
CAUSES_OF_LOSS = ["HURRICANE", "TORNADO", "HAIL", "LIGHTNING", "FIRE", "BURST_PIPE",
                  "THEFT", "VANDALISM", "TREE_FALL", "SINKHOLE", "FLOOD", "WIND"]

FIRST_NAMES = ["James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael", "Linda",
               "David", "Elizabeth", "William", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
               "Thomas", "Sarah", "Christopher", "Karen", "Daniel", "Lisa", "Matthew", "Nancy",
               "Anthony", "Betty", "Mark", "Margaret", "Charles", "Sandra", "Steven", "Ashley",
               "Paul", "Dorothy", "Andrew", "Kimberly", "Joshua", "Emily", "Kenneth", "Donna"]

LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
              "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
              "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
              "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson"]

STREETS = ["Oak", "Maple", "Cedar", "Pine", "Elm", "Magnolia", "Palm", "Cypress",
           "Willow", "Birch", "Pecan", "Hickory", "Walnut", "Laurel", "Bay"]

STREET_TYPES = ["St", "Ave", "Blvd", "Dr", "Ln", "Ct", "Way", "Pl"]


# ──────────────────────────────────────────────────────────────────────
# Helper functions
# ──────────────────────────────────────────────────────────────────────

def random_date(start: datetime, end: datetime) -> datetime:
    delta = end - start
    random_days = random.randint(0, delta.days)
    return start + timedelta(days=random_days)


def random_email(first: str, last: str) -> str:
    domains = ["gmail.com", "yahoo.com", "outlook.com", "aol.com", "icloud.com"]
    sep = random.choice([".", "_", ""])
    num = random.randint(1, 999)
    return f"{first.lower()}{sep}{last.lower()}{num}@{random.choice(domains)}"


def random_zip(state: str) -> str:
    """Generate a plausible zip code prefix for a given state."""
    zip_prefixes = {
        "FL": range(320, 350), "TX": range(750, 800), "LA": range(700, 715),
        "SC": range(290, 300), "NC": range(270, 290), "GA": range(300, 320),
        "AL": range(350, 370), "MS": range(386, 398), "CA": range(900, 962),
        "IL": range(600, 630),
    }
    prefix = random.choice(list(zip_prefixes.get(state, range(100, 999))))
    suffix = random.randint(10, 99)
    return f"{prefix}{suffix}"


# ──────────────────────────────────────────────────────────────────────
# Data generators
# ──────────────────────────────────────────────────────────────────────

def generate_properties(n: int) -> list[dict]:
    """Generate property records with realistic geographic and risk attributes."""
    properties = []
    for i in range(1, n + 1):
        state_abbr, cities = random.choice(STATES)
        city = random.choice(cities)
        year_built = random.randint(1950, 2024)
        created = random_date(datetime(2020, 1, 1), datetime(2024, 6, 1))

        properties.append({
            "property_id": f"PROP-{i:07d}",
            "street_address": f"{random.randint(100, 9999)} {random.choice(STREETS)} {random.choice(STREET_TYPES)}",
            "city": city,
            "state": state_abbr,
            "zip_code": random_zip(state_abbr),
            "county": f"{city} County",
            "latitude": round(random.uniform(25.0, 42.0), 7),
            "longitude": round(random.uniform(-124.0, -80.0), 7),
            "year_built": year_built,
            "square_footage": random.choice(range(800, 5001, 100)),
            "construction_type": random.choice(CONSTRUCTION_TYPES),
            "roof_type": random.choice(ROOF_TYPES),
            "stories": random.choices([1, 2, 3], weights=[40, 50, 10])[0],
            "occupancy_type": random.choice(OCCUPANCY_TYPES),
            "flood_zone": random.choice(FLOOD_ZONES),
            "wind_zone": random.choice(WIND_ZONES),
            "property_value": round(random.uniform(80000, 1500000), 2),
            "created_at": created.isoformat(),
            "updated_at": (created + timedelta(days=random.randint(0, 180))).isoformat(),
        })
    return properties


def generate_policies(n: int, properties: list[dict]) -> list[dict]:
    """Generate policy records linked to properties."""
    policies = []
    for i in range(1, n + 1):
        prop = random.choice(properties)
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)
        effective = random_date(datetime(2021, 1, 1), datetime(2025, 6, 1))
        expiration = effective + timedelta(days=365)
        premium = round(random.uniform(500, 15000), 2)
        created = effective - timedelta(days=random.randint(1, 30))

        policies.append({
            "policy_id": f"POL-{i:07d}",
            "policyholder_first_name": first,
            "policyholder_last_name": last,
            "policyholder_email": random_email(first, last),
            "property_id": prop["property_id"],
            "coverage_type_code": random.choice(COVERAGE_TYPES),
            "effective_date": effective.strftime("%Y-%m-%d"),
            "expiration_date": expiration.strftime("%Y-%m-%d"),
            "status": random.choice(POLICY_STATUSES),
            "annual_premium": premium,
            "deductible": random.choice([500, 1000, 1500, 2000, 2500, 5000]),
            "coverage_limit": random.choice([100000, 200000, 300000, 500000, 750000, 1000000]),
            "agent_id": f"AGT-{random.randint(1, 500):04d}",
            "channel": random.choice(CHANNELS),
            "created_at": created.isoformat(),
            "updated_at": (created + timedelta(days=random.randint(0, 90))).isoformat(),
        })
    return policies


def generate_claims(policies: list[dict], claims_ratio: float = 0.15) -> list[dict]:
    """Generate claims for a subset of policies."""
    claims = []
    claim_counter = 0
    for policy in policies:
        if random.random() > claims_ratio:
            continue
        num_claims = random.choices([1, 2, 3], weights=[70, 25, 5])[0]
        effective = datetime.strptime(policy["effective_date"], "%Y-%m-%d")
        for _ in range(num_claims):
            claim_counter += 1
            claim_date = random_date(effective, effective + timedelta(days=365))
            reported_date = claim_date + timedelta(days=random.randint(0, 14))
            status = random.choice(CLAIM_STATUSES)
            closed_date = (reported_date + timedelta(days=random.randint(7, 180))) if status == "CLOSED" else None
            claim_amount = round(random.uniform(500, 250000), 2)
            approved = round(claim_amount * random.uniform(0.3, 1.0), 2) if status in ("APPROVED", "CLOSED") else 0

            claims.append({
                "claim_id": f"CLM-{claim_counter:07d}",
                "policy_id": policy["policy_id"],
                "claim_date": claim_date.strftime("%Y-%m-%d"),
                "reported_date": reported_date.strftime("%Y-%m-%d"),
                "closed_date": closed_date.strftime("%Y-%m-%d") if closed_date else "",
                "claim_type": random.choice(CLAIM_TYPES),
                "claim_status": status,
                "claim_amount": claim_amount,
                "approved_amount": approved,
                "deductible_applied": float(policy["deductible"]) if approved > 0 else 0,
                "adjuster_id": f"ADJ-{random.randint(1, 200):04d}",
                "cause_of_loss": random.choice(CAUSES_OF_LOSS),
                "description": f"Claim for {random.choice(CAUSES_OF_LOSS).lower().replace('_', ' ')} damage",
                "created_at": reported_date.isoformat(),
                "updated_at": (closed_date or reported_date + timedelta(days=random.randint(1, 30))).isoformat(),
            })
    return claims


def generate_premiums(policies: list[dict]) -> list[dict]:
    """Generate premium payment records for each policy."""
    premiums = []
    premium_counter = 0
    for policy in policies:
        effective = datetime.strptime(policy["effective_date"], "%Y-%m-%d")
        annual = float(policy["annual_premium"])
        # Generate monthly or quarterly payments
        frequency = random.choice(["MONTHLY", "QUARTERLY", "SEMI_ANNUAL", "ANNUAL"])
        if frequency == "MONTHLY":
            num_payments, amount = 12, round(annual / 12, 2)
        elif frequency == "QUARTERLY":
            num_payments, amount = 4, round(annual / 4, 2)
        elif frequency == "SEMI_ANNUAL":
            num_payments, amount = 2, round(annual / 2, 2)
        else:
            num_payments, amount = 1, annual

        for j in range(num_payments):
            premium_counter += 1
            if frequency == "MONTHLY":
                due_date = effective + timedelta(days=30 * j)
            elif frequency == "QUARTERLY":
                due_date = effective + timedelta(days=90 * j)
            elif frequency == "SEMI_ANNUAL":
                due_date = effective + timedelta(days=182 * j)
            else:
                due_date = effective

            pay_status = random.choice(PAYMENT_STATUSES)
            days_offset = random.randint(-5, 15)
            payment_date = due_date + timedelta(days=days_offset)
            period_end = due_date + timedelta(days=(365 // num_payments))

            premiums.append({
                "premium_id": f"PRM-{premium_counter:08d}",
                "policy_id": policy["policy_id"],
                "payment_date": payment_date.strftime("%Y-%m-%d"),
                "due_date": due_date.strftime("%Y-%m-%d"),
                "amount": amount,
                "payment_method": random.choice(PAYMENT_METHODS),
                "payment_status": pay_status,
                "billing_period": frequency,
                "period_start_date": due_date.strftime("%Y-%m-%d"),
                "period_end_date": period_end.strftime("%Y-%m-%d"),
                "created_at": payment_date.isoformat(),
            })
    return premiums


# ──────────────────────────────────────────────────────────────────────
# File output
# ──────────────────────────────────────────────────────────────────────

def write_csv(records: list[dict], filepath: str) -> None:
    """Write a list of dicts to CSV."""
    if not records:
        return
    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=records[0].keys())
        writer.writeheader()
        writer.writerows(records)
    print(f"  Wrote {len(records):,} records to {filepath}")


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic insurance data")
    parser.add_argument("--rows", type=int, default=10000, help="Number of policies to generate")
    parser.add_argument("--output-dir", type=str, default=".", help="Output directory")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    args = parser.parse_args()

    random.seed(args.seed)
    os.makedirs(args.output_dir, exist_ok=True)

    print(f"Generating synthetic insurance data ({args.rows:,} policies)...")

    # Properties: ~80% of policy count (some properties have multiple policies)
    num_properties = int(args.rows * 0.8)
    print(f"\n1. Generating {num_properties:,} properties...")
    properties = generate_properties(num_properties)
    write_csv(properties, os.path.join(args.output_dir, "raw_properties.csv"))

    print(f"\n2. Generating {args.rows:,} policies...")
    policies = generate_policies(args.rows, properties)
    write_csv(policies, os.path.join(args.output_dir, "raw_policies.csv"))

    print(f"\n3. Generating claims (~15% of policies)...")
    claims = generate_claims(policies)
    write_csv(claims, os.path.join(args.output_dir, "raw_claims.csv"))

    print(f"\n4. Generating premium payments...")
    premiums = generate_premiums(policies)
    write_csv(premiums, os.path.join(args.output_dir, "raw_premiums.csv"))

    print(f"\nData generation complete!")
    print(f"  Properties: {len(properties):,}")
    print(f"  Policies:   {len(policies):,}")
    print(f"  Claims:     {len(claims):,}")
    print(f"  Premiums:   {len(premiums):,}")


if __name__ == "__main__":
    main()
