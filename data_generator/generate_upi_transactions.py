"""Synthetic UPI transaction generator.

This script creates production-scale Synthetic Data for a lakehouse demo:
- upi_transactions.csv
- customer_dim.csv

The generated records are intentionally synthetic and must never be treated
as real banking data.
"""

from __future__ import annotations

import argparse
import csv
import ipaddress
import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Sequence, Tuple

from faker import Faker


fake = Faker("en_IN")

MERCHANT_CATEGORIES = [
    "grocery",
    "food_delivery",
    "utility",
    "electronics",
    "travel",
    "fashion",
    "healthcare",
    "education",
    "gaming",
    "fuel",
    "marketplace",
    "subscription",
]

BANKS = [
    "sbi",
    "hdfc",
    "icici",
    "axis",
    "kotak",
    "bob",
    "pnb",
    "canara",
]

INDIAN_CITIES = [
    "Mumbai",
    "Delhi",
    "Bengaluru",
    "Hyderabad",
    "Chennai",
    "Pune",
    "Kolkata",
    "Ahmedabad",
    "Jaipur",
    "Lucknow",
    "Surat",
    "Indore",
]

FAILURE_REASONS = [
    "insufficient_balance",
    "invalid_upi_pin",
    "beneficiary_not_found",
    "bank_timeout",
    "daily_limit_exceeded",
]


def slugify_name(name: str) -> str:
    return (
        name.lower()
        .replace(".", " ")
        .replace("'", "")
        .replace("-", " ")
        .split()
    )


def make_vpa(name: str, bank: str) -> str:
    parts = slugify_name(name)
    base = parts[0] if parts else "user"
    suffix = random.choice(["upi", "pay", "bank", "wallet", "secure"])
    return f"{base}.{suffix}@{bank}"


def random_ipv4() -> str:
    return str(ipaddress.IPv4Address(random.randint(1, 2**32 - 1)))


@dataclass(frozen=True)
class CustomerProfile:
    user_id: str
    name: str
    bank: str
    kyc_status: str
    device_id: str
    location_city: str
    effective_date: str
    sender_vpa: str


def build_customer_profiles(n_customers: int) -> List[CustomerProfile]:
    profiles: List[CustomerProfile] = []
    for idx in range(1, n_customers + 1):
        name = fake.name()
        bank = random.choice(BANKS)
        city = random.choice(INDIAN_CITIES)
        device_id = f"dev-{uuid.uuid4().hex[:12]}"
        effective_date = (datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=random.randint(0, 365))).date().isoformat()
        kyc_status = random.choices(["FULL_KYC", "MIN_KYC", "PENDING"], weights=[0.7, 0.2, 0.1])[0]
        user_id = f"USR{idx:08d}"
        profiles.append(
            CustomerProfile(
                user_id=user_id,
                name=name,
                bank=bank,
                kyc_status=kyc_status,
                device_id=device_id,
                location_city=city,
                effective_date=effective_date,
                sender_vpa=make_vpa(name, bank),
            )
        )
    return profiles


def tx_record(
    *,
    transaction_dt: datetime,
    profile: CustomerProfile,
    amount: float,
    receiver_vpa: str,
    merchant_category: str,
    location_city: str,
    device_id: str,
    status: str,
    failure_reason: str = "",
) -> Dict[str, str]:
    transaction_id = f"TXN-{uuid.uuid4().hex[:18].upper()}"
    utr_number = f"UTR{transaction_dt.strftime('%Y%m%d%H%M%S')}{uuid.uuid4().hex[:10].upper()}"
    return {
        "transaction_id": transaction_id,
        "utr_number": utr_number,
        "user_id": profile.user_id,
        "timestamp": transaction_dt.isoformat(sep=" ", timespec="seconds"),
        "amount": f"{amount:.2f}",
        "sender_vpa": profile.sender_vpa,
        "receiver_vpa": receiver_vpa,
        "merchant_category": merchant_category,
        "location_city": location_city,
        "device_id": device_id,
        "ip_address": random_ipv4(),
        "status": status,
        "failure_reason": failure_reason,
    }


def generate_velocity_cluster(profile: CustomerProfile) -> List[Dict[str, str]]:
    base_ts = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=random.randint(1, 20), hours=random.randint(0, 20))
    city = profile.location_city
    device_id = profile.device_id
    receiver = make_vpa(fake.name(), random.choice(BANKS))
    rows: List[Dict[str, str]] = []
    for i in range(6):
        rows.append(
            tx_record(
                transaction_dt=base_ts + timedelta(seconds=i * 8),
                profile=profile,
                amount=random.uniform(8500, 11000),
                receiver_vpa=receiver,
                merchant_category=random.choice(MERCHANT_CATEGORIES),
                location_city=city,
                device_id=device_id,
                status="SUCCESS",
            )
        )
    return rows


def generate_geo_anomaly(profile: CustomerProfile) -> List[Dict[str, str]]:
    base_ts = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=random.randint(1, 20), hours=random.randint(0, 20))
    city_a, city_b = random.sample(INDIAN_CITIES, 2)
    receiver = make_vpa(fake.name(), random.choice(BANKS))
    return [
        tx_record(
            transaction_dt=base_ts,
            profile=profile,
            amount=random.uniform(100, 4000),
            receiver_vpa=receiver,
            merchant_category=random.choice(MERCHANT_CATEGORIES),
            location_city=city_a,
            device_id=profile.device_id,
            status="SUCCESS",
        ),
        tx_record(
            transaction_dt=base_ts + timedelta(minutes=4),
            profile=profile,
            amount=random.uniform(100, 4000),
            receiver_vpa=receiver,
            merchant_category=random.choice(MERCHANT_CATEGORIES),
            location_city=city_b,
            device_id=profile.device_id,
            status="SUCCESS",
        ),
    ]


def generate_mule_cluster(profiles: Sequence[CustomerProfile]) -> List[Dict[str, str]]:
    if len(profiles) < 3:
        raise ValueError("Need at least 3 customer profiles for mule cluster generation")

    base_ts = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=random.randint(1, 20), hours=random.randint(0, 20))
    shared_device = f"dev-{uuid.uuid4().hex[:12]}"
    receiver = make_vpa(fake.name(), random.choice(BANKS))
    rows: List[Dict[str, str]] = []
    for offset, profile in enumerate(random.sample(list(profiles), 3)):
        rows.append(
            tx_record(
                transaction_dt=base_ts + timedelta(minutes=offset * 3),
                profile=profile,
                amount=random.uniform(500, 7000),
                receiver_vpa=receiver,
                merchant_category=random.choice(MERCHANT_CATEGORIES),
                location_city=random.choice(INDIAN_CITIES),
                device_id=shared_device,
                status="SUCCESS",
            )
        )
    return rows


def generate_bruteforce_cluster(profile: CustomerProfile) -> List[Dict[str, str]]:
    base_ts = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=random.randint(1, 20), hours=random.randint(0, 20))
    receiver = make_vpa(fake.name(), random.choice(BANKS))
    rows: List[Dict[str, str]] = []
    for i in range(5):
        rows.append(
            tx_record(
                transaction_dt=base_ts + timedelta(minutes=i),
                profile=profile,
                amount=random.uniform(100, 500),
                receiver_vpa=receiver,
                merchant_category=random.choice(MERCHANT_CATEGORIES),
                location_city=profile.location_city,
                device_id=profile.device_id,
                status="FAILED",
                failure_reason=random.choice(FAILURE_REASONS),
            )
        )
    rows.append(
        tx_record(
            transaction_dt=base_ts + timedelta(minutes=5),
            profile=profile,
            amount=random.uniform(100, 500),
            receiver_vpa=receiver,
            merchant_category=random.choice(MERCHANT_CATEGORIES),
            location_city=profile.location_city,
            device_id=profile.device_id,
            status="SUCCESS",
        )
    )
    return rows


def generate_normal_transaction(profile: CustomerProfile) -> Dict[str, str]:
    txn_dt = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(
        days=random.randint(0, 30),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59),
    )
    receiver = make_vpa(fake.name(), random.choice(BANKS))
    status = random.choices(["SUCCESS", "FAILED"], weights=[0.93, 0.07])[0]
    failure_reason = random.choice(FAILURE_REASONS) if status == "FAILED" else ""
    amount = random.uniform(10, 25000)
    location_city = profile.location_city if random.random() > 0.15 else random.choice(INDIAN_CITIES)
    device_id = profile.device_id if random.random() > 0.12 else f"dev-{uuid.uuid4().hex[:12]}"
    return tx_record(
        transaction_dt=txn_dt,
        profile=profile,
        amount=amount,
        receiver_vpa=receiver,
        merchant_category=random.choice(MERCHANT_CATEGORIES),
        location_city=location_city,
        device_id=device_id,
        status=status,
        failure_reason=failure_reason,
    )


def build_transaction_stream(
    profiles: Sequence[CustomerProfile],
    total_rows: int,
    injected_clusters_each: int = 8,
) -> Iterator[Dict[str, str]]:
    """Yield transaction rows without holding the full dataset in memory."""

    if total_rows < 1:
        return

    injected_rows: List[Dict[str, str]] = []
    profile_pool = list(profiles)
    for _ in range(injected_clusters_each):
        injected_rows.extend(generate_velocity_cluster(random.choice(profile_pool)))
        injected_rows.extend(generate_geo_anomaly(random.choice(profile_pool)))
        injected_rows.extend(generate_bruteforce_cluster(random.choice(profile_pool)))
        injected_rows.extend(generate_mule_cluster(profile_pool))

    normal_needed = max(total_rows - len(injected_rows), 0)
    for _ in range(normal_needed):
        yield generate_normal_transaction(random.choice(profile_pool))

    for row in injected_rows[:total_rows]:
        yield row


def write_csv(path: Path, fieldnames: Sequence[str], rows: Iterable[Dict[str, str]]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
            count += 1
    return count


def generate_customer_dim_rows(profiles: Sequence[CustomerProfile]) -> Iterable[Dict[str, str]]:
    for profile in profiles:
        yield {
            "user_id": profile.user_id,
            "name": profile.name,
            "bank": profile.bank,
            "kyc_status": profile.kyc_status,
            "device_id": profile.device_id,
            "location_city": profile.location_city,
            "effective_date": profile.effective_date,
        }


def generate_dataset(
    output_dir: Path,
    total_transactions: int,
    total_customers: int,
    transactions_file: str,
    customers_file: str,
) -> Tuple[Path, Path]:
    profiles = build_customer_profiles(total_customers)
    transactions_path = output_dir / transactions_file
    customers_path = output_dir / customers_file

    tx_fields = [
        "transaction_id",
        "utr_number",
        "user_id",
        "timestamp",
        "amount",
        "sender_vpa",
        "receiver_vpa",
        "merchant_category",
        "location_city",
        "device_id",
        "ip_address",
        "status",
        "failure_reason",
    ]
    customer_fields = [
        "user_id",
        "name",
        "bank",
        "kyc_status",
        "device_id",
        "location_city",
        "effective_date",
    ]

    tx_count = write_csv(
        transactions_path,
        tx_fields,
        build_transaction_stream(profiles, total_transactions),
    )
    cust_count = write_csv(customers_path, customer_fields, generate_customer_dim_rows(profiles))

    print(f"Wrote {tx_count:,} synthetic transactions to {transactions_path}")
    print(f"Wrote {cust_count:,} synthetic customer rows to {customers_path}")
    return transactions_path, customers_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate synthetic UPI transaction data.")
    parser.add_argument("--output-dir", type=Path, default=Path("sample_data"))
    parser.add_argument("--transactions", type=int, default=50_000)
    parser.add_argument("--customers", type=int, default=10_000)
    parser.add_argument("--transactions-file", default="upi_transactions.csv")
    parser.add_argument("--customers-file", default="customer_dim.csv")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    random.seed(42)
    Faker.seed(42)
    generate_dataset(
        output_dir=args.output_dir,
        total_transactions=args.transactions,
        total_customers=args.customers,
        transactions_file=args.transactions_file,
        customers_file=args.customers_file,
    )


if __name__ == "__main__":
    main()
