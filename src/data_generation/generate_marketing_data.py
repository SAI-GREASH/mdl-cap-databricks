
# ============================================================
# MDL-CAP: Marketing Data Lake & Campaign Activation Platform
# Module: Synthetic Marketing Data Generation
# Purpose: Generate fictional advertising and campaign data
# ============================================================

import csv
import random
from datetime import date, timedelta
from pathlib import Path

# Reproducible data generation
RANDOM_SEED = 42
random.seed(RANDOM_SEED)

# Dataset configuration
START_DATE = date(2026, 10, 1)
NUM_DAYS = 30
RECORDS_PER_DAY = 10

# Unity Catalog Volume landing location
LANDING_ROOT = Path(
    "/Volumes/workspace/mdl_cap_bronze/landing"
)

# Marketing data sources
SOURCES = [
    "google_ads",
    "youtube_ads",
    "facebook_ads",
    "internal_marketing",
]

# ============================================================
# Generate synthetic campaign performance records
# ============================================================

def generate_campaign_rows(source):
    rows = []

    for day_offset in range(NUM_DAYS):
        campaign_date = START_DATE + timedelta(days=day_offset)

        for campaign_number in range(1, RECORDS_PER_DAY + 1):

            # Generate fictional marketing metrics
            impressions = random.randint(1000, 50000)
            clicks = int(impressions * random.uniform(0.01, 0.08))
            conversions = int(clicks * random.uniform(0.01, 0.12))
            spend = round(clicks * random.uniform(0.25, 3.20), 2)

            # Construct one campaign performance record
            row = {
                "campaign_date": campaign_date.isoformat(),
                "source": source,
                "campaign_id": f"{source.upper()}_{campaign_number:03d}",
                "campaign_name": f"{source.replace('_', ' ').title()} Campaign {campaign_number:02d}",
                "impressions": impressions,
                "clicks": clicks,
                "conversions": conversions,
                "spend": spend,
            }

            rows.append(row)

    return rows

# ============================================================
# Save generated campaign records into Unity Catalog Volumes
# ============================================================

def save_campaign_rows(source, rows):

    if not rows:
        raise ValueError(f"No records generated for {source}")

    # Select the correct source-specific landing directory
    output_dir = LANDING_ROOT / source

    # Ensure the destination directory exists
    output_dir.mkdir(parents=True, exist_ok=True)

    # Create a deterministic filename for the date range
    end_date = START_DATE + timedelta(days=NUM_DAYS - 1)

    filename = (
        f"{source}_campaigns_"
        f"{START_DATE:%Y%m%d}_{end_date:%Y%m%d}.csv"
    )

    output_path = output_dir / filename

    # Define the CSV columns
    fieldnames = [
        "campaign_date",
        "source",
        "campaign_id",
        "campaign_name",
        "impressions",
        "clicks",
        "conversions",
        "spend",
    ]

    # Write campaign records into the CSV file
    with output_path.open(
        mode="w",
        newline="",
        encoding="utf-8"
    ) as csv_file:

        writer = csv.DictWriter(
            csv_file,
            fieldnames=fieldnames
        )

        writer.writeheader()
        writer.writerows(rows)

    print(f"Saved {len(rows)} records to {output_path}")

    return output_path


# ============================================================
# Main execution: Generate and save all marketing datasets
# ============================================================

def main():
    total_records = 0

    print("Starting MDL-CAP synthetic data generation...")

    for source in SOURCES:
        print(f"\nProcessing source: {source}")

        # Generate synthetic campaign records
        rows = generate_campaign_rows(source)

        # Save records to the Bronze landing volume
        save_campaign_rows(source, rows)

        # Track total generated records
        total_records += len(rows)

    print("\nData generation completed successfully!")
    print(f"Sources processed: {len(SOURCES)}")
    print(f"Total records generated: {total_records}")


if __name__ == "__main__":
    main()

