import csv
import boto3
from botocore.exceptions import ClientError

# 🔴 VERY IMPORTANT:
# Set this to EXACTLY what you see in the DynamoDB console.
TABLE_NAME = "speaking-leads-v3-multi"

OUTPUT_FILE = "leads.csv"

# Optional: filter by specific agent(s), or leave empty for all
# e.g. ["first_gen_student_success_agent", "student_athlete_leadership_agent"]
FILTER_SOURCES = []

# 🔥 Force region to match your Terraform / Lambda (us-east-1)
dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
table = dynamodb.Table(TABLE_NAME)


def scan_table():
    items = []
    scan_kwargs = {}
    done = False
    start_key = None

    while not done:
        if start_key:
            scan_kwargs["ExclusiveStartKey"] = start_key

        try:
            response = table.scan(**scan_kwargs)
        except ClientError as e:
            print(f"Error scanning table: {e}")
            return []  # bail out instead of continuing with 0
        batch = response.get("Items", [])
        items.extend(batch)

        start_key = response.get("LastEvaluatedKey", None)
        done = start_key is None

    return items


def filter_items(items):
    if not FILTER_SOURCES:
        return items

    filtered = [i for i in items if i.get("source") in FILTER_SOURCES]
    return filtered


def write_csv(items, filename):
    fieldnames = ["url", "title", "contact_email", "source", "scraped_at"]

    with open(filename, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for item in items:
            row = {field: item.get(field, "") for field in fieldnames}
            writer.writerow(row)


def main():
    print(f"Scanning DynamoDB table: {TABLE_NAME} ...")
    items = scan_table()
    print(f"Total items scanned: {len(items)}")

    items = filter_items(items)
    print(f"Total items after filter: {len(items)}")

    write_csv(items, OUTPUT_FILE)
    print(f"Exported to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
