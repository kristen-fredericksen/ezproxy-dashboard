"""List all platforms and databases found in the log file, showing mapping status.

Usage:
    python3 list_resources.py data/institutions.csv test_logs/ezproxyspu_2026_02.log
"""

import sys
from collections import Counter
from pathlib import Path

# Import the shared functions from dashboard.py
from dashboard import (
    load_database_names, parse_log_line,
    extract_platform_name, extract_database_name
)


def main():
    if len(sys.argv) != 3:
        print("Usage: python3 list_resources.py [IP CSV] [log file]")
        sys.exit(1)

    log_path = sys.argv[2]

    # Load database name mapping
    script_dir = Path(__file__).resolve().parent
    db_names_path = script_dir / 'data' / 'database_names.json'
    db_names = load_database_names(str(db_names_path))

    # Parse all log lines
    records = []
    with open(log_path, encoding='utf-8', errors='replace') as f:
        for line in f:
            parsed = parse_log_line(line.strip())
            if parsed:
                records.append(parsed)

    print(f"Parsed {len(records):,} records from {log_path}\n")

    # ---------------------------------------------------------------
    # PLATFORMS (vendor/domain level)
    # ---------------------------------------------------------------
    platform_counts = Counter()
    for r in records:
        platform = extract_platform_name(r['url'], db_names)
        if platform:
            platform_counts[platform] += 1

    # Determine which platforms are "mapped" (have a friendly name in our JSON)
    # vs raw domains that just pass through
    known_platforms = set(['EBSCO', 'Gale'])  # explicitly grouped
    known_platforms.update(db_names['domains'].values())  # domain-mapped names

    print("=" * 70)
    print("ALL PLATFORMS (vendor/domain level)")
    print("=" * 70)
    print(f"{'Platform':<45} {'Count':>7}  {'Mapped?'}")
    print("-" * 70)

    mapped_platforms = []
    unmapped_platforms = []

    for name, count in platform_counts.most_common():
        is_mapped = name in known_platforms
        if is_mapped:
            mapped_platforms.append((name, count))
        else:
            unmapped_platforms.append((name, count))

    # Print mapped first
    print("\n  MAPPED platforms (have a friendly name):")
    for name, count in sorted(mapped_platforms, key=lambda x: -x[1]):
        print(f"    {name:<43} {count:>7}")

    print(f"\n  UNMAPPED platforms (raw domain, no mapping):")
    for name, count in sorted(unmapped_platforms, key=lambda x: -x[1]):
        print(f"    {name:<43} {count:>7}")

    print(f"\n  Total mapped: {len(mapped_platforms)}")
    print(f"  Total unmapped: {len(unmapped_platforms)}")
    print(f"  Total platforms: {len(platform_counts)}")

    # ---------------------------------------------------------------
    # DATABASES (specific database level)
    # ---------------------------------------------------------------
    db_counts = Counter()
    for r in records:
        db_name = extract_database_name(r['url'], db_names)
        if db_name:
            db_counts[db_name] += 1

    # Determine mapping status:
    # - "EBSCO (xyz)" = unmapped EBSCO code
    # - "Gale (XYZ)" = unmapped Gale code
    # - raw domain = unmapped domain
    # - Everything else = mapped
    known_db_names = set(db_names['ebsco'].values())
    known_db_names.update(db_names['gale'].values())
    known_db_names.update(db_names['domains'].values())

    print("\n\n" + "=" * 70)
    print("ALL DATABASES (specific database/resource level)")
    print("=" * 70)

    mapped_dbs = []
    unmapped_dbs = []

    for name, count in db_counts.most_common():
        # Check if it's an unmapped code
        is_unmapped_ebsco = name.startswith("EBSCO (") and name.endswith(")")
        is_unmapped_gale = name.startswith("Gale (") and name.endswith(")")
        is_known = name in known_db_names

        if is_unmapped_ebsco or is_unmapped_gale:
            unmapped_dbs.append((name, count, "unmapped code"))
        elif is_known:
            mapped_dbs.append((name, count))
        else:
            # It's a raw domain that passed through
            unmapped_dbs.append((name, count, "unmapped domain"))

    print(f"\n  MAPPED databases (have a friendly name):")
    print(f"  {'Database Name':<45} {'Count':>7}")
    print(f"  {'-'*45} {'-'*7}")
    for name, count in sorted(mapped_dbs, key=lambda x: -x[1]):
        print(f"    {name:<43} {count:>7}")

    print(f"\n  UNMAPPED databases (raw code or domain):")
    print(f"  {'Raw Value':<45} {'Count':>7}  {'Type'}")
    print(f"  {'-'*45} {'-'*7}  {'-'*15}")
    for name, count, kind in sorted(unmapped_dbs, key=lambda x: -x[1]):
        print(f"    {name:<43} {count:>7}  {kind}")

    print(f"\n  Total mapped: {len(mapped_dbs)}")
    print(f"  Total unmapped: {len(unmapped_dbs)}")
    print(f"  Total databases: {len(db_counts)}")


if __name__ == '__main__':
    main()
