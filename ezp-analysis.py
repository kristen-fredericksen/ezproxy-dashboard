# Originally by Robin Camille Davis
# created 2014-03-28 // revised 2018-05-16
# Modified 2026-03-04 to read IP ranges from a SharePoint-exported CSV

## Script runs over all EZproxy-generated logs in a given directory.
## Best used with SPU logs.
##
## IP ranges are loaded from a CSV file exported from the CUNY SharePoint
## Institutions list, so you never need to edit this script when IPs change.
## Just re-export the CSV from SharePoint.
##
## See http://emerging.commons.gc.cuny.edu/2014/04/analyzing-ezproxy-logs-python/
##
## Usage:
##   python3 ezp-analysis.py [IP CSV file] [directory of SPU logs] [output.csv]
##
## Example:
##   python3 ezp-analysis.py data/institutions.csv logs/ output.csv
##
## Output columns (per log file):
##   - Filename
##   - Total connections
##   - One column per institution (connection count)
##   - Unknown connections (IPs not matching any institution)
##   - # student sessions (off-campus)
##   - % student sessions of off-campus total
##   - # faculty/staff sessions (off-campus)
##   - % faculty/staff sessions of off-campus total

import csv
import glob
import ipaddress
import os
import re
import sys


def parse_ip_csv(csv_path: str) -> dict:
    """Read the SharePoint-exported CSV and return a dictionary of IP ranges.

    Each key is an institution name (str).
    Each value is a list of (start_int, end_int) tuples representing IP ranges.

    The CSV has two columns: "Institution" and "IP Addresses".
    IP entries can be:
      - A single IP:       38.140.189.46
      - A range:           150.210.0.0 - 150.210.231.123
      - An IP with a note: 128.228.0.57 (OLS EZproxy Server)
      - A text-only note:  Note: The CO IP range is very fragmented...
    Multiple entries per institution are separated by newlines inside the cell.
    """
    institution_ranges = {}

    with open(csv_path, newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row['Institution'].strip()
            ip_text = row.get('IP Addresses', '')

            if not ip_text or not ip_text.strip():
                continue  # skip institutions with no IP data (e.g. Network Zone)

            ranges = []
            for entry in ip_text.strip().split('\n'):
                entry = entry.strip()

                # Remove parenthetical notes like "(OLS EZproxy Server)"
                entry = re.sub(r'\(.*?\)', '', entry).strip()

                # Skip lines that don't contain any IP address
                if not re.search(r'\d+\.\d+\.\d+\.\d+', entry):
                    continue

                if ' - ' in entry:
                    # It's a range like "150.210.0.0 - 150.210.231.123"
                    parts = entry.split(' - ')
                    try:
                        start = int(ipaddress.IPv4Address(parts[0].strip()))
                        end = int(ipaddress.IPv4Address(parts[1].strip()))
                        ranges.append((start, end))
                    except (ipaddress.AddressValueError, IndexError):
                        print(f"  Warning: Could not parse range '{entry}' for {name}")
                else:
                    # It's a single IP like "38.140.189.46"
                    try:
                        ip_int = int(ipaddress.IPv4Address(entry.strip()))
                        ranges.append((ip_int, ip_int))
                    except ipaddress.AddressValueError:
                        print(f"  Warning: Could not parse IP '{entry}' for {name}")

            if ranges:
                institution_ranges[name] = ranges

    return institution_ranges


def extract_ip(line: str) -> str | None:
    """Pull the first IP address out of a log line.

    EZproxy SPU log lines typically start with the IP address, like:
      146.95.12.34 session_id ...
    """
    match = re.search(r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})', line)
    if match:
        return match.group(1)
    return None


def classify_ip(ip_str: str, institution_ranges: dict) -> str:
    """Check which institution an IP belongs to.

    Returns the institution name, or "Unknown" if it doesn't match any range.
    """
    try:
        ip_int = int(ipaddress.IPv4Address(ip_str))
    except ipaddress.AddressValueError:
        return "Unknown"

    for name, ranges in institution_ranges.items():
        for start, end in ranges:
            if start <= ip_int <= end:
                return name
    return "Unknown"


# TODO: To add library-specific IP detection later, you could add a
# "Library IPs" column to the SharePoint CSV and load it here with
# a similar parse function. Then check each on-campus IP against
# those library ranges in the main loop below.


def main():
    """EZproxy log analysis: classify connections by CUNY institution."""

    if len(sys.argv) != 4:
        print("Usage: python3 ezp-analysis.py [IP CSV file] [log directory] [output.csv]")
        print("Example: python3 ezp-analysis.py data/institutions.csv logs/ output.csv")
        sys.exit(1)

    csv_path = sys.argv[1]   # path to the SharePoint-exported IP CSV
    dirname = sys.argv[2]    # directory containing .log files
    output = sys.argv[3]     # output CSV filename

    # --- Load institution IP ranges from CSV ---
    print(f"Loading IP ranges from {csv_path}...")
    institution_ranges = parse_ip_csv(csv_path)
    print(f"Loaded IP ranges for {len(institution_ranges)} institutions:")
    for name in sorted(institution_ranges.keys()):
        range_count = len(institution_ranges[name])
        print(f"  {name}: {range_count} range(s)")
    print()

    # Build a sorted list of institution names for consistent column order
    institution_names = sorted(institution_ranges.keys())

    # --- Build CSV header ---
    header_parts = ['filename', 'total connections']
    for name in institution_names:
        header_parts.append(name)
    header_parts.extend([
        'Unknown (off-campus or unmatched)',
        '# student sessions (off-campus)',
        '% student of off-campus sessions',
        '# fac/staff sessions (off-campus)',
        '% fac/staff of off-campus sessions',
    ])

    # --- Process log files ---
    log_files = sorted(glob.glob(os.path.join(dirname, '*.log')))

    if not log_files:
        print(f"No .log files found in {dirname}")
        sys.exit(1)

    print(f"Found {len(log_files)} log file(s). Starting analysis...\n")

    with open(output, 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(header_parts)

        for filepath in log_files:
            print(f"  Analyzing {filepath}...")

            with open(filepath, encoding='utf-8', errors='replace') as f:
                lines = [line.strip() for line in f]

            # Per-institution connection counters
            counts = {name: 0 for name in institution_names}
            unknown_count = 0
            total = 0

            # Session tracking for student/faculty counts
            studcount = 0
            faccount = 0
            seen_sessions = set()

            for line in lines:
                ip_str = extract_ip(line)
                if not ip_str:
                    continue  # skip lines without an IP

                total += 1
                institution = classify_ip(ip_str, institution_ranges)

                if institution == "Unknown":
                    unknown_count += 1
                else:
                    counts[institution] += 1

                # --- Student/faculty session counting (off-campus only) ---
                # Session IDs are only assigned to off-campus connections.
                # Multiple connections can share a session; we only count
                # each session once.
                session_match = re.search(r'.* - ([0-9A-Z].*?)\s', line)
                if session_match:
                    session = re.search(r'- .*', session_match.group())
                    session_id = session.group()[2:]
                    if session_id not in seen_sessions:
                        seen_sessions.add(session_id)
                        if re.search(r'Default\+OPAC\+Student', line):
                            studcount += 1
                        elif re.search(r'Default\+OPAC\+Staff', line):
                            faccount += 1

            # --- Calculate percentages ---
            total_offcamp_sessions = studcount + faccount
            if total_offcamp_sessions != 0:
                studfrac = round((studcount / total_offcamp_sessions) * 100, 1)
                facfrac = round((faccount / total_offcamp_sessions) * 100, 1)
            else:
                studfrac = 'n/a'
                facfrac = 'n/a'

            # --- Write row ---
            row = [filepath, total]
            for name in institution_names:
                row.append(counts[name])
            row.extend([
                unknown_count,
                studcount, studfrac,
                faccount, facfrac,
            ])
            writer.writerow(row)

    print(f"\nAll done!\n\nOutput: {output}")


if __name__ == '__main__':
    main()
