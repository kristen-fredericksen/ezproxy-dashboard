"""Quick analysis of a CSI EZproxy SPU log file.

Reads the IP CSV and the log, then prints a summary of:
  - Connections by institution (on-campus vs off-campus)
  - Unique users (by barcode) and sessions
  - Authenticated vs unauthenticated connections
  - Top databases/resources used
  - Busiest days and hours
  - HTTP status code breakdown
  - Connections referred from Primo
"""

import csv
import ipaddress
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime
from urllib.parse import urlparse

# ---------------------------------------------------------------------------
# IP parsing (reused from ezp-analysis.py)
# ---------------------------------------------------------------------------

def parse_ip_csv(csv_path: str) -> dict:
    """Read the SharePoint-exported CSV and return {institution: [(start, end), ...]}."""
    institution_ranges = {}
    with open(csv_path, newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row['Institution'].strip()
            ip_text = row.get('IP Addresses', '')
            if not ip_text or not ip_text.strip():
                continue
            ranges = []
            for entry in ip_text.strip().split('\n'):
                entry = re.sub(r'\(.*?\)', '', entry).strip()
                if not re.search(r'\d+\.\d+\.\d+\.\d+', entry):
                    continue
                if ' - ' in entry:
                    parts = entry.split(' - ')
                    try:
                        start = int(ipaddress.IPv4Address(parts[0].strip()))
                        end = int(ipaddress.IPv4Address(parts[1].strip()))
                        ranges.append((start, end))
                    except (ipaddress.AddressValueError, IndexError):
                        pass
                else:
                    try:
                        ip_int = int(ipaddress.IPv4Address(entry.strip()))
                        ranges.append((ip_int, ip_int))
                    except ipaddress.AddressValueError:
                        pass
            if ranges:
                institution_ranges[name] = ranges
    return institution_ranges


def classify_ip(ip_str: str, institution_ranges: dict) -> str:
    """Return institution name or 'Off-campus' for an IP."""
    try:
        ip_int = int(ipaddress.IPv4Address(ip_str))
    except ipaddress.AddressValueError:
        return "Off-campus"
    for name, ranges in institution_ranges.items():
        for start, end in ranges:
            if start <= ip_int <= end:
                return name
    return "Off-campus"


# ---------------------------------------------------------------------------
# Log parsing
# ---------------------------------------------------------------------------

def parse_log_line(line: str) -> dict | None:
    """Parse a tab-separated SPU log line into a dictionary.

    Expected format:
    [timestamp] \t IP \t barcode \t session_token \t action \t referrer \t URL \t status
    """
    # Extract timestamp
    ts_match = re.match(r'\[(.+?)\]', line)
    if not ts_match:
        return None

    parts = line.split('\t')
    # Remove leading/trailing whitespace from each part
    parts = [p.strip() for p in parts]

    if len(parts) < 8:
        return None

    try:
        timestamp = datetime.strptime(ts_match.group(1), '%d/%b/%Y:%H:%M:%S %z')
    except ValueError:
        return None

    return {
        'timestamp': timestamp,
        'ip': parts[1],
        'emplid': parts[2] if parts[2] != '-' else None,  # 8-digit CUNY ID
        'session': parts[3] if parts[3] != '-' else None,
        'action': parts[4],
        'referrer': parts[5] if parts[5] != '-' else None,
        'url': parts[6],
        'status': parts[7],
    }


def extract_resource_name(url: str) -> str:
    """Pull a clean database/resource name from a URL.

    Extracts the domain and simplifies it (e.g., 'jstor.org', 'ebscohost.com').
    """
    try:
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        # Remove common subdomains
        for prefix in ['www.', 'search.', 'login.', 'link.', 'go.', 'find.',
                       'openurl.', 'advance.', 'logon.']:
            if domain.startswith(prefix):
                domain = domain[len(prefix):]
        return domain
    except Exception:
        return 'unknown'


# ---------------------------------------------------------------------------
# Main analysis
# ---------------------------------------------------------------------------

def main():
    if len(sys.argv) != 3:
        print("Usage: python3 analyze_log.py [IP CSV file] [log file]")
        sys.exit(1)

    csv_path = sys.argv[1]
    log_path = sys.argv[2]

    # Load IPs
    institution_ranges = parse_ip_csv(csv_path)

    # Parse all log lines
    records = []
    with open(log_path, encoding='utf-8', errors='replace') as f:
        for line in f:
            parsed = parse_log_line(line.strip())
            if parsed:
                records.append(parsed)

    if not records:
        print("No records parsed from log file.")
        sys.exit(1)

    # --- Basic counts ---
    total = len(records)
    print(f"{'='*60}")
    print(f"  EZproxy SPU Log Analysis")
    print(f"  File: {log_path}")
    print(f"  Total connections: {total:,}")
    print(f"  Date range: {records[0]['timestamp'].strftime('%b %d')} – "
          f"{records[-1]['timestamp'].strftime('%b %d, %Y')}")
    print(f"{'='*60}\n")

    # --- Connections by institution ---
    inst_counts = Counter()
    for r in records:
        inst = classify_ip(r['ip'], institution_ranges)
        r['institution'] = inst  # tag each record for later use
        inst_counts[inst] += 1

    on_campus_total = sum(c for name, c in inst_counts.items() if name != 'Off-campus')

    print("CONNECTIONS BY SOURCE")
    print("-" * 50)
    print(f"  {'On-campus (all CUNY)':<35} {on_campus_total:>6}  ({on_campus_total/total*100:.1f}%)")
    print(f"  {'Off-campus':<35} {inst_counts['Off-campus']:>6}  ({inst_counts['Off-campus']/total*100:.1f}%)")
    print()

    # Show institution breakdown (skip off-campus, only show non-zero)
    print("  On-campus breakdown:")
    for name, count in inst_counts.most_common():
        if name == 'Off-campus' or count == 0:
            continue
        print(f"    {name:<40} {count:>5}  ({count/total*100:.1f}%)")
    print()

    # --- Unique users and sessions ---
    emplids = set()
    sessions = set()
    for r in records:
        if r['emplid']:
            emplids.add(r['emplid'])
        if r['session']:
            sessions.add(r['session'])

    authenticated = sum(1 for r in records if r['emplid'])
    unauthenticated = total - authenticated

    print("USERS & SESSIONS")
    print("-" * 50)
    print(f"  Unique EMPLIDs:                {len(emplids):>6}")
    print(f"  Unique session tokens:         {len(sessions):>6}")
    print(f"  Authenticated connections:     {authenticated:>6}  ({authenticated/total*100:.1f}%)")
    print(f"  Unauthenticated connections:   {unauthenticated:>6}  ({unauthenticated/total*100:.1f}%)")
    print()

    # --- Top databases/resources ---
    resource_counts = Counter()
    for r in records:
        resource = extract_resource_name(r['url'])
        if resource:
            resource_counts[resource] += 1

    print("TOP 15 RESOURCES (by domain)")
    print("-" * 50)
    for resource, count in resource_counts.most_common(15):
        bar = '█' * int(count / total * 100)
        print(f"  {resource:<35} {count:>5}  ({count/total*100:.1f}%)  {bar}")
    print()

    # --- Primo referrals ---
    primo_referrals = sum(1 for r in records
                          if r['referrer'] and 'primo' in r['referrer'].lower())
    print("DISCOVERY")
    print("-" * 50)
    print(f"  Connections referred from Primo:  {primo_referrals:>5}  ({primo_referrals/total*100:.1f}%)")
    print()

    # --- Action types ---
    action_counts = Counter(r['action'] for r in records)
    print("ACTION TYPES")
    print("-" * 50)
    for action, count in action_counts.most_common():
        print(f"  {action:<35} {count:>5}  ({count/total*100:.1f}%)")
    print()

    # --- HTTP status codes ---
    status_counts = Counter(r['status'] for r in records)
    print("HTTP STATUS CODES")
    print("-" * 50)
    for status, count in status_counts.most_common():
        print(f"  {status:<35} {count:>5}  ({count/total*100:.1f}%)")
    print()

    # --- Busiest days ---
    day_counts = Counter(r['timestamp'].strftime('%a %b %d') for r in records)
    print("BUSIEST DAYS (top 10)")
    print("-" * 50)
    for day, count in day_counts.most_common(10):
        bar = '█' * int(count / max(day_counts.values()) * 30)
        print(f"  {day:<20} {count:>5}  {bar}")
    print()

    # --- Busiest hours ---
    hour_counts = Counter(r['timestamp'].hour for r in records)
    print("CONNECTIONS BY HOUR")
    print("-" * 50)
    for hour in range(24):
        count = hour_counts.get(hour, 0)
        bar = '█' * int(count / max(hour_counts.values()) * 40) if count > 0 else ''
        ampm = f"{hour % 12 or 12}{'am' if hour < 12 else 'pm'}"
        print(f"  {ampm:>5}  {count:>5}  {bar}")
    print()


if __name__ == '__main__':
    main()
