#!/usr/bin/env python3
"""
Cypherpunk Email Archive Parser

Parses mbox-style email archives from the Cypherpunk mailing list.
Extracts headers, body, and flags any issues for manual review.

Output: JSON file with parsed emails + separate file for flagged issues.
"""

import re
import json
import hashlib
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, field, asdict
from typing import Optional
from email.utils import parsedate_to_datetime
import argparse


@dataclass
class ParsedEmail:
    """Represents a parsed email with all extracted fields."""
    id: str  # Generated unique ID
    year: int
    source_file: str
    line_number: int

    # Headers
    from_raw: str = ""
    from_name: Optional[str] = None
    from_email: Optional[str] = None
    date_raw: str = ""
    date_parsed: Optional[str] = None  # ISO format
    subject: str = ""
    message_id: str = ""
    in_reply_to: Optional[str] = None
    references: list = field(default_factory=list)
    to: str = ""
    cc: str = ""
    content_type: str = "text/plain"

    # Body
    body: str = ""
    body_length: int = 0
    has_pgp: bool = False

    # Flags for manual review
    flags: list = field(default_factory=list)


class EmailParser:
    """Parser for Cypherpunk mailing list archives."""

    # Delimiter pattern for MHonArc archives
    DELIMITER = re.compile(
        r'^From cypherpunks@MHonArc\.venona\s+Wed Dec 17 23:17:14 2003$',
        re.MULTILINE
    )

    # From header patterns (order matters - try most specific first)
    FROM_PATTERNS = [
        # "Name" <email@domain.com>
        re.compile(r'^"?([^"<]+?)"?\s*<([^>]+)>$'),
        # email@domain.com (Name)
        re.compile(r'^([^\s(]+@[^\s(]+)\s*\(([^)]+)\)$'),
        # <email@domain.com>
        re.compile(r'^<([^>]+)>$'),
        # email@domain.com
        re.compile(r'^([^\s]+@[^\s]+)$'),
        # Just a name or identifier
        re.compile(r'^(.+)$'),
    ]

    # Email extraction pattern
    EMAIL_PATTERN = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')

    # Obfuscated email patterns (common substitutions)
    OBFUSCATION_PATTERNS = [
        (re.compile(r'[A-Z](?=[a-z0-9.-]+\.[a-z]{2,})'), '@'),  # Capital letter before domain
        (re.compile(r'\s*at\s*', re.IGNORECASE), '@'),
        (re.compile(r'\s*\[at\]\s*', re.IGNORECASE), '@'),
        (re.compile(r'\s*dot\s*', re.IGNORECASE), '.'),
        (re.compile(r'\s*\[dot\]\s*', re.IGNORECASE), '.'),
    ]

    def __init__(self, input_dir: Path, output_dir: Path):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.emails: list[ParsedEmail] = []
        self.flagged: list[dict] = []
        self.stats = {
            'total_parsed': 0,
            'total_flagged': 0,
            'by_year': {},
            'flag_types': {},
        }

    def parse_all(self):
        """Parse all .txt files in the input directory."""
        txt_files = sorted(self.input_dir.glob('cyp-*.txt'))

        for filepath in txt_files:
            print(f"Parsing {filepath.name}...")
            self.parse_file(filepath)

        self._save_output()
        self._print_summary()

    def parse_file(self, filepath: Path):
        """Parse a single archive file."""
        # Extract year from filename (e.g., cyp-1994.txt -> 1994)
        year_match = re.search(r'cyp-(\d{4})\.txt', filepath.name)
        year = int(year_match.group(1)) if year_match else 0

        with open(filepath, 'r', errors='replace') as f:
            content = f.read()

        # Split on delimiter
        parts = self.DELIMITER.split(content)

        # Track line numbers for each email
        line_num = 1
        for i, part in enumerate(parts):
            if i == 0:
                # Content before first delimiter (should be empty or minimal)
                line_num += part.count('\n')
                continue

            email = self._parse_email(part, year, filepath.name, line_num)
            self.emails.append(email)

            # Track stats
            self.stats['total_parsed'] += 1
            self.stats['by_year'][year] = self.stats['by_year'].get(year, 0) + 1

            if email.flags:
                self.stats['total_flagged'] += 1
                self.flagged.append({
                    'id': email.id,
                    'year': year,
                    'file': filepath.name,
                    'line': line_num,
                    'subject': email.subject,
                    'from_raw': email.from_raw,
                    'flags': email.flags,
                })
                for flag in email.flags:
                    flag_type = flag.split(':')[0]
                    self.stats['flag_types'][flag_type] = self.stats['flag_types'].get(flag_type, 0) + 1

            line_num += part.count('\n') + 1  # +1 for delimiter line

    def _parse_email(self, raw: str, year: int, source_file: str, line_num: int) -> ParsedEmail:
        """Parse a single email from raw text."""
        # Split headers from body (first blank line)
        parts = raw.split('\n\n', 1)
        header_text = parts[0].strip()
        body = parts[1] if len(parts) > 1 else ""

        # Parse headers
        headers = self._parse_headers(header_text)

        # Create email object
        email = ParsedEmail(
            id="",  # Will generate after parsing
            year=year,
            source_file=source_file,
            line_number=line_num,
        )

        # Extract standard headers
        email.from_raw = headers.get('from', '')
        email.date_raw = headers.get('date', '')
        email.subject = headers.get('subject', '')
        email.message_id = headers.get('message-id', '').strip('<>')
        email.in_reply_to = headers.get('in-reply-to', '').strip('<>') or None
        email.to = headers.get('to', '')
        email.cc = headers.get('cc', '')
        email.content_type = headers.get('content-type', 'text/plain').split(';')[0].strip()

        # Parse References header (can contain multiple message IDs)
        refs_raw = headers.get('references', '')
        if refs_raw:
            email.references = [r.strip('<>') for r in re.findall(r'<[^>]+>', refs_raw)]

        # Parse From header
        from_name, from_email, from_flags = self._parse_from(email.from_raw)
        email.from_name = from_name
        email.from_email = from_email
        email.flags.extend(from_flags)

        # Parse Date header
        date_parsed, date_flags = self._parse_date(email.date_raw)
        email.date_parsed = date_parsed
        email.flags.extend(date_flags)

        # Process body
        email.body = body.strip()
        email.body_length = len(email.body)
        email.has_pgp = 'BEGIN PGP' in body

        # Flag body issues
        if email.body_length < 10:
            email.flags.append(f"EMPTY_BODY: body length = {email.body_length}")

        # Flag missing required headers
        if not email.message_id:
            email.flags.append("MISSING_HEADER: Message-ID")
        if not email.subject:
            email.flags.append("MISSING_HEADER: Subject")

        # Generate unique ID
        email.id = self._generate_id(email)

        return email

    def _parse_headers(self, header_text: str) -> dict:
        """Parse email headers into a dictionary."""
        headers = {}
        current_key = None
        current_value = []

        for line in header_text.split('\n'):
            # Check for header continuation (starts with whitespace)
            if line and line[0] in ' \t':
                if current_key:
                    current_value.append(line.strip())
                continue

            # Save previous header
            if current_key:
                headers[current_key.lower()] = ' '.join(current_value)

            # Parse new header
            if ':' in line:
                key, _, value = line.partition(':')
                current_key = key.strip()
                current_value = [value.strip()]
            else:
                current_key = None
                current_value = []

        # Don't forget the last header
        if current_key:
            headers[current_key.lower()] = ' '.join(current_value)

        return headers

    def _parse_from(self, from_raw: str) -> tuple[Optional[str], Optional[str], list[str]]:
        """
        Parse From header to extract name and email.
        Returns: (name, email, flags)
        """
        flags = []
        from_raw = from_raw.strip()

        if not from_raw:
            return None, None, ["MISSING_HEADER: From"]

        # Try to extract email directly
        email_match = self.EMAIL_PATTERN.search(from_raw)

        if email_match:
            email = email_match.group(0).lower()

            # Try to extract name
            name = None
            for pattern in self.FROM_PATTERNS[:2]:  # First two patterns have names
                match = pattern.match(from_raw)
                if match:
                    groups = match.groups()
                    if len(groups) == 2:
                        if '@' in groups[0]:
                            name = groups[1].strip() if groups[1] else None
                        else:
                            name = groups[0].strip() if groups[0] else None
                    break

            return name, email, flags

        # No standard email found - check for obfuscation
        deobfuscated = from_raw
        was_obfuscated = False

        for pattern, replacement in self.OBFUSCATION_PATTERNS:
            new_val = pattern.sub(replacement, deobfuscated)
            if new_val != deobfuscated:
                was_obfuscated = True
                deobfuscated = new_val

        # Try to find email in deobfuscated version
        email_match = self.EMAIL_PATTERN.search(deobfuscated)

        if email_match:
            flags.append(f"OBFUSCATED_EMAIL: '{from_raw}' -> '{email_match.group(0)}'")
            return None, email_match.group(0).lower(), flags

        # Still no email - flag for review
        flags.append(f"NO_EMAIL_FOUND: '{from_raw}'")

        # Return the raw value as name
        return from_raw, None, flags

    def _parse_date(self, date_raw: str) -> tuple[Optional[str], list[str]]:
        """
        Parse Date header to ISO format.
        Returns: (iso_date_string, flags)
        """
        flags = []
        date_raw = date_raw.strip()

        if not date_raw:
            return None, ["MISSING_HEADER: Date"]

        # Check for corrupted date (contains unexpected data)
        if 'sendmail' in date_raw.lower() or 'id AA' in date_raw:
            flags.append(f"CORRUPTED_DATE: '{date_raw[:80]}...'")
            # Try to extract date from corrupted string
            date_match = re.search(r'((?:Mon|Tue|Wed|Thu|Fri|Sat|Sun),?\s+\d+\s+\w+\s+\d+\s+[\d:]+)', date_raw)
            if date_match:
                date_raw = date_match.group(1)

        # Try standard email date parsing
        try:
            dt = parsedate_to_datetime(date_raw)
            return dt.isoformat(), flags
        except (ValueError, TypeError):
            pass

        # Try alternative formats
        alt_formats = [
            '%a %b %d %H:%M:%S %Y',  # Unix ctime: Tue Sep 07 12:36:54 1999
            '%d %b %Y %H:%M:%S',     # 07 Sep 1999 12:36:54
            '%d %b %y %H:%M:%S',     # 07 Sep 99 12:36:54
            '%a, %d %b %Y %H:%M:%S', # Standard without timezone
            '%a, %d %b %y %H:%M:%S', # Two-digit year
        ]

        # Remove timezone info for parsing
        date_clean = re.sub(r'\s*[-+]\d{4}|\s*\([A-Z]{3,4}\)|\s*[A-Z]{3,4}$', '', date_raw).strip()

        for fmt in alt_formats:
            try:
                dt = datetime.strptime(date_clean, fmt)
                # Handle 2-digit years
                if dt.year < 100:
                    dt = dt.replace(year=dt.year + 1900)
                return dt.isoformat(), flags
            except ValueError:
                continue

        # Could not parse
        flags.append(f"UNPARSEABLE_DATE: '{date_raw}'")
        return None, flags

    def _generate_id(self, email: ParsedEmail) -> str:
        """Generate a unique ID for the email."""
        # Use message_id if available
        if email.message_id:
            return hashlib.sha256(email.message_id.encode()).hexdigest()[:16]

        # Otherwise create from content
        content = f"{email.year}:{email.date_raw}:{email.from_raw}:{email.subject}:{email.body[:100]}"
        return hashlib.sha256(content.encode()).hexdigest()[:16]

    def _save_output(self):
        """Save parsed emails and flagged items to files."""
        # Save all emails as JSON
        emails_file = self.output_dir / 'parsed_emails.json'
        with open(emails_file, 'w') as f:
            json.dump(
                [asdict(e) for e in self.emails],
                f,
                indent=2,
                ensure_ascii=False
            )
        print(f"\nSaved {len(self.emails):,} emails to {emails_file}")

        # Save flagged items separately for easy review
        flagged_file = self.output_dir / 'flagged_for_review.json'
        with open(flagged_file, 'w') as f:
            json.dump(self.flagged, f, indent=2, ensure_ascii=False)
        print(f"Saved {len(self.flagged):,} flagged items to {flagged_file}")

        # Save stats
        stats_file = self.output_dir / 'parse_stats.json'
        with open(stats_file, 'w') as f:
            json.dump(self.stats, f, indent=2)
        print(f"Saved stats to {stats_file}")

    def _print_summary(self):
        """Print parsing summary."""
        print("\n" + "=" * 60)
        print("PARSING SUMMARY")
        print("=" * 60)
        print(f"Total emails parsed: {self.stats['total_parsed']:,}")
        print(f"Total flagged for review: {self.stats['total_flagged']:,} ({100*self.stats['total_flagged']/self.stats['total_parsed']:.1f}%)")

        print("\nEmails by year:")
        for year in sorted(self.stats['by_year'].keys()):
            count = self.stats['by_year'][year]
            print(f"  {year}: {count:,}")

        print("\nFlag types:")
        for flag_type, count in sorted(self.stats['flag_types'].items(), key=lambda x: -x[1]):
            print(f"  {flag_type}: {count:,}")


def main():
    parser = argparse.ArgumentParser(description='Parse Cypherpunk email archives')
    parser.add_argument(
        '--input', '-i',
        type=Path,
        default=Path('/Users/maariabajwa/zypherpunk_cypherpunk/cryptome'),
        help='Input directory containing cyp-*.txt files'
    )
    parser.add_argument(
        '--output', '-o',
        type=Path,
        default=Path('/Users/maariabajwa/zypherpunk_cypherpunk/parsed'),
        help='Output directory for parsed data'
    )

    args = parser.parse_args()

    parser = EmailParser(args.input, args.output)
    parser.parse_all()


if __name__ == '__main__':
    main()
