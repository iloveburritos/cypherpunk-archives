#!/usr/bin/env python3
"""
Cypherpunk Email Archive Cleanup

Fixes flagged issues from the initial parse:
1. NO_EMAIL_FOUND - Lookup known entities and fill in emails
2. MISSING_HEADER: Message-ID - Remove spam messages
3. OBFUSCATED_EMAIL - Already corrected, just remove flag
4. Leave Anonymous/unknown senders as-is
"""

import json
import re
from pathlib import Path
from collections import Counter


# Known entity email mappings (derived from most frequent sender addresses)
KNOWN_ENTITIES = {
    # John Gilmore
    'gnu': 'gnu@toad.com',
    'gnu (john gilmore)': 'gnu@toad.com',

    # Eric Hughes
    'hughes (eric hughes)': 'hughes@ah.com',
    'hughes': 'hughes@ah.com',

    # Timothy C. May
    'tcmay': 'tcmay@got.net',
    'tcmay (timothy c. may)': 'tcmay@got.net',

    # Ed Carp
    'khijol!erc (ed carp [sysadmin])': 'erc@dal1820.computek.net',
    'khijol!erc (ed carp)': 'erc@dal1820.computek.net',
    '"ed carp [khijol sysadmin]" <khijol!erc>': 'erc@dal1820.computek.net',
    'erc@khijol (ed carp)': 'erc@dal1820.computek.net',

    # Peter Trei
    '"peter trei" <trei>': 'trei@process.com',

    # Peter Davidson
    'wet!naga (peter davidson)': 'naga@laphroaig.mira.net.au',

    # Jim Miller
    'jim@bilbo (jim miller)': 'jim@bilbo.suite.com',

    # Hugh Daniel
    'hugh (hugh daniel)': 'hugh@ecotone.toad.com',

    # L. Detweiler
    '"l. detweiler" <ld231782>': 'ld231782@longs.lance.colostate.edu',

    # Phil Karn
    'karn (phil karn)': 'karn@qualcomm.com',

    # Lee Tien
    'tien (lee tien)': 'tien@well.com',

    # Chip Morningstar
    'grand-central!amix!chip (chip morningstar -- "software without moving parts")': 'chip@communities.com',

    # Russell Nelson
    'nelson@crynwr (russell nelson)': 'nelson@crynwr.com',

    # Forrest Aldrich
    'forrest aldrich <visgraph!forrie>': 'forrie@visgraph.com',

    # David Taffs (fix double @)
    'dat@@.spock.ebt.com (david taffs)': 'dat@spock.ebt.com',

    # Owner/system addresses
    'owner-cypherpunks': 'owner-cypherpunks@toad.com',
    'mail delivery subsystem <mailer-daemon>': 'mailer-daemon@toad.com',
}

# Patterns that indicate intentionally anonymous senders (leave as-is)
ANONYMOUS_PATTERNS = [
    r'^anonymous',
    r'^nobody@',
    r'^<>$',
    r'@nowhere',
    r'use-author-address-header',
    r'^0x[0-9a-f]+@',
    r'^[0-9a-f]{8}@',
    r'damien lucifer',
    r'root@hellspawn',
]


def is_anonymous(from_raw: str) -> bool:
    """Check if this is an intentionally anonymous sender."""
    from_lower = from_raw.lower()
    return any(re.search(p, from_lower) for p in ANONYMOUS_PATTERNS)


def fix_obfuscated_email(from_raw: str) -> str:
    """Fix common email obfuscations (N instead of @)."""
    # Pattern: letterN followed by domain-like structure
    # e.g., hughesNsoda.berkeley.edu -> hughes@soda.berkeley.edu
    fixed = re.sub(
        r'([a-zA-Z0-9._-]+)N([a-zA-Z0-9.-]+\.[a-zA-Z]{2,})',
        r'\1@\2',
        from_raw
    )
    return fixed


def is_spam(email: dict) -> bool:
    """Detect spam messages based on various signals."""
    # Missing Message-ID is a strong spam indicator for this archive
    if any('MISSING_HEADER: Message-ID' in f for f in email.get('flags', [])):
        return True

    # Additional spam patterns in subject
    subject = email.get('subject', '').lower()
    spam_subjects = [
        'financial information',
        'urgent buy',
        'buy alert',
        'sizzler',
        'immediate cash',
        'cable descrambler',
        'save your life',
        'online business',
        'make money',
        'work from home',
        'mlm',
        'adult',
        'xxx',
    ]
    if any(p in subject for p in spam_subjects):
        return True

    # Spam-like sender patterns
    from_email = email.get('from_email', '') or ''
    if re.match(r'^\d{8}@', from_email):  # Numeric sender IDs
        return True

    return False


def cleanup_emails(input_path: Path, output_path: Path):
    """Main cleanup function."""
    print("Loading parsed emails...")
    with open(input_path, 'r') as f:
        emails = json.load(f)

    print(f"Loaded {len(emails):,} emails")

    stats = {
        'original_count': len(emails),
        'spam_removed': 0,
        'emails_fixed': 0,
        'known_entity_lookups': 0,
        'obfuscation_fixes': 0,
        'anonymous_kept': 0,
        'still_no_email': 0,
    }

    cleaned_emails = []
    still_flagged = []

    for email in emails:
        # Check for spam first
        if is_spam(email):
            stats['spam_removed'] += 1
            continue

        # Process flags
        new_flags = []
        was_fixed = False

        for flag in email.get('flags', []):
            if flag.startswith('NO_EMAIL_FOUND:'):
                from_raw = email['from_raw']
                from_lower = from_raw.lower().strip()

                # Check known entities
                if from_lower in KNOWN_ENTITIES:
                    email['from_email'] = KNOWN_ENTITIES[from_lower]
                    stats['known_entity_lookups'] += 1
                    was_fixed = True
                    # Don't add flag back

                # Check if anonymous (keep flag but mark differently)
                elif is_anonymous(from_raw):
                    stats['anonymous_kept'] += 1
                    # Keep original flag for awareness but don't mark as error
                    new_flags.append(f"ANONYMOUS_SENDER: '{from_raw}'")

                else:
                    # Still can't resolve - keep flag
                    stats['still_no_email'] += 1
                    new_flags.append(flag)

            elif flag.startswith('OBFUSCATED_EMAIL:'):
                # These were already fixed during parsing, just remove flag
                stats['obfuscation_fixes'] += 1
                was_fixed = True
                # Don't add flag back

            elif flag.startswith('MISSING_HEADER: Message-ID'):
                # Should have been caught by spam filter, but just in case
                new_flags.append(flag)

            else:
                # Keep other flags as-is
                new_flags.append(flag)

        if was_fixed:
            stats['emails_fixed'] += 1

        email['flags'] = new_flags
        cleaned_emails.append(email)

        if new_flags:
            still_flagged.append({
                'id': email['id'],
                'year': email['year'],
                'file': email['source_file'],
                'line': email['line_number'],
                'subject': email['subject'],
                'from_raw': email['from_raw'],
                'from_email': email.get('from_email'),
                'flags': new_flags,
            })

    # Save cleaned emails
    output_path.mkdir(parents=True, exist_ok=True)

    cleaned_file = output_path / 'cleaned_emails.json'
    with open(cleaned_file, 'w') as f:
        json.dump(cleaned_emails, f, indent=2, ensure_ascii=False)
    print(f"\nSaved {len(cleaned_emails):,} cleaned emails to {cleaned_file}")

    # Save remaining flagged items
    flagged_file = output_path / 'remaining_flags.json'
    with open(flagged_file, 'w') as f:
        json.dump(still_flagged, f, indent=2, ensure_ascii=False)
    print(f"Saved {len(still_flagged):,} remaining flagged items to {flagged_file}")

    # Save stats
    stats['final_count'] = len(cleaned_emails)
    stats['final_flagged'] = len(still_flagged)

    stats_file = output_path / 'cleanup_stats.json'
    with open(stats_file, 'w') as f:
        json.dump(stats, f, indent=2)

    # Print summary
    print("\n" + "=" * 60)
    print("CLEANUP SUMMARY")
    print("=" * 60)
    print(f"Original emails:        {stats['original_count']:,}")
    print(f"Spam removed:           {stats['spam_removed']:,}")
    print(f"Final email count:      {stats['final_count']:,}")
    print()
    print(f"Emails fixed:           {stats['emails_fixed']:,}")
    print(f"  - Known entity lookup: {stats['known_entity_lookups']:,}")
    print(f"  - Obfuscation fixes:   {stats['obfuscation_fixes']:,}")
    print()
    print(f"Anonymous kept as-is:   {stats['anonymous_kept']:,}")
    print(f"Still missing email:    {stats['still_no_email']:,}")
    print(f"Total remaining flags:  {stats['final_flagged']:,}")

    # Breakdown of remaining flags
    print("\nRemaining flag breakdown:")
    flag_types = Counter()
    for item in still_flagged:
        for flag in item['flags']:
            flag_type = flag.split(':')[0]
            flag_types[flag_type] += 1
    for ftype, count in flag_types.most_common():
        print(f"  {ftype}: {count:,}")


def main():
    input_path = Path('/Users/maariabajwa/zypherpunk_cypherpunk/parsed/parsed_emails.json')
    output_path = Path('/Users/maariabajwa/zypherpunk_cypherpunk/cleaned')

    cleanup_emails(input_path, output_path)


if __name__ == '__main__':
    main()
