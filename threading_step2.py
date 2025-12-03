#!/usr/bin/env python3
"""
Threading Step 2: Identify orphan emails

Orphans are emails that:
1. Have "Re:" (or similar) in subject, indicating they're replies
2. But have no parent_id set (either no in_reply_to, or parent not in archive)

These are candidates for subject-based matching in Step 3.

Output:
- threading_step2.json: All emails with orphan_status field added
- threading_step2_stats.json: Statistics about orphans
- threading_step2_orphans.json: Just the orphan emails for review
"""

import json
import re
from pathlib import Path
from collections import Counter


def normalize_subject(subject: str) -> str:
    """
    Normalize subject for matching.
    Strips Re:, Fwd:, [tags], etc.
    """
    if not subject:
        return ''

    s = subject.strip()

    # Remove common prefixes (repeatedly, as they can be nested)
    prefixes = [
        r'^Re:\s*',
        r'^RE:\s*',
        r'^Fwd:\s*',
        r'^FWD:\s*',
        r'^Fw:\s*',
        r'^FW:\s*',
        r'^\[[\w\s-]+\]\s*',  # [tag] style prefixes
    ]

    changed = True
    while changed:
        changed = False
        for prefix in prefixes:
            new_s = re.sub(prefix, '', s, flags=re.IGNORECASE)
            if new_s != s:
                s = new_s.strip()
                changed = True

    return s.strip().lower()


def is_reply_subject(subject: str) -> bool:
    """Check if subject indicates this is a reply."""
    if not subject:
        return False

    reply_patterns = [
        r'^Re:',
        r'^RE:',
        r'^Fw:',
        r'^FW:',
        r'^Fwd:',
        r'^FWD:',
    ]

    return any(re.match(p, subject.strip()) for p in reply_patterns)


def identify_orphans(input_path: Path, output_path: Path):
    """Identify orphan emails that look like replies but have no parent."""

    print("Loading emails from Step 1...")
    with open(input_path, 'r') as f:
        emails = json.load(f)

    print(f"Loaded {len(emails):,} emails")

    stats = {
        'total_emails': len(emails),
        'emails_with_parent': 0,
        'emails_without_parent': 0,
        'reply_subjects_with_parent': 0,
        'reply_subjects_without_parent': 0,  # These are orphans
        'non_reply_without_parent': 0,  # True roots
        'orphan_types': {
            'has_in_reply_to_but_unmatched': 0,
            'no_in_reply_to_but_re_subject': 0,
        },
    }

    orphans = []
    subject_groups = {}  # normalized_subject -> [emails]

    print("Analyzing emails...")
    for email in emails:
        has_parent = email.get('parent_id') is not None
        is_reply = is_reply_subject(email.get('subject', ''))
        has_in_reply_to = email.get('in_reply_to') is not None

        # Track normalized subjects
        norm_subj = normalize_subject(email.get('subject', ''))
        if norm_subj:
            if norm_subj not in subject_groups:
                subject_groups[norm_subj] = []
            subject_groups[norm_subj].append(email['id'])

        if has_parent:
            stats['emails_with_parent'] += 1
            if is_reply:
                stats['reply_subjects_with_parent'] += 1
            email['orphan_status'] = 'has_parent'
        else:
            stats['emails_without_parent'] += 1

            if is_reply:
                stats['reply_subjects_without_parent'] += 1
                email['orphan_status'] = 'orphan_reply'

                # Categorize orphan type
                if has_in_reply_to:
                    stats['orphan_types']['has_in_reply_to_but_unmatched'] += 1
                    email['orphan_type'] = 'in_reply_to_unmatched'
                else:
                    stats['orphan_types']['no_in_reply_to_but_re_subject'] += 1
                    email['orphan_type'] = 'no_in_reply_to'

                orphans.append({
                    'id': email['id'],
                    'subject': email.get('subject', ''),
                    'normalized_subject': norm_subj,
                    'from': email.get('from_email') or email.get('from_raw', ''),
                    'date': email.get('date_parsed') or email.get('date_raw', ''),
                    'year': email.get('year'),
                    'in_reply_to': email.get('in_reply_to'),
                    'orphan_type': email['orphan_type'],
                })
            else:
                stats['non_reply_without_parent'] += 1
                email['orphan_status'] = 'true_root'

        # Add normalized subject to email
        email['normalized_subject'] = norm_subj

    # Analyze subject groups for potential matches
    stats['unique_normalized_subjects'] = len(subject_groups)
    stats['subjects_with_multiple_emails'] = sum(1 for v in subject_groups.values() if len(v) > 1)
    stats['largest_subject_group'] = max(len(v) for v in subject_groups.values()) if subject_groups else 0

    # Find subjects with both orphans and potential parents
    orphan_subjects = set(o['normalized_subject'] for o in orphans if o['normalized_subject'])
    subjects_with_roots = set()

    by_id = {e['id']: e for e in emails}
    for norm_subj, email_ids in subject_groups.items():
        for eid in email_ids:
            email = by_id[eid]
            if email.get('orphan_status') == 'true_root' or email.get('orphan_status') == 'has_parent':
                subjects_with_roots.add(norm_subj)
                break

    matchable_orphan_subjects = orphan_subjects & subjects_with_roots
    stats['orphan_subjects_with_potential_match'] = len(matchable_orphan_subjects)
    stats['orphan_subjects_no_match'] = len(orphan_subjects - subjects_with_roots)

    # Count orphans that could be matched
    matchable_orphans = [o for o in orphans if o['normalized_subject'] in matchable_orphan_subjects]
    stats['orphans_matchable'] = len(matchable_orphans)
    stats['orphans_unmatchable'] = len(orphans) - len(matchable_orphans)

    # Save outputs
    output_path.mkdir(parents=True, exist_ok=True)

    emails_file = output_path / 'threading_step2.json'
    with open(emails_file, 'w') as f:
        json.dump(emails, f, indent=2, ensure_ascii=False)
    print(f"\nSaved {len(emails):,} emails to {emails_file}")

    orphans_file = output_path / 'threading_step2_orphans.json'
    with open(orphans_file, 'w') as f:
        json.dump(orphans, f, indent=2, ensure_ascii=False)
    print(f"Saved {len(orphans):,} orphans to {orphans_file}")

    stats_file = output_path / 'threading_step2_stats.json'
    with open(stats_file, 'w') as f:
        json.dump(stats, f, indent=2)
    print(f"Saved stats to {stats_file}")

    # Print summary
    print("\n" + "=" * 60)
    print("STEP 2: ORPHAN IDENTIFICATION")
    print("=" * 60)
    print(f"Total emails:                  {stats['total_emails']:,}")
    print(f"\nEmails with parent (Step 1):   {stats['emails_with_parent']:,}")
    print(f"Emails without parent:         {stats['emails_without_parent']:,}")
    print(f"  - True roots (no Re:):       {stats['non_reply_without_parent']:,}")
    print(f"  - Orphan replies (Re:):      {stats['reply_subjects_without_parent']:,}")

    print(f"\nOrphan breakdown:")
    print(f"  - Has in_reply_to but unmatched: {stats['orphan_types']['has_in_reply_to_but_unmatched']:,}")
    print(f"  - No in_reply_to, just Re:       {stats['orphan_types']['no_in_reply_to_but_re_subject']:,}")

    print(f"\nSubject analysis:")
    print(f"  Unique normalized subjects:  {stats['unique_normalized_subjects']:,}")
    print(f"  Subjects with 2+ emails:     {stats['subjects_with_multiple_emails']:,}")
    print(f"  Largest subject group:       {stats['largest_subject_group']:,}")

    print(f"\nOrphan matching potential:")
    print(f"  Orphans with potential match: {stats['orphans_matchable']:,} ({100*stats['orphans_matchable']/len(orphans):.1f}%)")
    print(f"  Orphans with no match:        {stats['orphans_unmatchable']:,} ({100*stats['orphans_unmatchable']/len(orphans):.1f}%)")

    return stats


def main():
    input_path = Path('/Users/maariabajwa/zypherpunk_cypherpunk/threading/threading_step1.json')
    output_path = Path('/Users/maariabajwa/zypherpunk_cypherpunk/threading')

    identify_orphans(input_path, output_path)


if __name__ == '__main__':
    main()
