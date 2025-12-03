#!/usr/bin/env python3
"""
Threading Step 3c: Merge same-subject emails by time proximity

For emails marked as true_root with identical normalized subjects,
merge them into a single thread if they're within a short time window.
This catches replies where remailers strip both headers and "Re:" prefix.

Strategy:
1. Group all true_root emails by normalized subject
2. For groups with multiple true_roots, sort by date
3. Link later emails to the earliest one if within time threshold

Input: threading/threading_step3b.json
Output: threading/threading_step3c.json, threading_step3c_stats.json
"""

import json
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict


def parse_date(date_str: str) -> datetime:
    """Parse ISO date string to datetime (naive, no timezone)."""
    if not date_str:
        return None
    try:
        date_str = date_str.replace('Z', '')
        if '+' in date_str:
            date_str = date_str.rsplit('+', 1)[0]
        if date_str.count('-') > 2:
            if 'T' in date_str:
                t_pos = date_str.index('T')
                last_dash = date_str.rfind('-')
                if last_dash > t_pos:
                    date_str = date_str[:last_dash]
        return datetime.fromisoformat(date_str.replace('T', ' ').split('.')[0])
    except (ValueError, TypeError):
        return None


def would_create_cycle(child_msg_id: str, parent_msg_id: str, by_message_id: dict) -> bool:
    """Check if setting child's parent to parent_msg_id would create a cycle."""
    visited = set()
    current_id = parent_msg_id

    while current_id:
        if current_id == child_msg_id:
            return True
        if current_id in visited:
            return True
        visited.add(current_id)

        parent_email = by_message_id.get(current_id)
        if not parent_email:
            break
        current_id = parent_email.get('parent_id')

    return False


def merge_by_time_proximity(input_path: Path, output_path: Path, max_days: int = 3):
    """Merge same-subject true_root emails by time proximity."""

    print("Loading emails from Step 3b...")
    with open(input_path, 'r') as f:
        emails = json.load(f)

    print(f"Loaded {len(emails):,} emails")

    # Build indexes
    print("Building indexes...")
    by_message_id = {e['message_id']: e for e in emails if e.get('message_id')}

    # Group true_root emails by normalized subject
    print("Grouping true_root emails by subject...")
    true_roots_by_subject = defaultdict(list)

    for email in emails:
        if email.get('orphan_status') == 'true_root':
            norm_subj = email.get('normalized_subject', '')
            if norm_subj and len(norm_subj) > 3:  # Skip very short subjects
                true_roots_by_subject[norm_subj].append(email)

    # Find subjects with multiple true_roots
    multi_root_subjects = {
        subj: roots for subj, roots in true_roots_by_subject.items()
        if len(roots) > 1
    }

    print(f"Found {len(multi_root_subjects):,} subjects with multiple true_roots")

    stats = {
        'total_emails': len(emails),
        'subjects_with_multi_roots': len(multi_root_subjects),
        'total_true_roots_in_groups': sum(len(roots) for roots in multi_root_subjects.values()),
        'matches_found': 0,
        'cycles_prevented': 0,
        'too_far_apart': 0,
    }

    # Process each group
    print(f"Merging by time proximity (within {max_days} days)...")
    matched_count = 0

    for norm_subj, roots in multi_root_subjects.items():
        # Sort by date
        roots_with_dates = []
        for r in roots:
            dt = parse_date(r.get('date_parsed'))
            if dt:
                roots_with_dates.append((r, dt))

        if len(roots_with_dates) < 2:
            continue

        roots_with_dates.sort(key=lambda x: x[1])

        # The earliest becomes the canonical root
        canonical_root, canonical_date = roots_with_dates[0]

        # Link later ones to the canonical root (if within time window)
        for later_root, later_date in roots_with_dates[1:]:
            days_apart = (later_date - canonical_date).days

            if days_apart > max_days:
                stats['too_far_apart'] += 1
                # This one becomes a new canonical root for subsequent emails
                canonical_root = later_root
                canonical_date = later_date
                continue

            # Check for cycles
            if would_create_cycle(later_root['message_id'], canonical_root['message_id'], by_message_id):
                stats['cycles_prevented'] += 1
                continue

            # Link them
            later_root['parent_id'] = canonical_root['message_id']
            later_root['orphan_status'] = 'matched_by_time_proximity'
            later_root['time_proximity_days'] = days_apart

            # Update parent's children
            if 'children_ids' not in canonical_root:
                canonical_root['children_ids'] = []
            if later_root['message_id'] not in canonical_root['children_ids']:
                canonical_root['children_ids'].append(later_root['message_id'])

            # Update index
            by_message_id[later_root['message_id']] = later_root

            stats['matches_found'] += 1
            matched_count += 1

            if matched_count % 500 == 0:
                print(f"  Matched {matched_count:,} emails...")

    # Save outputs
    output_path.mkdir(parents=True, exist_ok=True)

    emails_file = output_path / 'threading_step3c.json'
    with open(emails_file, 'w') as f:
        json.dump(emails, f, indent=2, ensure_ascii=False)
    print(f"\nSaved {len(emails):,} emails to {emails_file}")

    stats_file = output_path / 'threading_step3c_stats.json'
    with open(stats_file, 'w') as f:
        json.dump(stats, f, indent=2)
    print(f"Saved stats to {stats_file}")

    # Print summary
    print("\n" + "=" * 60)
    print("STEP 3c: TIME-PROXIMITY MATCHING")
    print("=" * 60)
    print(f"Total emails:              {stats['total_emails']:,}")
    print(f"Subjects w/ multi-roots:   {stats['subjects_with_multi_roots']:,}")
    print(f"True roots in groups:      {stats['total_true_roots_in_groups']:,}")
    print(f"Matches found:             {stats['matches_found']:,}")
    print(f"Too far apart (>{max_days}d):     {stats['too_far_apart']:,}")
    print(f"Cycles prevented:          {stats['cycles_prevented']:,}")

    return stats


def main():
    input_path = Path('/Users/maariabajwa/zypherpunk_cypherpunk/threading/threading_step3b.json')
    output_path = Path('/Users/maariabajwa/zypherpunk_cypherpunk/threading')

    # Use 3-day window for time proximity matching
    merge_by_time_proximity(input_path, output_path, max_days=3)


if __name__ == '__main__':
    main()
