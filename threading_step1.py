#!/usr/bin/env python3
"""
Threading Step 1: Build thread trees using in_reply_to relationships

This step creates parent-child relationships based on the in_reply_to header.
No subject matching yet - pure message_id based threading.

Output:
- threading_step1.json: All emails with parent_id and children fields added
- threading_step1_stats.json: Statistics about this step
"""

import json
from pathlib import Path
from collections import defaultdict, Counter


def build_thread_trees(input_path: Path, output_path: Path):
    """Build parent-child relationships using in_reply_to."""

    print("Loading emails...")
    with open(input_path, 'r') as f:
        emails = json.load(f)

    print(f"Loaded {len(emails):,} emails")

    # Build message_id index
    print("Building message_id index...")
    by_message_id = {e['message_id']: e for e in emails if e.get('message_id')}

    stats = {
        'total_emails': len(emails),
        'emails_with_message_id': len(by_message_id),
        'emails_with_in_reply_to': 0,
        'in_reply_to_matched': 0,
        'in_reply_to_unmatched': 0,
        'root_messages': 0,
        'child_messages': 0,
        'emails_as_parents': 0,
        'max_children': 0,
        'max_children_subject': '',
    }

    # Track parent-child relationships
    children_of = defaultdict(list)  # parent_message_id -> [child_ids]
    parent_of = {}  # message_id -> parent_message_id

    print("Building parent-child relationships...")
    for email in emails:
        msg_id = email.get('message_id')
        reply_to = email.get('in_reply_to')

        if reply_to:
            stats['emails_with_in_reply_to'] += 1

            if reply_to in by_message_id:
                # Found the parent
                stats['in_reply_to_matched'] += 1
                parent_of[msg_id] = reply_to
                children_of[reply_to].append(msg_id)
            else:
                # Parent not in archive
                stats['in_reply_to_unmatched'] += 1

    # Calculate additional stats
    stats['emails_as_parents'] = len(children_of)

    if children_of:
        max_parent = max(children_of.items(), key=lambda x: len(x[1]))
        stats['max_children'] = len(max_parent[1])
        stats['max_children_subject'] = by_message_id.get(max_parent[0], {}).get('subject', 'N/A')

    # Identify root messages (have children but no parent, OR no in_reply_to)
    for email in emails:
        msg_id = email.get('message_id')
        if msg_id not in parent_of:
            stats['root_messages'] += 1
        else:
            stats['child_messages'] += 1

    # Add threading info to each email
    print("Adding threading info to emails...")
    for email in emails:
        msg_id = email.get('message_id')

        # Add parent reference
        email['parent_id'] = parent_of.get(msg_id)

        # Add children references
        email['children_ids'] = children_of.get(msg_id, [])

        # Mark if this is a root (no parent in our archive)
        email['is_thread_root'] = msg_id not in parent_of

    # Distribution of children count
    children_distribution = Counter(len(v) for v in children_of.values())
    stats['children_distribution'] = dict(sorted(children_distribution.items()))

    # Save output
    output_path.mkdir(parents=True, exist_ok=True)

    emails_file = output_path / 'threading_step1.json'
    with open(emails_file, 'w') as f:
        json.dump(emails, f, indent=2, ensure_ascii=False)
    print(f"\nSaved {len(emails):,} emails to {emails_file}")

    stats_file = output_path / 'threading_step1_stats.json'
    with open(stats_file, 'w') as f:
        json.dump(stats, f, indent=2)
    print(f"Saved stats to {stats_file}")

    # Print summary
    print("\n" + "=" * 60)
    print("STEP 1: THREAD TREE BUILDING (in_reply_to only)")
    print("=" * 60)
    print(f"Total emails:              {stats['total_emails']:,}")
    print(f"Emails with message_id:    {stats['emails_with_message_id']:,}")
    print(f"Emails with in_reply_to:   {stats['emails_with_in_reply_to']:,} ({100*stats['emails_with_in_reply_to']/stats['total_emails']:.1f}%)")
    print(f"  - Parent found:          {stats['in_reply_to_matched']:,} ({100*stats['in_reply_to_matched']/stats['emails_with_in_reply_to']:.1f}%)")
    print(f"  - Parent not in archive: {stats['in_reply_to_unmatched']:,} ({100*stats['in_reply_to_unmatched']/stats['emails_with_in_reply_to']:.1f}%)")
    print(f"\nRoot messages:             {stats['root_messages']:,}")
    print(f"Child messages:            {stats['child_messages']:,}")
    print(f"Emails that are parents:   {stats['emails_as_parents']:,}")
    print(f"Max children for one msg:  {stats['max_children']}")
    print(f"  Subject: {stats['max_children_subject'][:50]}")

    print("\nChildren count distribution:")
    for num_children, count in sorted(stats['children_distribution'].items())[:10]:
        print(f"  {num_children} children: {count:,} emails")

    return stats


def main():
    input_path = Path('/Users/maariabajwa/zypherpunk_cypherpunk/filtered/emails_no_spam.json')
    output_path = Path('/Users/maariabajwa/zypherpunk_cypherpunk/threading')

    build_thread_trees(input_path, output_path)


if __name__ == '__main__':
    main()
