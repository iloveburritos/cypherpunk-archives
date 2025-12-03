#!/usr/bin/env python3
"""
Threading Step 4: Create thread metadata

Builds complete thread objects with:
- All messages in the thread
- Participant list
- Date range
- Message count
- Thread depth
- Root message info

Output:
- threading_step4_threads.json: Complete thread objects
- threading_step4_emails.json: Emails with thread_id added
- threading_step4_stats.json: Thread statistics
"""

import json
from pathlib import Path
from datetime import datetime
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


def build_threads(input_path: Path, output_path: Path):
    """Build complete thread objects from parent-child relationships."""

    print("Loading emails from Step 3...")
    with open(input_path, 'r') as f:
        emails = json.load(f)

    print(f"Loaded {len(emails):,} emails")

    # Build indexes
    by_id = {e['id']: e for e in emails}
    by_message_id = {e['message_id']: e for e in emails if e.get('message_id')}

    # Find all root messages (no parent)
    roots = [e for e in emails if e.get('parent_id') is None]
    print(f"Found {len(roots):,} root messages")

    # Build threads from each root
    print("Building threads...")
    threads = []

    def get_thread_messages(root_email, depth=0):
        """Recursively get all messages in a thread."""
        messages = [(root_email, depth)]

        for child_msg_id in root_email.get('children_ids', []):
            child = by_message_id.get(child_msg_id)
            if child:
                messages.extend(get_thread_messages(child, depth + 1))

        return messages

    for i, root in enumerate(roots):
        thread_messages = get_thread_messages(root)

        # Sort by date
        thread_messages.sort(
            key=lambda x: parse_date(x[0].get('date_parsed')) or datetime.min
        )

        # Extract info
        message_ids = [m[0]['id'] for m in thread_messages]
        participants = list(set(
            m[0].get('from_email') or m[0].get('from_raw', '')
            for m in thread_messages
            if m[0].get('from_email') or m[0].get('from_raw')
        ))

        dates = [
            parse_date(m[0].get('date_parsed'))
            for m in thread_messages
            if parse_date(m[0].get('date_parsed'))
        ]

        max_depth = max(m[1] for m in thread_messages) if thread_messages else 0

        thread = {
            'id': f"thread_{root['id']}",
            'root_message_id': root['id'],
            'subject': root.get('subject', ''),
            'normalized_subject': root.get('normalized_subject', ''),
            'message_ids': message_ids,
            'message_count': len(message_ids),
            'participants': participants,
            'participant_count': len(participants),
            'date_start': min(dates).isoformat() if dates else None,
            'date_end': max(dates).isoformat() if dates else None,
            'year_start': min(dates).year if dates else None,
            'depth': max_depth,
            'root_author': root.get('from_email') or root.get('from_raw', ''),
            'has_pgp': any(m[0].get('has_pgp') for m in thread_messages),
        }

        threads.append(thread)

        # Mark emails with thread_id
        for msg, _ in thread_messages:
            msg['thread_id'] = thread['id']

        if (i + 1) % 10000 == 0:
            print(f"  Processed {i+1:,} threads...")

    # Calculate stats
    stats = {
        'total_emails': len(emails),
        'total_threads': len(threads),
        'single_message_threads': sum(1 for t in threads if t['message_count'] == 1),
        'multi_message_threads': sum(1 for t in threads if t['message_count'] > 1),
        'avg_messages_per_thread': sum(t['message_count'] for t in threads) / len(threads) if threads else 0,
        'max_messages_in_thread': max(t['message_count'] for t in threads) if threads else 0,
        'avg_participants': sum(t['participant_count'] for t in threads) / len(threads) if threads else 0,
        'max_participants': max(t['participant_count'] for t in threads) if threads else 0,
        'avg_depth': sum(t['depth'] for t in threads) / len(threads) if threads else 0,
        'max_depth': max(t['depth'] for t in threads) if threads else 0,
        'threads_with_pgp': sum(1 for t in threads if t['has_pgp']),
    }

    # Thread size distribution
    size_dist = defaultdict(int)
    for t in threads:
        if t['message_count'] == 1:
            size_dist['1'] += 1
        elif t['message_count'] <= 5:
            size_dist['2-5'] += 1
        elif t['message_count'] <= 10:
            size_dist['6-10'] += 1
        elif t['message_count'] <= 25:
            size_dist['11-25'] += 1
        elif t['message_count'] <= 50:
            size_dist['26-50'] += 1
        elif t['message_count'] <= 100:
            size_dist['51-100'] += 1
        else:
            size_dist['100+'] += 1

    stats['thread_size_distribution'] = dict(size_dist)

    # Find largest threads
    largest_threads = sorted(threads, key=lambda t: -t['message_count'])[:20]
    stats['largest_threads'] = [
        {
            'subject': t['subject'][:60],
            'messages': t['message_count'],
            'participants': t['participant_count'],
            'depth': t['depth'],
        }
        for t in largest_threads
    ]

    # Save outputs
    output_path.mkdir(parents=True, exist_ok=True)

    threads_file = output_path / 'threading_step4_threads.json'
    with open(threads_file, 'w') as f:
        json.dump(threads, f, indent=2, ensure_ascii=False)
    print(f"\nSaved {len(threads):,} threads to {threads_file}")

    emails_file = output_path / 'threading_step4_emails.json'
    with open(emails_file, 'w') as f:
        json.dump(emails, f, indent=2, ensure_ascii=False)
    print(f"Saved {len(emails):,} emails to {emails_file}")

    stats_file = output_path / 'threading_step4_stats.json'
    with open(stats_file, 'w') as f:
        json.dump(stats, f, indent=2)
    print(f"Saved stats to {stats_file}")

    # Print summary
    print("\n" + "=" * 60)
    print("STEP 4: THREAD METADATA")
    print("=" * 60)
    print(f"Total emails:           {stats['total_emails']:,}")
    print(f"Total threads:          {stats['total_threads']:,}")
    print(f"  Single message:       {stats['single_message_threads']:,}")
    print(f"  Multi message:        {stats['multi_message_threads']:,}")

    print(f"\nThread statistics:")
    print(f"  Avg messages/thread:  {stats['avg_messages_per_thread']:.1f}")
    print(f"  Max messages:         {stats['max_messages_in_thread']}")
    print(f"  Avg participants:     {stats['avg_participants']:.1f}")
    print(f"  Max participants:     {stats['max_participants']}")
    print(f"  Avg depth:            {stats['avg_depth']:.1f}")
    print(f"  Max depth:            {stats['max_depth']}")
    print(f"  Threads with PGP:     {stats['threads_with_pgp']:,}")

    print(f"\nThread size distribution:")
    for size, count in sorted(stats['thread_size_distribution'].items(),
                               key=lambda x: int(x[0].split('-')[0].replace('+', ''))):
        print(f"  {size:8} messages: {count:,}")

    print(f"\nTop 10 largest threads:")
    for i, t in enumerate(stats['largest_threads'][:10], 1):
        print(f"  {i}. {t['messages']:4} msgs, {t['participants']:3} ppl: {t['subject']}")

    return stats


def main():
    input_path = Path('/Users/maariabajwa/zypherpunk_cypherpunk/threading/threading_step3.json')
    output_path = Path('/Users/maariabajwa/zypherpunk_cypherpunk/threading')

    build_threads(input_path, output_path)


if __name__ == '__main__':
    main()
