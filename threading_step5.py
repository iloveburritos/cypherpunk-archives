#!/usr/bin/env python3
"""
Threading Step 5: Output final threaded structure for GUI

Creates the final output files optimized for GUI consumption:
1. threads.json - Thread list with metadata (for browsing)
2. emails.json - All emails with threading info (for detail view)
3. index files - For quick lookups (by year, by author, etc.)

Output:
- final/threads.json: Thread list sorted by date
- final/emails.json: All emails with full threading info
- final/index_by_year.json: Thread IDs grouped by year
- final/index_by_author.json: Thread IDs by root author
- final/stats.json: Final statistics
"""

import json
from pathlib import Path
from collections import defaultdict


def create_final_output(threads_path: Path, emails_path: Path, output_path: Path):
    """Create final output structure for GUI."""

    print("Loading threads and emails...")
    with open(threads_path, 'r') as f:
        threads = json.load(f)
    with open(emails_path, 'r') as f:
        emails = json.load(f)

    print(f"Loaded {len(threads):,} threads and {len(emails):,} emails")

    # Sort threads by date (most recent first for GUI)
    threads_sorted = sorted(
        threads,
        key=lambda t: t.get('date_start') or '0000',
        reverse=True
    )

    # Add rank/position
    for i, thread in enumerate(threads_sorted):
        thread['rank'] = i + 1

    # Create email lookup optimized for GUI
    emails_by_id = {e['id']: e for e in emails}

    # Build indexes
    print("Building indexes...")

    # By year
    by_year = defaultdict(list)
    for thread in threads:
        year = thread.get('year_start')
        if year:
            by_year[year].append(thread['id'])

    # By author (root author)
    by_author = defaultdict(list)
    for thread in threads:
        author = thread.get('root_author', '').lower()
        if author:
            by_author[author].append(thread['id'])

    # Top authors by thread count
    author_thread_counts = {
        author: len(thread_ids)
        for author, thread_ids in by_author.items()
    }
    top_authors = sorted(
        author_thread_counts.items(),
        key=lambda x: -x[1]
    )[:100]

    # By subject keyword (for search)
    # Just store normalized subjects for now
    subject_index = {}
    for thread in threads:
        norm_subj = thread.get('normalized_subject', '')
        if norm_subj and len(norm_subj) > 3:
            if norm_subj not in subject_index:
                subject_index[norm_subj] = []
            subject_index[norm_subj].append(thread['id'])

    # Final stats
    stats = {
        'total_threads': len(threads),
        'total_emails': len(emails),
        'threads_by_year': {str(y): len(ids) for y, ids in sorted(by_year.items())},
        'emails_by_year': {},
        'top_authors': [
            {'author': a, 'thread_count': c}
            for a, c in top_authors[:50]
        ],
        'thread_size_summary': {
            'single_message': sum(1 for t in threads if t['message_count'] == 1),
            'small_2_5': sum(1 for t in threads if 2 <= t['message_count'] <= 5),
            'medium_6_25': sum(1 for t in threads if 6 <= t['message_count'] <= 25),
            'large_26_100': sum(1 for t in threads if 26 <= t['message_count'] <= 100),
            'very_large_100plus': sum(1 for t in threads if t['message_count'] > 100),
        },
        'unique_normalized_subjects': len(subject_index),
    }

    # Emails by year
    for email in emails:
        year = email.get('year')
        if year:
            stats['emails_by_year'][str(year)] = stats['emails_by_year'].get(str(year), 0) + 1

    # Save outputs
    output_path.mkdir(parents=True, exist_ok=True)

    # Threads (sorted by date)
    threads_file = output_path / 'threads.json'
    with open(threads_file, 'w') as f:
        json.dump(threads_sorted, f, indent=2, ensure_ascii=False)
    print(f"\nSaved {len(threads_sorted):,} threads to {threads_file}")

    # Emails
    emails_file = output_path / 'emails.json'
    with open(emails_file, 'w') as f:
        json.dump(emails, f, indent=2, ensure_ascii=False)
    print(f"Saved {len(emails):,} emails to {emails_file}")

    # Index by year
    year_index_file = output_path / 'index_by_year.json'
    with open(year_index_file, 'w') as f:
        json.dump({str(k): v for k, v in sorted(by_year.items())}, f, indent=2)
    print(f"Saved year index to {year_index_file}")

    # Index by author (top authors only to save space)
    author_index = {
        author: thread_ids
        for author, thread_ids in by_author.items()
        if len(thread_ids) >= 5  # Only authors with 5+ threads
    }
    author_index_file = output_path / 'index_by_author.json'
    with open(author_index_file, 'w') as f:
        json.dump(author_index, f, indent=2, ensure_ascii=False)
    print(f"Saved author index ({len(author_index):,} authors) to {author_index_file}")

    # Stats
    stats_file = output_path / 'stats.json'
    with open(stats_file, 'w') as f:
        json.dump(stats, f, indent=2)
    print(f"Saved stats to {stats_file}")

    # Print summary
    print("\n" + "=" * 60)
    print("STEP 5: FINAL OUTPUT")
    print("=" * 60)
    print(f"Total threads:          {stats['total_threads']:,}")
    print(f"Total emails:           {stats['total_emails']:,}")

    print(f"\nThreads by year:")
    for year, count in sorted(stats['threads_by_year'].items()):
        email_count = stats['emails_by_year'].get(year, 0)
        print(f"  {year}: {count:,} threads, {email_count:,} emails")

    print(f"\nThread sizes:")
    for size, count in stats['thread_size_summary'].items():
        print(f"  {size}: {count:,}")

    print(f"\nTop 10 authors by thread count:")
    for i, author_info in enumerate(stats['top_authors'][:10], 1):
        print(f"  {i}. {author_info['author']}: {author_info['thread_count']} threads")

    print(f"\nOutput files:")
    print(f"  {threads_file}")
    print(f"  {emails_file}")
    print(f"  {year_index_file}")
    print(f"  {author_index_file}")
    print(f"  {stats_file}")

    return stats


def main():
    threads_path = Path('/Users/maariabajwa/zypherpunk_cypherpunk/threading/threading_step4_threads.json')
    emails_path = Path('/Users/maariabajwa/zypherpunk_cypherpunk/threading/threading_step4_emails.json')
    output_path = Path('/Users/maariabajwa/zypherpunk_cypherpunk/final')

    create_final_output(threads_path, emails_path, output_path)


if __name__ == '__main__':
    main()
