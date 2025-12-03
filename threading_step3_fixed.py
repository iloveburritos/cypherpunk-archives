#!/usr/bin/env python3
"""
Threading Step 3 (FIXED): Match orphans to existing threads by subject

FIXED: Added cycle detection to prevent circular parent references.

Strategy:
1. For each orphan, find emails with same normalized subject
2. Find the most likely parent:
   - Must be earlier than the orphan
   - Prefer closest in time
   - Prefer thread roots or emails that are already parents
   - MUST NOT create a cycle
3. Link orphan to that parent

Output:
- threading_step3.json: All emails with updated parent_id for matched orphans
- threading_step3_stats.json: Statistics about matching
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


def would_create_cycle(child_msg_id: str, parent_msg_id: str, by_message_id: dict) -> bool:
    """Check if setting child's parent to parent_msg_id would create a cycle."""
    # Trace from parent up to see if we reach child
    visited = set()
    current_id = parent_msg_id

    while current_id:
        if current_id == child_msg_id:
            return True  # Would create cycle
        if current_id in visited:
            return True  # Already a cycle exists
        visited.add(current_id)

        parent_email = by_message_id.get(current_id)
        if not parent_email:
            break
        current_id = parent_email.get('parent_id')

    return False


def match_orphans(input_path: Path, output_path: Path):
    """Match orphan emails to threads by subject."""

    print("Loading emails from Step 2...")
    with open(input_path, 'r') as f:
        emails = json.load(f)

    print(f"Loaded {len(emails):,} emails")

    # Build indexes
    print("Building indexes...")
    by_id = {e['id']: e for e in emails}
    by_message_id = {e['message_id']: e for e in emails if e.get('message_id')}

    # Group by normalized subject
    by_subject = defaultdict(list)
    for email in emails:
        norm_subj = email.get('normalized_subject', '')
        if norm_subj:
            by_subject[norm_subj].append(email)

    # Sort each subject group by date
    for subj in by_subject:
        by_subject[subj].sort(key=lambda e: e.get('date_parsed') or e.get('date_raw') or '')

    stats = {
        'total_emails': len(emails),
        'orphans_before': sum(1 for e in emails if e.get('orphan_status') == 'orphan_reply'),
        'orphans_matched': 0,
        'orphans_unmatched': 0,
        'cycles_prevented': 0,
        'match_types': {
            'to_root': 0,
            'to_parent_email': 0,
            'to_sibling_chain': 0,
        },
        'time_gaps': {
            'same_day': 0,
            'within_week': 0,
            'within_month': 0,
            'over_month': 0,
        },
    }

    print("Matching orphans to threads (with cycle detection)...")
    matched_count = 0

    for email in emails:
        if email.get('orphan_status') != 'orphan_reply':
            continue

        norm_subj = email.get('normalized_subject', '')
        if not norm_subj:
            stats['orphans_unmatched'] += 1
            continue

        same_subject = by_subject.get(norm_subj, [])
        if len(same_subject) <= 1:
            stats['orphans_unmatched'] += 1
            continue

        orphan_date = parse_date(email.get('date_parsed'))

        # Find best parent candidate
        best_parent = None
        best_score = -1
        best_time_gap = None

        for candidate in same_subject:
            if candidate['id'] == email['id']:
                continue

            cand_date = parse_date(candidate.get('date_parsed'))

            # Must be earlier than orphan
            if orphan_date and cand_date and cand_date >= orphan_date:
                continue

            # CHECK FOR CYCLES
            if would_create_cycle(email['message_id'], candidate['message_id'], by_message_id):
                stats['cycles_prevented'] += 1
                continue

            # Score the candidate
            score = 0

            if candidate.get('orphan_status') == 'true_root':
                score += 10
            if candidate.get('children_ids'):
                score += 5
            if candidate.get('parent_id'):
                score += 3

            if orphan_date and cand_date:
                time_gap = (orphan_date - cand_date).total_seconds()
                if time_gap < 86400:
                    score += 5
                elif time_gap < 604800:
                    score += 3
                elif time_gap < 2592000:
                    score += 1
            else:
                time_gap = None

            if score > best_score:
                best_score = score
                best_parent = candidate
                best_time_gap = time_gap

        if best_parent:
            # Match found - update relationships
            email['parent_id'] = best_parent['message_id']
            email['orphan_status'] = 'matched_by_subject'
            email['match_score'] = best_score

            if email['message_id'] not in best_parent.get('children_ids', []):
                if 'children_ids' not in best_parent:
                    best_parent['children_ids'] = []
                best_parent['children_ids'].append(email['message_id'])

            # Update by_message_id to reflect new parent
            by_message_id[email['message_id']] = email

            stats['orphans_matched'] += 1
            matched_count += 1

            if best_parent.get('orphan_status') == 'true_root':
                stats['match_types']['to_root'] += 1
            elif best_parent.get('children_ids') and len(best_parent['children_ids']) > 1:
                stats['match_types']['to_parent_email'] += 1
            else:
                stats['match_types']['to_sibling_chain'] += 1

            if best_time_gap is not None:
                if best_time_gap < 86400:
                    stats['time_gaps']['same_day'] += 1
                elif best_time_gap < 604800:
                    stats['time_gaps']['within_week'] += 1
                elif best_time_gap < 2592000:
                    stats['time_gaps']['within_month'] += 1
                else:
                    stats['time_gaps']['over_month'] += 1

            if matched_count % 5000 == 0:
                print(f"  Matched {matched_count:,} orphans...")
        else:
            stats['orphans_unmatched'] += 1

    # Recalculate thread stats
    stats['root_messages_after'] = sum(1 for e in emails if e.get('parent_id') is None)
    stats['child_messages_after'] = len(emails) - stats['root_messages_after']

    # Save outputs
    output_path.mkdir(parents=True, exist_ok=True)

    emails_file = output_path / 'threading_step3.json'
    with open(emails_file, 'w') as f:
        json.dump(emails, f, indent=2, ensure_ascii=False)
    print(f"\nSaved {len(emails):,} emails to {emails_file}")

    stats_file = output_path / 'threading_step3_stats.json'
    with open(stats_file, 'w') as f:
        json.dump(stats, f, indent=2)
    print(f"Saved stats to {stats_file}")

    # Print summary
    print("\n" + "=" * 60)
    print("STEP 3 (FIXED): ORPHAN MATCHING BY SUBJECT")
    print("=" * 60)
    print(f"Total emails:            {stats['total_emails']:,}")
    print(f"Orphans before:          {stats['orphans_before']:,}")
    print(f"Orphans matched:         {stats['orphans_matched']:,} ({100*stats['orphans_matched']/stats['orphans_before']:.1f}%)")
    print(f"Orphans still unmatched: {stats['orphans_unmatched']:,}")
    print(f"Cycles prevented:        {stats['cycles_prevented']:,}")

    print(f"\nMatch types:")
    print(f"  To true root:          {stats['match_types']['to_root']:,}")
    print(f"  To parent email:       {stats['match_types']['to_parent_email']:,}")
    print(f"  To sibling chain:      {stats['match_types']['to_sibling_chain']:,}")

    print(f"\nTime gaps:")
    print(f"  Same day:              {stats['time_gaps']['same_day']:,}")
    print(f"  Within week:           {stats['time_gaps']['within_week']:,}")
    print(f"  Within month:          {stats['time_gaps']['within_month']:,}")
    print(f"  Over month:            {stats['time_gaps']['over_month']:,}")

    print(f"\nAfter matching:")
    print(f"  Root messages:         {stats['root_messages_after']:,}")
    print(f"  Child messages:        {stats['child_messages_after']:,}")

    return stats


def main():
    input_path = Path('/Users/maariabajwa/zypherpunk_cypherpunk/threading/threading_step2.json')
    output_path = Path('/Users/maariabajwa/zypherpunk_cypherpunk/threading')

    match_orphans(input_path, output_path)


if __name__ == '__main__':
    main()
