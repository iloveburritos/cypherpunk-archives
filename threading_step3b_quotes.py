#!/usr/bin/env python3
"""
Threading Step 3b: Match emails by quoted body text

For emails that are marked as true_root but contain quoted text (> lines),
try to find the source of those quotes in other emails with the same subject.
This catches replies sent through anonymous remailers that strip headers.

Strategy:
1. Find emails marked as true_root that have quoted lines in body
2. Extract the quoted text
3. Search for emails with same normalized subject whose body contains that text
4. If found and that email is earlier, link as parent-child

Input: threading/threading_step3.json
Output: threading/threading_step3b.json, threading_step3b_stats.json
"""

import json
from pathlib import Path
from datetime import datetime
import re


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


def extract_quoted_lines(body: str) -> list:
    """Extract lines that start with > (quoted text)."""
    if not body:
        return []

    quoted = []
    for line in body.split('\n'):
        # Match lines starting with one or more >
        if line.strip().startswith('>'):
            # Remove the > prefix(es) and clean up
            cleaned = re.sub(r'^>+\s*', '', line.strip())
            if cleaned and len(cleaned) > 20:  # Only meaningful quotes
                quoted.append(cleaned)

    return quoted


def normalize_for_matching(text: str) -> str:
    """Normalize text for fuzzy matching."""
    # Lowercase, collapse whitespace, remove punctuation
    text = text.lower()
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'[^\w\s]', '', text)
    return text.strip()


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


def match_by_quotes(input_path: Path, output_path: Path):
    """Match emails by quoted body text."""

    print("Loading emails from Step 3...")
    with open(input_path, 'r') as f:
        emails = json.load(f)

    print(f"Loaded {len(emails):,} emails")

    # Build indexes
    print("Building indexes...")
    by_id = {e['id']: e for e in emails}
    by_message_id = {e['message_id']: e for e in emails if e.get('message_id')}

    # Group by normalized subject
    from collections import defaultdict
    by_subject = defaultdict(list)
    for email in emails:
        norm_subj = email.get('normalized_subject', '')
        if norm_subj:
            by_subject[norm_subj].append(email)

    # Build a searchable index of email body content
    # Map normalized body snippets to emails
    print("Building body content index...")
    body_index = defaultdict(list)  # normalized_snippet -> [email_ids]

    for email in emails:
        body = email.get('body', '')
        if not body or len(body) < 50:
            continue

        # Index meaningful lines from the body
        for line in body.split('\n'):
            line = line.strip()
            if len(line) > 30:  # Only index substantial lines
                normalized = normalize_for_matching(line)
                if len(normalized) > 20:
                    body_index[normalized].append(email['id'])

    print(f"Indexed {len(body_index):,} unique body snippets")

    stats = {
        'total_emails': len(emails),
        'true_roots_checked': 0,
        'roots_with_quotes': 0,
        'matches_found': 0,
        'cycles_prevented': 0,
        'no_match_found': 0,
    }

    # Find true_root emails that have quoted text
    print("Finding true_root emails with quoted text...")
    candidates = []
    for email in emails:
        if email.get('orphan_status') != 'true_root':
            continue

        stats['true_roots_checked'] += 1

        body = email.get('body', '')
        quoted_lines = extract_quoted_lines(body)

        if quoted_lines:
            stats['roots_with_quotes'] += 1
            candidates.append((email, quoted_lines))

    print(f"Found {len(candidates):,} true_root emails with quoted text")

    # Try to match each candidate
    print("Matching by quoted text...")
    matched_count = 0

    for email, quoted_lines in candidates:
        norm_subj = email.get('normalized_subject', '')
        email_date = parse_date(email.get('date_parsed'))

        if not norm_subj:
            continue

        # Get emails with same subject
        same_subject = by_subject.get(norm_subj, [])
        if len(same_subject) <= 1:
            # Also try partial subject match
            similar_subjects = []
            for subj, subj_emails in by_subject.items():
                if norm_subj in subj or subj in norm_subj:
                    similar_subjects.extend(subj_emails)
            same_subject = similar_subjects

        if not same_subject:
            stats['no_match_found'] += 1
            continue

        # Try to find the source of the quoted text
        best_match = None
        best_score = 0

        for quoted_line in quoted_lines[:5]:  # Check first few quoted lines
            normalized_quote = normalize_for_matching(quoted_line)

            if len(normalized_quote) < 20:
                continue

            # Search in body index
            potential_sources = body_index.get(normalized_quote, [])

            for source_id in potential_sources:
                source_email = by_id.get(source_id)
                if not source_email:
                    continue

                # Must be different email
                if source_email['id'] == email['id']:
                    continue

                # Must be earlier
                source_date = parse_date(source_email.get('date_parsed'))
                if email_date and source_date and source_date >= email_date:
                    continue

                # Prefer same subject
                source_subj = source_email.get('normalized_subject', '')
                score = 1
                if source_subj == norm_subj:
                    score += 10
                elif norm_subj in source_subj or source_subj in norm_subj:
                    score += 5

                # Prefer closer in time
                if email_date and source_date:
                    days_apart = (email_date - source_date).days
                    if days_apart < 1:
                        score += 5
                    elif days_apart < 7:
                        score += 3
                    elif days_apart < 30:
                        score += 1

                if score > best_score:
                    best_score = score
                    best_match = source_email

        if best_match:
            # Check for cycles
            if would_create_cycle(email['message_id'], best_match['message_id'], by_message_id):
                stats['cycles_prevented'] += 1
                continue

            # Link them
            email['parent_id'] = best_match['message_id']
            email['orphan_status'] = 'matched_by_quote'
            email['quote_match_score'] = best_score

            # Update parent's children
            if 'children_ids' not in best_match:
                best_match['children_ids'] = []
            if email['message_id'] not in best_match['children_ids']:
                best_match['children_ids'].append(email['message_id'])

            # Update index
            by_message_id[email['message_id']] = email

            stats['matches_found'] += 1
            matched_count += 1

            if matched_count % 100 == 0:
                print(f"  Matched {matched_count:,} emails...")
        else:
            stats['no_match_found'] += 1

    # Save outputs
    output_path.mkdir(parents=True, exist_ok=True)

    emails_file = output_path / 'threading_step3b.json'
    with open(emails_file, 'w') as f:
        json.dump(emails, f, indent=2, ensure_ascii=False)
    print(f"\nSaved {len(emails):,} emails to {emails_file}")

    stats_file = output_path / 'threading_step3b_stats.json'
    with open(stats_file, 'w') as f:
        json.dump(stats, f, indent=2)
    print(f"Saved stats to {stats_file}")

    # Print summary
    print("\n" + "=" * 60)
    print("STEP 3b: QUOTE-BASED MATCHING")
    print("=" * 60)
    print(f"Total emails:            {stats['total_emails']:,}")
    print(f"True roots checked:      {stats['true_roots_checked']:,}")
    print(f"Roots with quoted text:  {stats['roots_with_quotes']:,}")
    print(f"Matches found:           {stats['matches_found']:,}")
    print(f"Cycles prevented:        {stats['cycles_prevented']:,}")
    print(f"No match found:          {stats['no_match_found']:,}")

    return stats


def main():
    input_path = Path('/Users/maariabajwa/zypherpunk_cypherpunk/threading/threading_step3.json')
    output_path = Path('/Users/maariabajwa/zypherpunk_cypherpunk/threading')

    match_by_quotes(input_path, output_path)


if __name__ == '__main__':
    main()
