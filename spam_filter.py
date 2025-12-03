#!/usr/bin/env python3
"""
Spam Filter for Cypherpunk Email Archive

Conservative spam detection that minimizes false positives.
Focuses on commercial spam patterns from the late 1990s.
"""

import json
import re
from collections import Counter
from pathlib import Path


# Legitimate numeric email services from the 90s
LEGIT_NUMERIC_DOMAINS = {
    'mcimail.com',
    'compuserve.com',
    'aol.com',
    'prodigy.com',
    'prodigy.net',
}


def is_spam(email: dict, sender_counts: dict) -> tuple[bool, list[str]]:
    """
    Determine if an email is spam.
    Returns (is_spam, reasons).
    """
    subject = email.get('subject', '').lower()
    body = email.get('body', '').lower()
    from_email = (email.get('from_email', '') or '').lower()

    reasons = []

    # Skip prolific posters (>5 messages) - unlikely to be spammers
    if sender_counts.get(from_email, 0) > 5:
        return False, []

    # Check if numeric sender is from legit service
    is_legit_numeric = any(
        from_email.endswith(f'@{domain}')
        for domain in LEGIT_NUMERIC_DOMAINS
    )

    # === HIGH CONFIDENCE SPAM PATTERNS ===

    # 1. Commercial spam subjects
    commercial_spam = [
        (r'(make|earn)\s*(big\s*)?(money|cash|\$|bucks)', 'make_money'),
        (r'work\s*(from\s*)?home.*\$', 'work_from_home'),
        (r'financial\s*(freedom|opportunity)', 'financial_opportunity'),
        (r'(xxx|adult|sexy)\s*(image|pic|video|site)', 'adult_content'),
        (r'cable\s*(tv\s*)?(de)?scrambler', 'cable_scrambler'),
        (r'(stock|buy|sell)\s*alert', 'stock_pump'),
        (r'get\s*rich\s*(quick|fast|now)', 'get_rich_quick'),
        (r'lose\s*(weight|pounds|lbs)', 'weight_loss'),
        (r'viagra|cialis|penis\s*enlarge', 'pharma_spam'),
        (r'bulk\s*e?-?mail\s*(list|address|software)', 'bulk_email'),
        (r'(\d+,?\d*,?\d*)\s*(fresh\s*)?(e?-?mail|address)', 'email_list_sale'),
        (r'free\s*(money|cash|offer|gift|trial)', 'free_money'),
        (r'limited\s*time\s*(only|offer)', 'limited_time'),
        (r'credit\s*card\s*(machine|terminal|processing)', 'cc_processing'),
        (r'internet\s*business\s*opportunity', 'biz_opp'),
        (r'mlm|multi.?level\s*market', 'mlm'),
        (r'home\s*based\s*business', 'home_business'),
        (r'amazing\s*(new\s*)?(product|offer|opportunity)', 'amazing_offer'),
        (r'double\s*your\s*(money|income)', 'double_money'),
    ]

    for pattern, name in commercial_spam:
        if re.search(pattern, subject):
            reasons.append(f'spam_subject:{name}')

    # 2. Spam body patterns
    spam_body_patterns = [
        (r'this\s*is\s*not\s*spam', 'not_spam_disclaimer'),
        (r'you\s*(are\s*)?receiving\s*this\s*(because|as)', 'receiving_disclaimer'),
        (r'to\s*(be\s*)?removed?\s*from\s*(this|our|the)\s*(list|mailing)', 'removal_instructions'),
        (r'unsubscribe\s*(instructions|link|here|below)', 'unsubscribe_spam'),
        (r'order\s*(now|today)\s*(and\s*)?(receive|get)', 'order_now'),
        (r'call\s*(now|today|toll.?free)\s*(to\s*order)?', 'call_now'),
        (r'act\s*(now|fast|today)', 'act_now'),
        (r'only\s*\$\d+', 'price_pitch'),
        (r'satisfaction\s*guarante', 'satisfaction_guarantee'),
        (r'risk.?free', 'risk_free'),
        (r'(huge|big|massive)\s*discount', 'discount'),
    ]

    body_spam_count = 0
    for pattern, name in spam_body_patterns:
        if re.search(pattern, body):
            body_spam_count += 1
            if body_spam_count <= 3:  # Only add first few reasons
                reasons.append(f'spam_body:{name}')

    # 3. HTML commercial spam (short HTML with spam indicators)
    if '<html' in body or '<body' in body:
        html_spam_signals = [
            r'click\s*here',
            r'<a\s*href',
            r'bgcolor',
            r'font\s*color',
        ]
        html_spam_count = sum(1 for p in html_spam_signals if re.search(p, body))
        if html_spam_count >= 2 and len(body) < 5000:
            reasons.append('html_commercial')

    # 4. Suspicious sender patterns (excluding legit numeric services)
    if not is_legit_numeric:
        if re.match(r'^\d{7,}@', from_email):  # Long numeric ID not from known service
            reasons.append('suspicious_numeric_sender')

        # Random-looking domains
        if re.search(r'@\d+\.(com|net|org)$', from_email):
            reasons.append('numeric_domain')

    # 5. ALL CAPS + excessive punctuation + commercial keywords (combined)
    if email.get('subject', '').isupper() and len(email.get('subject', '')) > 20:
        if re.search(r'[!$]{2,}|\$\$\$', email.get('subject', '')):
            if sender_counts.get(from_email, 0) <= 1:
                reasons.append('caps_punct_commercial')

    # === DECISION LOGIC ===

    # High confidence: any spam_subject pattern
    if any(r.startswith('spam_subject:') for r in reasons):
        return True, reasons

    # Medium confidence: multiple body patterns or HTML spam
    if body_spam_count >= 3:
        return True, reasons

    if 'html_commercial' in reasons and body_spam_count >= 1:
        return True, reasons

    # Suspicious sender + any spam signal
    if ('suspicious_numeric_sender' in reasons or 'numeric_domain' in reasons):
        if body_spam_count >= 1 or 'caps_punct_commercial' in reasons:
            return True, reasons

    # Combined signals
    if len(reasons) >= 3:
        return True, reasons

    return False, []


def filter_spam(input_path: Path, output_path: Path):
    """Filter spam from cleaned emails."""

    print("Loading emails...")
    with open(input_path, 'r') as f:
        emails = json.load(f)

    print(f"Loaded {len(emails):,} emails")

    # Count messages per sender
    sender_counts = Counter(e.get('from_email', '') for e in emails)

    spam_emails = []
    clean_emails = []

    for email in emails:
        is_spam_email, reasons = is_spam(email, sender_counts)

        if is_spam_email:
            spam_emails.append({
                'id': email['id'],
                'year': email['year'],
                'subject': email['subject'],
                'from': email.get('from_email') or email['from_raw'],
                'reasons': reasons,
                'body_preview': email['body'][:200] if email.get('body') else '',
            })
        else:
            clean_emails.append(email)

    # Save results
    output_path.mkdir(parents=True, exist_ok=True)

    # Save clean emails
    clean_file = output_path / 'emails_no_spam.json'
    with open(clean_file, 'w') as f:
        json.dump(clean_emails, f, indent=2, ensure_ascii=False)
    print(f"\nSaved {len(clean_emails):,} clean emails to {clean_file}")

    # Save spam for review
    spam_file = output_path / 'spam_detected.json'
    with open(spam_file, 'w') as f:
        json.dump(spam_emails, f, indent=2, ensure_ascii=False)
    print(f"Saved {len(spam_emails):,} spam emails to {spam_file}")

    # Stats
    print("\n" + "=" * 60)
    print("SPAM FILTER RESULTS")
    print("=" * 60)
    print(f"Total emails:    {len(emails):,}")
    print(f"Spam detected:   {len(spam_emails):,} ({100*len(spam_emails)/len(emails):.2f}%)")
    print(f"Clean emails:    {len(clean_emails):,} ({100*len(clean_emails)/len(emails):.2f}%)")

    # Breakdown by reason
    reason_counts = Counter()
    for s in spam_emails:
        for r in s['reasons']:
            reason_type = r.split(':')[0]
            reason_counts[reason_type] += 1

    print("\nSpam by primary reason:")
    for reason, count in reason_counts.most_common(10):
        print(f"  {reason}: {count}")

    # Year distribution
    spam_by_year = Counter(s['year'] for s in spam_emails)
    print("\nSpam by year:")
    for year in sorted(spam_by_year.keys()):
        total_year = sum(1 for e in emails if e['year'] == year)
        spam_count = spam_by_year[year]
        print(f"  {year}: {spam_count:4} / {total_year:,} ({100*spam_count/total_year:.1f}%)")

    return spam_emails, clean_emails


def main():
    input_path = Path('/Users/maariabajwa/zypherpunk_cypherpunk/cleaned/cleaned_emails.json')
    output_path = Path('/Users/maariabajwa/zypherpunk_cypherpunk/filtered')

    spam, clean = filter_spam(input_path, output_path)

    # Show spam samples
    print("\n" + "=" * 60)
    print("SPAM SAMPLES (first 20)")
    print("=" * 60)
    for s in spam[:20]:
        print(f"\nSubject: {s['subject'][:60]}")
        print(f"From: {s['from'][:50]}")
        print(f"Reasons: {', '.join(s['reasons'][:3])}")


if __name__ == '__main__':
    main()
