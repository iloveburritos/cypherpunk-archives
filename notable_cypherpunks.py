#!/usr/bin/env python3
"""
Notable Cypherpunks Filter

Filters the email archive to include only threads that involve notable cypherpunks.
Includes all messages in threads where at least one notable person participated.

Output:
- notable/threads.json: Filtered threads
- notable/emails.json: All emails in those threads
- notable/stats.json: Statistics about the filtered data
- notable/notable_authors.json: The list of notable authors used for filtering
"""

import json
from pathlib import Path
from collections import defaultdict
import re

# Notable Cypherpunks - mapping canonical name to list of email patterns
NOTABLE_CYPHERPUNKS = {
    "Adam Back": [
        "aba@dcs.ex.ac.uk",
        "a.back@exeter.ac.uk",
        "aba@exe.ex.ac.uk",
        "aba@atlas.ex.ac.uk",
    ],
    "Bram Cohen": [
        "bram@gawth.com",
    ],
    "Bruce Schneier": [
        "schneier@counterpane.com",
    ],
    "Eric Hughes": [
        "hughes@ah.com",
        "hughes@soda.berkeley.edu",
        "eric@remailer.net",
        "eric@sac.net",
        "hugh0015@algonquinc.on.ca",
        "hughes@csua.berkeley.edu",
        "hughes@huge.cum",
        "hughes@toad.com",
    ],
    "Hal Finney": [
        "hfinney@shell.portal.com",
        "hal@rain.org",
        "hal@alumni.cco.caltech.edu",
        "ghsvax!hal@uunet.uu.net",
    ],
    "Ian Grigg": [
        "iang@systemics.com",
    ],
    "Jim Bell": [
        "jimbell@pacifier.com",
    ],
    "John Gilmore": [
        "gnu@toad.com",
        "gnu@cygnus.com",
        "gnu@eff.org",
        "gnu@om.toad.com",
        "owner-cypherpunks@toad.com",
    ],
    "John Perry Barlow": [
        "barlow@eff.org",
    ],
    "Julian Assange": [
        "proff@suburbia.net",
        "proff@iq.org",
        "proff@.suburbia.net",
    ],
    "Martin Hellman": [
        "hellman@isl.stanford.edu",
    ],
    "Matt Blaze": [
        "mab@crypto.com",
        "mab@research.att.com",
        "mab@nsa.tempo.att.com",
    ],
    "Mitch Kapor": [
        "mkapor@kei.com",
    ],
    "Nick Szabo": [
        "szabo@netcom.com",
        "szabo@techbook.com",
    ],
    "Perry E. Metzger": [
        "perry@piermont.com",
        "perry@imsi.com",
        "pmetzger@lehman.com",
        "perry@snark.imsi.com",
        "pmetzger@shearson.com",
        "perry@panix.com",
        "perry@bacon.imsi.com",
        "pmetzger@snark.shearson.com",
    ],
    "Philip Zimmermann": [
        "prz@acm.org",
        "prz@columbine.cgd.ucar.edu",
        "prz@pgp.com",
        "prz@sage.cgd.ucar.edu",
    ],
    "Ryan Lackey": [
        "rdl@mit.edu",
        "ryan@systemics.ai",
        "ryan@netaxs.com",
        "ryan@arianrhod.systemics.ai",
        "ryan@pobox.com",
        "ryan@venona.com",
    ],
    "Timothy C. May": [
        "tcmay@got.net",
        "tcmay@netcom.com",
        "tcmay@sensemedia.net",
        "tcmay@localhost.netcom.com",
        # Excluding known impersonators:
        # "tm@dev.null" - possible impersonator
        # "dlv@bwalk.dm.com" - "Timmy May" impersonator
        # "rjc@pseudospoofer.exploit.org" - obvious spoofer
    ],
    "Wei Dai": [
        "weidai@eskimo.com",
    ],
    "Whitfield Diffie": [
        "whitfield.diffie@eng.sun.com",
    ],
    "Bryce 'Zooko' Wilcox": [
        "zooko@xs4all.nl",
        "wilcoxb@nagina.cs.colorado.edu",
        "bryce@digicash.com",
        "wilcoxb@nag.cs.colorado.edu",
        "wilcoxb@taussky.cs.colorado.edu",
        "wilcoxb@nagtje.cs.colorado.edu",
        "wilcoxb@land.cs.colorado.edu",
        "zooko@wildgoose.dagny",
        "zooko@wildgoose.tandu.com",
    ],
    "Duncan Frissell": [
        "frissell@panix.com",
    ],
    "Marc Andreessen": [
        "marca@mcom.com",
        "marca@neon.mcom.com",
    ],
    "Black Unicorn": [
        "unicorn@schloss.li",
        "unicorn@access.digex.net",
        "unicorn@polaris.mindport.net",
        "unicorn@access3.digex.net",
        "unicorn@xanadu.mindport.net",
    ],
    "James A. Donald": [
        "jamesd@echeque.com",
        "jamesd@netcom.com",
        "jamesd@com.informix.com",
        "jamesd@informix.com",
        "jamesdon@infoserv.com",
    ],
    "Rishab Aiyer Ghosh": [
        "rishab@dxm.ernet.in",
        "rishab@best.com",
        "rishab@dxm.org",
        "rishab@m-net.arbornet.org",
    ],
    "Lucky Green": [
        "shamrock@netcom.com",
        "shamrock@cypherpunks.to",
    ],
    "Eric Blossom": [
        "eb@comsec.com",
        "eb@srlr14.sr.hp.com",
        "eb@sr.hp.com",
        "eb@well.sf.ca.us",
        "eb@mwmax.sr.hp.com",
    ],
    "Vipul Ved Prakash": [
        "vipul@pobox.com",
        "vipul@best.com",
        "mail@vipul.net",
    ],
    "Lance Cottrell": [
        "loki@infonex.com",
        "lcottrell@popmail.ucsd.edu",
        "loki@obscura.com",
        "loki@nately.ucsd.edu",
        "loki@convex1.tcs.tulane.edu",
        "lcottrell@infonex.com",
        "lcottrell@anonymizer.com",
    ],
    "Johan Helsingius": [
        "julf@penet.fi",
        "julf@util.eunet.fi",
    ],
    "Phil Karn": [
        "karn@qualcomm.com",
        "karn@unix.ka9q.ampr.org",
        "karn@homer.ka9q.ampr.org",
    ],
    "Carl Ellison": [
        "cme@tis.com",
        "cme@ellisun.sw.stratus.com",
        "cme@sw.stratus.com",
        "cme@cybercash.com",
        "cme@acm.org",
        "cme@clark.net",
        "carl_ellison@vos.stratus.com",
        "cme@world.std.com",
    ],
    "Ian Goldberg": [
        "iang@cs.berkeley.edu",
        "iagoldbe@csclub.uwaterloo.ca",
        "iagoldbe@calum.csclub.uwaterloo.ca",
        "iang@cory.eecs.berkeley.edu",
    ],
}


def build_email_patterns():
    """Build a set of normalized email patterns for matching."""
    patterns = set()
    email_to_person = {}

    for person, emails in NOTABLE_CYPHERPUNKS.items():
        for email in emails:
            normalized = email.lower().strip()
            patterns.add(normalized)
            email_to_person[normalized] = person

    return patterns, email_to_person


def is_notable_email(from_email: str, patterns: set) -> bool:
    """Check if an email address matches a notable cypherpunk."""
    if not from_email:
        return False

    normalized = from_email.lower().strip()

    # Direct match
    if normalized in patterns:
        return True

    # Check if any pattern is contained in the from_email
    for pattern in patterns:
        if pattern in normalized:
            return True

    return False


def get_notable_person(from_email: str, email_to_person: dict) -> str:
    """Get the canonical name of a notable person from their email."""
    if not from_email:
        return None

    normalized = from_email.lower().strip()

    # Direct match
    if normalized in email_to_person:
        return email_to_person[normalized]

    # Check if any pattern is contained in the from_email
    for pattern, person in email_to_person.items():
        if pattern in normalized:
            return person

    return None


def filter_notable_threads(
    threads_path: Path,
    emails_path: Path,
    output_path: Path
):
    """Filter threads to only include those with notable cypherpunks."""

    print("Loading data...")
    with open(threads_path, 'r') as f:
        all_threads = json.load(f)

    with open(emails_path, 'r') as f:
        all_emails = json.load(f)

    print(f"Loaded {len(all_threads):,} threads and {len(all_emails):,} emails")

    # Build indexes
    emails_by_id = {e['id']: e for e in all_emails}
    threads_by_id = {t['id']: t for t in all_threads}

    # Build email patterns
    patterns, email_to_person = build_email_patterns()
    print(f"Matching against {len(patterns)} email patterns for {len(NOTABLE_CYPHERPUNKS)} notable people")

    # Find all emails from notable people
    print("Finding emails from notable cypherpunks...")
    notable_emails = []
    notable_email_ids = set()

    for email in all_emails:
        from_email = email.get('from_email', '')
        if is_notable_email(from_email, patterns):
            notable_emails.append(email)
            notable_email_ids.add(email['id'])

    print(f"Found {len(notable_emails):,} emails from notable cypherpunks")

    # Find all threads that contain at least one notable email
    print("Finding threads with notable participants...")
    notable_thread_ids = set()

    for email in notable_emails:
        thread_id = email.get('thread_id')
        if thread_id:
            notable_thread_ids.add(thread_id)

    print(f"Found {len(notable_thread_ids):,} threads with notable participants")

    # Get all threads and all emails in those threads
    filtered_threads = []
    filtered_email_ids = set()

    for thread in all_threads:
        if thread['id'] in notable_thread_ids:
            filtered_threads.append(thread)
            # Add all email IDs in this thread
            for email_id in thread.get('message_ids', []):
                filtered_email_ids.add(email_id)

    # Get all emails in those threads
    filtered_emails = [
        emails_by_id[eid] for eid in filtered_email_ids
        if eid in emails_by_id
    ]

    print(f"Filtered to {len(filtered_threads):,} threads with {len(filtered_emails):,} emails")

    # Compute statistics
    notable_posts_by_person = defaultdict(int)
    threads_by_person = defaultdict(set)

    for email in filtered_emails:
        from_email = email.get('from_email', '')
        person = get_notable_person(from_email, email_to_person)
        if person:
            notable_posts_by_person[person] += 1
            thread_id = email.get('thread_id')
            if thread_id:
                threads_by_person[person].add(thread_id)

    stats = {
        'total_threads': len(filtered_threads),
        'total_emails': len(filtered_emails),
        'notable_emails': len(notable_emails),
        'notable_people_count': len(NOTABLE_CYPHERPUNKS),
        'posts_by_person': dict(sorted(
            notable_posts_by_person.items(),
            key=lambda x: -x[1]
        )),
        'threads_by_person': {
            person: len(threads)
            for person, threads in sorted(
                threads_by_person.items(),
                key=lambda x: -len(x[1])
            )
        },
    }

    # Save outputs
    output_path.mkdir(parents=True, exist_ok=True)

    # Save filtered threads
    threads_file = output_path / 'threads.json'
    with open(threads_file, 'w') as f:
        json.dump(filtered_threads, f, indent=2, ensure_ascii=False)
    print(f"\nSaved {len(filtered_threads):,} threads to {threads_file}")

    # Save filtered emails
    emails_file = output_path / 'emails.json'
    with open(emails_file, 'w') as f:
        json.dump(filtered_emails, f, indent=2, ensure_ascii=False)
    print(f"Saved {len(filtered_emails):,} emails to {emails_file}")

    # Save stats
    stats_file = output_path / 'stats.json'
    with open(stats_file, 'w') as f:
        json.dump(stats, f, indent=2)
    print(f"Saved stats to {stats_file}")

    # Save the notable authors config (for reference/modification)
    authors_file = output_path / 'notable_authors.json'
    with open(authors_file, 'w') as f:
        json.dump(NOTABLE_CYPHERPUNKS, f, indent=2)
    print(f"Saved notable authors config to {authors_file}")

    # Print summary
    print("\n" + "=" * 60)
    print("NOTABLE CYPHERPUNKS FILTER")
    print("=" * 60)
    print(f"Total threads:     {stats['total_threads']:,}")
    print(f"Total emails:      {stats['total_emails']:,}")
    print(f"Notable emails:    {stats['notable_emails']:,}")
    print(f"Notable people:    {stats['notable_people_count']}")

    print(f"\nTop 10 notable contributors (by post count):")
    for i, (person, count) in enumerate(list(stats['posts_by_person'].items())[:10], 1):
        thread_count = stats['threads_by_person'].get(person, 0)
        print(f"  {i:2}. {person}: {count:,} posts in {thread_count:,} threads")

    return stats


def main():
    threads_path = Path('/Users/maariabajwa/zypherpunk_cypherpunk/final/threads.json')
    emails_path = Path('/Users/maariabajwa/zypherpunk_cypherpunk/final/emails.json')
    output_path = Path('/Users/maariabajwa/zypherpunk_cypherpunk/notable')

    filter_notable_threads(threads_path, emails_path, output_path)


if __name__ == '__main__':
    main()
