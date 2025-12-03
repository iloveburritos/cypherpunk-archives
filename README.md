# Cypherpunk Mailing List Archive (1992-1998)

Parsed and cleaned archive of the historic Cypherpunk mailing list.

## Dataset Statistics

| Metric | Count |
|--------|-------|
| Total emails | 97,648 |
| Years covered | 1992-1998 |
| Unique senders | ~8,600 |
| With threading info | 40.8% |
| With PGP content | ~10% |

## Getting the Raw Data

Download the raw text files from the Cypherpunks Venona archive:

**Source:** https://cypherpunks.venona.com/raw/

Place the files in a `cryptome/` directory:
```
cryptome/
├── cyp-1992.txt
├── cyp-1993.txt
├── cyp-1994.txt
├── cyp-1995.txt
├── cyp-1996.txt
├── cyp-1997.txt
└── cyp-1998.txt
```

## Usage

### 1. Parse raw emails
```bash
python3 parse_emails.py
```
Creates `parsed/parsed_emails.json` with all extracted emails.

### 2. Clean up known entities and fix issues
```bash
python3 cleanup_emails.py
```
Creates `cleaned/cleaned_emails.json` with fixed sender info.

### 3. Filter spam
```bash
python3 spam_filter.py
```
Creates `filtered/emails_no_spam.json` (97,648 clean emails).

## Output Directory Structure

```
├── parsed/                    # Initial parse output
│   ├── parsed_emails.json    # All parsed emails (98,259)
│   ├── flagged_for_review.json
│   └── parse_stats.json
│
├── cleaned/                   # After cleanup
│   ├── cleaned_emails.json   # Fixed known entities (97,734)
│   ├── remaining_flags.json
│   └── cleanup_stats.json
│
├── filtered/                  # Final output
│   ├── emails_no_spam.json   # Clean emails (97,648)
│   └── spam_detected.json    # Spam removed (86)
```

## Email Schema

```json
{
  "id": "unique_hash",
  "year": 1994,
  "source_file": "cyp-1994.txt",
  "line_number": 1234,
  "from_raw": "Timothy C. May <tcmay@netcom.com>",
  "from_name": "Timothy C. May",
  "from_email": "tcmay@netcom.com",
  "date_raw": "Mon, 14 Feb 94 10:23:45 PST",
  "date_parsed": "1994-02-14T10:23:45-08:00",
  "subject": "Re: Crypto Anarchy",
  "message_id": "9402141823.AA12345@netcom.com",
  "in_reply_to": "9402140912.AA09876@toad.com",
  "references": [],
  "to": "cypherpunks@toad.com",
  "body": "...",
  "body_length": 2345,
  "has_pgp": true,
  "flags": []
}
```

## Notable Contributors

- Timothy C. May (tcmay@got.net, tcmay@netcom.com)
- John Gilmore (gnu@toad.com)
- Eric Hughes (hughes@ah.com)
- Hal Finney (hfinney@shell.portal.com)
- Perry Metzger (perry@imsi.com)
- Jim Bell (jimbell@pacifier.com)
- Adam Back (aba@dcs.ex.ac.uk)

## Processing Pipeline

1. **Parse** (`parse_emails.py`): Split raw mbox files on MHonArc delimiter, extract headers
2. **Cleanup** (`cleanup_emails.py`): Fix known entities (gnu, hughes, etc.), remove initial spam
3. **Filter** (`spam_filter.py`): Remove commercial spam (86 emails)

## Cleanup Details

### Known Entity Resolution
The cleanup script resolves sender identities for common name-only headers:
- `gnu` → `gnu@toad.com` (John Gilmore)
- `hughes` → `hughes@ah.com` (Eric Hughes)
- `tcmay` → `tcmay@got.net` (Timothy C. May)
- And 20+ other frequent contributors

### Spam Detection
Conservative spam filter targeting late-90s commercial spam:
- Make money schemes
- Email list sales
- Stock pump-and-dump
- MLM/home business spam
- Adult content spam

Spam was virtually non-existent until 1997, then increased in 1998.
