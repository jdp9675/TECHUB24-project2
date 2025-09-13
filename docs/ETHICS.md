# Ethics and Legal Considerations

## Robots.txt and Terms of Service
- Before running any scraper, we must manually check **https://www.footballdb.com/robots.txt**.
- If defensive stats pages are **disallowed**, scraping must be abandoned.
- Terms of Service must also be reviewed to confirm data may be programmatically collected.

## Responsible Crawling
- Identify ourselves with a **clear User-Agent string**.
- Apply **crawl delays of 2–5 seconds** between requests.
- Run **single-threaded crawls** to avoid server strain.
- Implement **exponential backoff** if we encounter 429 (Too Many Requests) or 403 (Forbidden).

## Data Integrity
- Validate collected data with sanity checks (e.g., interceptions ≥ 0).
- Detect and log schema changes or missing data instead of silently failing.

## Privacy
- FootballDB contains **public sports statistics**, not personal data.
- We commit to never scrape, log, or store **PII (personally identifiable information)**.

## Academic and Professional Integrity
- The scraper is built for **educational and analytical purposes**.
- Any extension toward commercial use must review licensing and consult FootballDB’s usage policies.
