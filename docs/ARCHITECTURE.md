## Data Collection Priorities

**First → Next:**

1. **NFL team defense season totals**  
   - Metrics: points allowed, sacks, interceptions (core signals).  
   - Source: FootballDB “Team Defense Totals” pages.  

2. **Active/career leader lists**  
   - Purpose: sanity checks on sacks/INTs counts.  
   - Source: FootballDB  

3. **(Nice-to-have later)** Weekly splits or game logs  
   - If available on FootballDB team pages.  

---

## Update Frequency Justification

- **In-season**: Weekly (defense totals change only after games).  
- **Offseason**: Monthly.  

---

## Fallback Strategies

- If a **page shape changes** → detect missing table columns and skip/write warning.  
- If **blocked by robots or 403** → stop; don’t bypass.  
- If a **request fails** → exponential backoff (max 3 retries), then log & continue.  

---

## Data Quality Rules

- Numeric columns must be integers ≥ 0.  
- Team names must be non-empty; season must be 4-digit year.  
- For a season, `sum(team INTs)` and `sum(team sacks)` must be > 0 (basic plausibility).  

---

## Schema

```json
{
  "season": "YYYY",
  "team": "string",
  "p
