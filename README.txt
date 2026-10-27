PHILIPPINE EARTHQUAKE MAPREDUCE — POLISHED UI

RUN ON WINDOWS:
1. Extract the ZIP.
2. Open PowerShell inside Philippine_Earthquake_MapReduce_Polished.
3. Run: python earthquake_mapreduce.py

No third-party Python libraries required. The included CSV has 142,487 rows.

INTERFACE:
- Fixed header and settings panel
- Overview: summary cards, Map/Shuffle/Reduce diagram, top-group chart
- Performance benchmark: 1/2/4/8 worker graph and readable results table
- Worker details: separate table with process IDs, records, timings and groups
- Reduced results: group count, average magnitude and depth
- How it works: demonstration walkthrough and caveats

Use Run & Compare to generate results. Use Benchmark to populate benchmark and worker details.
Export CSV exports the latest Run & Compare results.

TECHNICAL NOTES:
MapReduce-style parallel processing on one computer, not Hadoop or a distributed cluster.
The CSV is community-maintained; source records have not been independently audited.
Elapsed times may vary by machine; parallel processing can be slower due to overhead.
