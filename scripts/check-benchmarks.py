"""Arithmetic verification only; never generates load or connects to a database."""
import csv,json,pathlib,statistics
root=pathlib.Path(__file__).resolve().parents[1]
rows=list(csv.DictReader((root/'docs/benchmarks/controlled-runs.csv').open()))
assert len(rows)==9 and sum(int(r['delivered']) for r in rows)==9000
assert sum(int(r['http_attempts']) for r in rows)==9000
p=json.loads((root/'docs/benchmarks/provenance.json').read_text())
for workers in [1,4,8]:
 values=[float(r['delivered_per_second']) for r in rows if int(r['workers'])==workers]
 assert len(values)==3
 assert abs(statistics.mean(values)-p['controlled_stats'][str(workers)]['mean_deliveries_per_second'])<1e-10
print('PASS: 9 retained runs,9000deliveries/attempts,3repeats per profile;arithmetic only,not a new load test.')
