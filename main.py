# =============================================================================
# ETL/main — Auto path detect karta hai, kuch hardcode nahi!
# =============================================================================

from datetime import datetime

# Auto-detect current notebook path → ETL folder ka path nikalo
current_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
# current_path = "/Users/abc@gmail.com/ETL/main"
# BASE          = "/Users/abc@gmail.com/ETL"
BASE = "/".join(current_path.split("/")[:-1])

print("Auto-detected BASE path:", BASE)

STAGES = [
    ("Bronze — Orders (API)",   f"{BASE}/Bronze/ingest_api"),
    ("Bronze — Products (CSV)", f"{BASE}/Bronze/ingest_csv"),
    ("Bronze — Customers (DB)", f"{BASE}/Bronze/ingest_db"),
    ("Silver — Clean",          f"{BASE}/Silver/clean"),
    ("Silver — Transform",      f"{BASE}/Silver/transform"),
    ("Silver — Optimize",       f"{BASE}/Silver/optimize"),
    ("Gold   — Build All",      f"{BASE}/Gold/gold_layer"),
]

print("Notebook paths to run:")
for name, path in STAGES:
    print(f"  {name} -> {path}")


def run_stage(name, path):
    try:
        dbutils.notebook.run(path, timeout_seconds=3600)
        return True
    except Exception as e:
        print("FAILED:", name, "->", str(e))
        return False


start   = datetime.now()
results = {}

print("\n" + "=" * 55)
print("  ECOMMERCE MEDALLION PIPELINE")
print("  Started:", start.strftime("%Y-%m-%d %H:%M:%S"))
print("=" * 55)

for name, path in STAGES:
    print(f"\n>>> Running: {name}")
    ok = run_stage(name, path)
    results[name] = ok
    print("PASS ✅" if ok else "FAIL ❌", "--", name)

end      = datetime.now()
duration = (end - start).seconds
passed   = sum(results.values())
total    = len(results)

print("\n" + "=" * 55)
print("  PIPELINE SUMMARY")
print("=" * 55)
for name, ok in results.items():
    print(("  ✅  " if ok else "  ❌  ") + name)

print(f"\n  Result  : {passed}/{total} stages passed")
print(f"  Duration: {duration} seconds")
print(f"  Finished: {end.strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 55)
