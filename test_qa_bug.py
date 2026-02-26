from src.trafficking.qa_engine import QAEngine

qa = QAEngine()
payloads = [{"site": "DV360", "placement_name": "test_Global_test"}]
cmp_mapped = {"brand_code": "DIS"}

results = qa.run_all_checks(payloads, cmp_mapped)
for r in results:
    print(r["check"])

