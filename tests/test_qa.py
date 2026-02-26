import pytest
from src.trafficking.qa_engine import QAEngine
from src.trafficking.engine import TraffickingPayload

@pytest.fixture
def qa():
    return QAEngine()

def test_missing_geo_fails(qa):
    campaign = {"campaign_name": "Test"}
    payloads = [TraffickingPayload("CMP-1", "CM360", "CREATE", {})]
    res = qa.check_targeting(payloads, campaign)
    assert res["result"] == "Fail"
    assert "Missing geo" in res["details"]

def test_sponsorship_requires_review(qa):
    campaign = {"campaign_name": "ESP_Sports_Sponsorship_US"}
    payloads = [TraffickingPayload("CMP-1", "CM360", "CREATE", {})]
    res = qa.check_content_exclusions(payloads, campaign)
    assert res["result"] == "Needs Review"
    assert "S&P review" in res["details"]

def test_non_https_landing_page_fails(qa):
    campaign = {"landing_page": "http://disney.com"}
    payloads = [TraffickingPayload("CMP-1", "CM360", "CREATE", {})]
    res = qa.check_landing_page(payloads, campaign)
    assert res["result"] == "Fail"
    assert "Non-HTTPS URL" in res["details"]

def test_run_all_checks_count(qa):
    campaign = {"landing_page": "https://disney.com", "targeting_geo": "US"}
    payloads = [TraffickingPayload("CMP-1", "CM360", "CREATE", {})]
    results = qa.run_all_checks(payloads, campaign)
    assert len(results) == 8
    
    # Asserting correct structure
    for res in results:
        assert "check" in res
        assert "result" in res
        assert res["result"] in ["Pass", "Fail", "Needs Review"]
