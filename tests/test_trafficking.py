import pytest
from src.trafficking.engine import TraffickingEngine

@pytest.fixture
def engine():
    return TraffickingEngine()

def test_eve_version_routing(engine):
    assert engine.get_eve_version("DV360", "ProgDisplay") == "V1"
    assert engine.get_eve_version("Yahoo DSP", "ProgDisplay") == "V2"
    assert engine.get_eve_version("CM360", "YouTube") == "V2.1"
    assert engine.get_eve_version("CM360", "ProgAudio") == "V2.2"
    assert engine.get_eve_version("Amazon DSP", "ProgCTV") == "V3"

def test_build_placement_taxonomy(engine):
    ticket = {}
    campaign = {
        "campaign_name": "DBUN_Bundle_Acq_US_ProgDisplay",
        "targeting_geo": "US",
        "language": "ENG",
        "brand_code": "DBUN",
        "title_name": "Bundle",
        "campaign_objective": "Acq",
        "channel_mapped": "ProgDisplay"
    }
    expected = "DBUN_Bundle_Acq_US_ProgDisplay|US|ENG|DBUN|Bundle|Acq|ProgDisplay"
    assert engine.build_placement_taxonomy(ticket, campaign) == expected

def test_new_campaign_setup_payloads(engine):
    ticket = {
        "request_type": "New Campaign",
        "platform": "DV360",
        "campaign_id": "CMP-9999"
    }
    campaign = {
        "campaign_name": "DBUN_Bundle_Acq_US",
        "start_date": "2026-03-01",
        "budget_usd": 50000,
        "audience_detailed": "A18-49"
    }
    
    payloads = engine.process_ticket(ticket, campaign)
    assert len(payloads) == 3
    assert payloads[0].action == "CREATE_CAMPAIGN_SHELL"
    assert payloads[0].platform == "CM360"
    
    assert payloads[1].action == "CREATE_PLACEMENT"
    assert payloads[1].platform == "CM360"
    
    assert payloads[2].action == "CREATE_INSERTION_ORDER"
    assert payloads[2].platform == "DV360"
    assert payloads[2].payload["budget"] == 50000

def test_creative_rotation_payload(engine):
    ticket = {
        "request_type": "Creative Rotation",
        "platform": "Meta",
        "campaign_id": "CMP-1234"
    }
    campaign = {"campaign_name": "Test_Campaign"}
    
    payloads = engine.process_ticket(ticket, campaign)
    assert len(payloads) == 1
    assert payloads[0].action == "ROTATE_CREATIVES"
    assert payloads[0].platform == "CM360"
    assert "new_assets" in payloads[0].payload
