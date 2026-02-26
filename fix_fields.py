import re

c_file = "src/airtable/client.py"
with open(c_file, "r") as f:
    c = f.read()

c = c.replace("{Stage}", "{stage}")
c = c.replace("{Assignee}", "{assignee}")
c = c.replace("{EVE Eligible}", "{eve_eligible}")
c = c.replace('"Stage":', '"stage":')
c = c.replace('"Notes":', '"notes":')
c = c.replace('"Ticket":', '"ticket_id":')
c = c.replace('"Check Name":', '"check_type":')
c = c.replace('"Result":', '"qa_status":')
c = c.replace('"Check Details":', '"fail_reason":')

with open(c_file, "w") as f:
    f.write(c)

o_file = "src/orchestrator.py"
with open(o_file, "r") as f:
    o = f.read()

o = o.replace('"Request Type"', '"request_type"')
o = o.replace('fields.get("Campaign", [])', 'fields.get("campaign_id", [])')
o = o.replace('"Ticket ID"', '"ticket_id"')
o = o.replace('"Platform"', '"platform"')
o = o.replace('"Targeting Geo"', '"targeting_geo"')
o = o.replace('"Brand"', '"brand"')
o = o.replace('"Campaign ID"', '"campaign_id"')
o = o.replace('"Campaign Name"', '"campaign_name"')
o = o.replace('"Brand Code"', '"brand_code"')
o = o.replace('"Title Name"', '"title_name"')
o = o.replace('"Campaign Objective"', '"campaign_objective"')
o = o.replace('"Channel Mapped"', '"channel_mapped"')
o = o.replace('"Start Date"', '"start_date"')
o = o.replace('"Budget USD"', '"budget_usd"')
o = o.replace('"Audience Detailed"', '"audience_detailed"')

with open(o_file, "w") as f:
    f.write(o)
