c_file = "src/trafficking/qa_engine.py"
with open(c_file, "r") as f:
    c = f.read()

# Airtable single select fields need an EXACT match
# Options available from generator: "Spec Compliance", "Tracking", "Geo Target", "Frequency Cap", "Content Exclusion", "Landing Page", "Taxonomy", "Floodlight"
c = c.replace('"Global Payload Check"', '"Spec Compliance"')
c = c.replace('"Spec Compliance Check"', '"Spec Compliance"')
c = c.replace('"Missing Geographic Targeting"', '"Geo Target"')
c = c.replace('"Taxonomy Validity"', '"Taxonomy"')

with open(c_file, "w") as f:
    f.write(c)

