c_file = "src/trafficking/qa_engine.py"
with open(c_file, "r") as f:
    c = f.read()

c = c.replace('"Check Name"', '"check"')
c = c.replace('"Global"', '"Global Payload Check"')
# Airtable restricts us if the check name isn't an existing select option, but we generated a CSV with specific values
# The values are: "Spec Compliance", "Tracking", "Geo Target", "Frequency Cap", "Content Exclusion", "Landing Page", "Taxonomy", "Floodlight"
c = c.replace('"Global Payload Check"', '"Spec Compliance"')

with open(c_file, "w") as f:
    f.write(c)

