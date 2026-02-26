import re

c_file = "src/airtable/client.py"
with open(c_file, "r") as f:
    c = f.read()

c = c.replace('"check_type":', '"check":')
c = c.replace('"qa_status":', '"result":')
c = c.replace('"fail_reason":', '"details":')

with open(c_file, "w") as f:
    f.write(c)
