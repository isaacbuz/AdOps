import re

c_file = "src/airtable/client.py"
with open(c_file, "r") as f:
    c = f.read()

c = c.replace('"check":', '"check_name":')
c = c.replace('"details":', '"check_details":')

with open(c_file, "w") as f:
    f.write(c)

