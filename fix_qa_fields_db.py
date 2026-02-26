# Fix the issue: If check_details became a Single Select, we can't write arbitrary strings to it.
# We will use Airtable's "typecast" feature which creates new select options on the fly
c_file = "src/airtable/client.py"
with open(c_file, "r") as f:
    c = f.read()

c = c.replace('table.create(fields)', 'table.create(fields, typecast=True)')

with open(c_file, "w") as f:
    f.write(c)

