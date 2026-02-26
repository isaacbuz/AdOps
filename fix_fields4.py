c_file = "src/airtable/client.py"
with open(c_file, "r") as f:
    c = f.read()

c = c.replace('fields["Notes"] = notes', 'fields["notes"] = notes')
c = c.replace('table.update(record_id, fields)', 'table.update(record_id, fields, typecast=True)')

with open(c_file, "w") as f:
    f.write(c)

