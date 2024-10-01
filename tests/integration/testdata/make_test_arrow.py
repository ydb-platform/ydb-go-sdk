#! /usr/bin/env python3
import pyarrow

schema = pyarrow.schema([("id", pyarrow.int64()), ("val", pyarrow.utf8())])

with open("bulk_upsert_test_schema.arrow", "bw") as schema_file:
    schema_file.write(schema.serialize().to_pybytes())

data = [
    pyarrow.array([123, 234]),
    pyarrow.array(["data1", "data2"]),
]

batch = pyarrow.record_batch(data, schema=schema)

with open("bulk_upsert_test_data.arrow", "bw") as data_file:
    data_file.write(batch.serialize())
