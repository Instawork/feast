# decode_redis_keys.py
import struct

import redis

r = redis.Redis(host="localhost", port=6379, decode_responses=False)

print("=" * 70)
print("Decoding Redis Keys")
print("=" * 70)

all_keys = r.keys(b"*")
print(f"\nTotal keys: {len(all_keys)}")

# Manually decode the sample key we saw
sample_key = b"\x01\x00\x00\x00\x02\x00\x00\x00\x0e\x00\x00\x00id_business_id\x04\x00\x00\x00\x08\x00\x00\x00\xb2g\x00\x00\x00\x00\x00\x00instawork_feature_store"

print(f"\nManual decode of sample key:")
print(f"Key: {sample_key}")

# Find 'id_business_id'
pos = sample_key.find(b"id_business_id")
print(f"Position of 'id_business_id': {pos}")

# The ID bytes are at position 36-44 (based on hex dump)
# \xb2g\x00\x00\x00\x00\x00\x00
id_bytes = sample_key[36:44]
print(f"ID bytes (hex): {id_bytes.hex()}")
print(f"ID bytes: {id_bytes}")

business_id = struct.unpack("<Q", id_bytes)[0]
print(f"Decoded business ID: {business_id}")

# Now decode all keys
worker_ids = set()
business_ids = set()

for key in all_keys:
    try:
        if b"id_worker_id" in key:
            pos = key.find(b"id_worker_id")
            # ID starts at: pos + len('id_worker_id') + 8 bytes
            id_start = pos + 12 + 8  # 12 = len('id_worker_id'), 8 = skip bytes
            id_end = id_start + 8

            if id_end <= len(key):
                id_bytes = key[id_start:id_end]
                worker_id = struct.unpack("<Q", id_bytes)[0]
                if worker_id < 10000000:  # Sanity check
                    worker_ids.add(worker_id)
                    print(f"Decoded worker: {worker_id}")

        elif b"id_business_id" in key:
            pos = key.find(b"id_business_id")
            id_start = pos + 14 + 8  # 14 = len('id_business_id'), 8 = skip bytes
            id_end = id_start + 8

            if id_end <= len(key):
                id_bytes = key[id_start:id_end]
                business_id = struct.unpack("<Q", id_bytes)[0]
                if business_id < 10000000:
                    business_ids.add(business_id)
                    print(f"Decoded business: {business_id}")

    except Exception as e:
        print(f"Error: {e}")

print(f"\n" + "=" * 70)
print(f"📊 Summary:")
print(f"  Unique Workers: {len(worker_ids)}")
print(f"  Unique Businesses: {len(business_ids)}")

if worker_ids:
    print(f"\n👥 Worker IDs:")
    print(sorted(worker_ids))

if business_ids:
    print(f"\n🏢 Business IDs:")
    print(sorted(business_ids))
