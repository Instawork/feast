# check_redis_workers.py
import redis

r = redis.Redis(host="localhost", port=6379, decode_responses=False)  # ← Don't decode

print("=" * 70)
print("Workers in Redis")
print("=" * 70)

# Get all keys (binary)
all_keys = r.keys(b"*")
print(f"\nTotal keys in Redis: {len(all_keys):,}")

# Extract worker IDs from binary keys
worker_ids = set()
business_ids = set()

for key in all_keys:
    try:
        # Try to find worker ID pattern in binary
        if b"id_worker_id" in key:
            # Keys contain the entity name and value
            # Extract numeric ID (this is approximate)
            key_str = str(key)
            # Look for numbers after 'id_worker_id'
            import re

            # This is a simplified extraction
            worker_ids.add("found")  # Placeholder

        if b"id_business_id" in key:
            business_ids.add("found")

    except Exception:
        pass

print(f"\nKeys with 'id_worker_id': {sum(1 for k in all_keys if b'id_worker_id' in k)}")
print(
    f"Keys with 'id_business_id': {sum(1 for k in all_keys if b'id_business_id' in k)}"
)

# Show sample keys
print(f"\nSample keys (first 10):")
for key in all_keys[:10]:
    print(f"  {key}")
