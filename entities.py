"""
Feast Entity Definitions
Auto-generated from Redshift metadata on 2025-11-20 14:27:02

This file contains all entity definitions for the Feast feature store.
"""

from feast import Entity, ValueType

# ============================================================================
# ENTITIES
# ============================================================================

business_entity = Entity(
    name="business",
    join_keys=["id_business_id"],
    value_type=ValueType.INT32,
    description="Business entity"
)

shift_entity = Entity(
    name="shift",
    join_keys=["id_shift_id"],
    value_type=ValueType.INT32,
    description="Shift entity"
)

worker_entity = Entity(
    name="worker",
    join_keys=["id_worker_id"],
    value_type=ValueType.INT32,
    description="Worker entity"
)

