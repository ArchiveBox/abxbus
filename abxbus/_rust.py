"""
Rust-accelerated core functions for abxbus.

Falls back to pure-Python implementations if the Rust extension is not available.
"""

from __future__ import annotations

try:
    from _abxbus_rust import (
        monotonic_datetime as rust_monotonic_datetime,
        uuid7str as rust_uuid7str,
        uuid5_handler_id as rust_uuid5_handler_id,
        compute_handler_id as rust_compute_handler_id,
        validate_event_name as rust_validate_event_name,
        validate_uuid_str as rust_validate_uuid_str,
        validate_event_path_entry as rust_validate_event_path_entry,
        is_valid_identifier as rust_is_valid_identifier,
        validate_bus_name as rust_validate_bus_name,
        check_reserved_event_fields as rust_check_reserved_event_fields,
        RustEventHistory,
    )

    RUST_AVAILABLE = True
except ImportError:
    RUST_AVAILABLE = False


__all__ = ['RUST_AVAILABLE']
