"""Keeps memory from growing back (target: under 50 MB at a typical scale, under 100 MB fully loaded). Measures 20,000 keys plus 20,000 consumer records, averaged per entry:
over 500 bytes each when stored as raw dicts, now 250 to 260 (a little more on Python 3.10). Once every customer is active, each adds a bit over 900 bytes more"""

import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "bench"))

from memory import measure  # noqa: E402


def test_synced_keys_and_consumer_records_stay_compact():
    entries = 20_000
    per_entry = measure(entries, entries) / (2 * entries)
    assert per_entry < 320, f"{per_entry:.0f} bytes per synced key or consumer record"


def test_each_active_customer_stays_under_a_kilobyte_and_a_bit():
    customers = 20_000
    synced = measure(customers, customers)
    active = measure(customers, customers, active=True)
    per_customer = (active - synced) / customers
    assert per_customer < 1200, f"{per_customer:.0f} bytes per active customer"


def test_saving_the_key_snapshot_does_not_keep_a_copy_alive():
    # The snapshot is saved by a real background thread: once done, the idle thread waiting for its next job mustn't still hold the whole snapshot (about 10 MB for 20,000 entries)
    entries = 20_000
    without = measure(entries, entries)
    with tempfile.TemporaryDirectory() as directory:
        with_snapshot = measure(entries, entries, snapshot_dir=directory)
    assert with_snapshot - without < 2 * 1024 * 1024, f"{(with_snapshot - without) / 1024 / 1024:.1f} MiB more with the snapshot saved"
