import os
import stat
import sys
import tempfile

import pytest

from usagerail.snapshot import FileSnapshotStore

SNAPSHOT = {"saved_at": "2026-10-20T12:00:00.000Z", "cursor": "c", "app": None, "keys": [], "consumers": []}
unix = hasattr(os, "getuid") and sys.platform != "win32"


def private_directory() -> str:
    # mkdtemp creates it as 0700
    return os.path.join(tempfile.mkdtemp(prefix="ur-snapshot-test-"), "store")


def test_a_saved_snapshot_loads_back_with_a_0700_directory_and_a_0600_file():
    directory = private_directory()
    store = FileSnapshotStore("urk_test_one", directory)
    assert store.load() is None
    store.save(SNAPSHOT)
    assert store.load() == SNAPSHOT
    files = os.listdir(directory)
    assert len(files) == 1
    if unix:
        assert stat.S_IMODE(os.stat(directory).st_mode) == 0o700
        assert stat.S_IMODE(os.stat(os.path.join(directory, files[0])).st_mode) == 0o600
    # Different appKeys get different files
    assert FileSnapshotStore("urk_test_two", directory).load() is None


@pytest.mark.skipif(not unix, reason="Unix permissions")
def test_a_symlink_or_a_file_others_can_read_in_place_of_the_snapshot_is_refused():
    directory = private_directory()
    store = FileSnapshotStore("urk_test_links", directory)
    store.save(SNAPSHOT)
    os.chmod(store.file, 0o644)
    with pytest.raises(PermissionError, match="not a private file"):
        store.load()

    planted = os.path.join(directory, "planted.json")
    descriptor = os.open(planted, os.O_WRONLY | os.O_CREAT, 0o600)
    os.write(descriptor, b'{"saved_at":"x","keys":[],"consumers":[]}')
    os.close(descriptor)
    os.remove(store.file)
    os.symlink(planted, store.file)
    with pytest.raises(OSError):
        store.load()


@pytest.mark.skipif(not unix, reason="Unix permissions")
def test_a_snapshot_directory_that_others_can_write_to_is_refused_for_reading_and_writing():
    directory = private_directory()
    store = FileSnapshotStore("urk_test_dir", directory)
    store.save(SNAPSHOT)
    os.chmod(directory, 0o777)
    with pytest.raises(PermissionError, match="only this user can access"):
        store.load()
    with pytest.raises(PermissionError, match="only this user can access"):
        store.save(SNAPSHOT)


def test_a_file_that_is_not_a_snapshot_is_ignored():
    directory = private_directory()
    store = FileSnapshotStore("urk_test_shape", directory)
    store.save({"something": "else"})
    assert store.load() is None
