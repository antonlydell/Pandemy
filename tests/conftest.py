"""
Fixtures for testing pandemy.
"""

# ===============================================================
# Imports
# ===============================================================

# Standard Library
import shutil
from pathlib import Path

# Third Party
import pytest

# ===============================================================
# Fixtures
# ===============================================================

@pytest.fixture(scope='session')
def sqlite_db_folder(tmp_path_factory):
    """
    The test folder containing the SQLite test databases.
    """

    return tmp_path_factory.mktemp('SQLite')


@pytest.fixture(scope='session')
def sqlite_db(sqlite_db_folder) -> Path:
    """
    The SQLite test database RSGeneralStore.db.

    The database contains tables with mocked information about
    General Stores in Runescape.
    """

    db_filename = 'RSGeneralStore.db'
    db_file = Path(__file__).parent / 'static_files' / 'SQLite' / db_filename
    target_db_file = sqlite_db_folder / db_filename

    shutil.copy2(db_file, target_db_file)  # copy the file with meta data

    return target_db_file


@pytest.fixture()
def sqlite_db_empty(sqlite_db_folder) -> Path:
    """
    An empty version of the SQLite test database RSGeneralStore.db.

    All tables in the database are empty.
    """

    db_filename = 'RSGeneralStore_empty.db'

    db_file = Path('static_files/SQLite') / db_filename
    target_db_file = sqlite_db_folder / db_filename

    shutil.copy2(db_file, target_db_file)

    return target_db_file
