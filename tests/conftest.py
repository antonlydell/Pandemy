"""
Fixtures for testing pandemy.
"""

# ===============================================================
# Imports
# ===============================================================

# Standard Library
import io
import shutil
from pathlib import Path

# Third Party
import pandas as pd
import pytest

# Local
import pandemy

# ===============================================================
# Constants
# ===============================================================

# The csv delimiter for reading DataFrames from io.StringIO
CSV_DELIM = ';'

# ===============================================================
# DbStatements
# ===============================================================


class SQLiteDbStatement(pandemy.DbStatement):
    """
    Container of statements for testing the SQLite DatabaseManager.

    The queries are used against the test database RSGeneralStore.db.
    """

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
def sqlite_db_file(sqlite_db_folder) -> Path:
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


@pytest.fixture(scope='session')
def sqlite_db(sqlite_db_file) -> pandemy.SQLiteDb:
    """
    An instance of pandemy.SQLiteDb.

    Connected to the test database RSGeneralStore.db provided in
    fixture `sqlite_db_file`.
    """

    return pandemy.SQLiteDb(file=sqlite_db_file, must_exist=True)


@pytest.fixture()
def sqlite_db_file_empty(sqlite_db_folder) -> Path:
    """
    An empty version of the SQLite test database RSGeneralStore_empty.db.

    All tables in the database are empty.
    """

    db_filename = 'RSGeneralStore_empty.db'

    db_file = Path(__file__).parent / 'static_files' / 'SQLite' / db_filename
    target_db_file = sqlite_db_folder / db_filename

    shutil.copy2(db_file, target_db_file)

    return target_db_file


@pytest.fixture()
def sqlite_db_empty(sqlite_db_file_empty) -> pandemy.SQLiteDb:
    """
    An instance of pandemy.SQLiteDb.

    Connected to the empty test database RSGeneralStore.db provided in
    fixture `sqlite_db_file_empty`.
    """

    return pandemy.SQLiteDb(file=sqlite_db_file_empty, must_exist=True)


# ===============================================================
# Fixtures - DataFrames
# ===============================================================

@pytest.fixture(scope='session')
def df_owner() -> pd.DataFrame:
    """
    DataFrame representation of the Owner table in test database RSGeneralStore.db.
    """

    data = io.StringIO(
        """
OwnerId;OwnerName;BirthDate
1;Shop keeper;1982-06-18
2;John General;1939-09-01
3;Gerhard General;1945-05-08
        """
    )

    dtypes = {
        'OwnerId': pd.UInt16Dtype(),
        'OwnerName': pd.StringDtype(),
        'BirthDate': None
    }

    return pd.read_csv(data, sep=CSV_DELIM, index_col='OwnerId', parse_dates=['BirthDate'],
                       dtype={key: val for key, val in dtypes.items() if val is not None})


@pytest.fixture(scope='session')
def df_customer() -> pd.DataFrame:
    """
    DataFrame representation of the Customer table in test database RSGeneralStore.db.
    """

    data = io.StringIO(
        """
CustomerId;CustomerName;BirthDate;Residence;IsAdventurer
1;'Zezima';'1990-07-14';'Yanille';1
2;'Dr Harlow';'1970-01-14';'Varrock';0
3;'Baraek';'1968-12-13';'Varrock';0
4;'Gypsy Aris';'1996-03-24';'Varrock';0
5;'Not a Bot';'2006-05-31';'Catherby';1
6;'Max Pure';'2007-08-20';'Port Sarim';1
        """
    )

    dtypes = {
        'CustomerId': pd.UInt16Dtype(),
        'CustomerName': pd.StringDtype(),
        'BirthDate': None,
        'Residence': pd.StringDtype(),
        'IsAdventurer': pd.BooleanDtype(),
    }

    return pd.read_csv(data, sep=CSV_DELIM, index_col='CustomerId', parse_dates=['BirthDate'],
                       dtype={key: val for key, val in dtypes.items() if val is not None})
