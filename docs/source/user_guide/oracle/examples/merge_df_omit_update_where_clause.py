r"""Example of using the OracleDb.merge_df method with the dry_run parameter.

Illustrates setting the parameter `omit_update_where_clause=False`.

This example is present in the section "Merge a DataFrame into a table" in the
Oracle part of the documentation.
"""

# ===============================================================
# Imports
# ===============================================================

# Standard Library
import io
import os
from unittest.mock import MagicMock

# Third Party
import pandas as pd
import pandemy

# ===============================================================
# Constants
# ===============================================================

DB_FILENAME: str = 'Runescape.db'
RUN_SETUP: bool = True
RUN_CLEANUP: bool = True

# ===============================================================
# Setup - Prepare the required state of the example
# ===============================================================

if RUN_SETUP:
    # SQL statement to create the table Item
    create_table_item = """
-- The available items in General Stores
CREATE TABLE IF NOT EXISTS Item (
    ItemId      INTEGER,
    ItemName    TEXT    NOT NULL,
    MemberOnly  INTEGER NOT NULL,
    Description TEXT,

    CONSTRAINT ItemPk PRIMARY KEY (ItemId),

    -- Validation
    CONSTRAINT MemberOnlyBool CHECK (MemberOnly IN (0,1))
);
    """

    db = pandemy.SQLiteDb(file=DB_FILENAME)  # Create the SQLite DatabaseManager instance

    data = io.StringIO(r"""
ItemId;ItemName;MemberOnly;Description
1;Pot;0;This pot is empty.
2;Jug;0;This jug is empty.
3;Shears;0;For shearing sheep.
4;Bucket;0;Its a wooden bucket.
5;Bowl;0;Useful for mixing things.
6;Cake tin;0;Useful for baking things.
7;Tinderbox;0;Useful for lighting a fire.
8;Chisel;0;Good for detailed Crafting.
9;Hammer;0;Good for hitting things.
10;Newcomer map;0;Issued to all new citizens of Gielinor.
11;Unstrung symbol;0;It needs a string so I can wear it.
12;Dragon Scimitar;1;A vicious, curved sword.
13;Amulet of glory;1;A very powerful dragonstone amulet.
14;Ranarr seed;1;A ranarr seed - plant in a herb patch.
15;Swordfish;0;Id better be careful eating this!
16;Red dragonhide Body;1;Made from 100% real dragonhide.
""")

    df = pd.read_csv(filepath_or_buffer=data, sep=';', index_col='ItemId')  # Create the DataFrame

    with db.engine.begin() as conn:
        db.execute(sql=create_table_item, conn=conn)
        db.save_df(df=df, table='Item', conn=conn)

# ===============================================================
# Example
# ===============================================================

db_sqlite = pandemy.SQLiteDb(file=DB_FILENAME, must_exist=True)

query = """SELECT * FROM Item ORDER BY ItemId ASC"""

dtypes = {
    'ItemId': 'int8',
    'ItemName': 'string',
    'MemberOnly': 'boolean',
    'Description': 'string'
}

with db_sqlite.engine.connect() as conn:
    df = db_sqlite.load_table(
        sql=query,
        conn=conn,
        index_col=['ItemId', 'ItemName'],
        dtypes=dtypes,
    )

db_oracle = pandemy.OracleDb(
    username='Fred_the_Farmer',
    password='Penguins-sheep-are-not',
    host='fred.farmer.rs',
    port=1234,
    service_name='woollysheep'
)

db_oracle.engine.begin = MagicMock()  # Mock the begin method

with db_oracle.engine.begin() as conn:
    merge_stmt = db_oracle.merge_df(
        df=df,
        table='Item',
        conn=conn,
        on_cols=['ItemId', 'ItemName'],
        merge_cols='all',
        merge_index_cols=True,
        omit_update_where_clause=False,
        dry_run=True
    )

print(f'MERGE statement:\n\n{merge_stmt}')

# ===============================================================
# Clean up
# ===============================================================

if RUN_CLEANUP:
    os.remove(DB_FILENAME)
