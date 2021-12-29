r"""Example of the using the save_df method of the SQLite DatabaseManager.

This example is present in the section "Save a DataFrame to a table" in the
"Getting started - Overview" part of the documentation.
"""

# ===============================================================
# Imports
# ===============================================================

# Standard Library
import io
import os

# Third Party
import pandas as pd
import pandemy

# ===============================================================
# Constants
# ===============================================================

DB_FILENAME: str = 'Runescape.db'
RUN_CLEANUP: bool = True

# ===============================================================
# Setup - Prepare the required state of the example (None)
# ===============================================================

# ===============================================================
# Example
# ===============================================================

# Data to save to the database
data = io.StringIO("""
ItemId,ItemName,MemberOnly,Description
1,Pot,0,This pot is empty
2,Jug,0,This jug is empty.
3,Shears,0,For shearing sheep
4,Bucket,0,It's a wooden bucket
5,Bowl,0,Useful for mixing things
6,Amulet of glory,1,A very powerful dragonstone amulet
""")

df = pd.read_csv(filepath_or_buffer=data, index_col='ItemId')  # Create a DataFrame

# SQL statement to create the table Item in which to save the DataFrame df
create_table_item = """
-- The available items in General Stores
CREATE TABLE IF NOT EXISTS Item (
    ItemId      INTEGER,
    ItemName    TEXT    NOT NULL,
    MemberOnly  INTEGER NOT NULL,
    Description TEXT,

    CONSTRAINT ItemPk PRIMARY KEY (ItemId)
);
"""

db = pandemy.SQLiteDb(file=DB_FILENAME)  # Create the SQLite DatabaseManager instance

with db.engine.connect() as conn:
    db.execute(sql=create_table_item, conn=conn)
    db.save_df(df=df, table='Item', conn=conn)

# ===============================================================
# Clean up
# ===============================================================

if RUN_CLEANUP:
    os.remove(DB_FILENAME)
