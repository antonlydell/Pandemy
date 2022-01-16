r"""Example of inserting data into a table with the execute method.

This example is present in the section "Parametrized SQL statements" in the
SQLite part of the documentation.
"""

# ===============================================================
# Imports
# ===============================================================

# Standard Library
import os

# Third Party
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
    # SQL statement to create the table Item in which to save the DataFrame df
    create_table_item = r"""
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

# ===============================================================
# Example
# ===============================================================

# SQL statement to insert values into the table Item
insert_into_table_item = """
INSERT INTO Item (ItemId, ItemName, MemberOnly, Description)
    VALUES (:itemid, :itemname, :memberonly, :description);
"""

params = {'itemid': 1, 'itemname': 'Pot', 'memberonly': 0, 'description': 'This pot is empty'}

db = pandemy.SQLiteDb(file=DB_FILENAME)  # Create the SQLite DatabaseManager instance

with db.engine.connect() as conn:
    db.execute(sql=insert_into_table_item, conn=conn, params=params)

# Add some more rows
params = [
    {'itemid': 2, 'itemname': 'Jug', 'memberonly': 0, 'description': 'This jug is empty'},
    {'itemid': 3, 'itemname': 'Shears', 'memberonly': 0, 'description': 'For shearing sheep'},
    {'itemid': 4, 'itemname': 'Bucket', 'memberonly': 0, 'description': 'It''s a wooden bucket.'}
]

with db.engine.connect() as conn:
    db.execute(sql=insert_into_table_item, conn=conn, params=params)

# Retrieve the inserted rows
query = """SELECT * FROM Item;"""

with db.engine.connect() as conn:
    result = db.execute(sql=query, conn=conn)

    for row in result:
        print(row)

# ===============================================================
# Clean up
# ===============================================================

if RUN_CLEANUP:
    os.remove(DB_FILENAME)
