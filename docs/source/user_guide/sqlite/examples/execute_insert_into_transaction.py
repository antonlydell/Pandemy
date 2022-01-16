r"""Example of the difference of using the begin and connect methods of the database engine.

This example is present in the section "Using transactions" in the
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

# ===============================================================
# Example
# ===============================================================

# Using the previously created database Runescape.db
db = pandemy.SQLiteDb(file=DB_FILENAME)

# Clear previous content in the table Item
with db.engine.connect() as conn:
    db.delete_all_records_from_table(table='Item', conn=conn)

# SQL statement to insert values into the table Item
insert_into_table_item = """
INSERT INTO Item (ItemId, ItemName, MemberOnly, Description)
    VALUES (:itemid, :itemname, :memberonly, :description);
"""

row_1 = {'itemid': 1, 'itemname': 'Pot', 'memberonly': 0, 'description': 'This pot is empty'}

with db.engine.connect() as conn:
    db.execute(sql=insert_into_table_item, conn=conn, params=row_1)

# Add a second row
row_2 = {'itemid': 2, 'itemname': 'Jug', 'memberonly': 0, 'description': 'This jug is empty'},

# Add some more rows (the last row contains a typo for the itemid parameter)
rows_3_4 = [{'itemid': 3, 'itemname': 'Shears', 'memberonly': 0, 'description': 'For shearing sheep'},
            {'itemi': 4, 'itemname': 'Bucket', 'memberonly': 0, 'description': 'It''s a wooden bucket.'}]

# Insert with a transaction
try:
    with db.engine.begin() as conn:
        db.execute(sql=insert_into_table_item, conn=conn, params=row_2)
        db.execute(sql=insert_into_table_item, conn=conn, params=rows_3_4)
except pandemy.ExecuteStatementError as e:
    print(f'{e.args}\n')

# Retrieve the inserted rows
query = """SELECT * FROM Item;"""

with db.engine.connect() as conn:
    result = db.execute(sql=query, conn=conn)
    result = result.fetchall()

print(f'All statements under the transaction are rolled back when an error occurs:\n{result}\n\n')

# Using connect instead of begin
try:
    with db.engine.connect() as conn:
        db.execute(sql=insert_into_table_item, conn=conn, params=row_2)
        db.execute(sql=insert_into_table_item, conn=conn, params=rows_3_4)
except pandemy.ExecuteStatementError as e:
    print(f'{e.args}\n')

# Retrieve the inserted rows
query = """SELECT * FROM Item;"""

with db.engine.connect() as conn:
    result = db.execute(sql=query, conn=conn)
    result = result.fetchall()

print(f'Only the statement with error is rolled back when using connect:\n{result}')

# ===============================================================
# Clean up
# ===============================================================

if RUN_CLEANUP:
    os.remove(DB_FILENAME)
