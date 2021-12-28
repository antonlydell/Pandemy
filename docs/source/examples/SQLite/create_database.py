r"""Example of creating an SQLite database.

This example is present in the section "The execute method" in the
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
RUN_CLEANUP: bool = True

# ===============================================================
# Setup - Prepare the required state of the example (None)
# ===============================================================

# ===============================================================
# Example
# ===============================================================

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
# Clean up
# ===============================================================

if RUN_CLEANUP:
    os.remove(DB_FILENAME)
