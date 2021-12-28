r"""Example of using the replace_placeholder method of the SQLContainer class.

It demonstrates how to replace multiple placeholders.

This example is present in the section "Replace placeholders" in the
SQLite part of the documentation.
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

    data = io.StringIO(r"""
ItemId;ItemName;MemberOnly;Description
1;Pot;0;This pot is empty.
2;Jug;0;This jug is empty.
3;Shears;0;For shearing sheep.
4;Bucket;0;It's a wooden bucket.
5;Bowl;0;Useful for mixing things.
6;Amulet of glory;1;A very powerful dragonstone amulet.
7;Tinderbox;0;Useful for lighting a fire.
8;Chisel;0;Good for detailed Crafting.
9;Hammer;0;Good for hitting things.
10;Newcomer map;0;Issued to all new citizens of Gielinor.
11;Unstrung symbol;0;It needs a string so I can wear it.
12;Dragon Scimitar;1;A vicious, curved sword.
13;Amulet of glory;1;A very powerful dragonstone amulet.
14;Ranarr seed;1;A ranarr seed - plant in a herb patch.
15;Swordfish;0;I'd better be careful eating this!
16;Red dragonhide Body;1;Made from 100% real dragonhide.
""")

    df = pd.read_csv(filepath_or_buffer=data, sep=';', index_col='ItemId')  # Create the DataFrame

    with db.engine.connect() as conn:
        db.execute(sql=create_table_item, conn=conn)
        db.save_df(df=df, table='Item', conn=conn, if_exists='replace')

# ===============================================================
# Example
# ===============================================================


class SQLiteSQLContainer(pandemy.SQLContainer):
    r""""A container of SQLite database statements."""

    get_items_by_id = """
    SELECT ItemId, ItemName, MemberOnly, Description
    FROM Item
    WHERE
        ItemId IN (:itemid)      AND
        MemberOnly = :memberonly AND
        Description LIKE :description
    ORDER BY :orderby;
    """


items = [10, 12, 13, 14, 16]  # The items to retrieve from table Item

# The placeholders with the replacement values
placeholders = [
    pandemy.Placeholder(key=':itemid', values=items, new_key=True),
    pandemy.Placeholder(key=':memberonly', values=1, new_key=True),
    pandemy.Placeholder(key=':description', values='A%', new_key=True),
    pandemy.Placeholder(key=':orderby', values='ItemId DESC', new_key=False),
]

db = pandemy.SQLiteDb(file=DB_FILENAME, container=SQLiteSQLContainer)

stmt, params = db.container.replace_placeholders(stmt=db.container.get_items_by_id, placeholders=placeholders)

print(f'get_items_by_id after replacements:\n{stmt}\n')
print(f'The new placeholders with mapped values:\n{params}\n')

with db.engine.connect() as conn:
    df = db.load_table(sql=stmt, conn=conn, params=params, index_col='ItemId')

print(f'The DataFrame from the parametrized query:\n{df}')

# ===============================================================
# Clean up
# ===============================================================

if RUN_CLEANUP:
    os.remove(DB_FILENAME)
