r"""Example of using the SQliteDb.upsert_table method with the dry_run parameter.

This example is present in the section "Update and insert data into a table (upsert)" in the
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
    # SQL statement to create the table Customer and insert data into it
    create_table_customer = """
-- Customers that have traded in a General Store
CREATE TABLE IF NOT EXISTS Customer (
    CustomerId         INTEGER,
    CustomerName       TEXT    NOT NULL,
    BirthDate          TEXT,
    Residence          TEXT,
    IsAdventurer       INTEGER NOT NULL, -- 1 if Adventurer and 0 if NPC 

    CONSTRAINT CustomerPk PRIMARY KEY (CustomerId),

    CONSTRAINT IsAdventurerBool CHECK (IsAdventurer IN (0, 1))
);
    """

    db = pandemy.SQLiteDb(file=DB_FILENAME)  # Create the SQLite DatabaseManager instance

    data = io.StringIO(r"""
CustomerId;CustomerName;BirthDate;Residence;IsAdventurer
1;Zezima;1990-07-14;Yanille;1
2;Dr Harlow;1970-01-14;Varrock;0
3;Baraek;1968-12-13;Varrock;0
4;Gypsy Aris;1996-03-24;Varrock;0
5;Not a Bot;2006-05-31;Catherby;1
6;Max Pure;2007-08-20;Port Sarim;1
7;Prince Ali;1969-06-20;Al Kharid;0
8;Mosol Rei;1983-04-30;Shilo Village;0
    """)

    df = pd.read_csv(filepath_or_buffer=data, sep=';', index_col='CustomerId')  # Create the DataFrame

    with db.engine.begin() as conn:
        db.execute(sql=create_table_customer, conn=conn)
        db.save_df(df=df, table='Customer', conn=conn)


# ===============================================================
# Example
# ===============================================================

db = pandemy.SQLiteDb(file=DB_FILENAME, must_exist=True)

query = """SELECT * FROM Customer ORDER BY CustomerId ASC"""

dtypes = {
    'CustomerName': 'string',
    'Residence': 'string',
    'IsAdventurer': 'boolean'
}

with db.engine.connect() as conn:
    df = db.load_table(
        sql=query,
        conn=conn,
        index_col='CustomerId',
        dtypes=dtypes,
        parse_dates=['BirthDate']
    )

print(f'Customer table:\n\n{df}')

with db.engine.begin() as conn:
    # Get the UPDATE and INSERT statements
    update_stmt, insert_stmt = db.upsert_table(
        df=df,
        table='Customer',
        conn=conn,
        where_cols=['CustomerName', 'BirthDate'],
        update_cols=['Residence', 'CustomerName'],
        update_index_cols=True,
        update_only=False,
        dry_run=True
    )

print(f'\n\nUPDATE statement:\n\n{update_stmt}')
print(f'INSERT statement:\n\n{insert_stmt}')

# ===============================================================
# Clean up
# ===============================================================

if RUN_CLEANUP:
    os.remove(DB_FILENAME)
