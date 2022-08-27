r"""Example of using the SQliteDb.upsert_table method.

This example is present in the section "Update and insert data into a table (upsert)" in the
SQLite part of the documentation.
"""

# ===============================================================
# Imports
# ===============================================================

# Standard Library
from datetime import datetime
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

print(f'Customer table original:\n\n{df}')

# Change some data
df.loc[1, ['BirthDate', 'Residence']] = [datetime(1891, 7, 15), 'Falador']
df.loc[4, 'IsAdventurer'] = True

# Add new data
df.loc[7, :] = ['Prince Ali', datetime(1969, 6, 20), 'Al Kharid', False]
df.loc[8, :] = ['Mosol Rei', datetime(1983, 4, 30), 'Shilo Village', False]

with db.engine.begin() as conn:
    # Update and insert the new data
    db.upsert_table(
        df=df,
        table='Customer',
        conn=conn,
        where_cols=['CustomerName'],
        update_index_cols=False,
        update_only=False,
        datetime_cols_dtype='str',
        datetime_format=r'%Y-%m-%d'
    )

    # Load the data back
    df_upsert = db.load_table(
        sql=query,
        conn=conn,
        index_col='CustomerId',
        dtypes=dtypes,
        parse_dates=['BirthDate']
    )

print(f'\n\nCustomer table after upsert:\n\n{df_upsert}')

# ===============================================================
# Clean up
# ===============================================================

if RUN_CLEANUP:
    os.remove(DB_FILENAME)
