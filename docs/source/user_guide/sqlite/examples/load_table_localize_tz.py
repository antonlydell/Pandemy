r"""Example of using the load_table method of the DatabaseManager with localize_tz and target_tz.

This example is located in the section "Working with datetimes and timezones"
in the SQLite part of the documentation.
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

# SQL statement to create the table Customer in which to save the DataFrame df
create_table_customer = """
-- Customers that have traded in a General Store
CREATE TABLE IF NOT EXISTS Customer (
    CustomerId         INTEGER,
    CustomerName       TEXT    NOT NULL,
    BirthDate          TEXT,
    Residence          TEXT,
    IsAdventurer       INTEGER NOT NULL, -- 1 if Adventurer and 0 if NPC

    CONSTRAINT CustomerPk PRIMARY KEY (CustomerId)
);
"""

db = pandemy.SQLiteDb(file=DB_FILENAME)  # Create the SQLite DatabaseManager instance

data = io.StringIO("""
CustomerId;CustomerName;BirthDate;Residence;IsAdventurer
1;Zezima;1990-07-14;Yanille;1
2;Dr Harlow;1970-01-14;Varrock;0
3;Baraek;1968-12-13;Varrock;0
4;Gypsy Aris;1996-03-24;Varrock;0
5;Not a Bot;2006-05-31;Catherby;1
6;Max Pure;2007-08-20;Port Sarim;1
""")

df = pd.read_csv(filepath_or_buffer=data, sep=';', index_col='CustomerId',
                 parse_dates=['BirthDate'])  # Create a DataFrame

with db.engine.connect() as conn:
    db.execute(sql=create_table_customer, conn=conn)
    db.save_df(df=df, table='Customer', conn=conn, if_exists='replace')

    df_naive = db.load_table(sql='Customer', conn=conn, index_col='CustomerId',
                             parse_dates=['Birthdate'])

    df_dt_aware = db.load_table(sql='Customer', conn=conn, index_col='CustomerId',
                                parse_dates=['Birthdate'], localize_tz='UTC', target_tz='CET')

print(f'df:\n{df}\n')
print(f'df_naive:\n{df_naive}\n')
print(f'df_dt_aware:\n{df_dt_aware}')

# ===============================================================
# Clean up
# ===============================================================

if RUN_CLEANUP:
    os.remove(DB_FILENAME)
