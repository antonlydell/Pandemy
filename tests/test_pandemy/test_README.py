"""Test the code example shown in the README file of the project."""

# ===============================================================
# Imports
# ===============================================================

# Standard Library
import io
import os

# Third Party
import pandas as pd 
from pandas.testing import assert_frame_equal

# Local
import pandemy

# =================================================
# Tests
# =================================================


class TestREADME:
    r"""Test the code example shown in the README file."""

    def test_readme_dataframe_round_trip(self):
        r"""Verify that the DataFrame round trip in a new database works.

        Create a new SQLite database with a table called Item.
        Write a DataFrame to the table and then read the DataFrame back
        into another DataFrame. Check that the two DataFrames are equal.
        """

        # Setup
        # ===========================================================

        data = io.StringIO("""
ItemId,ItemName,MemberOnly,Description
1,Pot,0,This pot is empty
2,Jug,0,This jug is empty.
3,Shears,0,For shearing sheep
4,Bucket,0,It's a wooden bucket
5,Bowl,0,Useful for mixing things
6,Amulet of glory,1,A very powerful dragonstone amulet""")

        df = pd.read_csv(data, index_col='ItemId')  # Create a DataFrame

        # SQL statement to create the table Item in which to save the DataFrame df
        create_table_item = """
-- The available items in General Stores
CREATE TABLE IF NOT EXISTS Item (
ItemId      INTEGER,
ItemName    TEXT    NOT NULL,
MemberOnly  INTEGER NOT NULL,
Description TEXT,

CONSTRAINT ItemPk PRIMARY KEY (ItemId)
);"""

        db_filename = 'Runescape.db'

        # Exercise
        # ===========================================================
        db = pandemy.SQLiteDb(file=db_filename)  # Create the SQLite DatabaseManager instance

        with db.engine.connect() as conn:
            db.execute(sql=create_table_item, conn=conn)
            db.save_df(df=df, table='Item', conn=conn)

        # Load the DataFrame from the table
        db = pandemy.SQLiteDb(file=db_filename, must_exist=True)

        sql = """SELECT * FROM Item ORDER BY ItemId;"""  # Query to read back the table Item to a DataFrame

        with db.engine.connect() as conn:
            df_loaded = db.load_table(sql=sql, conn=conn, index_col='ItemId')

        # Verify
        # ===========================================================
        assert_frame_equal(df, df_loaded, check_dtype=False)

        # Clean up
        # ===========================================================
        os.remove(db_filename)
