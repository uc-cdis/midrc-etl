import json
import os

import pandas as pd
from sqlalchemy import create_engine, text


def create_summit_engine(creds):
    db_str = (
        "postgresql+psycopg2://{username}:{password}@{hostname}:{port}/{db}".format(
            **creds
        )
    )
    eng = create_engine(db_str)
    return eng


def init_summit():
    with open(os.path.expanduser("~/.summit_db_creds.json")) as f:
        creds = json.load(f)

    eng = create_summit_engine(creds)

    return eng


eng = init_summit()


def select_query(q):
    with eng.connect().execution_options(autocommit=True) as conn:
        df = pd.read_sql(text(q), con=conn)
    return df


def get_all_cases():
    return select_query('select * from public."cases"')


def get_all_studies():
    return select_query('select * from public."studies"')


def get_all_series():
    return select_query('select * from public."series"')


def get_all_instances():
    return select_query('select * from public."instances"')


def process_submission(filepath, submission):
    df = pd.read_csv(filepath, sep="\t")

    # No longer needed, keeping for explicit NOT RENAMING
    # df = df.rename(columns={"case_ids": "case_id", "study_uid": "study_id", "series_uid": "series_id", "instance_uid": "instance_id"})

    # remove all bucket information to keep only filepath
    # for RSNA, need separate handling for ACR
    df["storage_urls"] = df["storage_urls"].str.replace(
        "s3://storage.ir.rsna.ai/", "", regex=False
    )
    df["file_name"] = df["file_name"].str.split("/").str[-1]

    # specify which submission this is
    df["submission_id"] = submission

    sub_df = df[
        [
            "storage_urls",
            "file_name",
            "file_size",
            "md5sum",
            "case_ids",
            "study_uid",
            "series_uid",
            "instance_uid",
            "submission_id",
        ]
    ]

    print(
        f'Some statistics about thise submission:\n{df[["case_ids","study_uid","series_uid","instance_uid"]].describe()}'
    )

    return sub_df

    # df = pd.read_sql('select count(*) from public."cases"', con='postgresql+psycopg2://ubuntu:password@localhost/summit')


def get_submission(submissions):
    if isinstance(submissions, str):
        where_statement = f"submission_id = '{submissions}'"
    elif isinstance(submissions, list):
        in_part = ", ".join(f"'{s}'" for s in submissions)
        where_statement = f"submission_id IN ({in_part})"
    with eng.connect().execution_options(autocommit=True) as conn:
        df = pd.read_sql(
            text(f'select * from public."everything" where {where_statement}'), con=conn
        )
    return df


def put_table(df, table):
    with eng.begin() as conn:
        df.to_sql(table, con=conn, schema="public", if_exists="append", index=False)


def put_cases(df):
    put_table(df, "cases")


def put_cases(df):
    put_table(df, "studies")


def put_cases(df):
    put_table(df, "series")


def put_cases(df):
    put_table(df, "instances")


def upsert_rows(df, table):
    with engine.begin() as conn:
        # step 0.0 - create test environment
        conn.exec_driver_sql(
            """CREATE TEMPORARY TABLE temp_table_name(column_list
);"""
        )
        conn.exec_driver_sql(
            "CREATE TABLE main_table (id int primary key, txt varchar(50))"
        )
        conn.exec_driver_sql(
            "INSERT INTO main_table (id, txt) VALUES (1, 'row 1 old text')"
        )
        # step 0.1 - create DataFrame to UPSERT
        df = pd.DataFrame(
            [(2, "new row 2 text"), (1, "row 1 new text")], columns=["id", "txt"]
        )

        # step 1 - create temporary table and upload DataFrame
        conn.exec_driver_sql(
            "CREATE TEMPORARY TABLE temp_table AS SELECT * FROM main_table WHERE false"
        )
        df.to_sql("temp_table", conn, index=False, if_exists="append")

        # step 2 - merge temp_table into main_table
        conn.exec_driver_sql(
            """\
            INSERT INTO main_table (id, txt)
            SELECT id, txt FROM temp_table
            ON CONFLICT (id) DO
                UPDATE SET txt = EXCLUDED.txt
            """
        )

        # step 3 - confirm results
        result = conn.exec_driver_sql("SELECT * FROM main_table ORDER BY id").all()
        print(result)  # [(1, 'row 1 new text'), (2, 'new row 2 text')]
