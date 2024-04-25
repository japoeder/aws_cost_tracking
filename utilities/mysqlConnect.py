"""Utility for connecting to MySQL database."""

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker


def paramDefs(envConnData):
    """Method to define connection parameters for MySQL."""
    connDict = {
        "user": envConnData["mysql_usr"],  # mysql user name
        "password": envConnData["mysql_pwd"],  # mysql password
        "database": envConnData["mysql_db"],  # mysql database
        "db_url": envConnData["mysql_url"],  # mysql url
        "uploadTargetTable": False,  # mysql upload destination table
        "df": False,  # dataframe for mysql upload
        "write": False,  # flag to upload data
        "dl": False,  # flag to return queried data
        "query": False,  # query to submit
        "overwrite": False,  # overwrite dest. table if exists
    }

    return connDict


def write_pandas(engine, df, uploadTargetTable, overwrite=False):
    """Method to write a pandas dataframe to a MySQL table."""
    if overwrite:
        df.to_sql(uploadTargetTable, con=engine, if_exists="replace", index=False)
    else:
        df.to_sql(uploadTargetTable, con=engine, if_exists="append", index=False)

    return


def mysqlQuery(connDict):
    """Method to query a MySQL database."""
    password = connDict["password"]
    user = connDict["user"]
    db_url = connDict["db_url"]
    database = connDict["database"]
    df = connDict["df"]
    write = connDict["write"]
    dl = connDict["dl"]
    uploadTargetTable = connDict["uploadTargetTable"]
    query = connDict["query"]
    overwrite = connDict["overwrite"]

    engine = create_engine(
        f"mysql+mysqlconnector://{user}:{password}@{db_url}/{database}"
    )

    resDF = None

    # When writing a DF to a table:
    if write is True:
        write_pandas(engine, df, uploadTargetTable, overwrite=overwrite)

    # When downloading query data
    if dl is True:
        resDF = pd.read_sql(query, engine)

    # When operating on the database and no upload / download
    if write is False and dl is False:
        Session = sessionmaker(bind=engine)
        session = Session()
        session.execute(text(query))

        # Submit an asynchronous query for execution.
        session.commit()

    return resDF
