"""
AWS - DATA PROCESSING
"""

from datetime import datetime, timedelta
import pandas as pd
from utilities.mysqlConnect import paramDefs, mysqlQuery
from utilities.collectAWS import process_report


def process_amazon(envConnData, dt_to_proc):
    """
    DRIVER METHOD FOR DATA PROCESSING
    """

    # Instantiate connection dictionary
    connDict = paramDefs(envConnData)

    ##########################
    ##### AWS LOAD DATA  #####
    ##########################

    # Start and end times for reporting
    # Process from dt_to_proc back 7 days
    end = dt_to_proc
    start_pre = datetime.strptime(dt_to_proc, "%Y-%m-%d") - timedelta(days=7)
    start = start_pre.strftime("%Y-%m-%d")

    # Grab data from AWS cost explorer API
    ceDF = process_report(start=start, end=end)

    # Create dataframe and clean up
    ceDF = ceDF.rename(
        columns={
            "Dimension/Tag": "SERVICE",
            "Dimension/Tag.1": "TAG",
            "Metric": "METRIC",
            "Granularity": "GRANULARITY",
            "Start": "STARTDATE",
            "End": "ENDDATE",
            "USD Amount": "USD",
            "Unit": "UNIT",
        }
    )

    ceDF["TAG"] = ceDF["TAG"].str.slice(5)
    # cceDF['EXTRACTDATE'] = ts
    ceDF = ceDF.sort_values(by=["SERVICE", "TAG", "STARTDATE"], ascending=True)
    ceDF["DAY"] = pd.to_datetime(ceDF["STARTDATE"]).dt.day
    ceDF["WEEK"] = pd.to_datetime(ceDF["STARTDATE"]).dt.isocalendar().week.astype(int)
    ceDF["MONTH"] = pd.to_datetime(ceDF["STARTDATE"]).dt.month.astype(int)
    ceDF["YEAR"] = pd.to_datetime(ceDF["STARTDATE"]).dt.year

    # Load new data into mysql table
    payload = connDict.copy()
    payload["role"] = "tech"
    payload["write"] = True
    payload["df"] = ceDF
    payload["uploadTargetTable"] = "AWS_COST_EXP_OUTPUT"
    mysqlQuery(payload)

    # Dedup extra data from 7 day overlap into tmp table
    dedupAWS = """
    create table AWS_COST_EXP_OUTPUT_tmp as
    select distinct *
    from AWS_COST_EXP_OUTPUT;
    """
    payload["write"] = False
    payload["query"] = dedupAWS
    payload["df"] = False
    payload["uploadTargetTable"] = False
    mysqlQuery(payload)

    # Drop old table and rename tmp table
    dropAWS = """
    drop table AWS_COST_EXP_OUTPUT;
    """
    renameAWS = """
    alter table AWS_COST_EXP_OUTPUT_tmp
    rename to AWS_COST_EXP_OUTPUT;
    """
    payload["write"] = False
    payload["query"] = dropAWS
    mysqlQuery(payload)
    payload["query"] = renameAWS
    mysqlQuery(payload)
