"""Processing AWS cost data summaries."""

import warnings
import datetime
import numpy as np
import pandas as pd
from utilities.mysqlConnect import mysqlQuery, paramDefs

np.seterr(divide="ignore", invalid="ignore")
# Use warnings to catch and ignore 'RuntimeWarning: Degrees of freedom <= 0 for slice' warnings
warnings.filterwarnings("ignore", "Degrees of freedom <= 0 for slice")


def extract_date(date):
    """Method to extract date from datetime object."""
    return date.strftime("%Y-%m-%d")


def summarize_amazon(envConnData, dt_to_proc):
    """Method to process AWS cost data and alert on anomalies."""
    #####################################
    #####   AWS - DATA PROCESSING  ######
    #####################################

    # TODO: tech debt - month summary doesn't align with daily spike alerting which is as of day - 1
    # whereas this is as of the current day. Should this be aligned?

    # Instantiate connection dictionary
    connDict = paramDefs(envConnData)
    payload = connDict.copy()

    # Query cat & cleaned data
    ccAWS = f"""SELECT
                    SERVICE
                    , STARTDATE
                    , DAY
                    , MONTH
                    , YEAR
                    , sum(usd) as TOTAL_SPEND 
                FROM AWS_COST_EXP_OUTPUT
                WHERE 
                
                    STARTDATE >= DATE_ADD(STR_TO_DATE('{dt_to_proc}'
                                            , '%Y-%m-%d')
                                            , INTERVAL -1 YEAR) and
                    STARTDATE <= STR_TO_DATE('{dt_to_proc}', '%Y-%m-%d')

                group by
                    service
                    , startdate;
    """

    payload["dl"] = True
    payload["query"] = ccAWS
    awsCosts = mysqlQuery(payload)

    awsCalcs = awsCosts[
        ["SERVICE", "STARTDATE", "DAY", "MONTH", "YEAR", "TOTAL_SPEND"]
    ].copy()

    # Create date string
    awsCalcs["DATE_STR"] = (
        awsCalcs["YEAR"] * 10000 + awsCalcs["MONTH"] * 100 + awsCalcs["DAY"]
    ).astype(str)
    # Clean up startdate
    awsCalcs["STARTDATE"] = awsCalcs["STARTDATE"].apply(extract_date)
    # Round MTD_SPEND to 2 decimal places
    awsCalcs["TOTAL_SPEND"] = awsCalcs["TOTAL_SPEND"].round(2)

    # Convert 'STARTDATE' to datetime for filtering
    awsCalcs['STARTDATE'] = pd.to_datetime(awsCalcs['STARTDATE'])

    # Filter data to include only the current month
    current_month_start = pd.to_datetime(datetime.datetime.now().strftime('%Y-%m-01'))
    next_month_start = pd.to_datetime((datetime.datetime.now().replace(day=28) + pd.DateOffset(days=4)).strftime('%Y-%m-01'))
    awsCalcs = awsCalcs[(awsCalcs['STARTDATE'] >= current_month_start) & (awsCalcs['STARTDATE'] < next_month_start)]

    # Aggregate the data by service, year, and month
    amz_tot_x_svc = (
        awsCalcs.groupby(["SERVICE"]).agg({"TOTAL_SPEND": "sum"}).reset_index()
    )
    amz_tot_x_svc_x_day = (
        awsCalcs.groupby(["DATE_STR", "SERVICE"])
        .agg({"TOTAL_SPEND": "sum"})
        .reset_index()
    )



    return [amz_tot_x_svc, amz_tot_x_svc_x_day]
