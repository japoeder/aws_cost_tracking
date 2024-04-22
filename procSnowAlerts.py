from datetime import datetime, timedelta
from utilities.mysqlConnect import *
from utilities.getCredentials import *
import pandas as pd
import numpy as np
from scipy.stats import zscore


def alerting_mysql(cf, z_in):
    #####################################
    #####   SF - DATA PROCESSING  ######
    #####################################

    # Grab credentials for AMZ (us-east)
    credList = get_cred(
        file=cf[0],
        env_user=cf[1]["sf_east_user"],
        env_pwd=cf[1]["sf_east_pwd"],
    )

    # Instantiate connection dictionary
    connDict = paramDefs(credList[0], credList[1])
    payload = connDict.copy()

    # Query cat & cleaned data
    dbUseQuery = """select * from JPOEDER_AA.DW.DATABASE_STORAGE_USAGE_HISTORY
                    where usage_date >= dateadd(year, -1, current_date);"""
    payload["dl"] = True
    payload["query"] = dbUseQuery
    storageCosts = snowQuery(payload)
    storageCosts["USD"] = storageCosts["DAILY_TERAS"] * 23

    # Query cat & cleaned data
    creditUseQuery = """select * from JPOEDER_AA.DW.WAREHOUSE_METERING_HISTORY
                        where START_TIME >= dateadd(year, -1, current_date);"""
    payload["dl"] = True
    payload["query"] = creditUseQuery
    computeCosts = snowQuery(payload)
    computeCosts["USD"] = computeCosts["CREDITS_USED"] * 2

    storageCalcs = storageCosts.copy()
    storageCalcs["DAY"] = pd.to_datetime(storageCalcs["USAGE_DATE"]).dt.day
    storageCalcs["WEEK"] = (
        pd.to_datetime(storageCalcs["USAGE_DATE"]).dt.isocalendar().week.astype(int)
    )
    storageCalcs["MONTH"] = storageCalcs["MONTH"].astype(int)
    storageCalcs["YEAR"] = storageCalcs["YEAR"].astype(int)
    storageCalcs["yearWK"] = (
        storageCalcs["YEAR"].astype(int) * 100 + storageCalcs["WEEK"]
    )

    computeCalcs = computeCosts.copy()
    computeCalcs["DAY"] = pd.to_datetime(computeCalcs["START_TIME"]).dt.day
    computeCalcs["WEEK"] = (
        pd.to_datetime(computeCalcs["START_TIME"]).dt.isocalendar().week.astype(int)
    )
    computeCalcs["MONTH"] = pd.to_datetime(computeCalcs["START_TIME"]).dt.month.astype(
        int
    )
    computeCalcs["YEAR"] = pd.to_datetime(computeCalcs["START_TIME"]).dt.year
    computeCalcs["yearWK"] = (
        computeCalcs["YEAR"].astype(int) * 100 + computeCalcs["WEEK"]
    )

    #         STORAGE              #

    #####################################
    ##### Region by day lvl calcs ######
    #####################################

    sfTotStorageDayAgg = storageCalcs[["REGION", "YEAR", "MONTH", "DAY", "USD"]].copy()
    sfTotStorageDayAgg = sfTotStorageDayAgg.groupby(
        ["REGION", "YEAR", "MONTH", "DAY"], as_index=True
    ).agg({"USD": ["sum"]})
    sfTotStorageDayAgg.columns = ["USD"]
    sfTotStorageDayAgg = sfTotStorageDayAgg.reset_index()

    # Z-score by service across days
    sfTotStorageDayZs = sfTotStorageDayAgg.copy()
    totStorageSpikePlots = sfTotStorageDayAgg.copy()
    sfTotStorageDayZs["tot_db_day_zscore"] = sfTotStorageDayZs.groupby(["REGION"])[
        "USD"
    ].transform(zscore)
    sfTotStorageDayZs = sfTotStorageDayZs.drop("USD", axis=1)

    # Moments by service across days
    dbTotDayMoments = sfTotStorageDayAgg.copy()
    dbTotDayMoments = dbTotDayMoments.groupby(["REGION"], as_index=True).agg(
        {"USD": ["min", "max", "mean"]}
    )
    dbTotDayMoments.columns = [
        "tot_db_day_usd_min",
        "tot_db_day_usd_max",
        "tot_db_day_usd_mean",
    ]
    dbTotDayMoments = dbTotDayMoments.reset_index()

    # Trend alerting calcs
    dbTotDayMACD = sfTotStorageDayAgg.copy()
    dbTotDayMACD["tot_db_day_usd_12ewm"] = dbTotDayMACD.groupby(["REGION"])[
        "USD"
    ].transform(lambda x: x.ewm(span=12, adjust=False).mean())
    dbTotDayMACD["tot_db_day_usd_24ewm"] = dbTotDayMACD.groupby(["REGION"])[
        "USD"
    ].transform(lambda x: x.ewm(span=24, adjust=False).mean())
    dbTotDayMACD["tot_db_day_usd_26ewm"] = dbTotDayMACD.groupby(["REGION"])[
        "USD"
    ].transform(lambda x: x.ewm(span=26, adjust=False).mean())
    dbTotDayMACD["tot_db_day_usd_52ewm"] = dbTotDayMACD.groupby(["REGION"])[
        "USD"
    ].transform(lambda x: x.ewm(span=52, adjust=False).mean())
    dbTotDayMACD["tot_db_day_usd_macd_26"] = (
        dbTotDayMACD["tot_db_day_usd_26ewm"] - dbTotDayMACD["tot_db_day_usd_12ewm"]
    )
    dbTotDayMACD["tot_db_day_usd_macd_52"] = (
        dbTotDayMACD["tot_db_day_usd_52ewm"] - dbTotDayMACD["tot_db_day_usd_24ewm"]
    )
    dbTotDayMACD["tot_db_day_usd_9ewm"] = dbTotDayMACD[
        "tot_db_day_usd_macd_26"
    ].transform(lambda x: x.ewm(span=9, adjust=False).mean())
    dbTotDayMACD["tot_db_day_usd_18ewm"] = dbTotDayMACD[
        "tot_db_day_usd_macd_52"
    ].transform(lambda x: x.ewm(span=18, adjust=False).mean())
    dbTotDayMACD = dbTotDayMACD.drop("USD", axis=1)

    #####################################
    ### Region by db & day lvl calcs ###
    #####################################

    sfStorageDayAgg = storageCalcs[
        ["REGION", "DATABASE_NAME", "YEAR", "MONTH", "DAY", "USD"]
    ].copy()
    sfStorageDayAgg = sfStorageDayAgg.groupby(
        ["REGION", "DATABASE_NAME", "YEAR", "MONTH", "DAY"], as_index=True
    ).agg({"USD": ["sum"]})
    sfStorageDayAgg.columns = ["USD"]
    sfStorageDayAgg = sfStorageDayAgg.reset_index()

    # Z-score by service across days
    sfStorageDayZs = sfStorageDayAgg.copy()
    storageSpikePlots = sfStorageDayAgg.copy()
    sfStorageDayZs["db_day_zscore"] = sfStorageDayZs.groupby(
        ["REGION", "DATABASE_NAME"]
    )["USD"].transform(zscore)
    sfStorageDayZs = sfStorageDayZs.drop("USD", axis=1)

    # Moments by service across days
    dbDayMoments = sfStorageDayAgg.copy()
    dbDayMoments = dbDayMoments.groupby(["REGION", "DATABASE_NAME"], as_index=True).agg(
        {"USD": ["min", "max", "mean"]}
    )
    dbDayMoments.columns = ["db_day_usd_min", "db_day_usd_max", "db_day_usd_mean"]
    dbDayMoments = dbDayMoments.reset_index()

    # Trend alerting calcs
    dbDayMACD = sfStorageDayAgg.copy()
    dbDayMACD["db_day_usd_12ewm"] = dbDayMACD.groupby(["REGION", "DATABASE_NAME"])[
        "USD"
    ].transform(lambda x: x.ewm(span=12, adjust=False).mean())
    dbDayMACD["db_day_usd_24ewm"] = dbDayMACD.groupby(["REGION", "DATABASE_NAME"])[
        "USD"
    ].transform(lambda x: x.ewm(span=24, adjust=False).mean())
    dbDayMACD["db_day_usd_26ewm"] = dbDayMACD.groupby(["REGION", "DATABASE_NAME"])[
        "USD"
    ].transform(lambda x: x.ewm(span=26, adjust=False).mean())
    dbDayMACD["db_day_usd_52ewm"] = dbDayMACD.groupby(["REGION", "DATABASE_NAME"])[
        "USD"
    ].transform(lambda x: x.ewm(span=52, adjust=False).mean())
    dbDayMACD["db_day_usd_macd_26"] = (
        dbDayMACD["db_day_usd_26ewm"] - dbDayMACD["db_day_usd_12ewm"]
    )
    dbDayMACD["db_day_usd_macd_52"] = (
        dbDayMACD["db_day_usd_52ewm"] - dbDayMACD["db_day_usd_24ewm"]
    )
    dbDayMACD["db_day_usd_9ewm"] = dbDayMACD["db_day_usd_macd_26"].transform(
        lambda x: x.ewm(span=9, adjust=False).mean()
    )
    dbDayMACD["db_day_usd_18ewm"] = dbDayMACD["db_day_usd_macd_52"].transform(
        lambda x: x.ewm(span=18, adjust=False).mean()
    )
    dbDayMACD = dbDayMACD.drop("USD", axis=1)

    ##################################
    # Storage Wk. level calculations #
    ##################################

    # AGGREGATE TO WEEKLY FIRST.
    sfStorageWkAgg = storageCalcs.copy()
    sfStorageWkAgg = sfStorageWkAgg.groupby(
        ["REGION", "DATABASE_NAME", "yearWK"], as_index=True
    ).agg({"USD": ["sum"]})
    sfStorageWkAgg.columns = ["USD"]
    sfStorageWkAgg = sfStorageWkAgg.reset_index()

    # Z-score by service across weeks
    sfStorageWkZs = sfStorageWkAgg.copy()
    sfStorageWkZs["svc_wk_zscore"] = sfStorageWkZs.groupby(["REGION", "DATABASE_NAME"])[
        "USD"
    ].transform(zscore)
    sfStorageWkZs = sfStorageWkZs.drop("USD", axis=1)

    # Moments by service across weeks
    dbWkMoments = sfStorageWkAgg.copy()
    dbWkMoments = dbWkMoments.groupby(["REGION", "DATABASE_NAME"], as_index=True).agg(
        {"USD": ["min", "max", "mean"]}
    )
    dbWkMoments.columns = ["db_wk_usd_min", "db_wk_usd_max", "db_wk_usd_mean"]
    dbWkMoments = dbWkMoments.reset_index()

    ####################################
    ######## FINAL DATA MERGE ##########
    ####################################

    # Merge everything back together
    storageCalcs = pd.merge(
        storageCalcs,
        sfStorageWkZs,
        how="left",
        left_on=["REGION", "DATABASE_NAME", "yearWK"],
        right_on=["REGION", "DATABASE_NAME", "yearWK"],
    )
    storageCalcs = pd.merge(
        storageCalcs,
        dbWkMoments,
        how="left",
        left_on=["REGION", "DATABASE_NAME"],
        right_on=["REGION", "DATABASE_NAME"],
    )
    storageCalcs = pd.merge(
        storageCalcs,
        sfStorageDayZs,
        how="left",
        left_on=["REGION", "DATABASE_NAME", "YEAR", "MONTH", "DAY"],
        right_on=["REGION", "DATABASE_NAME", "YEAR", "MONTH", "DAY"],
    )
    storageCalcs = pd.merge(
        storageCalcs,
        dbDayMoments,
        how="left",
        left_on=["REGION", "DATABASE_NAME"],
        right_on=["REGION", "DATABASE_NAME"],
    )
    storageCalcs = pd.merge(
        storageCalcs,
        dbDayMACD,
        how="left",
        left_on=["REGION", "DATABASE_NAME", "YEAR", "MONTH", "DAY"],
        right_on=["REGION", "DATABASE_NAME", "YEAR", "MONTH", "DAY"],
    )
    storageCalcs = storageCalcs.sort_values(
        by=["REGION", "DATABASE_NAME", "YEAR", "MONTH", "DAY"]
    )

    #         COMPUTE              #

    #####################################
    ##### Region by day lvl calcs ######
    #####################################
    sfTotComputeDayAgg = computeCalcs[["REGION", "YEAR", "MONTH", "DAY", "USD"]].copy()
    sfTotComputeDayAgg = sfTotComputeDayAgg.groupby(
        ["REGION", "YEAR", "MONTH", "DAY"], as_index=True
    ).agg({"USD": ["sum"]})
    sfTotComputeDayAgg.columns = ["USD"]
    sfTotComputeDayAgg = sfTotComputeDayAgg.reset_index()

    # Z-score by service across days
    sfTotComputeDayZs = sfTotComputeDayAgg.copy()
    totComputeSpikePlots = sfTotComputeDayAgg.copy()
    sfTotComputeDayZs["tot_wh_day_zscore"] = sfTotComputeDayZs.groupby(["REGION"])[
        "USD"
    ].transform(zscore)
    sfTotComputeDayZs = sfTotComputeDayZs.drop("USD", axis=1)

    # Moments by service across days
    whTotDayMoments = sfTotComputeDayAgg.copy()
    whTotDayMoments = whTotDayMoments.groupby(["REGION"], as_index=True).agg(
        {"USD": ["min", "max", "mean"]}
    )
    whTotDayMoments.columns = [
        "tot_wh_day_usd_min",
        "tot_wh_day_usd_max",
        "tot_wh_day_usd_mean",
    ]
    whTotDayMoments = whTotDayMoments.reset_index()

    # Trend alerting calcs
    whTotDayMACD = sfTotComputeDayAgg.copy()
    whTotDayMACD["tot_wh_day_usd_12ewm"] = whTotDayMACD.groupby(["REGION"])[
        "USD"
    ].transform(lambda x: x.ewm(span=12, adjust=False).mean())
    whTotDayMACD["tot_wh_day_usd_24ewm"] = whTotDayMACD.groupby(["REGION"])[
        "USD"
    ].transform(lambda x: x.ewm(span=24, adjust=False).mean())
    whTotDayMACD["tot_wh_day_usd_26ewm"] = whTotDayMACD.groupby(["REGION"])[
        "USD"
    ].transform(lambda x: x.ewm(span=26, adjust=False).mean())
    whTotDayMACD["tot_wh_day_usd_52ewm"] = whTotDayMACD.groupby(["REGION"])[
        "USD"
    ].transform(lambda x: x.ewm(span=52, adjust=False).mean())
    whTotDayMACD["tot_wh_day_usd_macd_26"] = (
        whTotDayMACD["tot_wh_day_usd_26ewm"] - whTotDayMACD["tot_wh_day_usd_12ewm"]
    )
    whTotDayMACD["tot_wh_day_usd_macd_52"] = (
        whTotDayMACD["tot_wh_day_usd_52ewm"] - whTotDayMACD["tot_wh_day_usd_24ewm"]
    )
    whTotDayMACD["tot_wh_day_usd_9ewm"] = whTotDayMACD[
        "tot_wh_day_usd_macd_26"
    ].transform(lambda x: x.ewm(span=9, adjust=False).mean())
    whTotDayMACD["tot_wh_day_usd_18ewm"] = whTotDayMACD[
        "tot_wh_day_usd_macd_52"
    ].transform(lambda x: x.ewm(span=18, adjust=False).mean())
    whTotDayMACD = whTotDayMACD.drop("USD", axis=1)

    ################################
    ##### WH by day lvl calcs ######
    ################################
    sfComputeDayAgg = computeCalcs[
        ["REGION", "WAREHOUSE_NAME", "YEAR", "MONTH", "DAY", "USD"]
    ].copy()
    sfComputeDayAgg = sfComputeDayAgg.groupby(
        ["REGION", "WAREHOUSE_NAME", "YEAR", "MONTH", "DAY"], as_index=True
    ).agg({"USD": ["sum"]})
    sfComputeDayAgg.columns = ["USD"]
    sfComputeDayAgg = sfComputeDayAgg.reset_index()

    # Z-score by service across days
    sfComputeDayZs = sfComputeDayAgg.copy()
    computeSpikePlots = sfComputeDayAgg.copy()
    sfComputeDayZs["wh_day_zscore"] = sfComputeDayZs.groupby(
        ["REGION", "WAREHOUSE_NAME"]
    )["USD"].transform(zscore)
    sfComputeDayZs = sfComputeDayZs.drop("USD", axis=1)

    # Moments by service across days
    whDayMoments = sfComputeDayAgg.copy()
    whDayMoments = whDayMoments.groupby(
        ["REGION", "WAREHOUSE_NAME"], as_index=True
    ).agg({"USD": ["min", "max", "mean"]})
    whDayMoments.columns = ["wh_day_usd_min", "wh_day_usd_max", "wh_day_usd_mean"]
    whDayMoments = whDayMoments.reset_index()

    # Trend alerting calcs
    whDayMACD = sfComputeDayAgg.copy()
    whDayMACD["wh_day_usd_12ewm"] = whDayMACD.groupby(["REGION", "WAREHOUSE_NAME"])[
        "USD"
    ].transform(lambda x: x.ewm(span=12, adjust=False).mean())
    whDayMACD["wh_day_usd_24ewm"] = whDayMACD.groupby(["REGION", "WAREHOUSE_NAME"])[
        "USD"
    ].transform(lambda x: x.ewm(span=24, adjust=False).mean())
    whDayMACD["wh_day_usd_26ewm"] = whDayMACD.groupby(["REGION", "WAREHOUSE_NAME"])[
        "USD"
    ].transform(lambda x: x.ewm(span=26, adjust=False).mean())
    whDayMACD["wh_day_usd_52ewm"] = whDayMACD.groupby(["REGION", "WAREHOUSE_NAME"])[
        "USD"
    ].transform(lambda x: x.ewm(span=52, adjust=False).mean())
    whDayMACD["wh_day_usd_macd_26"] = (
        whDayMACD["wh_day_usd_26ewm"] - whDayMACD["wh_day_usd_12ewm"]
    )
    whDayMACD["wh_day_usd_macd_52"] = (
        whDayMACD["wh_day_usd_52ewm"] - whDayMACD["wh_day_usd_24ewm"]
    )
    whDayMACD["wh_day_usd_9ewm"] = whDayMACD["wh_day_usd_macd_26"].transform(
        lambda x: x.ewm(span=9, adjust=False).mean()
    )
    whDayMACD["wh_day_usd_18ewm"] = whDayMACD["wh_day_usd_macd_52"].transform(
        lambda x: x.ewm(span=18, adjust=False).mean()
    )
    whDayMACD = whDayMACD.drop("USD", axis=1)

    ##################################
    # Compute Wk. level calculations #
    ##################################

    # AGGREGATE TO WEEKLY FIRST.
    sfComputeWkAgg = computeCalcs.copy()
    sfComputeWkAgg = sfComputeWkAgg.groupby(
        ["REGION", "WAREHOUSE_NAME", "yearWK"], as_index=True
    ).agg({"USD": ["sum"]})
    sfComputeWkAgg.columns = ["USD"]
    sfComputeWkAgg = sfComputeWkAgg.reset_index()

    # Z-score by service across weeks
    sfComputeWkZs = sfComputeWkAgg.copy()
    sfComputeWkZs["wh_wk_zscore"] = sfComputeWkZs.groupby(["REGION", "WAREHOUSE_NAME"])[
        "USD"
    ].transform(zscore)
    sfComputeWkZs = sfComputeWkZs.drop("USD", axis=1)

    # Moments by service across weeks
    whWkMoments = sfComputeWkAgg.copy()
    whWkMoments = whWkMoments.groupby(["REGION", "WAREHOUSE_NAME"], as_index=True).agg(
        {"USD": ["min", "max", "mean"]}
    )
    whWkMoments.columns = ["wh_wk_usd_min", "wh_wk_usd_max", "wh_wk_usd_mean"]
    whWkMoments = whWkMoments.reset_index()

    ####################################
    ######## FINAL DATA MERGE ##########
    ####################################

    # Merge everything back together
    computeCalcs = pd.merge(
        computeCalcs,
        sfComputeWkZs,
        how="left",
        left_on=["REGION", "WAREHOUSE_NAME", "yearWK"],
        right_on=["REGION", "WAREHOUSE_NAME", "yearWK"],
    )
    computeCalcs = pd.merge(
        computeCalcs,
        whWkMoments,
        how="left",
        left_on=["REGION", "WAREHOUSE_NAME"],
        right_on=["REGION", "WAREHOUSE_NAME"],
    )
    computeCalcs = pd.merge(
        computeCalcs,
        sfComputeDayZs,
        how="left",
        left_on=["REGION", "WAREHOUSE_NAME", "YEAR", "MONTH", "DAY"],
        right_on=["REGION", "WAREHOUSE_NAME", "YEAR", "MONTH", "DAY"],
    )
    computeCalcs = pd.merge(
        computeCalcs,
        whDayMoments,
        how="left",
        left_on=["REGION", "WAREHOUSE_NAME"],
        right_on=["REGION", "WAREHOUSE_NAME"],
    )
    computeCalcs = pd.merge(
        computeCalcs,
        whDayMACD,
        how="left",
        left_on=["REGION", "WAREHOUSE_NAME", "YEAR", "MONTH", "DAY"],
        right_on=["REGION", "WAREHOUSE_NAME", "YEAR", "MONTH", "DAY"],
    )
    computeCalcs = computeCalcs.sort_values(
        by=["REGION", "WAREHOUSE_NAME", "YEAR", "MONTH", "DAY"]
    )

    # Create some DFs for MACD charting

    sfComputeMacdTesting = whDayMACD.copy()
    sfComputeMacdTesting["date"] = pd.to_datetime(
        dict(
            year=sfComputeMacdTesting.YEAR,
            month=sfComputeMacdTesting.MONTH,
            day=sfComputeMacdTesting.DAY,
        )
    )
    sfComputeMacdTesting = sfComputeMacdTesting.sort_values(
        by=["REGION", "WAREHOUSE_NAME", "date"]
    )

    sfTotComputeMacdTesting = whTotDayMACD.copy()
    sfTotComputeMacdTesting["date"] = (
        pd.to_datetime(
            dict(
                year=sfTotComputeMacdTesting.YEAR,
                month=sfTotComputeMacdTesting.MONTH,
                day=sfTotComputeMacdTesting.DAY,
            )
        )
    ).dt.strftime("%Y-%m-%d")
    # sfTotComputeMacdTesting['date'] = sfTotComputeMacdTesting['date'].dt.strftime("%Y-%m-%d")
    sfTotComputeMacdTesting = sfTotComputeMacdTesting.sort_values(by=["REGION", "date"])

    sfStorageMacdTesting = dbDayMACD.copy()
    sfStorageMacdTesting["date"] = pd.to_datetime(
        dict(
            year=sfStorageMacdTesting.YEAR,
            month=sfStorageMacdTesting.MONTH,
            day=sfStorageMacdTesting.DAY,
        )
    )
    sfStorageMacdTesting = sfStorageMacdTesting.sort_values(
        by=["REGION", "DATABASE_NAME", "date"]
    )

    sfTotStorageMacdTesting = dbTotDayMACD.copy()
    sfTotStorageMacdTesting["date"] = pd.to_datetime(
        dict(
            year=sfTotStorageMacdTesting.YEAR,
            month=sfTotStorageMacdTesting.MONTH,
            day=sfTotStorageMacdTesting.DAY,
        )
    )
    sfTotStorageMacdTesting = sfTotStorageMacdTesting.sort_values(by=["REGION", "date"])

    ####################################
    #####  STORAGE TREND ALERTS  #######
    ####################################

    # Region x database x day

    conditions = [
        (
            sfStorageMacdTesting.groupby(["REGION", "DATABASE_NAME"])[
                "db_day_usd_macd_52"
            ].shift(1)
            >= (
                sfStorageMacdTesting.groupby(["REGION", "DATABASE_NAME"])[
                    "db_day_usd_18ewm"
                ].shift(1)
            )
        )
        & (
            sfStorageMacdTesting["db_day_usd_macd_52"]
            < sfStorageMacdTesting["db_day_usd_18ewm"]
        ),
        (
            sfStorageMacdTesting.groupby(["REGION", "DATABASE_NAME"])[
                "db_day_usd_macd_52"
            ].shift(1)
            <= (
                sfStorageMacdTesting.groupby(["REGION", "DATABASE_NAME"])[
                    "db_day_usd_18ewm"
                ].shift(1)
            )
        )
        & (
            sfStorageMacdTesting["db_day_usd_macd_52"]
            > sfStorageMacdTesting["db_day_usd_18ewm"]
        ),
        (
            sfStorageMacdTesting.groupby(["REGION", "DATABASE_NAME"])[
                "db_day_usd_macd_52"
            ].shift(1)
            >= (
                sfStorageMacdTesting.groupby(["REGION", "DATABASE_NAME"])[
                    "db_day_usd_18ewm"
                ].shift(1)
            )
        )
        & (
            sfStorageMacdTesting["db_day_usd_macd_52"]
            >= sfStorageMacdTesting["db_day_usd_18ewm"]
        ),
        (
            sfStorageMacdTesting.groupby(["REGION", "DATABASE_NAME"])[
                "db_day_usd_macd_52"
            ].shift(1)
            <= (
                sfStorageMacdTesting.groupby(["REGION", "DATABASE_NAME"])[
                    "db_day_usd_18ewm"
                ].shift(1)
            )
        )
        & (
            sfStorageMacdTesting["db_day_usd_macd_52"]
            <= sfStorageMacdTesting["db_day_usd_18ewm"]
        ),
        (
            np.isnan(
                sfStorageMacdTesting.groupby(["REGION", "DATABASE_NAME"])[
                    "db_day_usd_macd_52"
                ].shift(1)
            )
        ),
    ]

    results = [-1, 1, 0, 0, 0]

    sfStorageMacdTesting["crossFlag"] = np.select(conditions, results)
    dtChk = (datetime.now()).strftime("%Y-%m-%d")
    storageTrendTest = sfStorageMacdTesting[sfStorageMacdTesting["date"] == dtChk]
    storageMacdAlerts = storageTrendTest[storageTrendTest["crossFlag"] != 0]

    # Region x day

    conditions = [
        (
            sfTotStorageMacdTesting.groupby(["REGION"])["tot_db_day_usd_macd_52"].shift(
                1
            )
            >= (
                sfTotStorageMacdTesting.groupby(["REGION"])[
                    "tot_db_day_usd_18ewm"
                ].shift(1)
            )
        )
        & (
            sfTotStorageMacdTesting["tot_db_day_usd_macd_52"]
            < sfTotStorageMacdTesting["tot_db_day_usd_18ewm"]
        ),
        (
            sfTotStorageMacdTesting.groupby(["REGION"])["tot_db_day_usd_macd_52"].shift(
                1
            )
            <= (
                sfTotStorageMacdTesting.groupby(["REGION"])[
                    "tot_db_day_usd_18ewm"
                ].shift(1)
            )
        )
        & (
            sfTotStorageMacdTesting["tot_db_day_usd_macd_52"]
            > sfTotStorageMacdTesting["tot_db_day_usd_18ewm"]
        ),
        (
            sfTotStorageMacdTesting.groupby(["REGION"])["tot_db_day_usd_macd_52"].shift(
                1
            )
            >= (
                sfTotStorageMacdTesting.groupby(["REGION"])[
                    "tot_db_day_usd_18ewm"
                ].shift(1)
            )
        )
        & (
            sfTotStorageMacdTesting["tot_db_day_usd_macd_52"]
            >= sfTotStorageMacdTesting["tot_db_day_usd_18ewm"]
        ),
        (
            sfTotStorageMacdTesting.groupby(["REGION"])["tot_db_day_usd_macd_52"].shift(
                1
            )
            <= (
                sfTotStorageMacdTesting.groupby(["REGION"])[
                    "tot_db_day_usd_18ewm"
                ].shift(1)
            )
        )
        & (
            sfTotStorageMacdTesting["tot_db_day_usd_macd_52"]
            <= sfTotStorageMacdTesting["tot_db_day_usd_18ewm"]
        ),
        (
            np.isnan(
                sfTotStorageMacdTesting.groupby(["REGION"])[
                    "tot_db_day_usd_macd_52"
                ].shift(1)
            )
        ),
    ]

    results = [-1, 1, 0, 0, 0]

    sfTotStorageMacdTesting["crossFlag"] = np.select(conditions, results)
    dtChk = (datetime.now()).strftime("%Y-%m-%d")
    totStorageTrendTest = sfTotStorageMacdTesting[
        sfTotStorageMacdTesting["date"] == dtChk
    ]
    totStorageMacdAlerts = totStorageTrendTest[totStorageTrendTest["crossFlag"] != 0]

    ####################################
    #####  COMPUTE TREND ALERTS  #######
    ####################################

    # Region x warehouse x day

    conditions = [
        (
            sfComputeMacdTesting.groupby(["REGION", "WAREHOUSE_NAME"])[
                "wh_day_usd_macd_52"
            ].shift(1)
            >= (
                sfComputeMacdTesting.groupby(["REGION", "WAREHOUSE_NAME"])[
                    "wh_day_usd_18ewm"
                ].shift(1)
            )
        )
        & (
            sfComputeMacdTesting["wh_day_usd_macd_52"]
            < sfComputeMacdTesting["wh_day_usd_18ewm"]
        ),
        (
            sfComputeMacdTesting.groupby(["REGION", "WAREHOUSE_NAME"])[
                "wh_day_usd_macd_52"
            ].shift(1)
            <= (
                sfComputeMacdTesting.groupby(["REGION", "WAREHOUSE_NAME"])[
                    "wh_day_usd_18ewm"
                ].shift(1)
            )
        )
        & (
            sfComputeMacdTesting["wh_day_usd_macd_52"]
            > sfComputeMacdTesting["wh_day_usd_18ewm"]
        ),
        (
            sfComputeMacdTesting.groupby(["REGION", "WAREHOUSE_NAME"])[
                "wh_day_usd_macd_52"
            ].shift(1)
            >= (
                sfComputeMacdTesting.groupby(["REGION", "WAREHOUSE_NAME"])[
                    "wh_day_usd_18ewm"
                ].shift(1)
            )
        )
        & (
            sfComputeMacdTesting["wh_day_usd_macd_52"]
            >= sfComputeMacdTesting["wh_day_usd_18ewm"]
        ),
        (
            sfComputeMacdTesting.groupby(["REGION", "WAREHOUSE_NAME"])[
                "wh_day_usd_macd_52"
            ].shift(1)
            <= (
                sfComputeMacdTesting.groupby(["REGION", "WAREHOUSE_NAME"])[
                    "wh_day_usd_18ewm"
                ].shift(1)
            )
        )
        & (
            sfComputeMacdTesting["wh_day_usd_macd_52"]
            <= sfComputeMacdTesting["wh_day_usd_18ewm"]
        ),
        (
            np.isnan(
                sfComputeMacdTesting.groupby(["REGION", "WAREHOUSE_NAME"])[
                    "wh_day_usd_macd_52"
                ].shift(1)
            )
        ),
    ]

    results = [-1, 1, 0, 0, 0]

    sfComputeMacdTesting["crossFlag"] = np.select(conditions, results)
    dtChk = (datetime.now()).strftime("%Y-%m-%d")
    computeTrendTest = sfComputeMacdTesting[sfComputeMacdTesting["date"] == dtChk]
    computeMacdAlerts = computeTrendTest[computeTrendTest["crossFlag"] != 0]

    # Region x day

    conditions = [
        (
            sfTotComputeMacdTesting.groupby(["REGION"])["tot_wh_day_usd_macd_52"].shift(
                1
            )
            >= (
                sfTotComputeMacdTesting.groupby(["REGION"])[
                    "tot_wh_day_usd_18ewm"
                ].shift(1)
            )
        )
        & (
            sfTotComputeMacdTesting["tot_wh_day_usd_macd_52"]
            < sfTotComputeMacdTesting["tot_wh_day_usd_18ewm"]
        ),
        (
            sfTotComputeMacdTesting.groupby(["REGION"])["tot_wh_day_usd_macd_52"].shift(
                1
            )
            <= (
                sfTotComputeMacdTesting.groupby(["REGION"])[
                    "tot_wh_day_usd_18ewm"
                ].shift(1)
            )
        )
        & (
            sfTotComputeMacdTesting["tot_wh_day_usd_macd_52"]
            > sfTotComputeMacdTesting["tot_wh_day_usd_18ewm"]
        ),
        (
            sfTotComputeMacdTesting.groupby(["REGION"])["tot_wh_day_usd_macd_52"].shift(
                1
            )
            >= (
                sfTotComputeMacdTesting.groupby(["REGION"])[
                    "tot_wh_day_usd_18ewm"
                ].shift(1)
            )
        )
        & (
            sfTotComputeMacdTesting["tot_wh_day_usd_macd_52"]
            >= sfTotComputeMacdTesting["tot_wh_day_usd_18ewm"]
        ),
        (
            sfTotComputeMacdTesting.groupby(["REGION"])["tot_wh_day_usd_macd_52"].shift(
                1
            )
            <= (
                sfTotComputeMacdTesting.groupby(["REGION"])[
                    "tot_wh_day_usd_18ewm"
                ].shift(1)
            )
        )
        & (
            sfTotComputeMacdTesting["tot_wh_day_usd_macd_52"]
            <= sfTotComputeMacdTesting["tot_wh_day_usd_18ewm"]
        ),
        (
            np.isnan(
                sfTotComputeMacdTesting.groupby(["REGION"])[
                    "tot_wh_day_usd_macd_52"
                ].shift(1)
            )
        ),
    ]

    results = [-1, 1, 0, 0, 0]

    sfTotComputeMacdTesting["crossFlag"] = np.select(conditions, results)
    dtChk = (datetime.now()).strftime("%Y-%m-%d")
    totComputeTrendTest = sfTotComputeMacdTesting[
        sfTotComputeMacdTesting["date"] == dtChk
    ]
    totComputeMacdAlerts = totComputeTrendTest[totComputeTrendTest["crossFlag"] != 0]

    ####################################
    #####  COMPUTE SPIKE ALERTS  #######
    ####################################

    # Region x warehouse x day

    computeSpikeTesting = sfComputeDayZs.copy()
    computeSpikeTesting["date"] = pd.to_datetime(
        dict(
            year=computeSpikeTesting.YEAR,
            month=computeSpikeTesting.MONTH,
            day=computeSpikeTesting.DAY,
        )
    )
    computeSpikeTesting = computeSpikeTesting.sort_values(
        by=["REGION", "WAREHOUSE_NAME", "date"]
    )
    dtChk = (datetime.now()).strftime("%Y-%m-%d")
    computeSpikeTesting = computeSpikeTesting[computeSpikeTesting["date"] == dtChk]
    computeSpikeAlerts = computeSpikeTesting[
        computeSpikeTesting["wh_day_zscore"] >= z_in
    ]

    # Region x day

    totComputeSpikeTesting = sfTotComputeDayZs.copy()
    totComputeSpikeTesting["date"] = pd.to_datetime(
        dict(
            year=totComputeSpikeTesting.YEAR,
            month=totComputeSpikeTesting.MONTH,
            day=totComputeSpikeTesting.DAY,
        )
    )
    totComputeSpikeTesting = totComputeSpikeTesting.sort_values(by=["REGION", "date"])
    dtChk = (datetime.now()).strftime("%Y-%m-%d")
    totComputeSpikeTesting = totComputeSpikeTesting[
        totComputeSpikeTesting["date"] == dtChk
    ]
    totComputeSpikeAlerts = totComputeSpikeTesting[
        totComputeSpikeTesting["tot_wh_day_zscore"] >= z_in
    ]

    ####################################
    #####  STORAGE SPIKE ALERTS  #######
    ####################################

    # Region x database x day

    storageSpikeTesting = sfStorageDayZs.copy()
    storageSpikeTesting["date"] = pd.to_datetime(
        dict(
            year=storageSpikeTesting.YEAR,
            month=storageSpikeTesting.MONTH,
            day=storageSpikeTesting.DAY,
        )
    )
    storageSpikeTesting = storageSpikeTesting.sort_values(
        by=["REGION", "DATABASE_NAME", "date"]
    )
    dtChk = (datetime.now()).strftime("%Y-%m-%d")
    storageSpikeTesting = storageSpikeTesting[storageSpikeTesting["date"] == dtChk]
    storageSpikeAlerts = storageSpikeTesting[
        storageSpikeTesting["db_day_zscore"] >= z_in
    ]

    # Region x day

    totStorageSpikeTesting = sfTotStorageDayZs.copy()
    totStorageSpikeTesting["date"] = pd.to_datetime(
        dict(
            year=totStorageSpikeTesting.YEAR,
            month=totStorageSpikeTesting.MONTH,
            day=totStorageSpikeTesting.DAY,
        )
    )
    totStorageSpikeTesting = totStorageSpikeTesting.sort_values(by=["REGION", "date"])
    dtChk = (datetime.now()).strftime("%Y-%m-%d")
    totStorageSpikeTesting = totStorageSpikeTesting[
        totStorageSpikeTesting["date"] == dtChk
    ]
    totStorageSpikeAlerts = totStorageSpikeTesting[
        totStorageSpikeTesting["tot_db_day_zscore"] >= z_in
    ]

    return [
        computeSpikeAlerts,
        computeSpikeTesting,
        computeMacdAlerts,
        sfComputeMacdTesting,
        storageSpikeAlerts,
        storageSpikeTesting,
        storageMacdAlerts,
        sfStorageMacdTesting,
        computeSpikePlots,
        storageSpikePlots,
        totComputeSpikeAlerts,
        totComputeSpikeTesting,
        totComputeMacdAlerts,
        sfTotComputeMacdTesting,
        totStorageSpikeAlerts,
        totStorageSpikeTesting,
        totStorageMacdAlerts,
        sfTotStorageMacdTesting,
        totComputeSpikePlots,
        totStorageSpikePlots,
    ]
