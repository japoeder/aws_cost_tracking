from datetime import datetime, timedelta
from utilities.mysqlConnect import *
import pandas as pd
import numpy as np
from scipy.stats import zscore


def alerting_amazon(user, pwd, cwd, z_in):
    #####################################
    #####   AWS - DATA PROCESSING  ######
    #####################################

    # Instantiate connection dictionary
    connDict = paramDefs(user, pwd)
    payload = connDict.copy()

    # Query cat & cleaned data
    ccAWS = """select * from JPOEDER_AA.DW.AWS_COST_EXP_OUTPUT
                where STARTDATE >= dateadd(year, -1, current_date);"""
    payload["dl"] = True
    payload["query"] = ccAWS
    awsCosts = snowQuery(payload)
    awsCosts["USD"] = awsCosts["USD"].astype(float)

    awsCalcs = awsCosts[
        ["SERVICE", "TAG", "STARTDATE", "DAY", "WEEK", "MONTH", "YEAR", "USD"]
    ].copy()
    awsCalcs["yearWK"] = awsCosts["YEAR"].astype(int) * 100 + awsCosts["WEEK"]

    ################################
    # Service day level calculations
    ################################
    awsSvcDayAgg = awsCalcs[["SERVICE", "YEAR", "MONTH", "DAY", "USD"]].copy()
    awsSvcDayAgg = awsSvcDayAgg.groupby(
        ["SERVICE", "YEAR", "MONTH", "DAY"], as_index=True
    ).agg({"USD": ["sum"]})
    awsSvcDayAgg.columns = ["USD"]
    awsSvcDayAgg = awsSvcDayAgg.reset_index()

    # Z-score by service across days
    awsSvcDayZs = awsSvcDayAgg.copy()
    awsSvcDayZs["svc_day_zscore"] = awsSvcDayZs.groupby(["SERVICE"])["USD"].transform(
        zscore
    )
    awsSvcDayZs = awsSvcDayZs.drop("USD", axis=1)

    awsSvcDaySpikes = awsSvcDayAgg.copy()

    # Moments by service across days
    svcDayMoments = awsSvcDayAgg.copy()
    svcDayMoments = svcDayMoments.groupby(["SERVICE"], as_index=True).agg(
        {"USD": ["min", "max", "mean"]}
    )
    svcDayMoments.columns = ["svc_day_usd_min", "svc_day_usd_max", "svc_day_usd_mean"]
    svcDayMoments = svcDayMoments.reset_index()

    # Moments by service across days
    svcDayMoments = awsSvcDayAgg.copy()
    svcDayMoments = svcDayMoments.groupby(["SERVICE"], as_index=True).agg(
        {"USD": ["min", "max", "mean"]}
    )
    svcDayMoments.columns = ["svc_day_usd_min", "svc_day_usd_max", "svc_day_usd_mean"]
    svcDayMoments = svcDayMoments.reset_index()

    # Trend alerting calcs
    svcDayMACD = awsSvcDayAgg.copy()
    svcDayMACD["svc_day_usd_12ewm"] = svcDayMACD.groupby("SERVICE")["USD"].transform(
        lambda x: x.ewm(span=12, adjust=False).mean()
    )
    svcDayMACD["svc_day_usd_26ewm"] = svcDayMACD.groupby("SERVICE")["USD"].transform(
        lambda x: x.ewm(span=26, adjust=False).mean()
    )
    svcDayMACD["svc_day_usd_macd"] = (
        svcDayMACD["svc_day_usd_26ewm"] - svcDayMACD["svc_day_usd_12ewm"]
    )
    svcDayMACD["svc_day_usd_9ewm"] = svcDayMACD["svc_day_usd_macd"].transform(
        lambda x: x.ewm(span=9, adjust=False).mean()
    )
    svcDayMACD = svcDayMACD.drop("USD", axis=1)

    ################################
    # Service-tag day level calculations
    ################################
    awsSvcTagDayAgg = awsCalcs[["SERVICE", "TAG", "YEAR", "MONTH", "DAY", "USD"]].copy()
    awsSvcTagDayAgg = awsSvcTagDayAgg.groupby(
        ["SERVICE", "TAG", "YEAR", "MONTH", "DAY"], as_index=True
    ).agg({"USD": ["sum"]})
    awsSvcTagDayAgg.columns = ["USD"]
    awsSvcTagDayAgg = awsSvcTagDayAgg.reset_index()

    # Z-score by service across days
    awsSvcTagDayZs = awsSvcTagDayAgg.copy()
    awsSvcTagDayZs["svc_tag_day_zscore"] = awsSvcTagDayZs.groupby(["SERVICE", "TAG"])[
        "USD"
    ].transform(zscore)
    awsSvcTagDayZs = awsSvcTagDayZs.drop("USD", axis=1)

    # Moments by service across days
    svcTagDayMoments = awsSvcTagDayAgg.copy()
    svcTagDayMoments = svcTagDayMoments.groupby(["SERVICE", "TAG"], as_index=True).agg(
        {"USD": ["min", "max", "mean"]}
    )
    svcTagDayMoments.columns = [
        "svc_tag_day_usd_min",
        "svc_tag_day_usd_max",
        "svc_tag_day_usd_mean",
    ]
    svcTagDayMoments = svcTagDayMoments.reset_index()

    ###############################
    # Service Wk. level calculations
    ###############################

    # AGGREGATE TO WEEKLY FIRST.
    awsSvcWkAgg = awsCalcs.copy()
    awsSvcWkAgg = awsSvcWkAgg.groupby(["SERVICE", "yearWK"], as_index=True).agg(
        {"USD": ["sum"]}
    )
    awsSvcWkAgg.columns = ["USD"]
    awsSvcWkAgg = awsSvcWkAgg.reset_index()

    # Z-score by service across weeks
    awsSvcWkZs = awsSvcWkAgg.copy()
    awsSvcWkZs["svc_wk_zscore"] = awsSvcWkZs.groupby(["SERVICE"])["USD"].transform(
        zscore
    )
    awsSvcWkZs = awsSvcWkZs.drop("USD", axis=1)

    # Moments by service across weeks
    svcWkMoments = awsSvcWkAgg.copy()
    svcWkMoments = svcWkMoments.groupby(["SERVICE"], as_index=True).agg(
        {"USD": ["min", "max", "mean"]}
    )
    svcWkMoments.columns = ["svc_wk_usd_min", "svc_wk_usd_max", "svc_wk_usd_mean"]
    svcWkMoments = svcWkMoments.reset_index()

    ####################################
    # Service-tag Wk. level calculations
    ####################################

    # AGGREGATE TO WEEKLY FIRST.
    awsServTagWkAgg = awsCalcs.copy()
    awsServTagWkAgg = awsServTagWkAgg.groupby(
        ["SERVICE", "TAG", "yearWK"], as_index=True
    ).agg({"USD": ["sum"]})
    awsServTagWkAgg.columns = ["USD"]
    awsServTagWkAgg = awsServTagWkAgg.reset_index()

    # Z-score by service and week
    awsSvcTagWkZs = awsServTagWkAgg.copy()
    awsSvcTagWkZs["svc_tag_wk_zscore"] = awsSvcTagWkZs.groupby(["SERVICE", "TAG"])[
        "USD"
    ].transform(zscore)
    awsSvcTagWkZs = awsSvcTagWkZs.drop("USD", axis=1)

    # Moments by service-tag across weeks
    svcTagWkMoments = awsServTagWkAgg.copy()
    svcTagWkMoments = svcTagWkMoments.groupby(["SERVICE", "TAG"], as_index=True).agg(
        {"USD": ["min", "max", "mean"]}
    )
    svcTagWkMoments.columns = [
        "svc_tag_wk_usd_min",
        "svc_tag_wk_usd_max",
        "svc_tag_wk_usd_mean",
    ]
    svcTagMoments = svcTagWkMoments.reset_index()

    # Merge everything back together
    awsCalcs = pd.merge(
        awsCalcs,
        awsSvcWkZs,
        how="left",
        left_on=["SERVICE", "yearWK"],
        right_on=["SERVICE", "yearWK"],
    )
    awsCalcs = pd.merge(
        awsCalcs, svcWkMoments, how="left", left_on=["SERVICE"], right_on=["SERVICE"]
    )
    awsCalcs = pd.merge(
        awsCalcs,
        awsSvcTagWkZs,
        how="left",
        left_on=["SERVICE", "TAG", "yearWK"],
        right_on=["SERVICE", "TAG", "yearWK"],
    )
    awsCalcs = pd.merge(
        awsCalcs,
        svcTagWkMoments,
        how="left",
        left_on=["SERVICE", "TAG"],
        right_on=["SERVICE", "TAG"],
    )
    awsCalcs = pd.merge(
        awsCalcs,
        awsSvcDayZs,
        how="left",
        left_on=["SERVICE", "YEAR", "MONTH", "DAY"],
        right_on=["SERVICE", "YEAR", "MONTH", "DAY"],
    )
    awsCalcs = pd.merge(
        awsCalcs, svcDayMoments, how="left", left_on=["SERVICE"], right_on=["SERVICE"]
    )
    awsCalcs = pd.merge(
        awsCalcs,
        svcDayMACD,
        how="left",
        left_on=["SERVICE", "YEAR", "MONTH", "DAY"],
        right_on=["SERVICE", "YEAR", "MONTH", "DAY"],
    )
    awsCalcs = pd.merge(
        awsCalcs,
        awsSvcTagDayZs,
        how="left",
        left_on=["SERVICE", "TAG", "YEAR", "MONTH", "DAY"],
        right_on=["SERVICE", "TAG", "YEAR", "MONTH", "DAY"],
    )
    awsCalcs = pd.merge(
        awsCalcs,
        svcTagDayMoments,
        how="left",
        left_on=["SERVICE", "TAG"],
        right_on=["SERVICE", "TAG"],
    )
    awsCalcs = awsCalcs.sort_values(by=["SERVICE", "TAG", "STARTDATE"])

    awsMacdTesting = svcDayMACD.copy()
    awsMacdTesting["date"] = pd.to_datetime(
        dict(
            year=awsMacdTesting.YEAR, month=awsMacdTesting.MONTH, day=awsMacdTesting.DAY
        )
    )
    awsMacdTesting = awsMacdTesting.sort_values(by=["SERVICE", "date"])

    conditions = [
        (
            awsMacdTesting.groupby(["SERVICE"])["svc_day_usd_macd"].shift(1)
            >= (awsMacdTesting.groupby(["SERVICE"])["svc_day_usd_9ewm"].shift(1))
        )
        & (awsMacdTesting["svc_day_usd_macd"] < awsMacdTesting["svc_day_usd_9ewm"]),
        (
            awsMacdTesting.groupby(["SERVICE"])["svc_day_usd_macd"].shift(1)
            <= (awsMacdTesting.groupby(["SERVICE"])["svc_day_usd_9ewm"].shift(1))
        )
        & (awsMacdTesting["svc_day_usd_macd"] > awsMacdTesting["svc_day_usd_9ewm"]),
        (
            awsMacdTesting.groupby(["SERVICE"])["svc_day_usd_macd"].shift(1)
            >= (awsMacdTesting.groupby(["SERVICE"])["svc_day_usd_9ewm"].shift(1))
        )
        & (awsMacdTesting["svc_day_usd_macd"] >= awsMacdTesting["svc_day_usd_9ewm"]),
        (
            awsMacdTesting.groupby(["SERVICE"])["svc_day_usd_macd"].shift(1)
            <= (awsMacdTesting.groupby(["SERVICE"])["svc_day_usd_9ewm"].shift(1))
        )
        & (awsMacdTesting["svc_day_usd_macd"] <= awsMacdTesting["svc_day_usd_9ewm"]),
        (np.isnan(awsMacdTesting.groupby(["SERVICE"])["svc_day_usd_macd"].shift(1))),
    ]

    results = [-1, 1, 0, 0, 0]

    awsMacdTesting["crossFlag"] = np.select(conditions, results)
    dtChk = (datetime.now()).strftime("%Y-%m-%d")
    trendTest = awsMacdTesting[awsMacdTesting["date"] == dtChk]
    macdAlerts = trendTest[trendTest["crossFlag"] != 0]

    # Service / Tag Spike Alerts
    tagSpikeTesting = awsSvcTagDayZs.copy()
    tagSpikeTesting["date"] = pd.to_datetime(
        dict(
            year=tagSpikeTesting.YEAR,
            month=tagSpikeTesting.MONTH,
            day=tagSpikeTesting.DAY,
        )
    )
    tagSpikeTesting = tagSpikeTesting.sort_values(by=["SERVICE", "TAG", "date"])
    dtChk = (datetime.now()).strftime("%Y-%m-%d")
    tagSpikeTest = tagSpikeTesting[tagSpikeTesting["date"] == dtChk]
    tagSpikeAlerts = tagSpikeTest[tagSpikeTest["svc_tag_day_zscore"] >= z_in]
    tagSpikeAlerts.head()

    # Service Spike Alerts
    svcSpikeTesting = awsSvcDayZs.copy()
    svcSpikeTesting["date"] = pd.to_datetime(
        dict(
            year=svcSpikeTesting.YEAR,
            month=svcSpikeTesting.MONTH,
            day=svcSpikeTesting.DAY,
        )
    )
    svcSpikeTesting = svcSpikeTesting.sort_values(by=["SERVICE", "date"])
    dtChk = (datetime.now()).strftime("%Y-%m-%d")
    svcSpikeTest = svcSpikeTesting[svcSpikeTesting["date"] == dtChk]
    svcSpikeAlerts = svcSpikeTest[svcSpikeTest["svc_day_zscore"] >= z_in]
    svcSpikeAlerts.head()

    return [
        awsMacdTesting,
        macdAlerts,
        tagSpikeTesting,
        tagSpikeAlerts,
        svcSpikeTesting,
        svcSpikeAlerts,
        awsSvcDaySpikes,
    ]
