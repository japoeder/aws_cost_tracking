"""Utility to collect AWS cost data using boto3."""

from datetime import datetime, timedelta
import boto3
import pandas as pd


def get_cost_and_usage(bclient: object, start: str, end: str) -> list:
    """Method to grab data"""
    cu = []

    while True:
        data = bclient.get_cost_and_usage(
            TimePeriod={
                "Start": start,
                "End": end,
            },
            Granularity="DAILY",
            Metrics=[
                "UnblendedCost",
            ],
            GroupBy=[
                {
                    "Type": "DIMENSION",
                    "Key": "SERVICE",
                },
                {
                    "Type": "TAG",
                    "Key": "Name",
                },
            ],
        )

        cu += data["ResultsByTime"]
        token = data.get("NextPageToken")

        if not token:
            break

    return cu


def process_report(start: str, end: str):
    """Method to process report"""
    # cost explorer
    profile = "default"
    SERVICE_NAME = "ce"
    bclient = boto3.Session(profile_name=profile).client(SERVICE_NAME)

    date_format = "%Y-%m-%d"
    s = datetime.strptime(start, date_format)
    e = datetime.strptime(end, date_format)
    s = s.date()
    e = e.date()
    delta = e - s
    outputData = []
    for d in range(delta.days):
        l_s = s + timedelta(days=d)
        l_e = l_s + timedelta(days=1)
        response = get_cost_and_usage(bclient=bclient, start=str(l_s), end=str(l_e))
        costData = response[0]

        startdate = l_s
        enddate = l_e
        unit = "USD"
        metric = "UnblendedCost"
        granularity = "DAILY"
        groups = costData["Groups"]
        for g in groups:
            service = g["Keys"][0]
            tag = g["Keys"][1].replace("Name$", "")
            usd = g["Metrics"]["UnblendedCost"]["Amount"]
            outputData = outputData + [
                [service, tag, metric, granularity, startdate, enddate, usd, unit]
            ]

    df = pd.DataFrame(
        columns=[
            "SERVICE",
            "TAG",
            "METRIC",
            "GRANULARITY",
            "STARTDATE",
            "ENDDATE",
            "USD",
            "UNIT",
        ],
        data=outputData,
    )

    return df
