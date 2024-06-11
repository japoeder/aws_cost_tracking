#!/usr/bin/env python
"""Program to run AWS cost alerting"""

# Import libraries
import datetime
import warnings
import os
from procSummaries import summarize_amazon
from procAmzAlerts import alerting_amazon
from procAmzData import process_amazon
from procPlotting import outerPlotLooper
from utilities.slackAlerting import slackAlert, compilePayloadText
from utilities.getCredentials import get_cred
from utilities.payloadBuilder import buildPayload
from utilities.projSetup import create_dirs

# Ignore UserWarning from slack
warnings.filterwarnings(
    "ignore",
    "The top-level `text` argument is missing in the request payload for a chat.postMessage call - It's a best practice to always provide a `text` argument when posting a message. The `text` argument is used in places where content cannot be rendered such as: system push notifications, assistive technology such as screen readers, etc.",
)


def main(procData, palert, z_in, rundate=None):
    """Driver method to run AWS cost alerting"""
    # Get environment info
    cwd = __file__
    cwd = cwd.rsplit("/", 1)[0]

    ####################################
    ## CREATE PLOT DIR IF NOT EXIST  ###
    ####################################
    # Need to create plot directory
    create_dirs(cwd)

    ####################################
    ####### SET PROCESSING DATE ########
    ####################################

    # Set processing date to 1 day prior to current date if rundate is not provided
    # This assumes automated processing is run daily up to the previous day
    if rundate is None:
        dt_to_proc = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime(
            "%Y-%m-%d"
        )
    else:
        # subtract 1 day from rundate
        dt_to_proc = (
            datetime.datetime.strptime(rundate, "%Y-%m-%d") - datetime.timedelta(days=1)
        ).strftime("%Y-%m-%d")

    ####################################
    ###### DAILY DATA PROCESSING  ######
    ####################################

    # Grab credentials for AMZ (us-east)
    envConnData = get_cred()

    # Process AMZ data and alerts
    if procData == 1:
        process_amazon(envConnData, dt_to_proc)

    ####################################
    #######  SUMMARY PROCESSING  #######
    ####################################

    amzSummary = summarize_amazon(envConnData, dt_to_proc)

    ####################################
    ########  ALERT PROCESSING  ########
    ####################################

    amzAlerts = alerting_amazon(envConnData, z_in, dt_to_proc)

    ####################################
    #########  VIZ PROCESSING  #########
    ####################################

    outerPlotLooper([amzSummary, amzAlerts], envConnData, cwd, z_in, dt_to_proc)

    ####################################
    #######  PAYLOAD PROCESSING  #######
    ####################################

    payloadFinal = []
    payloadFinal = buildPayload(amzAlerts, z_in, dt_to_proc)
    msg = f"[{compilePayloadText(payloadFinal, envConnData['s3_url'])}]"
    #print(msg)
    # print()

    ####################################
    ######   SEND SLACK PAYLOAD  #######
    ####################################

    if palert == 1:
        print("running public alert")
        # public
        slackAlert(os.getenv("slack_public"), envConnData["slack_token"], msg)
    else:
        # private
        slackAlert(os.getenv("slack_private"), envConnData["slack_token"], msg)

    return [[amzSummary, amzAlerts], cwd, payloadFinal]


if __name__ == "__main__":
    proc_data = 1
    public_alert = 1
    z_cutoff = 1
    ec2_local = 0
    if ec2_local == 0:
        zshrc = "/Users/jpoeder/.zshrc"
    else:
        zshrc = "/home/ubuntu/.zshrc"

    # If specifying a date to process, set it here one day AHEAD of desired end date
    #in_date = datetime.datetime(2024, 5, 23).strftime("%Y-%m-%d")
    in_date = None

    # Kick off cost reporting
    runMain = main(
        proc_data, public_alert, z_cutoff, rundate=in_date
    )
