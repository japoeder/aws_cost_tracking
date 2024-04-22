"""Program to run AWS cost alerting"""

# Import libraries
from procAmzAlerts import alerting_amazon
from procAmzData import process_amazon
from procPlotting import outerPlotLooper
from utilities.slackAlerting import slackAlert
from utilities.getCredentials import get_cred
from utilities.payloadBuilder import buildPayload, compilePayloadText
from utilities.projSetup import create_dirs


def main(inputs, procData, palert, z_in):
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
    ##### DATA & ALERT PROCESSING  #####
    ####################################

    # Grab credentials for AMZ (us-east)
    credFiles = inputs
    envConnData = get_cred(credFiles[0], credFiles[1])

    # Process AMZ data and alerts
    if procData == 1:
        process_amazon(envConnData, cwd)

    amzAlerts = alerting_amazon(cwd, z_in)

    ####################################
    #########  VIZ PROCESSING  #########
    ####################################

    outerPlotLooper(amzAlerts, cwd, z_in)

    ####################################
    #######  PAYLOAD PROCESSING  #######
    ####################################

    payloadFinal = []
    payloadFinal = buildPayload(amzAlerts, z_in)
    msg = f"[{compilePayloadText(payloadFinal, envConnData['s3_url'])}]"
    # print(msg)
    # print()

    ####################################
    ######   SEND SLACK PAYLOAD  #######
    ####################################

    if palert == 1:
        print("running public alert")
        # public
        slackAlert("C070ALFTDAM", envConnData["slack_token"], msg)
    else:
        # private
        slackAlert("C070ALFTDAM", envConnData["slack_token"], msg)

    return [amzAlerts, cwd, payloadFinal]


if __name__ == "__main__":
    proc_data = 1
    public_alert = 0
    z_cutoff = 1
    ec2_local = 0
    mysqlCredDict = {
        "mysql_usr": "COLLECTABILITY_COST_TRACKING_MYSQL_USER",
        "mysql_pwd": "COLLECTABILITY_COST_TRACKING_MYSQL_PWD",
        "mysql_url": "COLLECTABILITY_COST_TRACKING_MYSQL_URL",
        "mysql_db": "COLLECTABILITY_COST_TRACKING_MYSQL_DB",
        "slack_token": "COLLECTABILITY_COST_TRACKING_SLACK_TOKEN",
        "s3_url": "COLLECTABILITY_COST_TRACKING_S3_URL",
    }
    if ec2_local == 0:
        zshrc = "/Users/jpoeder/.zshrc"
    else:
        zshrc = "/home/ubuntu/.zshrc"

    # Kick off cost reporting
    runMain = main([zshrc, mysqlCredDict], proc_data, public_alert, z_cutoff)
