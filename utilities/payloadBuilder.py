"""Build Slack payload for alerts"""

from datetime import datetime


def genText(
    top_n,
    region,
    top_sort_by,
    alertDF,
    upLabel,
    downLabel,
    resPos,
    resConditions,
    prefix,
    svc_override,
):
    """Text generator for alerts"""
    pbTemp = {}
    if region is not False:
        alertDF = alertDF[alertDF["REGION"] == region]
    else:
        pass

    file_dt = datetime.now().strftime("%Y%m%d")

    #####################################
    ####           ALERTING          ####
    #####################################
    top_list = alertDF.sort_values(by=top_sort_by, ascending=False).iloc[0:top_n, :]
    res_set = top_list.values.tolist()
    upList = []
    downList = []
    if len(alertDF) == 0:
        pass
        # pbTemp[upLabel] = [noAlertMsg]
        # pbTemp[downLabel] = [noAlertMsg]
    else:
        for a in range(len(res_set)):
            # print(res_set[a][resPos[1] : resPos[2]])
            if svc_override is None:
                svc = f"{res_set[a][resPos[1]: resPos[2]][0]}_"
                formatted_output = res_set[a][resPos[1] : resPos[2]]
            elif svc_override == "dt_conv":
                svc = ""
                formatted_output = datetime.date(res_set[a][resPos[1] : resPos[2]][0])
                formatted_output = [formatted_output.strftime("%Y-%m-%d")]
            else:
                svc = ""

            # print(formatted_output)
            if res_set[a][resPos[0]] >= resConditions[0]:
                # print(f"{prefix}{svc}{region}_{file_dt}", )
                upList.append(
                    [
                        formatted_output,
                        f"{prefix}{svc}{region}_{file_dt}",
                    ]
                )
            elif res_set[a][resPos[0]] <= resConditions[1]:
                # print(f"{prefix}{svc}{region}_{file_dt}", )
                downList.append(
                    [
                        formatted_output,
                        f"{prefix}{svc}{region}_{file_dt}",
                    ]
                )

        if len(upList) == 0:
            pass
            # pbTemp[upLabel] = [noAlertMsg]
        else:
            pbTemp[upLabel] = upList

        if len(downList) == 0:
            pass
            # pbTemp[downLabel] = [noAlertMsg]
        else:
            pbTemp[downLabel] = downList

    return pbTemp


def buildPayload(amzAlerts, z_in):
    """Driver method build and send payload text for alerts"""

    top_n = 999
    payloadRaw = {}
    payloadRaw["awsTrends"] = genText(
        top_n=top_n,
        region=False,
        top_sort_by="crossFlag",
        alertDF=amzAlerts[1],
        upLabel="Upward Trending AWS Services",
        downLabel="Downward Trending AWS Services",
        resPos=[9, 0, 1],
        resConditions=[1, -1],
        prefix="amz_",
        svc_override=None,
    )

    payloadRaw["awsTagSpikes"] = genText(
        top_n=top_n,
        region=False,
        top_sort_by="svc_tag_day_zscore",
        alertDF=amzAlerts[3],
        upLabel="Spike up in AWS Services / Tags",
        downLabel="Spike down in AWS Services / Tags",
        resPos=[5, 0, 2],
        resConditions=[z_in, -1 * z_in],
        prefix=None,
        svc_override=None,
    )

    payloadRaw["awsSvcSpikes"] = genText(
        top_n=top_n,
        region=False,
        top_sort_by="svc_day_zscore",
        alertDF=amzAlerts[5],
        upLabel="Spike up in AWS Services",
        downLabel="Spike down in AWS Services",
        resPos=[4, 0, 1],
        resConditions=[z_in, -1 * z_in],
        prefix="amz_tot_",
        svc_override=None,
    )

    return payloadRaw
