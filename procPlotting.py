"""Program to control outer plot loop"""

# Import libraries
from utilities.plotLooper import innerPlotLoop


def outerPlotLooper(loopData, envConnData, cwd, z_in, dt_to_proc):
    """Driver method to control outer plot loop"""
    ####################################
    ###          PLOTTING           ####
    ####################################

    # loopData => [amzSummary, amzAlerts]

    # amzSummary => [amz_tot_x_svc, amz_tot_x_svc_x_day]

    # amzAlerts => [awsMacdTesting, macdAlerts, tagSpikeTesting, tagSpikeAlerts
    #             , svcSpikeTesting, svcSpikeAlerts, awsSvcDaySpikes]

    amzSummaryData = loopData[0]
    amzAlerts = loopData[1]

    # AGGREGATE PLOTS
    innerPlotLoop(
        awsList=[],
        in_df=amzSummaryData,
        in_filter="SERVICE",
        loop_code="a_s",
        cwd=cwd,
        plot_title=None,
        z_in=z_in,
        envConnData=envConnData,
        dt_to_proc=dt_to_proc,
    )

    # TREND ALERT PLOTS
    amzServiceTrendAlerts = (
        amzAlerts[1]["SERVICE"].copy().drop_duplicates().values.tolist()
    )
    innerPlotLoop(
        awsList=amzServiceTrendAlerts,
        in_df=amzAlerts[0],
        in_filter="SERVICE",
        loop_code="a_md",
        cwd=cwd,
        plot_title=None,
        z_in=z_in,
        envConnData=envConnData,
        dt_to_proc=dt_to_proc,
    )

    # TREND ALERT PLOTS
    amzServiceSpikeAlerts = (
        amzAlerts[5]["SERVICE"].copy().drop_duplicates().values.tolist()
    )
    innerPlotLoop(
        awsList=amzServiceSpikeAlerts,
        in_df=amzAlerts[6],
        in_filter="SERVICE",
        loop_code="a_spk",
        cwd=cwd,
        plot_title=None,
        z_in=z_in,
        envConnData=envConnData,
        dt_to_proc=dt_to_proc,
    )
