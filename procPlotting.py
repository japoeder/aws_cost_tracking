# Import libraries
from utilities.plotLooper import *


def outerPlotLooper(amzAlerts, sfAlerts, cwd, z_in):
    ####################################
    ###          PLOTTING           ####
    ####################################

    # amzAlerts => [awsMacdTesting, macdAlerts, tagSpikeTesting, tagSpikeAlerts
    #             , svcSpikeTesting, svcSpikeAlerts, awsSvcDaySpikes]

    # TREND ALERT PLOTS
    amzServiceTrendAlerts = (
        amzAlerts[1]["SERVICE"].copy().drop_duplicates().values.tolist()
    )
    innerPlotLoop(
        awsList=amzServiceTrendAlerts,
        sfList=[],
        in_df=amzAlerts[0],
        filter="SERVICE",
        loop_code="a_md",
        cwd=cwd,
        plot_title=None,
        z_in=z_in,
    )

    # TREND ALERT PLOTS
    amzServiceSpikeAlerts = (
        amzAlerts[5]["SERVICE"].copy().drop_duplicates().values.tolist()
    )
    innerPlotLoop(
        awsList=amzServiceSpikeAlerts,
        sfList=[],
        in_df=amzAlerts[6],
        filter="SERVICE",
        loop_code="a_spk",
        cwd=cwd,
        plot_title=None,
        z_in=z_in,
    )

    # 0: computeSpikeAlerts     1: computeSpikeTesting
    # 2: computeMacdAlerts      3: sfComputeMacdTesting
    # 4: storageSpikeAlerts     5: storageSpikeTesting
    # 6: storageMacdAlerts      7: sfStorageMacdTesting
    # 8: computeSpikePlots      9: storageSpikePlots
    # 10: totComputeSpikeAlerts 11: totComputeSpikeTesting
    # 12: totComputeMacdAlerts  13: sfTotComputeMacdTesting,
    # 14: totStorageSpikeAlerts 15: totStorageSpikeTesting,
    # 16: totStorageMacdAlerts  17: sfTotStorageMacdTesting
    # 18: totComputeSpikePlots  19: totStorageSpikePlots

    # Compute trends - east
    sfTotComputeTrendAlertsEast_pre = sfAlerts[12][
        (sfAlerts[12]["REGION"] == "east")
    ].copy()
    sfTotComputeTrendAlertsEast = (
        sfTotComputeTrendAlertsEast_pre["REGION"]
        .copy()
        .drop_duplicates()
        .values.tolist()
    )
    innerPlotLoop(
        awsList=[],
        sfList=sfTotComputeTrendAlertsEast,
        in_df=sfAlerts[13][(sfAlerts[13]["REGION"] == "east")].copy(),
        filter="REGION",
        loop_code="tc_md",
        cwd=cwd,
        plot_title="SF Regional Compute Trend Alert - East",
        z_in=z_in,
    )

    # Compute trends - west
    sfTotComputeTrendAlertsWest_pre = sfAlerts[12][
        sfAlerts[12]["REGION"] == "west"
    ].copy()
    sfTotComputeTrendAlertsWest = (
        sfTotComputeTrendAlertsWest_pre["REGION"]
        .copy()
        .drop_duplicates()
        .values.tolist()
    )
    innerPlotLoop(
        awsList=[],
        sfList=sfTotComputeTrendAlertsWest,
        in_df=sfAlerts[13][(sfAlerts[13]["REGION"] == "west")].copy(),
        filter="REGION",
        loop_code="tc_md",
        cwd=cwd,
        plot_title="SF Regional Compute Trend Alert - West",
        z_in=z_in,
    )

    # Compute trends by warehouse - east
    sfComputeTrendAlertsEast_pre = sfAlerts[2][(sfAlerts[2]["REGION"] == "east")].copy()
    sfComputeTrendAlertsEast = (
        sfComputeTrendAlertsEast_pre["WAREHOUSE_NAME"]
        .copy()
        .drop_duplicates()
        .values.tolist()
    )
    innerPlotLoop(
        awsList=[],
        sfList=sfComputeTrendAlertsEast,
        in_df=sfAlerts[3][(sfAlerts[3]["REGION"] == "east")].copy(),
        filter="WAREHOUSE_NAME",
        loop_code="c_md",
        cwd=cwd,
        plot_title=None,
        z_in=z_in,
    )

    # Compute trends by warehouse - west
    sfComputeTrendAlertsWest_pre = sfAlerts[2][sfAlerts[2]["REGION"] == "west"].copy()
    sfComputeTrendAlertsWest = (
        sfComputeTrendAlertsWest_pre["WAREHOUSE_NAME"]
        .copy()
        .drop_duplicates()
        .values.tolist()
    )
    innerPlotLoop(
        awsList=[],
        sfList=sfComputeTrendAlertsWest,
        in_df=sfAlerts[3][(sfAlerts[3]["REGION"] == "west")].copy(),
        filter="WAREHOUSE_NAME",
        loop_code="c_md",
        cwd=cwd,
        plot_title=None,
        z_in=z_in,
    )

    # Storage trends - east
    sfTotStorageTrendAlertsEast_pre = sfAlerts[16][
        (sfAlerts[16]["REGION"] == "east")
    ].copy()
    sfTotStorageTrendAlertsEast = (
        sfTotStorageTrendAlertsEast_pre["REGION"]
        .copy()
        .drop_duplicates()
        .values.tolist()
    )
    innerPlotLoop(
        awsList=[],
        sfList=sfTotStorageTrendAlertsEast,
        in_df=sfAlerts[17][(sfAlerts[7]["REGION"] == "east")].copy(),
        filter="REGION",
        loop_code="ts_md",
        cwd=cwd,
        plot_title="SF Regional Storage Trend Alert - East",
        z_in=z_in,
    )

    # Storage trends - west
    sfTotStorageTrendAlertsWest_pre = sfAlerts[16][
        (sfAlerts[16]["REGION"] == "west")
    ].copy()
    sfTotStorageTrendAlertsWest = (
        sfTotStorageTrendAlertsWest_pre["REGION"]
        .copy()
        .drop_duplicates()
        .values.tolist()
    )
    innerPlotLoop(
        awsList=[],
        sfList=sfTotStorageTrendAlertsWest,
        in_df=sfAlerts[17][(sfAlerts[7]["REGION"] == "west")].copy(),
        filter="REGION",
        loop_code="ts_md",
        cwd=cwd,
        plot_title="SF Regional Storage Trend Alert - West",
        z_in=z_in,
    )

    # Storage trends by database - east
    sfStorageTrendAlertsEast_pre = sfAlerts[6][(sfAlerts[6]["REGION"] == "east")].copy()
    sfStorageTrendAlertsEast = (
        sfStorageTrendAlertsEast_pre["DATABASE_NAME"]
        .copy()
        .drop_duplicates()
        .values.tolist()
    )
    innerPlotLoop(
        awsList=[],
        sfList=sfStorageTrendAlertsEast,
        in_df=sfAlerts[7][(sfAlerts[7]["REGION"] == "east")].copy(),
        filter="DATABASE_NAME",
        loop_code="s_md",
        cwd=cwd,
        plot_title=None,
        z_in=z_in,
    )

    # Storage trends by database - west
    sfStorageTrendAlertsWest_pre = sfAlerts[6][(sfAlerts[6]["REGION"] == "west")].copy()
    sfStorageTrendAlertsWest = (
        sfStorageTrendAlertsWest_pre["DATABASE_NAME"]
        .copy()
        .drop_duplicates()
        .values.tolist()
    )
    innerPlotLoop(
        awsList=[],
        sfList=sfStorageTrendAlertsWest,
        in_df=sfAlerts[7][(sfAlerts[7]["REGION"] == "west")].copy(),
        filter="DATABASE_NAME",
        loop_code="s_md",
        cwd=cwd,
        plot_title=None,
        z_in=z_in,
    )

    # Storage spikes by region - west
    sfTotStorageSpikeAlertsWest_pre = sfAlerts[14][
        (sfAlerts[14]["REGION"] == "west")
    ].copy()
    sfTotStorageSpikeAlertsWest = (
        sfTotStorageSpikeAlertsWest_pre["REGION"]
        .copy()
        .drop_duplicates()
        .values.tolist()
    )
    innerPlotLoop(
        awsList=[],
        sfList=sfTotStorageSpikeAlertsWest,
        in_df=sfAlerts[19][(sfAlerts[19]["REGION"] == "west")].copy(),
        filter="REGION",
        loop_code="sf_s_spk",
        cwd=cwd,
        plot_title="SF Regional Storage Spike - West",
        z_in=z_in,
    )

    # Storage spikes by region - east
    sfTotStorageSpikeAlertsEast_pre = sfAlerts[14][
        (sfAlerts[14]["REGION"] == "east")
    ].copy()
    sfTotStorageSpikeAlertsEast = (
        sfTotStorageSpikeAlertsEast_pre["REGION"]
        .copy()
        .drop_duplicates()
        .values.tolist()
    )
    innerPlotLoop(
        awsList=[],
        sfList=sfTotStorageSpikeAlertsEast,
        in_df=sfAlerts[19][(sfAlerts[19]["REGION"] == "east")].copy(),
        filter="REGION",
        loop_code="sf_s_spk",
        cwd=cwd,
        plot_title="SF Regional Storage Spike - East",
        z_in=z_in,
    )

    # Storage spikes by database and region - west
    sfStorageSpikeAlertsWest_pre = sfAlerts[4][(sfAlerts[4]["REGION"] == "west")].copy()
    sfStorageSpikeAlertsWest = (
        sfStorageSpikeAlertsWest_pre["DATABASE_NAME"]
        .copy()
        .drop_duplicates()
        .values.tolist()
    )
    innerPlotLoop(
        awsList=[],
        sfList=sfStorageSpikeAlertsWest,
        in_df=sfAlerts[9][(sfAlerts[9]["REGION"] == "west")].copy(),
        filter="DATABASE_NAME",
        loop_code="sf_s_spk",
        cwd=cwd,
        plot_title=None,
        z_in=z_in,
    )

    # Storage spikes by database and region - east
    sfTotStorageSpikeAlertsEast_pre = sfAlerts[4][
        (sfAlerts[4]["REGION"] == "east")
    ].copy()
    sfTotStorageSpikeAlertsEast = (
        sfTotStorageSpikeAlertsEast_pre["DATABASE_NAME"]
        .copy()
        .drop_duplicates()
        .values.tolist()
    )
    innerPlotLoop(
        awsList=[],
        sfList=sfTotStorageSpikeAlertsEast,
        in_df=sfAlerts[9][(sfAlerts[9]["REGION"] == "east")].copy(),
        filter="DATABASE_NAME",
        loop_code="sf_s_spk",
        cwd=cwd,
        plot_title=None,
        z_in=z_in,
    )

    # Compute spikes by region - west
    sfTotComputeSpikeAlertsWest_pre = sfAlerts[10][
        (sfAlerts[10]["REGION"] == "west")
    ].copy()
    sfTotComputeSpikeAlertsWest = (
        sfTotComputeSpikeAlertsWest_pre["REGION"]
        .copy()
        .drop_duplicates()
        .values.tolist()
    )
    innerPlotLoop(
        awsList=[],
        sfList=sfTotComputeSpikeAlertsWest,
        in_df=sfAlerts[18][(sfAlerts[18]["REGION"] == "west")].copy(),
        filter="REGION",
        loop_code="sf_c_spk",
        cwd=cwd,
        plot_title="SF Regional Compute Spike - West",
        z_in=z_in,
    )

    # Compute spikes by region - east
    sfTotComputeSpikeAlertsEast_pre = sfAlerts[10][
        (sfAlerts[10]["REGION"] == "east")
    ].copy()
    sfTotComputeSpikeAlertsEast = (
        sfTotComputeSpikeAlertsEast_pre["REGION"]
        .copy()
        .drop_duplicates()
        .values.tolist()
    )
    innerPlotLoop(
        awsList=[],
        sfList=sfTotComputeSpikeAlertsEast,
        in_df=sfAlerts[18][(sfAlerts[18]["REGION"] == "east")].copy(),
        filter="REGION",
        loop_code="sf_c_spk",
        cwd=cwd,
        plot_title="SF Regional Compute Spike - East",
        z_in=z_in,
    )

    # Compute spikes by warehouse and region - west
    sfComputeSpikeAlertsWest_pre = sfAlerts[0][(sfAlerts[0]["REGION"] == "west")].copy()
    sfComputeSpikeAlertsWest = (
        sfComputeSpikeAlertsWest_pre["WAREHOUSE_NAME"]
        .copy()
        .drop_duplicates()
        .values.tolist()
    )
    innerPlotLoop(
        awsList=[],
        sfList=sfComputeSpikeAlertsWest,
        in_df=sfAlerts[8][(sfAlerts[8]["REGION"] == "west")].copy(),
        filter="WAREHOUSE_NAME",
        loop_code="sf_c_spk",
        cwd=cwd,
        plot_title=None,
        z_in=z_in,
    )

    # Storage spikes by warehouse and region - east
    sfComputeSpikeAlertsEast_pre = sfAlerts[0][(sfAlerts[1]["REGION"] == "east")].copy()
    sfComputeSpikeAlertsEast = (
        sfComputeSpikeAlertsEast_pre["WAREHOUSE_NAME"]
        .copy()
        .drop_duplicates()
        .values.tolist()
    )
    innerPlotLoop(
        awsList=[],
        sfList=sfComputeSpikeAlertsEast,
        in_df=sfAlerts[8][(sfAlerts[8]["REGION"] == "east")].copy(),
        filter="WAREHOUSE_NAME",
        loop_code="sf_c_spk",
        cwd=cwd,
        plot_title=None,
        z_in=z_in,
    )
