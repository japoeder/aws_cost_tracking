"""Program to loop through plot creation"""

from datetime import datetime
import pandas as pd
from utilities.genMacdPlot import macd_plotter
from utilities.genSpikePlot import spike_plotter
from utilities.genSummaryPlot import summary_plotter


def innerPlotLoop(
    awsList, in_df, in_filter, loop_code, cwd, plot_title, z_in, envConnData, dt_to_proc
):
    """Driver method to loop through plot creation"""
    file_dt = datetime.strptime(dt_to_proc, "%Y-%m-%d").strftime("%Y%m%d")

    if loop_code == "a_s":
        # Generate summary plots
        summary_plotter(
            in_df,
            file_dt,
            cwd,
            envConnData=envConnData,
        )

    if loop_code == "a_md":
        for s in awsList:
            svc = s.replace(" ", "")
            # Generate MACD plots
            macd_plotter(
                in_df,
                in_filter,
                s,
                "svc_day_usd_macd",
                "MACD",
                "svc_day_usd_9ewm",
                "9EWM",
                f"amz_{svc}_{file_dt}",
                cwd,
                None,
                plot_title=plot_title,
                envConnData=envConnData,
            )

    if loop_code == "a_spk":
        for s in awsList:
            svc = s.replace(" ", "")
            # Generate Spike plots
            in_df["DATE"] = pd.to_datetime(
                [f"{y}-{m}-{d}" for y, m, d in zip(in_df.YEAR, in_df.MONTH, in_df.DAY)]
            )
            in_df["DATE"] = in_df["DATE"].astype(str)
            # print(in_df.dtypes)
            spike_plotter(
                in_df,
                in_filter,
                s,
                "USD",
                f"amz_tot_{svc}_{file_dt}",
                cwd,
                region=None,
                plot_title=plot_title,
                z_in=z_in,
                envConnData=envConnData,
            )
