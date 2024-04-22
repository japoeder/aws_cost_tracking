"""Program to loop through plot creation"""

from datetime import datetime
import pandas as pd
from utilities.genMacdPlot import macd_plotter
from utilities.genSpikePlot import spike_plotter


def innerPlotLoop(
    awsList, sfList, in_df, in_filter, loop_code, cwd, plot_title, z_in, envConnData
):
    """Driver method to loop through plot creation"""
    file_dt = datetime.now().strftime("%Y%m%d")
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
            )

    if loop_code == "a_spk":
        for s in sfList:
            svc = s.replace(" ", "")
            # Generate Spike plots
            in_df["DATE"] = pd.to_datetime(
                [f"{y}-{m}-{d}" for y, m, d in zip(in_df.YEAR, in_df.MONTH, in_df.DAY)]
            )
            h_date = str(datetime.date(max(in_df["DATE"])))
            in_df["DATE"] = in_df["DATE"].astype(str)
            # print(in_df.dtypes)
            spike_plotter(
                in_df,
                in_filter,
                s,
                "USD",
                h_date,
                plot_title,
                f"amz_tot_{svc}_{file_dt}",
                cwd,
                region=None,
                plot_title=plot_title,
                z_in=z_in,
                envConnData=envConnData,
            )

    if loop_code == "c_md":
        for s in sfList:
            svc = s.replace(" ", "")
            # Generate MACD plots
            region = in_df["REGION"].unique()[0]
            macd_plotter(
                in_df,
                in_filter,
                s,
                "wh_day_usd_macd_52",
                "MACD",
                "wh_day_usd_18ewm",
                "18EWM",
                f"sf_c_tr_{svc}_{region}_{file_dt}",
                cwd,
                region,
                plot_title=plot_title,
                envConnData=envConnData,
            )

    if loop_code == "s_md":
        for s in sfList:
            svc = s.replace(" ", "")
            # Generate MACD plots
            region = in_df["REGION"].unique()[0]
            # print(in_df["REGION"].unique())
            macd_plotter(
                in_df,
                in_filter,
                s,
                "db_day_usd_macd_52",
                "MACD",
                "db_day_usd_18ewm",
                "18EWM",
                f"sf_s_tr_{svc}_{region}_{file_dt}",
                cwd,
                region,
                plot_title=plot_title,
                envConnData=envConnData,
            )

    if loop_code == "ts_md":
        for s in sfList:
            svc = s.replace(" ", "")
            # Generate MACD plots
            region = in_df["REGION"].unique()[0]
            # print(in_df["REGION"].unique())
            macd_plotter(
                in_df,
                in_filter,
                s,
                "tot_db_day_usd_macd_52",
                "MACD",
                "tot_db_day_usd_18ewm",
                "18EWM",
                f"sf_tots_tr_{region}_{file_dt}",
                cwd,
                region,
                plot_title=plot_title,
                envConnData=envConnData,
            )

    if loop_code == "tc_md":
        for s in sfList:
            svc = s.replace(" ", "")
            # Generate MACD plots
            region = in_df["REGION"].unique()[0]
            # print(in_df["REGION"].unique())
            macd_plotter(
                in_df,
                in_filter,
                s,
                "tot_wh_day_usd_macd_52",
                "MACD",
                "tot_wh_day_usd_18ewm",
                "18EWM",
                f"sf_totc_tr_{region}_{file_dt}",
                cwd,
                region,
                plot_title=plot_title,
                envConnData=envConnData,
            )

    if loop_code == "sf_s_spk":
        # print('running sf spike')
        for s in sfList:
            region = in_df["REGION"].unique()[0]
            if s == region:
                svc = "_"
            else:
                svc = f'_{s.replace(" ", "")}_'

            in_df["DATE"] = pd.to_datetime(
                [f"{y}-{m}-{d}" for y, m, d in zip(in_df.YEAR, in_df.MONTH, in_df.DAY)]
            )
            h_date = str(datetime.date(max(in_df["DATE"])))
            in_df["DATE"] = in_df["DATE"].astype(str)
            spike_plotter(
                in_df,
                in_filter,
                s,
                "USD",
                h_date,
                f"sf_s_spk{svc}{region}_{file_dt}",
                cwd,
                region,
                plot_title=plot_title,
                z_in=z_in,
                envConnData=envConnData,
            )

    if loop_code == "sf_c_spk":
        # print('running sf spike')
        for s in sfList:
            region = in_df["REGION"].unique()[0]
            if s == region:
                svc = "_"
            else:
                svc = f'_{s.replace(" ", "")}_'
            # Generate Spike plots
            in_df["DATE"] = pd.to_datetime(
                [f"{y}-{m}-{d}" for y, m, d in zip(in_df.YEAR, in_df.MONTH, in_df.DAY)]
            )
            h_date = str(datetime.date(max(in_df["DATE"])))
            in_df["DATE"] = in_df["DATE"].astype(str)
            spike_plotter(
                in_df,
                in_filter,
                s,
                "USD",
                h_date,
                f"sf_c_spk{svc}{region}_{file_dt}",
                cwd,
                region,
                plot_title=plot_title,
                z_in=z_in,
                envConnData=envConnData,
            )
