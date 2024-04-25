"""Program to create plotting for AWS cost trend changes"""

import plotly.express as px
import boto3


def macd_plotter(
    df_in,
    filter_on,
    filter_val,
    md_att,
    md_att_label,
    signal,
    signal_label,
    file_name,
    cwd,
    region,
    plot_title,
    envConnData,
):
    """Driver method to create spike plot for AWS cost trend changes"""

    df_in["DATE"] = (
        df_in["YEAR"] * 10000 + df_in["MONTH"] * 100 + df_in["DAY"]
    ).astype(str)
    # round MTD_SPEND to 2 decimal places
    df_in[md_att] = df_in[md_att].round(2)
    # round signal to 2 decimal places
    df_in[signal] = df_in[signal].round(2)

    df_in_red = df_in[df_in[filter_on] == filter_val].copy()
    if plot_title is None:
        if region is not None:
            fig = px.line(title=f"{filter_val} ({region})")
        else:
            fig = px.line(title=f"{filter_val}")
    else:
        fig = px.line(title=f"{plot_title}")

    fig.add_scatter(
        x=df_in_red["DATE"],
        y=df_in_red[md_att],
        mode="lines",
        name=md_att_label,
        line={"color": "blue"},
    )
    fig.add_scatter(
        x=df_in_red["DATE"],
        y=df_in_red[signal],
        mode="lines",
        name=signal_label,
        line={"color": "red"},
    )

    # Center plot title
    fig.update_layout(title_x=0.5)
    # rotate x-axis labels 45 degrees
    fig.update_xaxes(tickangle=-45)
    # Set y-axis format to $0.00
    fig.update_yaxes(tickprefix="$", tickformat=",0.00f")
    # y-axis title
    fig.update_yaxes(title_text="$ Spend")
    # x-axis title
    fig.update_xaxes(title_text="Date")

    fig.write_html(f"{cwd}/output/plots/{file_name}.html", auto_open=False)
    s3 = boto3.client("s3")
    s3.upload_file(
        f"{cwd}/output/plots/{file_name}.html",
        envConnData["s3_bucket"],
        f"{file_name}.html",
        ExtraArgs={"ContentType": "html"},
    )
