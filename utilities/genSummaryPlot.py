"""Program to create plotting for AWS cost trend changes"""

import plotly.express as px
import boto3


def summary_plotter(
    df_in,
    file_dt,
    cwd,
    envConnData,
):
    """Driver method to create spike plot for AWS cost trend changes"""

    file_name = f"amz_mtdSpendxService_{file_dt}"

    # Round both df_in[0] and df_in[1] total_spend to 2 decimal places
    df_in[0]["TOTAL_SPEND"] = df_in[0]["TOTAL_SPEND"].round(2)
    df_in[1]["TOTAL_SPEND"] = df_in[1]["TOTAL_SPEND"].round(2)

    fig = px.bar(
        df_in[0], x="SERVICE", y="TOTAL_SPEND", title="Month to Date Spend by Service"
    )
    # Center plot title
    fig.update_layout(title_x=0.5)
    # Rotate x-axis labels
    fig.update_xaxes(tickangle=-45)
    # Set y-axis format to $0.00
    fig.update_yaxes(tickprefix="$", tickformat=",0.00f")
    # y-axis title
    fig.update_yaxes(title_text="MTD $ Spend")
    # x-axis title
    fig.update_xaxes(title_text="Service")

    fig.write_html(f"{cwd}/output/plots/{file_name}.html", auto_open=False)
    s3 = boto3.client("s3")
    s3.upload_file(
        f"{cwd}/output/plots/{file_name}.html",
        envConnData["s3_bucket"],
        f"{file_name}.html",
        ExtraArgs={"ContentType": "html"},
    )

    file_name = f"amz_dailySpendxService_{file_dt}"
    fig = px.bar(
        df_in[1],
        x="DATE_STR",
        y="TOTAL_SPEND",
        color="SERVICE",
        title="Daily Spend x Service",
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
