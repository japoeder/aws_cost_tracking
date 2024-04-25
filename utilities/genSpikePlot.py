"""Program to create plotting for AWS cost spikes"""

import plotly.express as px
import boto3


def spike_plotter(
    df_in,
    filter_on,
    filter_val,
    spike_att,
    file_name,
    cwd,
    region,
    plot_title,
    z_in,
    envConnData,
):
    """Driver method to create spike plot for AWS cost spikes"""

    # Format spike_att to 2 decimal places
    df_in[spike_att] = df_in[spike_att].round(2)

    df_in_red = df_in[df_in[filter_on] == filter_val].copy().tail(n=365)

    # Create date string
    df_in_red["DATE"] = (
        df_in_red["YEAR"] * 10000 + df_in_red["MONTH"] * 100 + df_in_red["DAY"]
    ).astype(str)
    # round spike_att to 2 decimal places
    df_in_red[spike_att] = df_in_red[spike_att].round(2)

    # Calculate mean and standard deviation
    mean = df_in_red[spike_att].mean()
    std_dev = df_in_red[spike_att].std()
    highlight_threshold = mean + z_in * std_dev

    # Select 'DATE' values into a list where spike_att is greater than highlight_threshold
    highlighted_dates = df_in_red[df_in_red[spike_att] > highlight_threshold][
        "DATE"
    ].tolist()

    colors = {date: "red" for date in highlighted_dates}
    default_color = "blue"
    color_discrete_map = {
        c: colors.get(c, default_color) for c in df_in_red.DATE.unique()
    }

    if plot_title is None:
        if region is not None:
            p_title = f"{filter_val} ({region})"
        else:
            p_title = f"{filter_val}"
    else:
        p_title = plot_title

    fig = px.bar(
        df_in_red,
        x="DATE",
        y=spike_att,
        color="DATE",
        color_discrete_map=color_discrete_map,
        title=f"{p_title}",
    )

    fig.add_shape(
        type="line",
        xref="paper",
        x0=0,
        y0=mean,
        x1=1,
        y1=mean,
        line={"color": "red", "width": 1, "dash": "dash"},
    )

    fig.add_shape(
        type="line",
        xref="paper",
        x0=0,
        y0=mean + z_in * std_dev,
        x1=1,
        y1=mean + z_in * std_dev,
        line={"color": "red", "width": 1},
    )

    fig.update_traces(showlegend=False)
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

    # fig.show()
    fig.write_html(f"{cwd}/output/plots/{file_name}.html", auto_open=False)
    s3 = boto3.client("s3")
    s3.upload_file(
        f"{cwd}/output/plots/{file_name}.html",
        envConnData["s3_bucket"],
        f"{file_name}.html",
        ExtraArgs={"ContentType": "html"},
    )
