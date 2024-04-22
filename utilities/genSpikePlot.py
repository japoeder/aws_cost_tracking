"""Program to create plotting for AWS cost spikes"""

import plotly.express as px
import boto3


def spike_plotter(
    df_in,
    filter_on,
    filter_val,
    spike_att,
    day_highlight,
    file_name,
    cwd,
    region,
    plot_title,
    z_in,
    envConnData,
):
    """Driver method to create spike plot for AWS cost spikes"""
    df_in_red = df_in[df_in[filter_on] == filter_val].copy().tail(n=365)
    default_color = "blue"
    colors = {day_highlight: "red"}
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

    mean = df_in_red[spike_att].mean()
    std_dev = df_in_red[spike_att].std()
    # print(mean, std_dev)

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
    fig.show()
    fig.write_html(f"{cwd}/output/plots/{file_name}.html", auto_open=False)
    s3 = boto3.client("s3")
    s3.upload_file(
        f"{cwd}/output/plots/{file_name}.html",
        envConnData["s3_bucket"],
        f"{file_name}.html",
        ExtraArgs={"ContentType": "html"},
    )
