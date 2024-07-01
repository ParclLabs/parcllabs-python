import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from parcllabs.beta.charting.utils import (
    create_labs_logo_dict,
    save_figure,
    sort_chart_data,
)
from parcllabs.beta.charting.styling import generate_annotation
from parcllabs.beta.charting.styling import STYLING_CONFIG

# config logo
labs_logo_dict = create_labs_logo_dict(
    src="labs",
    y=1.07,
    x=1,
    xanchor="right",
    yanchor="top",
    sizex=0.15,
    sizey=0.15,
)


# technical chart definition
def build_technical_chart(
    line_chart_df: pd.DataFrame,
    bar_chart_df: pd.DataFrame,
    save_path: str = None,
    value_name_main: str = None,
    value_name_secondary: str = None,
    header_left_primary_text: str = None,
    header_left_secondary_text: str = None,
    header_right_primary_text: str = None,
    line_chart_left_primary_text: str = None,
    bar_chart_left_primary_text: str = None,
    height=700,
    width=1200,
    moving_average_window: int = 6,
    styling_config: dict = STYLING_CONFIG,
):

    bar_chart_df = sort_chart_data(bar_chart_df)
    line_chart_df = sort_chart_data(line_chart_df)

    # Get the date range for the x-axis
    date_range = [
        min(line_chart_df["date"].min(), bar_chart_df["date"].min()),
        max(line_chart_df["date"].max(), bar_chart_df["date"].max()),
    ]

    # Create subplots: 2 rows, 1 column
    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=False,
        row_heights=[0.8, 0.2],
        vertical_spacing=0.05,  # Increased vertical spacing
    )

    # Add traces for positive and negative segments based on daily_return
    for i in range(1, len(line_chart_df)):
        color = "green" if line_chart_df["daily_return"].iloc[i] >= 0 else "red"
        fig.add_trace(
            go.Scatter(
                x=[line_chart_df["date"].iloc[i - 1], line_chart_df["date"].iloc[i]],
                y=[
                    line_chart_df[value_name_main].iloc[i - 1],
                    line_chart_df[value_name_main].iloc[i],
                ],
                mode="lines",
                line=dict(width=3, color=color),
                showlegend=False,
                hoverinfo="none",
            ),
            row=1,
            col=1,
        )

    # Plot the secondary data as a bar chart
    fig.add_trace(
        go.Bar(
            x=bar_chart_df["date"],
            y=bar_chart_df[value_name_secondary],
            name="Sales Volume",
            marker=dict(
                color=bar_chart_df["volColor"]
            ),  # Use volColor for the bar chart colors
        ),
        row=2,
        col=1,
    )
    # Add the 6-month moving average line
    bar_chart_df["ma"] = (
        bar_chart_df[value_name_secondary]
        .rolling(window=moving_average_window)
        .mean()
        .round(2)
    )
    ma_last = bar_chart_df["ma"].iloc[-1]
    fig.add_trace(
        go.Scatter(
            x=bar_chart_df["date"],
            y=bar_chart_df["ma"],
            mode="lines",
            name=f"{moving_average_window}-Month Moving Average",
            line=dict(width=2, color="#FFA500"),
            showlegend=False,
        ),
        row=2,
        col=1,
    )

    # update date ranges for consistency
    styling_config["xaxis"]["range"] = date_range
    styling_config["xaxis2"]["range"] = date_range
    styling_config["yaxis"]["range"] = [
        line_chart_df[value_name_main].min() - 10,
        line_chart_df[value_name_main].max() + 10,
    ]
    styling_config["yaxis2"]["range"] = [
        bar_chart_df[value_name_secondary].min() - 10,
        bar_chart_df[value_name_secondary].max() + 10,
    ]
    # Update layout
    fig.update_layout(
        height=height,
        width=width,
        showlegend=styling_config[
            "showlegend"
        ],  # Show the legend to include the moving average line
        plot_bgcolor=styling_config[
            "plot_bgcolor"
        ],  # Dark background for better contrast
        paper_bgcolor=styling_config["paper_bgcolor"],  # Dark background for the paper
        font=styling_config["font"],
        hovermode=styling_config[
            "hovermode"
        ],  # Unified hover mode for better interactivity
        xaxis=styling_config["xaxis"],
        xaxis2=styling_config["xaxis2"],
        yaxis=styling_config["yaxis"],
        yaxis2=styling_config["yaxis2"],
        hoverlabel=styling_config["hoverlabel"],
        margin=styling_config["margin"],
    )

    fig.add_annotation(
        generate_annotation(
            text=line_chart_left_primary_text,
            x=0.002,
            y=1,
            xanchor="left",
            yanchor="top",
        )
    )

    fig.add_annotation(
        generate_annotation(
            text=header_left_primary_text,
            x=0,
            y=1.07,
            xanchor="left",
            yanchor="top",
        )
    )

    fig.add_annotation(
        generate_annotation(
            text=header_left_secondary_text,
            x=0,
            y=1.04,
            xanchor="left",
            yanchor="top",
        )
    )

    fig.add_annotation(
        generate_annotation(
            text=header_right_primary_text,
            x=1,
            y=1.04,
            xanchor="right",
            yanchor="top",
        )
    )

    fig.add_annotation(
        generate_annotation(
            text=bar_chart_left_primary_text,
            x=0.003,
            y=0.15,
            xanchor="left",
            yanchor=None,
        )
    )

    fig.add_annotation(
        generate_annotation(
            text=f"--- MA({moving_average_window}) {ma_last:,.2f}",
            x=0.003,
            y=0.12,
            xanchor="left",
            yanchor=None,
        )
    )

    # Add borders and hover effect for both charts
    fig.update_xaxes(**styling_config["update_xaxes"])

    fig.update_yaxes(**styling_config["update_yaxes"])

    # Add Parcl Labs logo
    fig.add_layout_image(labs_logo_dict)

    save_figure(fig, save_path=save_path, width=width, height=height)

    # Show the plot
    fig.show()
