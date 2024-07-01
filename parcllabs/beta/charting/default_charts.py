import pandas as pd
import plotly.graph_objects as go

from parcllabs.beta.charting.styling import default_style_config
from parcllabs.beta.charting.utils import (
    create_labs_logo_dict,
    save_figure,
    sort_chart_data,
)


def create_dual_axis_chart(
    title: str,
    line_data: pd.DataFrame,
    line_series: str = "line_series",
    bar1_data: pd.DataFrame = None,
    bar1_series: str = None,
    bar2_data: pd.DataFrame = None,
    bar2_series: str = None,
    save_path: str = None,
    yaxis1_title: str = "Primary Y-Axis",
    yaxis2_title: str = "Secondary Y-Axis",
    height=675,
    width=1200,
    style_config=default_style_config,
):
    fig = go.Figure()

    line_data = sort_chart_data(line_data)

    if bar1_data is not None and bar1_series is not None:
        bar1_data = sort_chart_data(bar1_data)
        fig.add_trace(
            go.Bar(
                x=bar1_data["date"],
                y=bar1_data[bar1_series],
                marker=dict(
                    color=style_config["bar1_color"],
                    opacity=style_config["bar1_opacity"],
                ),
                name=bar1_series,
                yaxis="y2",
            )
        )

    if bar2_data is not None and bar2_series is not None:
        bar2_data = sort_chart_data(bar2_data)
        fig.add_trace(
            go.Bar(
                x=bar2_data["date"],
                y=bar2_data[bar2_series],
                marker=dict(
                    color=style_config["bar2_color"],
                    opacity=style_config["bar2_opacity"],
                ),
                name=bar2_series,
                yaxis="y2",
            )
        )

    fig.add_trace(
        go.Scatter(
            x=line_data["date"],
            y=line_data[line_series],
            mode="lines+markers",
            line=dict(
                width=style_config["line_width"], color=style_config["line_color"]
            ),
            marker=dict(
                size=style_config["marker_size"],
                color=style_config["marker_color"],
                line=dict(width=1, color=style_config["marker_outline_color"]),
            ),
            name=yaxis1_title,
        )
    )

    fig.data = fig.data[::-1]

    fig.update_layout(
        margin=dict(l=40, r=40, t=80, b=40),
        height=height,
        width=width,
        title={
            "text": title,
            "y": 0.95,
            "x": 0.5,
            "xanchor": "center",
            "yanchor": "top",
            "font": style_config["title_font"],
        },
        plot_bgcolor=style_config["background_color"],
        paper_bgcolor=style_config["background_color"],
        font=dict(color=style_config["font_color"]),
        xaxis=dict(
            title_text="",
            showgrid=style_config["showgrid"],
            gridwidth=style_config["gridwidth"],
            gridcolor=style_config["grid_color"],
            tickangle=style_config["tick_angle"],
            tickfont=style_config["axis_font"],
            linecolor=style_config["line_color_axis"],
            linewidth=style_config["linewidth"],
            titlefont=style_config["title_font_axis"],
        ),
        yaxis=dict(
            title_text=yaxis1_title,
            showgrid=style_config["showgrid"],
            gridwidth=style_config["gridwidth"],
            gridcolor=style_config["grid_color"],
            tickfont=style_config["axis_font"],
            tickprefix=style_config["tick_prefix"],
            zeroline=False,
            linecolor=style_config["line_color_axis"],
            linewidth=style_config["linewidth"],
            titlefont=style_config["title_font_axis"],
        ),
        yaxis2=dict(
            title_text=yaxis2_title,
            showgrid=False,
            gridwidth=style_config["gridwidth"],
            tickfont=style_config["axis_font"],
            zeroline=False,
            linecolor=style_config["line_color_axis"],
            linewidth=style_config["linewidth"],
            overlaying="y",
            side="right",
            ticksuffix=style_config["tick_suffix"],
            titlefont=style_config["title_font_axis"],
        ),
        hovermode="x unified",
        hoverlabel=dict(
            bgcolor=style_config["hover_bg_color"],
            font_size=style_config["hover_font_size"],
            font_family=style_config["hover_font_family"],
            font_color=style_config["hover_font_color"],
        ),
        legend=dict(
            x=style_config["legend_x"],
            y=style_config["legend_y"],
            xanchor=style_config["legend_xanchor"],
            yanchor=style_config["legend_yanchor"],
            font=style_config["legend_font"],
            bgcolor="rgba(0, 0, 0, 0)",
        ),
        barmode=style_config["barmode"] if bar2_data is not None else "group",
    )

    fig.add_layout_image(create_labs_logo_dict())

    save_figure(fig, save_path=save_path, width=width, height=height)

    fig.show()
