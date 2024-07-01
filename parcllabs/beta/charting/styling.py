STYLING_CONFIG = {
    "hovermode": "x unified",  # Unified hover mode for better interactivity
    "showlegend": False,  # show the legend
    "plot_bgcolor": "#000000",  # Dark background for better contrast
    "paper_bgcolor": "#000000",  # Dark background for the paper
    "font": {"color": "#FFFFFF"},
    "xaxis": {
        "title_text": "",
        "showgrid": True,
        "gridcolor": "rgba(255, 255, 255, 0.2)",
        "tickangle": 0,
        "tickfont": {"size": 14},
        "linecolor": "rgba(255, 255, 255, 0.7)",
        "linewidth": 1,
        "tickformat": "`%y",
        "range": [],
    },
    "xaxis2": {
        "title_text": "",
        "showgrid": True,
        "gridcolor": "rgba(255, 255, 255, 0.2)",
        "tickangle": 0,
        "tickfont": {"size": 14},
        "linecolor": "rgba(255, 255, 255, 0.7)",
        "linewidth": 1,
        "tickformat": "`%y",
        "range": [],
    },
    "yaxis": {
        "showgrid": True,
        "gridwidth": 0.5,
        "gridcolor": "rgba(255, 255, 255, 0.2)",
        "tickfont": {"size": 14},
        "ticksuffix": "",
        "zeroline": False,
        "linecolor": "rgba(255, 255, 255, 0.7)",
        "linewidth": 1,
        "side": "right",
        "ticks": "outside",
        "rangemode": "normal",
        "range": [],
    },
    "yaxis2": {
        "showgrid": True,
        "gridwidth": 0.5,
        "gridcolor": "rgba(255, 255, 255, 0.2)",
        "tickfont": {"size": 14},
        "ticksuffix": "",
        "zeroline": False,
        "linecolor": "rgba(255, 255, 255, 0.7)",
        "linewidth": 1,
        "side": "right",
        "ticks": "outside",
        "rangemode": "normal",
        "range": [],
    },
    "hoverlabel": {"bgcolor": "#1F1F1F", "font_size": 14, "font_family": "Rockwell"},
    "margin": {
        "l": 10,  # left margin
        "r": 10,  # right margin
        "t": 50,  # top margin
        "b": 10,  # bottom margin
    },
    "default_annotation_styling": {
        "font": {"size": 14, "color": "#FFFFFF"},
        # 'bgcolor': '#1F1F1F',
        # 'bordercolor': '#FFFFFF',
        # 'borderwidth': 1,
        # 'borderpad': 4,
        "xref": "paper",
        "yref": "paper",
        "x": 0.5,
        "y": 1.04,
        "xanchor": "center",
        "yanchor": "bottom",
        "showarrow": False,
    },
    "update_xaxes": {
        "showline": True,
        "linewidth": 2,
        "linecolor": "#FFFFFF",
        "mirror": True,
    },
    "update_yaxes": {
        "showline": True,
        "linewidth": 2,
        "linecolor": "#FFFFFF",
        "mirror": True,
        "ticks": "outside",
    },
}

default_style_config = {
    "line_color": "#2ca02c",
    "line_width": 3,
    "marker_size": 8,
    "marker_color": "#2ca02c",
    "marker_outline_color": "#ffffff",
    "bar1_color": "#1f77b4",
    "bar1_opacity": 0.8,
    "bar2_color": "#ff7f0e",
    "bar2_opacity": 0.8,
    "background_color": "#1e1e1e",
    "font_color": "#ffffff",
    "title_font": dict(size=24, color="#ffffff", family="Arial Black"),
    "axis_font": dict(size=12, family="Arial", color="#ffffff"),
    "title_font_axis": dict(size=14, family="Arial Black", color="#ffffff"),
    "grid_color": "rgba(255, 255, 255, 0.2)",
    "line_color_axis": "rgba(255, 255, 255, 0.7)",
    "hover_bg_color": "#2f2f2f",
    "hover_font_size": 12,
    "hover_font_family": "Arial",
    "hover_font_color": "#ffffff",
    "legend_font": dict(size=12, color="#ffffff", family="Arial"),
    "legend_x": 0.01,
    "legend_y": 0.98,
    "legend_xanchor": "left",
    "legend_yanchor": "top",
    "tick_angle": -45,
    "tick_prefix": "$",
    "tick_suffix": " units",
    "showgrid": True,
    "gridwidth": 0.5,
    "linewidth": 1,
    "barmode": "stack",  # Use 'group' if you prefer grouping
}


SIZE_CONFIG = {
    "x": {"height": 675, "width": 1200},
    "instgram_square": {"height": 1080, "width": 1080},
    "instagram_portrait": {"height": 1080, "width": 1350},
    "instagram_landscape": {"height": 1080, "width": 566},
    "linkedin": {"height": 627, "width": 1200},
    "blog": {"height": 630, "width": 1200},
}


def generate_annotation(**kwargs):
    """
    Generate an annotation dictionary for plotly.
    """
    config = STYLING_CONFIG["default_annotation_styling"]
    config.update(kwargs)
    return config
