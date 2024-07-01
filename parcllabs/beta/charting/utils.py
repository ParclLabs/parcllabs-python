labs_logo_lookup = {
    "labs": {
        "blue": "https://parcllabs-assets.s3.amazonaws.com/powered-by-parcllabs-api.png",
        "white": "https://parcllabs-assets.s3.amazonaws.com/powered-by-parcllabs-api-logo-white+(1).svg",
    }
}


def create_labs_logo_dict(
    src: str = "labs",
    color: str = "white",
    xref: str = "paper",
    yref: str = "paper",
    x: float = 1,
    y: float = 0,
    sizex: float = 0.15,
    sizey: float = 0.15,
    xanchor: str = "right",
    yanchor: str = "bottom",
):
    return dict(
        source=labs_logo_lookup[src].get(color, "white"),
        xref=xref,
        yref=yref,
        x=x,
        y=y,
        sizex=sizex,
        sizey=sizey,
        xanchor=xanchor,
        yanchor=yanchor,
    )


def save_figure(fig, save_path: str, width: int = 800, height: int = 600):
    if save_path:
        fig.write_image(save_path, width=width, height=height)


def sort_chart_data(df):
    if "date" in df.columns:
        df = df.sort_values(by="date")
    return df
