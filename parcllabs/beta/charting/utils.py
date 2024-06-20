labs_logo_lookup = {
    'labs': {
        'blue': 'https://parcllabs-assets.s3.amazonaws.com/powered-by-parcllabs-api.png',
        'white': 'https://parcllabs-assets.s3.amazonaws.com/powered-by-parcllabs-api-logo-white+(1).svg'
    }
}

def create_labs_logo_dict(
        src: str='labs',
        color: str='white',
        xref: str='paper',
        yref: str='paper',
        x: float=0.5,
        y: float=1.04,
        sizex: float=0.15,
        sizey: float=0.15,
        xanchor: str='center',
        yanchor: str='bottom'
):
    return dict(
        source=labs_logo_lookup[src].get(color, 'white'),
        xref=xref,
        yref=yref,
        x=x,
        y=y,
        sizex=sizex,
        sizey=sizey,
        xanchor=xanchor,
        yanchor=yanchor
    )