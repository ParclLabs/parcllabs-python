STYLING_CONFIG = {
    'hovermode': 'x unified', # Unified hover mode for better interactivity
    'showlegend': False, # show the legend
    'plot_bgcolor': '#000000', # Dark background for better contrast
    'paper_bgcolor': '#000000', # Dark background for the paper
    'font': {'color': '#FFFFFF'},
    'xaxis': {
        'title_text': '',
        'showgrid': True,
        'gridcolor': 'rgba(255, 255, 255, 0.2)',
        'tickangle': 0,
        'tickfont': {'size': 14},
        'linecolor': 'rgba(255, 255, 255, 0.7)',
        'linewidth': 1,
        'tickformat': '`%y',
        'range': []
    },
    'xaxis2': {
        'title_text': '',
        'showgrid': True,
        'gridcolor': 'rgba(255, 255, 255, 0.2)',
        'tickangle': 0,
        'tickfont': {'size': 14},
        'linecolor': 'rgba(255, 255, 255, 0.7)',
        'linewidth': 1,
        'tickformat': '`%y',
        'range': []
    },
    'yaxis': {
        'showgrid': True,
        'gridwidth': 0.5,
        'gridcolor': 'rgba(255, 255, 255, 0.2)',
        'tickfont': {'size': 14},
        'ticksuffix': '',
        'zeroline': False,
        'linecolor': 'rgba(255, 255, 255, 0.7)',
        'linewidth': 1,
        'side': 'right',
        'ticks': 'outside',
        'rangemode': 'normal',
        'range': []
    },
    'yaxis2': {
        'showgrid': True,
        'gridwidth': 0.5,
        'gridcolor': 'rgba(255, 255, 255, 0.2)',
        'tickfont': {'size': 14},
        'ticksuffix': '',
        'zeroline': False,
        'linecolor': 'rgba(255, 255, 255, 0.7)',
        'linewidth': 1,
        'side': 'right',
        'ticks': 'outside',
        'rangemode': 'normal',
        'range': []
    },
    'hoverlabel': {
        'bgcolor': '#1F1F1F',
        'font_size': 14,
        'font_family': 'Rockwell'
    },
    'margin': {
        'l': 10, # left margin
        'r': 10, # right margin
        't': 50, # top margin
        'b': 10  # bottom margin
    },
    'default_annotation_styling': {
        'font': {'size': 14, 'color': '#FFFFFF'},
        # 'bgcolor': '#1F1F1F',
        # 'bordercolor': '#FFFFFF',
        # 'borderwidth': 1,
        # 'borderpad': 4,
        'xref': 'paper',
        'yref': 'paper',
        'x': 0.5,
        'y': 1.04,
        'xanchor': 'center',
        'yanchor': 'bottom',
        'showarrow': False
    },
    'update_xaxes': {
        'showline': True,
        'linewidth': 2,
        'linecolor': '#FFFFFF',
        'mirror': True
    },

    'update_yaxes': {
        'showline': True,
        'linewidth': 2,
        'linecolor': '#FFFFFF',
        'mirror': True,
        'ticks': 'outside'
    }
}

def generate_annotation(**kwargs):
    """
    Generate an annotation dictionary for plotly.
    """
    config = STYLING_CONFIG['default_annotation_styling']
    config.update(kwargs)
    return config