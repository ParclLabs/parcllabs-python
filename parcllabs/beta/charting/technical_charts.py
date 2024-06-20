import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from parcllabs.beta.charting.utils import create_labs_logo_dict


# config logo
labs_logo_dict = create_labs_logo_dict(
    src='labs',
    y=1.07,
    x=1,
    xanchor='right',
    yanchor='top',
    sizex=0.15,
    sizey=0.15,
)


# technical chart definition
def build_technical_chart(
    data_main: pd.DataFrame,
    data_secondary: pd.DataFrame,
    save_path: str = None,
    value_name_main: str = None,
    value_name_secondary: str = None,
    ticker_msg: str = None,
    volume_msg: str = None,
    pricefeed_msg: str = None,
    last_pf_date: str = None,
    msg: str = None,
    height=700,
    width=1200,
    moving_average_window: int = 6
):

    data_main['date'] = pd.to_datetime(data_main['date'])
    data_secondary['date'] = pd.to_datetime(data_secondary['date'])

    # Get the date range for the x-axis
    date_range = [min(data_main['date'].min(), data_secondary['date'].min()), max(data_main['date'].max(), data_secondary['date'].max())]

    # Create subplots: 2 rows, 1 column
    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=False,
        row_heights=[0.8, 0.2],
        vertical_spacing=0.05  # Increased vertical spacing
    )

    # Add traces for positive and negative segments based on daily_return
    for i in range(1, len(data_main)):
        color = 'green' if data_main['daily_return'].iloc[i] >= 0 else 'red'
        fig.add_trace(
            go.Scatter(
                x=[data_main['date'].iloc[i-1], data_main['date'].iloc[i]],
                y=[data_main[value_name_main].iloc[i-1], data_main[value_name_main].iloc[i]],
                mode='lines',
                line=dict(width=3, color=color),
                showlegend=False,
                hoverinfo='none'
            ),
            row=1, col=1
        )

    # Plot the secondary data as a bar chart
    fig.add_trace(
        go.Bar(
            x=data_secondary['date'],
            y=data_secondary[value_name_secondary],
            name='Sales Volume',
            marker=dict(color=data_secondary['volColor'])  # Use volColor for the bar chart colors
        ),
        row=2, col=1
    )
    # Add the 6-month moving average line
    data_secondary['ma'] = data_secondary[value_name_secondary].rolling(window=moving_average_window).mean().round(2)
    ma_last = data_secondary['ma'].iloc[-1]
    fig.add_trace(
        go.Scatter(
            x=data_secondary['date'],
            y=data_secondary['ma'],
            mode='lines',
            name=f'{moving_average_window}-Month Moving Average',
            line=dict(width=2, color='#FFA500'),  # Orange for the moving average line
            showlegend=False
        ),
        row=2, col=1
    )

    # Update layout
    fig.update_layout(
        height=height,
        width=width,
        plot_bgcolor='#000000',  # Dark background for better contrast
        paper_bgcolor='#000000',  # Dark background for the paper
        font=dict(color='#FFFFFF'),
        xaxis=dict(
            title_text='',
            showgrid=True,  # Disable vertical grid lines
            gridcolor='rgba(255, 255, 255, 0.2)',  # Vertical grid line color with opacity
            tickangle=0,
            tickfont=dict(size=14),
            linecolor='rgba(255, 255, 255, 0.7)',  # Axis line color with opacity
            linewidth=1,  # Axis line width
            tickformat='`%y',
            range=date_range,
        ),
        xaxis2=dict(
            title_text='',
            showgrid=True,  # Disable vertical grid lines
            gridcolor='rgba(255, 255, 255, 0.2)',  # Horizontal grid line color with opacity
            tickangle=0,
            tickfont=dict(size=14),
            linecolor='rgba(255, 255, 255, 0.7)',  # Axis line color with opacity
            linewidth=1,  # Axis line width
            tickformat='`%y',
            range=date_range,
        ),
        yaxis=dict(
            showgrid=True,
            gridwidth=0.5,  # Horizontal grid line width
            gridcolor='rgba(255, 255, 255, 0.2)',  # Horizontal grid line color with opacity
            tickfont=dict(size=14),
            ticksuffix='',  # Remove dollar sign suffix
            zeroline=False,
            linecolor='rgba(255, 255, 255, 0.7)',  # Axis line color with opacity
            linewidth=1,  # Axis line width
            side='right',
            ticks='outside',
            rangemode='normal',
            range=[data_main[value_name_main].min(), data_main[value_name_main].max() + 10]
        ),
        yaxis2=dict(
            showgrid=True,
            gridwidth=0.5,  # Horizontal grid line width
            gridcolor='rgba(255, 255, 255, 0.2)',  # Horizontal grid line color with opacity
            tickfont=dict(size=14),
            ticksuffix='',  # Remove dollar sign suffix
            zeroline=False,
            linecolor='rgba(255, 255, 255, 0.7)',  # Axis line color with opacity
            linewidth=1,  # Axis line width
            side='right',
            ticks='outside',
            rangemode='normal',
            range=[data_secondary[value_name_secondary].min(), data_secondary[value_name_secondary].max() + 10]
        ),
        hovermode='x unified',  # Unified hover mode for better interactivity
        hoverlabel=dict(
            bgcolor='#1F1F1F',
            font_size=14,
            font_family="Rockwell"
        ),
        showlegend=False,  # Show the legend to include the moving average line
    )

    # Add stats annotation
    fig.add_annotation(
        dict(
            text=msg,
            x=1,
            y=1.04,
            xref='paper',
            yref='paper',
            xanchor='right',
            yanchor='top',
            showarrow=False,
            font=dict(size=14, color='#FFFFFF')
        )
    )

    # Add placeholder text in the top left of each chart
    fig.add_annotation(
        dict(
            text=pricefeed_msg,
            x=0.003,
            y=0.99,
            xref='paper',
            yref='paper',
            xanchor='left',
            showarrow=False,
            font=dict(size=14, color='#FFFFFF')
        )
    )

    fig.add_annotation(
        dict(
            text=volume_msg,
            x=0.003,
            y=0.15,
            xref='paper',
            yref='paper',
            xanchor='left',
            showarrow=False,
            font=dict(size=14, color='#FFFFFF')
        )
    )

    fig.add_annotation(
        dict(
            text=f'--- MA({moving_average_window}) {ma_last:,.2f}',
            x=0.003,
            y=0.12,
            xref='paper',
            yref='paper',
            xanchor='left',
            showarrow=False,
            font=dict(size=14, color='#FFFFFF')
        )
    )

    fig.add_annotation(
        dict(
            text=last_pf_date,
            x=0,
            y=1.04,
            xref='paper',
            yref='paper',
            xanchor='left',
            yanchor='top',
            showarrow=False,
            font=dict(size=14, color='#FFFFFF')
        )
    )

    # Add ticker message
    fig.add_annotation(
        dict(
            text=ticker_msg,
            x=0,
            y=1.07,
            xref='paper',
            yref='paper',
            xanchor='left',
            yanchor='top',
            showarrow=False,
            font=dict(size=14, color='#FFFFFF')
        )
    )

    # Add borders and hover effect for both charts
    fig.update_xaxes(
        showline=True,
        linewidth=2,
        linecolor='#FFFFFF',
        mirror=True
    )
    fig.update_yaxes(
        showline=True,
        linewidth=2,
        linecolor='#FFFFFF',
        mirror=True,
        ticks="outside"
    )

    fig.update_layout(
    margin=dict(
        l=10,  # Left margin
        r=10,  # Right margin
        b=10,  # Bottom margin
        t=50   # Top margin
    )
)

    # Add Parcl Labs logo
    fig.add_layout_image(
        labs_logo_dict
    )

    if save_path:
        fig.write_image(save_path, width=width, height=height)
    
    # Show the plot
    fig.show()
