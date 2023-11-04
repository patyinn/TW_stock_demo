import numpy as np
import pandas as pd

from matplotlib import pyplot as plt

plt.rcParams["figure.figsize"] = (9, 5)


def plot_candles(start_time, end_time, pricing, title=None,
                 volume_bars=False,
                 color_function=None,
                 overlays=None,
                 technicals=None,
                 technicals_titles=None):
    """ Plots a candlestick chart using quantopian pricing data.
    
    Author: Daniel Treiman
    
    Args:
      pricing: A pandas dataframe with columns ['open_price', 'close_price', 'high', 'low', 'volume']
      title: An optional title for the chart
      volume_bars: If True, plots volume bars
      color_function: A function which, given a row index and price series, returns a candle color.
      overlays: A list of additional data series to overlay on top of pricing.  Must be the same length as pricing.
      technicals: A list of additional data series to display as subplots.
      technicals_titles: A list of titles to display for each technical indicator.
    """

    pricing = pricing[start_time:end_time]
    if overlays is not None:
        overlays = [o[start_time:end_time] for o in overlays]
    if technicals is not None:
        technicals = [t[start_time:end_time] for t in technicals]

    def default_color(index, open_price, close_price, low, high):
        return 'g' if open_price[index] > close_price[index] else 'r'

    # x = a or b. If bool(a) returns False, then x is assigned the value of b
    # Reference: https://stackoverflow.com/questions/21566106/what-does-an-x-y-or-z-assignment-do-in-python
    color_function = color_function or default_color
    overlays = overlays or []
    technicals = technicals or []
    technicals_titles = technicals_titles or []
    open_price = pricing['open']
    close_price = pricing['close']
    low = pricing['low']
    high = pricing['high']
    oc_min = pd.concat([open_price, close_price], axis=1).min(axis=1)
    oc_max = pd.concat([open_price, close_price], axis=1).max(axis=1)

    pos = 0

    subplot_count = 1
    # volune: 成交量
    if volume_bars:
        subplot_count = 2
    if technicals:
        subplot_count += len(technicals)

    total_plotspace = 7 + 4 * (subplot_count - 1)

    if subplot_count == 1:
        fig, ax1 = plt.subplots(1, 1)
    else:
        ratios = np.insert(np.full(subplot_count - 1, 1), 0, 3)
        # https://matplotlib.org/3.1.0/api/_as_gen/matplotlib.pyplot.subplots.html
        fig, subplots = plt.subplots(subplot_count, 1, sharex=True, gridspec_kw={'height_ratios': ratios})
        ax1 = subplots[0]
    ax1 = plt.subplot2grid((total_plotspace, 9), (pos, 0), rowspan=6, colspan=9)
    pos += 7
    plt.setp(ax1.get_xticklabels(), visible=False)

    if title:
        ax1.set_title(title, loc='right')
    x = np.arange(len(pricing))
    candle_colors = [color_function(i, open_price, close_price, low, high) for i in x]
    candles = ax1.bar(x, oc_max - oc_min, bottom=oc_min, color=candle_colors, linewidth=0)
    lines = ax1.vlines(x, low, high, color=candle_colors, linewidth=1)  # + 0.4
    ax1.xaxis.grid(False)
    ax1.xaxis.set_tick_params(which='major', length=3.0, direction='in', top=False)
    # Assume minute frequency if first two bars are in the same day.
    frequency = 'minute' if (pricing.index[1] - pricing.index[0]).days == 0 else 'day'
    time_format = '%Y-%m-%d'
    if frequency == 'minute':
        time_format = '%H:%M'

    # Set X axis tick labels.
    ticks = [date.strftime(time_format) for date in pricing.index]
    space = max(int(len(ticks) / 20), 1)
    # Reference: https://www.itread01.com/content/1510942965.html
    for i, t in enumerate(ticks):
        ticks[i] = t if i % space == 0 or i == len(ticks) - 1 else ''
    # plt.xticks(x, ticks, rotation='vertical')

    for overlay in overlays:
        ax1.plot(x, overlay)

    # Plot volume bars if needed
    if volume_bars:
        ax2 = subplots[1]
        ax2 = plt.subplot2grid((total_plotspace, 9), (pos, 0), rowspan=3, colspan=9, sharex=ax1)
        pos += 4
        plt.setp(ax2.get_xticklabels(), visible=False)
        volume = pricing['volume']
        volume_scale = None
        scaled_volume = volume
        if volume.max() > 1000000:
            volume_scale = 'M'
            scaled_volume = volume / 1000000
        elif volume.max() > 1000:
            volume_scale = 'K'
            scaled_volume = volume / 1000
        ax2.bar(x, scaled_volume, color=candle_colors)
        volume_title = 'Volume'
        if volume_scale:
            volume_title = 'Volume (%s)' % volume_scale
        ax2.set_title(volume_title, loc='right')
        ax2.xaxis.grid(False)

    print(total_plotspace)

    # Plot additional technical indicators
    for (i, technical) in enumerate(technicals):
        ax = subplots[i - len(technicals)]  # Technical indicator plots are shown last
        print(i)
        print(pos)
        ax = plt.subplot2grid((total_plotspace, 9), (pos, 0), rowspan=3, colspan=9, sharex=ax1)
        pos += 4
        plt.setp(ax.get_xticklabels(), visible=False) if i - len(technicals) != -1 else plt.setp(ax.get_xticklabels(),
                                                                                                 visible=True)
        ax.plot(x, technical)
        if i < len(technicals_titles):
            ax.set_title(technicals_titles[i], loc='right')

    plt.xticks(x, ticks, rotation='vertical')
    plt.show()
