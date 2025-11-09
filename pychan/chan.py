import pandas as pd

from pychan.segment import Segment
from pychan.sign import Sign


class Chan:

    def __init__(self, symbol):
        self.symbol = symbol
        self.segment = Segment(symbol)
        self.stroke_sign = Sign(self.segment.stroke)
        self.segment_sign = Sign(self.segment)

    def get_sticks(self, interval):
        df = self.segment.source.data[interval]
        if df is None:
            return None

        stick_df = self.segment.stroke.fractal.stick.data[interval]
        if stick_df is not None and len(stick_df) > 0:
            stick_df = stick_df.copy().rename(columns={
                'high': 'top',
                'low': 'bottom'
            })
            df = df.join(stick_df[['top', 'bottom']], how='left')
        return df

    def get_fractals(self, interval):
        df = self.segment.stroke.fractal.data[interval]
        if df is None:
            return None

        df = df[df['high'].notna() | df['low'].notna()]
        df = df.copy().rename(columns={
            'high': 'top',
            'low': 'bottom'
        })
        return df[['top', 'bottom']]

    def get_strokes(self, interval):
        df = self.segment.stroke.data[interval]
        if df is None:
            return None

        df['stroke'] = df['high'].fillna(df['low'])
        return df[['stroke']]

    def get_stroke_pivots(self, interval):
        df = self.stroke_sign.trend.data[interval]
        if df is None:
            return None

        return self._get_pivots(df)

    def get_stroke_pivot_trends(self, interval):
        df = self.stroke_sign.trend.data[interval][1:-1]
        if df is None:
            return None

        return self._get_trends(df)

    def get_stroke_pivot_signals(self, interval):
        df = self.stroke_sign.data[interval]
        if df is None:
            return None

        return self._get_signals(df)

    def get_segments(self, interval):
        df = self.segment.data[interval]
        if df is None:
            return None

        df['segment'] = df['high'].fillna(df['low'])
        return df[['segment']]

    def get_segment_pivots(self, interval):
        df = self.segment_sign.trend.data[interval]
        if df is None:
            return None

        return self._get_pivots(df)

    def get_segment_pivot_trends(self, interval):
        df = self.segment_sign.trend.data[interval][1:-1]
        if df is None:
            return None

        return self._get_trends(df)

    def get_segment_pivot_signals(self, interval):
        df = self.segment_sign.data[interval]
        if df is None:
            return None

        return self._get_signals(df)

    @staticmethod
    def _get_pivots(df):
        df = pd.DataFrame({
            'start': df.index.values[::2],
            'end': df.index.values[1::2],
            'start_high': df['high'].values[::2],
            'end_high': df['high'].values[1::2],
            'start_low': df['low'].values[::2],
            'end_low': df['low'].values[1::2],
            'start_macd': df['macd'].values[::2],
            'end_macd': df['macd'].values[1::2],
            'trend': df['trend'].values[::2],
            'divergence': df['divergence'].values[::2]
        })
        df['high'] = df['start_high'].fillna(df['end_high'])
        df['low'] = df['start_low'].fillna(df['end_low'])
        return df[['start', 'end', 'high', 'low', 'start_macd', 'end_macd', 'trend', 'divergence']]

    @staticmethod
    def _get_trends(df):
        return pd.DataFrame({
            'start': df.index.values[::2],
            'end': df.index.values[1::2],
            'start_price': df['price'].values[::2],
            'end_price': df['price'].values[1::2]
        })

    @staticmethod
    def _get_signals(df):
        df['price'] = df['high'].fillna(df['low'])
        return df[['price', 'signal']]


if __name__ == '__main__':
    chan = Chan('AAPL')
    print(chan.get_sticks('1m'))
    print(chan.get_fractals('1m'))
    print(chan.get_strokes('1m'))
    print(chan.get_stroke_pivots('1m'))
    print(chan.get_stroke_pivot_trends('1m'))
    print(chan.get_stroke_pivot_signals('1m'))
    print(chan.get_segments('1m'))
    print(chan.get_segment_pivots('1m'))
    print(chan.get_segment_pivot_trends('1m'))
    print(chan.get_segment_pivot_signals('1m'))
