import os
import pandas as pd
from datetime import datetime, timedelta

class DataManager:
    PUT_OI_CSV_PATH = "./database/high_oi/puts_oi.csv"
    CALL_OI_CSV_PATH = "./database/high_oi/calls_oi.csv"
    OPTION_CSV_PATH = './database/option_chains/option_chain.csv'
    CANDLE_CSV_PATH = './database/candle_history'
    ORDER_CSV_PATH = './database/option_chains/orders.csv'


    @staticmethod
    def store_data(data_type, df, file_name):
        csv_path = DataManager._get_csv_path(data_type, file_name)
        file_exists = os.path.isfile(csv_path)
        mode = 'a' if file_exists else 'w'
        header = not file_exists

        with open(csv_path, mode) as f:
            df.to_csv(f, header=header, index=False, float_format='%.2f', sep='\t')


    @staticmethod
    def create_dataframe(data_type, data):
        if data_type == 'candles':
            return DataManager._create_candle_dataframe(data)
        elif data_type == 'options':
            return DataManager._create_option_dataframe(data)
        elif data_type == 'high_oi':
            return DataManager._create_high_oi_dataframe(data)
        elif data_type == 'order':
            return DataManager._create_order_dataframe(data)
        else:
            raise ValueError(f"Unsupported data type: {data_type}")


    @staticmethod
    def _create_candle_dataframe(candles):
        data = []
        for ohlcv in candles['candles']:
            datetime_value = ohlcv.get('datetime')
            try:
                # Try to parse datetime as milliseconds since epoch
                datetime_parsed = pd.to_datetime(datetime_value, unit='ms')
            except (ValueError, TypeError):
                # If the above fails, treat it as a standard string format
                datetime_parsed = pd.to_datetime(datetime_value)
            # Adjust for timezone difference
            datetime_adjusted = datetime_parsed - timedelta(hours=7)

            candle_data = {
                'Datetime': datetime_adjusted,
                'Open': ohlcv.get('open'),
                'High': ohlcv.get('high'),
                'Low': ohlcv.get('low'),
                'Close': ohlcv.get('close'),
                'Volume': ohlcv.get('volume')
            }
            data.append(candle_data)
        return pd.DataFrame(data)


    @staticmethod
    def _create_option_dataframe(options):
        data = []
        exp_date_map = options.get('callExpDateMap') or options.get('putExpDateMap')
        for exp_date, strikes in exp_date_map.items():
            for strike, options_list in strikes.items():
                for option in options_list:
                    option_data = {
                        'Put/Call': option.get('putCall'),
                        'Symbol': option.get('symbol'),
                        'Description': option.get('description'),
                        'Bid': option.get('bid'),
                        'Ask': option.get('ask'),
                        'Volume': option.get('totalVolume'),
                        'Delta': option.get('delta'),
                        'OI': option.get('openInterest'),
                        'Expiration': exp_date,
                        'Strike': strike,
                        'ITM': option.get('inTheMoney')
                    }
                    data.append(option_data)
        return pd.DataFrame(data)


    @staticmethod
    def _create_high_oi_dataframe(data):
        now = datetime.now().strftime("%m/%d/%Y")
        df = pd.DataFrame({
            'Datetime': [now],
            'Level1': [str(data[0]).ljust(10)],
            'OpenInterest1': [str(data[1]).ljust(10)],
            'Level2': [str(data[2]).ljust(10)],
            'OpenInterest2': [str(data[3]).ljust(10)],
            'Level3': [str(data[4]).ljust(10)],
            'OpenInterest3': [str(data[5]).ljust(10)],            
            'Level4': [str(data[6]).ljust(10)],
            'OpenInterest4': [str(data[7]).ljust(10)],
            'Level5': [str(data[8]).ljust(10)],
            'OpenInterest5': [str(data[9]).ljust(10)]
        })
        return df


    @staticmethod
    def _create_order_dataframe(order):
        df = pd.DataFrame([order])
        df['Instruction'] = df['orderLegCollection'].apply(lambda x: x[0]['instruction'])
        df['Quantity'] = df['orderLegCollection'].apply(lambda x: x[0]['quantity'])
        df['Symbol'] = df['orderLegCollection'].apply(lambda x: x[0]['instrument']['symbol'])
        df['AssetType'] = df['orderLegCollection'].apply(lambda x: x[0]['instrument']['assetType'])
        df = df[['Instruction', 'Symbol', 'price', 'Quantity', 'AssetType']]
        return df


    @staticmethod
    def _get_csv_path(data_type, file_name):
        if data_type == 'candles':
            return os.path.join(DataManager.CANDLE_CSV_PATH, file_name)
        elif data_type == 'options':
            return DataManager.OPTION_CSV_PATH
        elif data_type in ['high_oi_calls', 'calls']:
            return DataManager.CALL_OI_CSV_PATH
        elif data_type in ['high_oi_puts', 'puts']:
            return DataManager.PUT_OI_CSV_PATH
        elif data_type == 'order':
            return DataManager.ORDER_CSV_PATH
        else:
            raise ValueError(f"Unsupported data type: {data_type}")


    @staticmethod
    # Needs work
    def load_data(data_type):
        csv_path = DataManager._get_csv_path(data_type)
        if os.path.exists(csv_path):
            return pd.read_csv(csv_path, sep='\t')
        return pd.DataFrame()