from crypt import methods
import json
import threading
import time
from datetime import datetime
from setting.dates  import dates
from PyQt5.QtCore import QThread, pyqtSignal
from cloud_services.api import Gmail
from database.data_manager import DataManager
from schwab.api import Schwab
from strategy import high_open_interest


class Client(QThread):
    log_signal = pyqtSignal(str)
    log_dict_signal = pyqtSignal(dict)
    position_update_signal = pyqtSignal(str, float, float, float, str)
    trade_update_signal = pyqtSignal(str, str, float, float, float, str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.call_strikes, self.put_strikes = high_open_interest.retrieveData()
        self.schwab = Schwab(log_signal=self.log_signal)
        self.schwab.request_input_signal.connect(parent.request_user_input)
        self.gmail = Gmail(log_signal=self.log_signal)
        self.database = DataManager()
        self.settings = {}
        self.schedule_auto_start = None
        self.max_position_size = None
        self.max_profit_percentage = None
        self.max_loss_percentage = None
        self.max_contract_price = None
        self.least_delta = None
        self.strategies = None
        self.today, self.tomorrow = dates()


    def run(self):
        """
        """
        self.log_signal.emit("Robot Connecting To API's...")
        self.schwab.update_tokens_automatic()
        self.log_signal.emit("All APIs Authenticated!")

        # Make a Database in sql and authenticate the account here
        self.database.create_dataframe('high_oi', self.call_strikes)
        self.database.create_dataframe('high_oi', self.put_strikes)

        self.log_signal.emit("Start Scrapping For Alerts...")

        threading.Thread(target=self.handleCallEvent, daemon=True).start()
        threading.Thread(target=self.handlePutEvent, daemon=True).start()  
              
        # Check for open positions
        # self.check_position(self.position_type())


    def handlePutEvent(self):
        """
        """
        while True:
            self.gmail.get_put_event().wait()
            
            open_position = self.gmail.get_current_position()
            self.log_signal.emit(f"Current position: {open_position}")

            if open_position != 'PUT':
                self.sell_position(self.position_type())
                put_contract = self.best_contract('PUT')

                if put_contract is not None:
                    self.buy_position(put_contract, 'PUT')
                    self.gmail.set_current_position('PUT')
                    self.log_signal.emit("Checking contracts market value...")
                    self.check_position('PUT')
                    
                self.gmail.reset_position()
            else:
                self.log_signal.emit("Ignoring PUT signal due to existing PUT position")
            self.gmail.get_put_event().clear()


    def handleCallEvent(self):
        """
        """
        while True:
            self.gmail.get_call_event().wait()
            
            open_position = self.gmail.get_current_position()
            self.log_signal.emit(f"Current position: {open_position}")

            if open_position != 'CALL':
                self.sell_position(self.position_type())
                call_contract = self.best_contract('CALL')

                if call_contract is not None:
                    self.buy_position(call_contract, 'CALL')
                    self.gmail.set_current_position('CALL')

                    self.log_signal.emit("Checking contracts market value...")
                    self.check_position('CALL')
                    
                self.gmail.reset_position()
            else:
                self.log_signal.emit("Ignoring CALL signal due to existing CALL position")
            self.gmail.get_call_event().clear()


    def best_contract(self, type):
        """
        """
        self.log_signal.emit(f"Searching for best {type} contract...")
        
        # Request the option chain
        options = self.schwab.get_chains('SPY', type, '7', 'TRUE', '', '', '', 'OTM', self.today, self.today).json()
        # Create a dataframe of the options
        strike_price_df = self.database.create_dataframe('options', options)

        if type == 'PUT':
            filtered_delta_result = strike_price_df.loc[strike_price_df['Delta'] <= -abs(self.get_least_delta())]
        elif type == 'CALL':
            filtered_delta_result = strike_price_df.loc[strike_price_df['Delta'] >= self.get_least_delta()]

        filtered_ask_result = filtered_delta_result.loc[filtered_delta_result['Ask'] <= self.get_max_contract_price()]

        if not filtered_ask_result.empty:
            contract = filtered_ask_result.iloc[0]
            buy_order = self.create_order(contract.get('Ask'), contract.get('Symbol'), 'BUY')
            return buy_order
        else:
            self.log_signal.emit("ERROR: No contracts met conditions, ignore signal")
            return None


    def buy_position(self, order, type):
        """
        """
        self.log_signal.emit(f"Buying {type} position...")
        
        # Request Hash value
        hash = self.schwab.account_numbers().json()[0].get('hashValue')
        
        # Lets create the buy order with the best contract
        symbol = order["orderLegCollection"][0]["instrument"]["symbol"]
        
        # Buy in price
        price = order["price"]

        try:
            # Post Buy Order
            self.schwab.post_orders(order, accountNumber=hash).json()
        except json.decoder.JSONDecodeError:
            # Update database
            self.database.create_dataframe('order', order)
            # Update trades table
            self.trade_update_signal.emit("BOUGHT", symbol, price, self.get_max_position_size(), 0.0, "Alert")
        

    def check_position(self, type):
        """
        """
        if type is None:
            return
        
        # Request Hash value
        hash = self.schwab.account_numbers().json()[0].get('hashValue')
            
        inPosition = True
        while inPosition:
            time.sleep(1)
            # Request open positions from account 
            open_position = self.schwab.account_number(hash, "positions").json()
            try:
                # Symbol of open position
                symbol = open_position["securitiesAccount"]["positions"][0]["instrument"]["symbol"]

                # Average price of initial buy
                price = open_position["securitiesAccount"]["positions"][0]["averagePrice"]
                
                # Current Market Value of contract
                market_value = open_position["securitiesAccount"]["positions"][0]["marketValue"] / 100

                # Contract profit and loss percentage
                price_change = market_value - price
                profit_percentage = (price_change / price) * 100
                
                # Update positions table
                self.position_update_signal.emit(symbol, round(market_value, 2), self.get_max_position_size(), round(profit_percentage, 2), "Alert")
                
                """
                TODO: Remove % logic and use the market value to determine
                """
                if profit_percentage >= self.get_max_profit_percentage() or profit_percentage <= self.get_max_loss_percentage():
                    self.sell_position(type)
                    inPosition = False
                    break   
            except KeyError as e:
                self.log_signal.emit(f"No open positions found!")
                inPosition = False
                break
    
    # TODO: Fix this logic becasue it sells when it doenst need
    def sell_position(self, type):
        """
        """
        if type is None:
            return
        
        self.log_signal.emit(f"Selling {type} position...")

        # Request hash value
        hash = self.schwab.account_numbers().json()[0].get('hashValue')
        
        # Request all open positions
        order = self.schwab.account_number(hash, "positions").json()

        try:
            # Market value of the first order
            market_value = order["securitiesAccount"]["positions"][0]["marketValue"] / 100
            
            # Average price of initial buy
            price = order["securitiesAccount"]["positions"][0]["averagePrice"]
            
            # Order Symbol
            symbol = order["securitiesAccount"]["positions"][0]["instrument"]["symbol"]
            
            # Contract profit and loss percentage
            price_change = market_value - price
            profit_percentage = (price_change / price) * 100
            
            # Create Sell Order
            sell_order = self.create_order(round(market_value, 2), symbol, 'SELL')
            
            # Post Sell Order
            self.schwab.post_orders(sell_order, accountNumber=hash).json()
        except json.decoder.JSONDecodeError:
            # Update database
            # self.database.create_dataframe('order', order)
            
            # TODO: figure out how to keep track of the quantity and what alert
            self.trade_update_signal.emit("SOLD", symbol, round(market_value, 2), self.get_max_position_size(), round(profit_percentage, 2), "Alert")
        except KeyError as e:
            self.log_signal.emit(f"No positions found..{e}")


    def create_order(self, price, symbol, type, var='OPEN'):
        """
        """
        # Read the settings.txt file and apply the settings of quantity and risk % of account
        # Add a way to check balance here and check if you have used a certain % of account already.
        # Total balance, Risk Target Balance, Current Balance
        if type != 'BUY':
            var='CLOSE'

        order = {
            'orderType': 'LIMIT',
            'session': 'NORMAL',
            'price': price,
            'duration': 'DAY',
            'orderStrategyType': 'SINGLE',
            'orderLegCollection': [{
                'instruction': f'{type}_TO_{var}',
                'quantity': self.get_max_position_size(),
                'instrument': {
                    'symbol': symbol,
                    'assetType': 'OPTION'
                }
            }]
        }
        return order
    

    def position_type(self):
        """
        """
        hash = self.schwab.account_numbers().json()[0].get('hashValue')
        order = self.schwab.account_number(hash, "positions").json()
        type = None
        try:
            # Symbol of open position
            symbol = order["securitiesAccount"]["positions"][0]["instrument"]["symbol"]

            type = symbol[12:13]

            if type == 'C':
                self.gmail.set_current_position('CALL')
               
            elif type == 'P':
                self.gmail.set_current_position('PUT')
            return self.gmail.get_current_position()  
            
        except KeyError as e:
            self.log_signal.emit(f"No open positions found!")
            return None


    def get_candle_history(self, ticker, periodType, period, frequencyType, frequency, startDate, endDate, fileName):
        """
        Retrieve and store historical candle data for a given ticker.

        Args:
            ticker (str): The ticker symbol to retrieve data for.
            periodType (str): The type of period for the historical data.
            period (int): The number of periods to retrieve.
            frequencyType (str): The type of frequency for the data points.
            frequency (int): The frequency of the data points.
            startDate (str): The start date for the historical data in 'YYYY-MM-DD' format.
            endDate (str): The end date for the historical data in 'YYYY-MM-DD' format.
            fileName (str): The name of the file to store the data.

        Returns:
            None
        """
        # response = self.schwab.price_history('SPY', 'day', 10, 'minute', 1, datetime.strptime('2024-01-05', "%Y-%m-%d"), datetime.strptime('2024-08-12', "%Y-%m-%d"), True, True)
        response = self.schwab.price_history(ticker, periodType, period, frequencyType, frequency, datetime.strptime(startDate, "%Y-%m-%d"), datetime.strptime(endDate, "%Y-%m-%d"), True, True)

        if response.ok:
            data = response.json()
            temp = data
            self.convert_epoch_to_datetime(temp["candles"])
            self.log_signal.emit(json.dumps(temp, indent=4))
            df = self.database.create_dataframe('candles', data)
            self.database.store_data('candles', df, fileName)
            self.log_signal.emit("Price History Request: Complete")


    def convert_epoch_to_datetime(self, candles):
        """
        """
        for candle in candles:
            epoch = candle["datetime"]
            dt = datetime.fromtimestamp(epoch / 1000.0)
            candle["datetime"] = dt.strftime('%Y-%m-%d %H:%M:%S')


    def set_settings(self, settings):
        """
        """
        self.settings = settings
        self.log_signal.emit(f"Settings updated: {settings}")

        if 'auto_start' in settings and settings['auto_start']:
            self.set_schedule_auto_start(settings['auto_start_time'])

        try:
            if 'max_position_size' in settings:
                max_position_size = int(settings['max_position_size'])
                if max_position_size > 0:
                    self.set_max_position_size(max_position_size)
                else:
                    raise ValueError("Max position size must be greater than 0")

            if 'max_profit_percentage' in settings:
                max_profit_percentage = float(settings['max_profit_percentage'])
                if 0 < max_profit_percentage:
                    self.set_max_profit_percentage(max_profit_percentage) 
                else:
                    raise ValueError("Max profit percentage must be greater than 0")

            if 'max_loss_percentage' in settings:
                max_loss_percentage = float(settings['max_loss_percentage'])
                if 0 <= max_loss_percentage <= 100:
                    self.set_max_loss_percentage(max_loss_percentage)
                else:
                    raise ValueError("Max loss percentage must be between 0 and 100")
                
            if 'max_contract_price' in settings:
                max_contract_price = float(settings['max_contract_price'])
                if 0 <= max_contract_price:
                    self.set_max_contract_price(max_contract_price)
                else:
                    raise ValueError("Max contract price must be greater than 0")

            if 'least_delta' in settings:
                least_delta = float(settings['least_delta'])
                if 0 <= least_delta <= 100:
                    self.set_least_delta(least_delta)
                else:
                    raise ValueError("delta must be between 0 and 100")

            if 'strategies' in settings:
                self.set_strategies(settings['strategies'])
                # Implement strategy selection logic here

        except ValueError as e:
            self.log_signal.emit(f"Error in settings: {str(e)}")
        except Exception as e:
            self.log_signal.emit(f"Unexpected error in settings: {str(e)}")


    def set_schedule_auto_start(self, time):
        """
        """
        # Implementation for scheduling auto start
        self.schedule_auto_start = time
        self.log_signal.emit(f"Auto start scheduled for {time}")


    def set_max_position_size(self, size):
        """
        """
        # Implementation for setting max position size
        self.max_position_size = size
        self.log_signal.emit(f"Max position size set to {size}")


    def set_max_profit_percentage(self, profit_percentage):
        """
        """
        # Implementation for setting max profit percentage
        self.max_profit_percentage = profit_percentage
        self.log_signal.emit(f"Max profit percentage set to {profit_percentage}%")


    def set_max_loss_percentage(self, loss_percentage):
        """
        """
        # Implementation for setting max loss percentage
        self.max_loss_percentage = -abs(100 - loss_percentage)
        self.log_signal.emit(f"Max loss percentage set to -{loss_percentage}%")


    def set_max_contract_price(self, contract_price):
        # Implementation for setting max loss percentage
        self.max_contract_price = contract_price
        self.log_signal.emit(f"Max contract price set to {contract_price}")


    def set_least_delta(self, delta):
        # Implementation for setting max loss percentage
        self.least_delta = delta
        self.log_signal.emit(f"Least delta is set to {delta}")


    def set_strategies(self, strategies):
        # Implementation for setting strategies
        self.strategies = strategies
        self.log_signal.emit(f"Strategies set to {strategies}")


    def get_schedule_auto_start(self):
        return self.schedule_auto_start
    

    def get_max_position_size(self):
        return self.max_position_size
    

    def get_max_profit_percentage(self):
        return self.max_profit_percentage
    

    def get_max_loss_percentage(self):
        return self.max_loss_percentage
    

    def get_max_contract_price(self):
        return self.max_contract_price
    

    def get_least_delta(self):
        return self.least_delta
    

    def get_strategies(self):
        return self.strategies
    