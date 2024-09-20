import os
import json
import time
import base64
import requests
import threading
import webbrowser
from .stream import Stream
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from PyQt5.QtCore import QObject, pyqtSignal, QThread


class TokenRefreshThread(QThread):
    def __init__(self, schwab):
        super().__init__()
        self.schwab = schwab

    def run(self):
        authUrl = f'https://api.schwabapi.com/v1/oauth/authorize?response_type=code&client_id={self.schwab.APP_KEY}&redirect_uri={self.schwab.CALLBACK_URL}'
        self.schwab.log_signal.emit("Opening browser for authorization...")
        webbrowser.open(authUrl)

        self.schwab.log_signal.emit("Waiting for redirect URL...")
        self.schwab.request_input_signal.emit("Please enter the redirect URL:")

        # Wait for callback_url to be set
        while self.schwab.callback_url is None:
            time.sleep(0.1)
        
        callbackURL = self.schwab.callback_url
        self.schwab.callback_url = None  # Reset for next use

        code = f"{callbackURL.split('code=')[1].split('%40')[0]}@"
        tokenDictionary = self.schwab._post_access_token('authorization_code', code)
        self.schwab._token_manager("set", datetime.now(), datetime.now(), tokenDictionary)
        self.schwab.log_signal.emit("Refresh and Access tokens updated")


class Schwab(QObject):
    request_input_signal = pyqtSignal(str)
    def __init__(self, log_signal =None):
        super().__init__()
        self._load_env()
        self.refreshToken = None
        self.accessToken = None
        self.idToken = None
        self.refreshTokenDateTime = None
        self.accessTokenDateTime = None
        self.refreshTokenTimeout = 7 # in days
        self.accessTokenTimeout = 1800 # in seconds
        self.log_signal = log_signal
        self.callback_url = None
        self.stream = Stream(self)
        self.token_refresh_thread = None
        self.timeout = 5

        self._check_keys()
        self._token_manager("init")
        self._check_tokens()


    def _load_env(self):
        load_dotenv(dotenv_path=Path('./schwab/app_info/.env'))
        self.APP_KEY = os.getenv("appKey")
        self.SECRET_KEY = os.getenv("secretKey")
        self.ACCOUNT_NUMBER = os.getenv("accountNumber")
        self.CALLBACK_URL = "https://127.0.0.1"
        self.ACCOUNT_ENDPOINT = "https://api.schwabapi.com/trader/v1"
        self.MARKET_ENDPOINT = "https://api.schwabapi.com/marketdata/v1"
        self.POST = "https://api.schwabapi.com/v1/oauth/token"


    def _check_keys(self):
        self.log_signal.emit("Checking keys...")

        if len(self.APP_KEY) != 32 or len(self.SECRET_KEY) != 16:
            self.log_signal.emit("Incorrect keys or no keys found, add keys in modules/.env")
            quit()
        else:
            self.log_signal.emit("Keys are valid")


    def _check_tokens(self):
        if (datetime.now() - self.refreshTokenDateTime).days >= (self.refreshTokenTimeout - 1):
            self.log_signal.emit("The refresh token has expired, updating automatically")
            self._refresh_token()
        elif ((datetime.now() - self.accessTokenDateTime).days >= 1) or \
             ((datetime.now() - self.accessTokenDateTime).seconds > (self.accessTokenTimeout - 60)):
            self.log_signal.emit("The access token has expired, updating automatically")
            self._access_token()
        else:
            self.log_signal.emit("Access and Refresh tokens are still valid")


    def _post_access_token(self, grant_type, code):
        headers = {
            'Authorization': f'Basic {base64.b64encode(bytes(f"{self.APP_KEY}:{self.SECRET_KEY}", "utf-8")).decode("utf-8")}',
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        if grant_type == 'authorization_code':
            data = {'grant_type': 'authorization_code', 'code': code, 'redirect_uri': self.CALLBACK_URL}
        elif grant_type == 'refresh_token':
            data = {'grant_type': 'refresh_token', 'refresh_token': code}
        else:
            self.log_signal.emit("Invalid grant type")
            return None
        return requests.post(self.POST, headers=headers, data=data).json()


    def _access_token(self):
        accessTokenFileTime, refreshTokenFileTime, tokenDictionary = self._token_manager("getFile")
        self._token_manager("set", datetime.now(), refreshTokenFileTime, self._post_access_token('refresh_token', tokenDictionary.get('refresh_token')))
        self.log_signal.emit(f"Access token updated: {accessTokenFileTime}")


    def _refresh_token(self):
        if self.token_refresh_thread is None or not self.token_refresh_thread.isRunning():
            self.token_refresh_thread = TokenRefreshThread(self)
            #self.token_refresh_thread.request_input_signal.connect(self.request_input_signal)
            self.token_refresh_thread.start()


    def set_callback_url(self, url):
        self.callback_url = url


    def _token_manager(self, todo=None, accessTokenTime=None, refreshTokenTime=None, tokenDict=None):
        envPath = "schwab/app_info/.env"
        accessTokenTimeFormat = "%Y-%m-%d %H:%M:%S"
        refreshTokenTimeFormat = "%Y-%m-%d %H:%M:%S"

        def write_token_var(att, rtt, td):
            self.log_signal.emit("Setting variales from Schwabs .env file")
            self.refreshToken = td.get("refresh_token")
            self.accessToken = td.get("access_token")
            self.accessTokenDateTime = att
            self.refreshTokenDateTime = rtt
            self.idToken = td.get("id_token")

        def write_token_file(newAccessTokenTime, newRefreshTokenTime, newTokenDict):
            self.log_signal.emit("Writing new values to Schwabs .env file")
            with open(envPath, "r") as file:
                lines = file.readlines()
            for i, line in enumerate(lines):
                if line.startswith("accessTokenDateTime"):
                    lines[i] = f"accessTokenDateTime = {newAccessTokenTime.strftime(accessTokenTimeFormat)}\n"
                elif line.startswith("refreshTokenDateTime"):
                    lines[i] = f"refreshTokenDateTime = {newRefreshTokenTime.strftime(refreshTokenTimeFormat)}\n"
                elif line.startswith("jsonDict"):
                    lines[i] = f"jsonDict = {json.dumps(newTokenDict)}\n"
                elif line.startswith("accessToken"):
                    lines[i] = f"accessToken = {newTokenDict.get('access_token')}\n"
                elif line.startswith("refreshToken"):
                    lines[i] = f"refreshToken = {newTokenDict.get('refresh_token')}\n"
                elif line.startswith("idToken"):
                    lines[i] = f"idToken = {newTokenDict.get('id_token')}\n"
                    break
            with open(envPath, "w") as file:
                file.writelines(lines)
                file.flush()

        def read_token_file():
            self.log_signal.emit("Reading Schwabs .env file")
            with open(envPath, "r") as file:
                formattedAccessTokenTime = datetime.strptime(file.readline().split('=')[1].strip(), accessTokenTimeFormat)
                formattedRefreshTokenTime = datetime.strptime(file.readline().split('=')[1].strip(), refreshTokenTimeFormat)
                formattedTokenDict = json.loads(file.readline().split('=')[1].strip())
                return formattedAccessTokenTime, formattedRefreshTokenTime, formattedTokenDict

        try:
            if todo == "getFile":
                return read_token_file()
            elif todo == "set":
                if accessTokenTime is not None and refreshTokenTime is not None and tokenDict is not None:
                    write_token_file(accessTokenTime, refreshTokenTime, tokenDict)
                    write_token_var(accessTokenTime, refreshTokenTime, tokenDict)
                else:
                    self.log_signal.emit("Error in setting token file, null values given")
            elif todo == "init":
                self.log_signal.emit("Initiate variables from Schwabs .env file")
                accessTokenTime, refreshTokenTime, tokenDict = read_token_file()
                write_token_var(accessTokenTime, refreshTokenTime, tokenDict)
        except Exception as e:
            self.log_signal.emit("ERROR: Writing new tokens to .env file")
            self._refresh_token()


    def _request_handler(self, response):
        try:
            if response.ok:
                return response.json()
        except json.decoder.JSONDecodeError:
            self.log_signal.emit(f"{response.status_code}: Order placed")
            return None
        except AttributeError:
            self.log_signal.emit(f"Error {response.status_code}: Object has no such attribute")
            return None


    def _params_parser(self, params):
        return {k: v for k, v in params.items() if v is not None}


    def _time_converter(self, dt, format=None):
        if format == "epoch" and dt is not None:
            return int(dt.timestamp() * 1000)
        elif format == "iso" and dt is not None:
            return dt + 'T00:00:00.000Z'
        elif dt is None:
            return None
        

    def update_tokens_automatic(self):
        def checker():
            while True:
                self._check_tokens()
                time.sleep(60)
        threading.Thread(target=checker, daemon=True).start()


    # Account methods
    def account_numbers(self):
        """
        Account numbers in plain text cannot be used outside of headers or request/response bodies. As the first step consumers must invoke this service to retrieve the list of plain text/encrypted value pairs, and use encrypted account values for all subsequent calls for any accountNumber request.
        :return: All linked account numbers and hashes
        :rtype: request.Response
        """
        return requests.get(f'{self.ACCOUNT_ENDPOINT}/accounts/accountNumbers', 
                                                  headers={'Authorization': f'Bearer {self.accessToken}'})

    def accounts(self, fields=None):
        """
        All the linked account information for the user logged in. The balances on these accounts are displayed by default however the positions on these accounts will be displayed based on the "positions" flag.
        :param fields: fields to return (options: "positions")
        :type fields: str
        :return: details for all linked accounts
        :rtype: request.Response
        """
        return requests.get(f'{self.ACCOUNT_ENDPOINT}/accounts/', 
                                                  headers={'Authorization': f'Bearer {self.accessToken}'}, 
                                                  params=self._params_parser({'fields': fields}))

    def account_number(self, accountNumber=None, fields=None):
        """
        Specific account information with balances and positions. The balance information on these accounts is displayed by default but Positions will be returned based on the "positions" flag.
        :param accountNumber: account hash from account_numbers()
        :type accountNumber: str
        :param fields: fields to return
        :type fields: str
        :return: details for one linked account
        :rtype: request.Response
        """
        return requests.get(f'{self.ACCOUNT_ENDPOINT}/accounts/{accountNumber}', 
                                                  headers={'accept': 'application/json', 'Authorization': f'Bearer {self.accessToken}'}, 
                                                  params=self._params_parser({'fields': fields}))

    # Order methods
    def get_orders(self, maxResults, fromEnteredTime, toEnteredTime, accountNumber=None, status=None):
        """
        All orders for a specific account. Orders retrieved can be filtered based on input parameters below. Maximum date range is 1 year.
        :param accountHash: account hash from account_linked()
        :type accountHash: str
        :param fromEnteredTime: from entered time
        :type fromEnteredTime: datetime | str
        :param toEnteredTime: to entered time
        :type toEnteredTime: datetime | str
        :param maxResults: maximum number of results
        :type maxResults: int
        :param status: status ("AWAITING_PARENT_ORDER"|"AWAITING_CONDITION"|"AWAITING_STOP_CONDITION"|"AWAITING_MANUAL_REVIEW"|"ACCEPTED"|"AWAITING_UR_OUT"|"PENDING_ACTIVATION"|"QUEUED"|"WORKING"|"REJECTED"|"PENDING_CANCEL"|"CANCELED"|"PENDING_REPLACE"|"REPLACED"|"FILLED"|"EXPIRED"|"NEW"|"AWAITING_RELEASE_TIME"|"PENDING_ACKNOWLEDGEMENT"|"PENDING_RECALL"|"UNKNOWN")
        :type status: str
        :return: orders for one linked account hash
        :rtype: request.Response
        """
        return requests.get(f'{self.ACCOUNT_ENDPOINT}/accounts/{accountNumber}/orders', 
                                                  headers={"Accept": "application/json", 'Authorization': f'Bearer {self.accessToken}'}, 
                                                  params=self._params_parser({
                                                      'maxResults': maxResults, 
                                                      'fromEnteredTime': self._time_converter(fromEnteredTime, format="iso"), 
                                                      'toEnteredTime': self._time_converter(toEnteredTime, format="iso"), 
                                                      'status': status
                                                  }))

    def post_orders(self, order, accountNumber=None):
        """
        Place an order for a specific account.
        :param accountHash: account hash from account_linked()
        :type accountHash: str
        :param order: order dictionary, examples in Schwab docs
        :type order: dict
        :return: order number in response header (if immediately filled then order number not returned)
        :rtype: request.Response
        """
        return requests.post(f'{self.ACCOUNT_ENDPOINT}/accounts/{accountNumber}/orders', 
                                                   headers={'Authorization': f'Bearer {self.accessToken}', 'Content-Type': 'application/json'}, 
                                                   json=order, timeout = self.timeout)


    def delete_order_id(self, orderId, accountNumber=None):
        """
        Cancel a specific order by its ID, for a specific account
        :param accountHash: account hash from account_linked()
        :type accountHash: str
        :param orderId: order id
        :type orderId: str
        :return: response code
        :rtype: request.Response
        """
        return requests.get(f'{self.ACCOUNT_ENDPOINT}/accounts/{accountNumber}/orders/{orderId}', 
                                                  headers={'Authorization': f'Bearer {self.accessToken}'}, 
                                                  params=self._params_parser({'orderId': orderId}))

    def get_order_id(self, orderId, accountNumber=None):
        """
        Get a specific order by its ID, for a specific account
        :param accountHash: account hash from account_linked()
        :type accountHash: str
        :param orderId: order id
        :type orderId: str
        :return: order details
        :rtype: request.Response
        """
        return requests.get(f'{self.ACCOUNT_ENDPOINT}/accounts/{accountNumber}/orders/{orderId}', 
                                                  headers={"Accept": "application/json", 'Authorization': f'Bearer {self.accessToken}'})

    # Option methods
    def get_chains(self, symbol, contractType=None, strikeCount=None, includeUnderlyingQuotes=None, 
                   strategy=None, interval=None, strike=None, range=None, fromDate=None, toDate=None, 
                   volatility=None, underlyingPrice=None, interestRate=None, daysToExpiration=None, 
                   expMonth=None, optionType=None, entitlement=None):
        """
        Get Option Chain including information on options contracts associated with each expiration for a ticker.
        :param symbol: ticker symbol
        :type symbol: str
        :param contractType: contract type ("CALL"|"PUT"|"ALL")
        :type contractType: str
        :param strikeCount: strike count
        :type strikeCount: int
        :param includeUnderlyingQuote: include underlying quote (True|False)
        :type includeUnderlyingQuote: boolean
        :param strategy: strategy ("SINGLE"|"ANALYTICAL"|"COVERED"|"VERTICAL"|"CALENDAR"|"STRANGLE"|"STRADDLE"|"BUTTERFLY"|"CONDOR"|"DIAGONAL"|"COLLAR"|"ROLL)
        :type strategy: str
        :param interval: Strike interval
        :type interval: str
        :param strike: Strike price
        :type strike: float
        :param range: range ("ITM"|"NTM"|"OTM"...)
        :type range: str
        :param fromDate: from date
        :type fromDate: datetime | str
        :param toDate: to date
        :type toDate: datetime | str
        :param volatility: volatility
        :type volatility: float
        :param underlyingPrice: underlying price
        :type underlyingPrice: float
        :param interestRate: interest rate
        :type interestRate: float
        :param daysToExpiration: days to expiration
        :type daysToExpiration: int
        :param expMonth: expiration month ("JAN"|"FEB"|"MAR"|"APR"|"MAY"|"JUN"|"JUL"|"AUG"|"SEP"|"OCT"|"NOV"|"DEC"|"ALL")
        :type expMonth: str
        :param optionType: option type ("CALL"|"PUT")
        :type optionType: str
        :param entitlement: entitlement ("PN"|"NP"|"PP")
        :type entitlement: str
        :return: list of option chains
        :rtype: request.Response
        """
        return requests.get(f'{self.MARKET_ENDPOINT}/chains', 
                                                  headers={"Accept": "application/json", 'Authorization': f'Bearer {self.accessToken}'}, 
                                                  params=self._params_parser({
                                                      'symbol': symbol, 'contractType': contractType, 'strikeCount': strikeCount, 
                                                      'includeUnderlyingQuotes': includeUnderlyingQuotes, 'strategy': strategy, 
                                                      'interval': interval, 'strike': strike, 'range': range, 'fromDate': fromDate, 
                                                      'toDate': toDate, 'volatility': volatility, 'underlyingPrice': underlyingPrice, 
                                                      'interestRate': interestRate, 'daysToExpiration': daysToExpiration, 
                                                      'expMonth': expMonth, 'optionType': optionType, 'entitlement': entitlement
                                                  }))

    def get_expiration_chain(self, symbol):
        """
        Get an option expiration chain for a ticker
        :param symbol: ticker symbol
        :type symbol: str
        :return: option expiration chain
        :rtype: request.Response
        """
        return requests.get(f'{self.MARKET_ENDPOINT}/expirationchain', 
                                                  headers={'Authorization': f'Bearer {self.accessToken}'}, 
                                                  params=self._params_parser({'symbol': symbol}))
    

    def order_replace(self, accountHash, orderId, order):
        """
        Replace an existing order for an account. The existing order will be replaced by the new order. Once replaced, the old order will be canceled and a new order will be created.
        :param accountHash: account hash from account_linked()
        :type accountHash: str
        :param orderId: order id
        :type orderId: str
        :param order: order dictionary, examples in Schwab docs
        :type order: dict
        :return: response code
        :rtype: request.Response
        """
        return requests.put(f'{self.ACCOUNT_ENDPOINT}/accounts/{accountHash}/orders/{orderId}',
                            headers={"Accept": "application/json", 'Authorization': f'Bearer {self.accessToken}',
                                     "Content-Type": "application/json"},
                            json=order)

    def account_orders_all(self, fromEnteredTime, toEnteredTime, maxResults=None, status=None):
        """
        Get all orders for all accounts
        :param fromEnteredTime: start date
        :type fromEnteredTime: datetime | str
        :param toEnteredTime: end date
        :type toEnteredTime: datetime | str
        :param maxResults: maximum number of results (set to None for default 3000)
        :type maxResults: int
        :param status: status ("AWAITING_PARENT_ORDER"|"AWAITING_CONDITION"|"AWAITING_STOP_CONDITION"|"AWAITING_MANUAL_REVIEW"|"ACCEPTED"|"AWAITING_UR_OUT"|"PENDING_ACTIVATION"|"QUEUED"|"WORKING"|"REJECTED"|"PENDING_CANCEL"|"CANCELED"|"PENDING_REPLACE"|"REPLACED"|"FILLED"|"EXPIRED"|"NEW"|"AWAITING_RELEASE_TIME"|"PENDING_ACKNOWLEDGEMENT"|"PENDING_RECALL"|"UNKNOWN")
        :type status: str
        :return: all orders
        :rtype: request.Response
        """
        return requests.get(f'{self.ACCOUNT_ENDPOINT}/orders',
                            headers={"Accept": "application/json", 'Authorization': f'Bearer {self.accessToken}'},
                            params=self._params_parser(
                                {'maxResults': maxResults, 'fromEnteredTime': self._time_converter(fromEnteredTime, format="iso"),
                                 'toEnteredTime': self._time_converter(toEnteredTime, format="iso"), 'status': status}))

    """
    def order_preview(self, accountHash, orderObject):
        #COMING SOON (waiting on Schwab)
        return requests.post(f'{self.ACCOUNT_ENDPOINT}/accounts/{accountHash}/previewOrder',
                             headers={'Authorization': f'Bearer {self.access_token}',
                                      "Content-Type": "application.json"}, data=orderObject)
    """

    def transactions(self, accountHash, startDate, endDate, types, symbol=None):
        """
        All transactions for a specific account. Maximum number of transactions in response is 3000. Maximum date range is 1 year.
        :param accountHash: account hash number
        :type accountHash: str
        :param startDate: start date
        :type startDate: datetime | str
        :param endDate: end date
        :type endDate: datetime | str
        :param types: transaction type ("TRADE, RECEIVE_AND_DELIVER, DIVIDEND_OR_INTEREST, ACH_RECEIPT, ACH_DISBURSEMENT, CASH_RECEIPT, CASH_DISBURSEMENT, ELECTRONIC_FUND, WIRE_OUT, WIRE_IN, JOURNAL, MEMORANDUM, MARGIN_CALL, MONEY_MARKET, SMA_ADJUSTMENT")
        :type types: str
        :param symbol: symbol
        :return: list of transactions for a specific account
        :rtype: request.Response
        """
        return requests.get(f'{self.ACCOUNT_ENDPOINT}/accounts/{accountHash}/transactions',
                            headers={'Authorization': f'Bearer {self.accessToken}'},
                            params=self._params_parser(
                                {'accountNumber': accountHash, 'startDate': self._time_converter(startDate, format="iso"),
                                 'endDate': self._time_converter(endDate, format="iso"), 'symbol': symbol, 'types': types}))

    def transaction_details(self, accountHash, transactionId):
        """
        Get specific transaction information for a specific account
        :param accountHash: account hash number
        :type accountHash: str
        :param transactionId: transaction id
        :type transactionId: int
        :return: transaction details of transaction id using accountHash
        :rtype: request.Response
        """
        return requests.get(f'{self.ACCOUNT_ENDPOINT}/accounts/{accountHash}/transactions/{transactionId}',
                            headers={'Authorization': f'Bearer {self.accessToken}'},
                            params={'accountNumber': accountHash, 'transactionId': transactionId})

    def preferences(self):
        """
        Get user preference information for the logged in user.
        :return: User Preferences and Streaming Info
        :rtype: request.Response
        """
        return requests.get(f'{self.ACCOUNT_ENDPOINT}/userPreference',
                            headers={'Authorization': f'Bearer {self.accessToken}'})

    # """
    # Market Data
    # """
    
    # def quotes(self, symbols=None, fields=None, indicative=False):
    #     """
    #     Get quotes for a list of tickers
    #     :param symbols: list of symbols strings (e.g. "AMD,INTC" or ["AMD", "INTC"])
    #     :type symbols: [str] | str
    #     :param fields: list of fields to get ("all", "quote", "fundamental")
    #     :type fields: list
    #     :param indicative: whether to get indicative quotes (True/False)
    #     :type indicative: boolean
    #     :return: list of quotes
    #     :rtype: request.Response
    #     """
    #     return requests.get(f'{self.MARKET_ENDPOINT}/quotes',
    #                         headers={'Authorization': f'Bearer {self.accessToken}'},
    #                         params=self._params_parser(
    #                             {'symbols': self._format_list(symbols), 'fields': fields, 'indicative': indicative}))

    # def quote(self, symbol_id, fields=None):
    #     """
    #     Get quote for a single symbol
    #     :param symbol_id: ticker symbol
    #     :type symbol_id: str (e.g. "AAPL", "/ES", "USD/EUR")
    #     :param fields: list of fields to get ("all", "quote", "fundamental")
    #     :type fields: list
    #     :return: quote for a single symbol
    #     :rtype: request.Response
    #     """
    #     return requests.get(f'{self.MARKET_ENDPOINT}/{urllib.parse.quote(symbol_id)}/quotes',
    #                         headers={'Authorization': f'Bearer {self.accessToken}'},
    #                         params=self._params_parser({'fields': fields}))


    # get price history for a ticker
    def price_history(self, symbol, periodType=None, period=None, frequencyType=None, frequency=None, startDate=None,
                      endDate=None, needExtendedHoursData=None, needPreviousClose=None):
        """
        Get price history for a ticker
        :param symbol: ticker symbol
        :type symbol: str
        :param periodType: period type ("day"|"month"|"year"|"ytd")
        :type periodType: str
        :param period: period
        :type period: int
        :param frequencyType: frequency type ("minute"|"daily"|"weekly"|"monthly")
        :type frequencyType: str
        :param frequency: frequency (1|5|10|15|30)
        :type frequency: int
        :param startDate: start date
        :type startDate: datetime | str
        :param endDate: end date
        :type endDate: datetime | str
        :param needExtendedHoursData: need extended hours data (True|False)
        :type needExtendedHoursData: boolean
        :param needPreviousClose: need previous close (True|False)
        :type needPreviousClose: boolean
        :return: dictionary of containing candle history
        :rtype: request.Response
        """
        return requests.get(f'{self.MARKET_ENDPOINT}/pricehistory',
                            headers={'Authorization': f'Bearer {self.accessToken}'},
                            params=self._params_parser({'symbol': symbol, 'periodType': periodType, 'period': period,
                                                        'frequencyType': frequencyType, 'frequency': frequency,
                                                        'startDate': self._time_converter(startDate, "epoch"),
                                                        'endDate': self._time_converter(endDate, "epoch"),
                                                        'needExtendedHoursData': needExtendedHoursData,
                                                        'needPreviousClose': needPreviousClose}))

    # get movers in a specific index and direction
    def movers(self, symbol, sort=None, frequency=None):
        """
        Get movers in a specific index and direction
        :param symbol: symbol ("$DJI"|"$COMPX"|"$SPX"|"NYSE"|"NASDAQ"|"OTCBB"|"INDEX_ALL"|"EQUITY_ALL"|"OPTION_ALL"|"OPTION_PUT"|"OPTION_CALL")
        :type symbol: str
        :param sort: sort ("VOLUME"|"TRADES"|"PERCENT_CHANGE_UP"|"PERCENT_CHANGE_DOWN")
        :type sort: str
        :param frequency: frequency (0|1|5|10|30|60)
        :type frequency: int
        :return: movers
        :rtype: request.Response
        """
        return requests.get(f'{self.MARKET_ENDPOINT}/movers/{symbol}',
                            headers={'Authorization': f'Bearer {self.accessToken}'},
                            params=self._params_parser({'sort': sort, 'frequency': frequency}))

    # get market hours for a list of markets
    def market_hours(self, symbols, date=None):
        """
        Get Market Hours for dates in the future across different markets.
        :param symbols: list of market symbols ("equity", "option", "bond", "future", "forex")
        :type symbols: list
        :param date: date
        :type date: datetime | str
        :return: market hours
        :rtype: request.Response
        """
        return requests.get(f'{self.MARKET_ENDPOINT}/markets',
                            headers={'Authorization': f'Bearer {self.accessToken}'},
                            params=self._params_parser(
                                {'markets': symbols, #self._format_list(symbols),
                                 'date': self._time_converter(date, 'YYYY-MM-DD')}))

    # get market hours for a single market
    def market_hour(self, market_id, date=None):
        """
        Get Market Hours for dates in the future for a single market.
        :param market_id: market id ("equity"|"option"|"bond"|"future"|"forex")
        :type market_id: str
        :param date: date
        :type date: datetime | str
        :return: market hours
        :rtype: request.Response
        """
        return requests.get(f'{self.MARKET_ENDPOINT}/markets/{market_id}',
                            headers={'Authorization': f'Bearer {self.accessToken}'},
                            params=self._params_parser({'date': self._time_converter(date, 'YYYY-MM-DD')}))

    # get instruments for a list of symbols
    def instruments(self, symbol, projection):
        """
        Get instruments for a list of symbols
        :param symbol: symbol
        :type symbol: str
        :param projection: projection ("symbol-search"|"symbol-regex"|"desc-search"|"desc-regex"|"search"|"fundamental")
        :type projection: str
        :return: instruments
        :rtype: request.Response
        """
        return requests.get(f'{self.MARKET_ENDPOINT}/instruments',
                            headers={'Authorization': f'Bearer {self.accessToken}'},
                            params={'symbol': symbol, 'projection': projection})

    # get instruments for a single cusip
    def instrument_cusip(self, cusip_id):
        """
        Get instrument for a single cusip
        :param cusip_id: cusip id
        :type cusip_id: str
        :return: instrument
        :rtype: request.Response
        """
        return requests.get(f'{self.MARKET_ENDPOINT}/instruments/{cusip_id}',
                            headers={'Authorization': f'Bearer {self.accessToken}'})