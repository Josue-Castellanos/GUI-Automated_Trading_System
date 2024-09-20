import json
import requests
import atexit
import websockets.exceptions
import asyncio
import websockets
import threading
from time import sleep
from datetime import datetime, time

class Stream:
    def __init__(self, schwab):
        self.streamer_info = None
        self.request_id = 1
        self.schwab = schwab
        self.websocket = None
        self.symbols = ["SPY"]
        self.subscriptions = {}
        self.active = False
        self._thread = None
        self.STREAM_ENDPOINT = "https://api.schwab.com/v1"

        atexit.register(self.stop_atexit)


    def stop_atexit(self):
        """
        Stop the stream gracefully if it's still active when the program exits.
        """
        if self.active:
            print("Stopping stream on exit")
            self.stop()


    def get_user_preferences(self):
        """
        
        """
        headers = {
            "Authorization": f"Bearer {self.schwab.accessToken}"
        }
        response = requests.get(f"{self.STREAM_ENDPOINT}/userpreferences", headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            return None


    async def on_message(self):
        """
        
        """
        async for message in self.websocket:
            data = json.loads(message)

            # Save the data to a file
            with open('/Users/josuecastellanos/Documents/Automated_Trading_System/data/candle_history/stream_data.json', 'a') as file:
                json.dump(data, file)
                file.write('\n')  # Write each message as a new line in the file

            print("Received and saved message:", data)


    async def subscribe_services(self):
        """
        
        """
        intervals = {
            "1MIN": "2",
            "5MIN": "3",
            "15MIN": "4",
            "30MIN": "5",
            "1HOUR": "6",
            "2HOUR": "7",
            "4HOUR": "8",
            "DAILY": "9"
        }

        for symbol in self.symbols:
            for interval, request_id in intervals.items():
                subscribe_request = {
                    "requests": [{
                        "requestid": self.request_id,
                        "service": "CHART_EQUITY",
                        "command": "SUBS",
                        "SchwabClientCustomerId": self.streamer_info.get("schwabClientCustomerId"),
                        "SchwabClientCorrelId": self.streamer_info.get("schwabClientCorrelId"),
                        "parameters": {
                            "keys": symbol,
                            "fields": "1,2,3,4,5,6,7,8,9,10",
                            "frequency": interval
                        }
                    }]
                }
                await self.websocket.send(json.dumps(subscribe_request))
                self._record_request(subscribe_request)
                self.request_id += 1



    async def _start_streamer(self, *args, **kwargs):
        """
        
        """
        response = self.schwab.preferences()
        if response.ok:
            self.streamer_info = response.json().get('streamerInfo', None)[0]

        while True:
            try:
                async with websockets.connect(self.streamer_info.get('streamerSocketUrl'), ping_interval=None) as self.websocket:
                    print("WebSocket connection opened")

                    # Login to the streamer
                    login_request = {
                        "requests": [{
                            "requestid": self.request_id,
                            "service": "ADMIN",
                            "command": "LOGIN",
                            "SchwabClientCustomerId": self.streamer_info.get("schwabClientCustomerId"),
                            "SchwabClientCorrelId": self.streamer_info.get("schwabClientCorrelId"),
                            "parameters": {
                                "Authorization": self.schwab.accessToken,
                                "SchwabClientChannel": self.streamer_info.get("schwabClientChannel"),
                                "SchwabClientFunctionId": self.streamer_info.get("schwabClientFunctionId")
                            }
                        }]
                    }
                    await self.websocket.send(json.dumps(login_request))
                    self.request_id += 1

                    # Subscribe to various interval chart data for the symbols
                    await self.subscribe_services()

                    # Handle incoming messages
                    await self.on_message()
                    
            except Exception as e:
                self.active = False
                if e is websockets.exceptions.ConnectionClosedOK or str(e) == "received 1000 (OK); then sent 1000 (OK)":  # catch logout request
                    print("Failed")
                    break
                elif e is websockets.exceptions.ConnectionClosedError or str(e) == "no close frame received or sent":  # catch no subscriptions kick
                    print("Failed")
                    break
                else:
                    print({e})


    def start(self, *args, **kwargs):
        """
        
        """
        if not self.active:
            def _start_async():
                asyncio.run(self._start_streamer(*args, **kwargs))

            self._thread = threading.Thread(target=_start_async, daemon=False)
            self._thread.start()
            sleep(1)  # Ensure the thread starts before the main program continues
        else:
            print("Stream already active.")


    def _record_request(self, request):
        """
        
        """
        def str_to_list(st):
            if type(st) is str: 
                return st.split(",")
            elif type(st) is list: 
                return st
        service = request["requests"][0].get("service", None)
        parameters = request["requests"][0].get("parameters", None)
        if parameters is not None:
            keys = str_to_list(parameters.get("keys", []))
            fields = str_to_list(parameters.get("fields", []))

            if service not in self.subscriptions:
                self.subscriptions[service] = {}

            for key in keys:
                self.subscriptions[service][key] = fields


    def stop(self, clear_subscriptions=True):
        """
        
        """
        if clear_subscriptions:
            self.subscriptions = {}
        self.active = False
        self.request_id += 1


    def send(self, requests):
        """
        
        """
        async def _send(to_send):
            await self.websocket.send(to_send)

        if type(requests) is not list:
            requests = [requests]

        for request in requests:
            self._record_request(request)

        if self.active:
            to_send = json.dumps({"requests": requests})
            asyncio.run(_send(to_send))
