import time
import os.path
import threading
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from googleapiclient.discovery import build
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow


class Gmail:
    def __init__(self, log_signal=None):
        self._load_env()
        self.creds = None
        self.creds_performance = None
        self.current_position = None
        self.CALLEVENT = threading.Event()
        self.PUTEVENT = threading.Event()
        self.log_signal = log_signal
        self.check = None
        self._initialize()


    def _load_env(self):
        load_dotenv(dotenv_path=Path('./cloud_services/app_info/.env'))
        self.CLIENT_ID = os.getenv("clientId")
        self.CLIENT_SECRET = os.getenv("clientSecret")
        self.SCOPES = ['https://www.googleapis.com/auth/gmail.modify']
        self.SERVICES = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']


    def _initialize(self):
        self._check_keys()
        self._token_manager("init")

        performance_path = './cloud_services/app_info/performance.json'
        self.creds_performance = service_account.Credentials.from_service_account_file(performance_path, scopes=self.SERVICES)

    def _check_keys(self):
        if len(self.CLIENT_ID) != 72 or len(self.CLIENT_SECRET) != 35:
            raise ValueError("Incorrect keys or no keys found, add keys in app_info/.env")

    
    def _refresh_token(self, creds_path):
        if self.creds and self.creds.expired and self.creds.refresh_token:
            try:
                self.creds.refresh(Request())
            except Exception as e:
                self.log_signal.emit(f"Error refreshing token: {e}")
                # Attempt to reauthorize if refresh fails
                self._authorize_new_token(creds_path)
        else:
            self._authorize_new_token(creds_path)
        self._token_manager("set", self.creds)


    def _authorize_new_token(self, creds_path):
        flow = InstalledAppFlow.from_client_secrets_file(creds_path, self.SCOPES)
        self.creds = flow.run_local_server(port=0)


    def _token_manager(self, todo=None, credentials=None):
        credentials_path = './cloud_services/app_info/credentials.json'
        token_path = './cloud_services/app_info/token.json'
        performance_path = './cloud_services/app_info/performance.json'

        self.creds_performance = service_account.Credentials.from_service_account_file(performance_path, scopes=self.SERVICES)
        
        if todo == "set":
            if credentials is not None:
                with open(token_path, "w") as token:
                    token.write(credentials.to_json())
                self.creds = credentials
            else:
                raise ValueError("Error in setting token file, null values given")
        elif todo == "init":
            try:
                self.creds = Credentials.from_authorized_user_file(token_path, self.SCOPES)
                if not self.creds or not self.creds.valid:
                    raise RuntimeError("ERROR: No valid credentials available.")
            except Exception:
                self._refresh_token(credentials_path)


    def _check_inbox(self):
        service = build("gmail", "v1", credentials=self.creds)
        results = service.users().messages().list(userId="me", labelIds=["INBOX"], q="is:unread").execute()
        signals = results.get("messages", [])

        if not signals:
            return

        for signal in signals[:1]:
            message = service.users().messages().get(userId="me", id=signal["id"]).execute()
            service.users().messages().modify(userId="me", id=signal["id"], body={"removeLabelIds": ["UNREAD"]}).execute()
            
            signal_type = message['snippet'][36:42].upper()
            
            if signal_type in ('CALL5-', 'CALL15', 'CALL30', 'CALL1H', 'CALL2H', 'CALL4H', 'C5----', 'C15---', 'C30---', 'C1H---', 'C2H---', 'C4H---'):
                if self.current_position != 'CALL' and not self.CALLEVENT.is_set():
                    self.CALLEVENT.set()
                    self.PUTEVENT.clear()
                    self.log_signal.emit(f"CALL signal received: {signal_type}")
            elif signal_type in ('PUT5--', 'PUT15-', 'PUT30-', 'PUT1H-', 'PUT2H-', 'PUT4H-', 'P5----', 'P15---', 'P30---', 'P1H---', 'P2H---', 'P4H---'):
                if self.current_position != 'PUT' and not self.PUTEVENT.is_set():
                    self.PUTEVENT.set()
                    self.CALLEVENT.clear()
                    self.log_signal.emit(f"PUT signal received: {signal_type}")


    def check_email_automatic(self):    
        def checker():
            while self.check:
                try:
                    self._check_inbox()
                except Exception as e:
                    self.log_signal.emit(f"Error checking inbox: {e}")
                time.sleep(1)
        threading.Thread(target=checker, daemon=True).start()
        

    def reset_position(self):
        self.set_current_position(None)
        self.CALLEVENT.clear()
        self.PUTEVENT.clear()


    def wait(self):
        self.CALLEVENT.wait()
        self.PUTEVENT.wait()
    

    def set_current_position(self, pos):
        self.current_position = pos


    def set_checker(self, check):
        self.check = check
 
   
    def get_call_event(self):
        return self.CALLEVENT


    def get_put_event(self):
        return self.PUTEVENT
    

    def get_current_position(self):
        return self.current_position

  