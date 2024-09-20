import json
import time
import threading
from datetime import datetime
from PyQt5.QtWidgets import QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLabel, QTextEdit, QTabWidget, QLineEdit, QGridLayout, QCheckBox, QTableWidget, QTableWidgetItem, QHeaderView
from PyQt5.QtGui import QIntValidator
from interface.client import Client


class ClientGUI(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Trading Bot GUI")
        self.setGeometry(100, 100, 800, 600)

        self.central_widget = QWidget()
        self.setCentralWidget(self.central_widget)
        self.layout = QVBoxLayout(self.central_widget)

        self.tabs = QTabWidget()
        self.layout.addWidget(self.tabs)

        self.create_main_tab()
        self.create_logs_tab()
        self.create_candles_tab()
        self.create_settings_tab()
        self.load_settings()
        self.create_input_tab()

        self.client = None
        self.positions = {} 

        # Start the time check thread
        # self.time_check_thread = threading.Thread(target=self._check_time, daemon=True)
        # self.time_check_thread.start()


    def create_main_tab(self):
        main_tab = QWidget()
        main_layout = QVBoxLayout(main_tab)

        # Status
        status_layout = QHBoxLayout()
        status_layout.addWidget(QLabel("Robot Status:"))
        self.status_label = QLabel("Stopped")
        status_layout.addWidget(self.status_label)
        main_layout.addLayout(status_layout)

        # Start/Stop button
        self.start_stop_button = QPushButton("Start Bot")
        self.start_stop_button.clicked.connect(self.toggle_bot)
        main_layout.addWidget(self.start_stop_button)

        # Current positions
        main_layout.addWidget(QLabel("Current Positions:"))
        self.positions_table = QTableWidget(0, 5)
        self.positions_table.setHorizontalHeaderLabels(["Symbol", "Quantity", "Price", "P/L%", "Alert"])
        #self.positions_table.setColumnWidth(0, 100)
        self.positions_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        main_layout.addWidget(self.positions_table)

        # Recent trades
        main_layout.addWidget(QLabel("Recent Trades:"))
        self.trades_table = QTableWidget(0, 6)
        self.trades_table.setHorizontalHeaderLabels(["Status", "Symbol", "Quantity", "Price", "P/L%", "Alert"])
        #self.positions_table.setColumnWidth(0, 100)
        self.trades_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeToContents)
        main_layout.addWidget(self.trades_table)

        self.tabs.addTab(main_tab, "Main")


    def create_logs_tab(self):
        logs_tab = QWidget()
        logs_layout = QVBoxLayout(logs_tab)

        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        logs_layout.addWidget(self.log_text)

        self.tabs.addTab(logs_tab, "Logs")


    def create_candles_tab(self):
        candles_tab = QWidget()
        candles_layout = QGridLayout(candles_tab)

        # Ticker
        candles_layout.addWidget(QLabel("Ticker:"), 0, 0)
        self.ticker = QLineEdit()
        candles_layout.addWidget(self.ticker, 0, 1)

        # Period Type
        candles_layout.addWidget(QLabel("Period Type ('day'|'month'|'year'|'ytd'):"), 1, 0)
        self.period_type = QLineEdit()
        candles_layout.addWidget(self.period_type, 1, 1)

        # Period
        candles_layout.addWidget(QLabel("Period:"), 2, 0)
        self.period = QLineEdit()
        self.period.setValidator(QIntValidator())
        candles_layout.addWidget(self.period, 2, 1)

        # Frequency Type
        candles_layout.addWidget(QLabel("Frequency Type ('minute'|'daily'|'weekly'|'monthly'):"), 3, 0)
        self.frequency_type = QLineEdit()
        candles_layout.addWidget(self.frequency_type, 3, 1)

        # Frequency 
        candles_layout.addWidget(QLabel("Frequency (1|5|10|15|30):"), 4, 0)
        self.frequency = QLineEdit()
        self.frequency.setValidator(QIntValidator())
        candles_layout.addWidget(self.frequency, 4, 1)

        # Start Date
        candles_layout.addWidget(QLabel("Start Date (xxxx-xx-xx):"), 5, 0)
        self.start_date = QLineEdit()
        candles_layout.addWidget(self.start_date, 5, 1)

        # End Date
        candles_layout.addWidget(QLabel("End Date (xxxx-xx-xx):"), 6, 0)
        self.end_date = QLineEdit()
        candles_layout.addWidget(self.end_date, 6, 1)

        # File Name
        candles_layout.addWidget(QLabel("File Name:"), 7, 0)
        self.file_name = QLineEdit()
        candles_layout.addWidget(self.file_name, 7, 1)

        # Save Button
        request_button = QPushButton("Request")
        request_button.clicked.connect(self.request_candle_history)
        candles_layout.addWidget(request_button, 9, 0, 1, 2)

        self.tabs.addTab(candles_tab, "Ticker History")


    def create_settings_tab(self):
        settings_tab = QWidget()
        settings_layout = QGridLayout(settings_tab)

        # Auto Start Toggle
        settings_layout.addWidget(QLabel("Auto Start:"), 0, 0)
        self.auto_start_toggle = QPushButton("Off", self)
        self.auto_start_toggle.setCheckable(True)
        self.auto_start_toggle.clicked.connect(self.toggle_auto_start)
        settings_layout.addWidget(self.auto_start_toggle, 0, 1)

        # Auto Start Time
        settings_layout.addWidget(QLabel("Auto Start Time:"), 1, 0)
        self.auto_start_time = QLineEdit()
        settings_layout.addWidget(self.auto_start_time, 1, 1)

        # Auto End Time
        settings_layout.addWidget(QLabel("Auto End Time:"), 2, 0)
        self.auto_end_time = QLineEdit()
        settings_layout.addWidget(self.auto_end_time, 2, 1)

        # Max Position Size
        settings_layout.addWidget(QLabel("Max Position Size:"), 3, 0)
        self.max_position_size = QLineEdit()
        settings_layout.addWidget(self.max_position_size, 3, 1)

        # Max Profit Percentage
        settings_layout.addWidget(QLabel("Max Profit Percentage:"), 4, 0)
        self.max_profit_percentage = QLineEdit()
        settings_layout.addWidget(self.max_profit_percentage, 4, 1)

        # Max Loss Percentage
        settings_layout.addWidget(QLabel("Max Loss Percentage:"), 5, 0)
        self.max_loss_percentage = QLineEdit()
        settings_layout.addWidget(self.max_loss_percentage, 5, 1)

        # Max Contract Price
        settings_layout.addWidget(QLabel("Max Contract Price:"), 6, 0)
        self.max_contract_price = QLineEdit()
        settings_layout.addWidget(self.max_contract_price, 6, 1)

        # Least Delta
        settings_layout.addWidget(QLabel("Least Delta:"), 7, 0)
        self.least_delta = QLineEdit()
        settings_layout.addWidget(self.least_delta, 7, 1)

        # Strategy Checkboxes
        settings_layout.addWidget(QLabel("Strategy:"), 8, 0)
        self.strategy_group = QWidget()
        strategy_layout = QHBoxLayout(self.strategy_group)
        self.strategy_checkboxes = {}
        strategies = ["Call", "CALL", "C", "Put", "PUT", "P", "ALL"]
        for strategy in strategies:
            checkbox = QCheckBox(strategy)
            self.strategy_checkboxes[strategy] = checkbox
            strategy_layout.addWidget(checkbox)
            if strategy != "ALL":
                checkbox.stateChanged.connect(self.update_all_checkbox)
        self.strategy_checkboxes["ALL"].stateChanged.connect(self.handle_all_checkbox)
        settings_layout.addWidget(self.strategy_group, 8, 1)

        # Save Button
        save_button = QPushButton("Save Settings")
        save_button.clicked.connect(self.save_settings)
        settings_layout.addWidget(save_button, 9, 0, 1, 2)

        self.tabs.addTab(settings_tab, "Settings")


    def create_input_tab(self):
        input_tab = QWidget()
        input_layout = QVBoxLayout(input_tab)

        # Add a label
        input_layout.addWidget(QLabel("Enter direct url:"))

        # Add a text input field
        self.input_field = QLineEdit()
        input_layout.addWidget(self.input_field)

        # Add a submit button
        submit_button = QPushButton("Submit")
        submit_button.clicked.connect(self.handle_input)
        input_layout.addWidget(submit_button)

        # Add a display area for the input
        self.input_display = QTextEdit()
        self.input_display.setReadOnly(True)
        input_layout.addWidget(self.input_display)

        self.tabs.addTab(input_tab, "Redirect URL")


    def toggle_auto_start(self):
        if self.auto_start_toggle.isChecked():
            self.auto_start_toggle.setText("On")
            self.log("Auto-start enabled")
            # Here you would add the logic to schedule the auto-start
        else:
            self.auto_start_toggle.setText("Off")
            self.log("Auto-start disabled")
            # Here you would add the logic to cancel the auto-start schedule


    def handle_all_checkbox(self, state):
        for strategy, checkbox in self.strategy_checkboxes.items():
            if strategy != "ALL":
                checkbox.setChecked(state == 2)  # 2 corresponds to Qt.Checked


    def update_all_checkbox(self):
        all_checked = all(checkbox.isChecked() for strategy, checkbox in self.strategy_checkboxes.items() if strategy != "ALL")
        self.strategy_checkboxes["ALL"].setChecked(all_checked)


    def handle_input(self):
        user_input = self.input_field.text()
        if user_input:
            self.input_display.append(f"Redirect URL received: {user_input}")
            self.input_field.clear()
            self.log(f"Processing redirect URL: {user_input}")
            if self.client and self.client.schwab:
                self.client.schwab.set_callback_url(user_input)
    

    def request_user_input(self, message):
        self.log(message)
        self.tabs.setCurrentIndex(self.tabs.indexOf(self.tabs.findChild(QWidget, "Redirect URL")))


    def request_candle_history(self):
        """
        Check if variables are appropriate
        """
        print(self.start_date.text())
        if not isinstance(self.ticker.text(), str):
            raise ValueError(self.log(f"ticker is not a string: {type(self.ticker.text())}"))
        if not isinstance(self.period_type.text(), str):
            raise ValueError(self.log(f"period type is not a string: {type(self.period_type.text())}"))
        if not isinstance(self.frequency_type.text(), str):
            raise ValueError(self.log(f"frequency type is not a string: {type(self.frequency_type.text())}"))
        else:
            self.client.get_candle_history(self.ticker.text(), self.period_type.text(), int(self.period.text()), self.frequency_type.text(), int(self.frequency.text()), self.start_date.text(), self.end_date.text(), self.file_name.text())


    def toggle_bot(self):
        if self.status_label.text() == "Stopped":
            self.start_bot()
        else:
            self.stop_bot()


    def load_settings(self):
        try:
            with open('/Users/josuecastellanos/Documents/Automated_Trading_System/setting/settings.txt', 'r') as f:
                settings = json.load(f)
            
            self.auto_start_toggle.setText("On" if settings.get('auto_start', False) else "Off")
            self.auto_start_time.setText(settings.get('auto_start_time'))
            self.auto_end_time.setText(settings.get('auto_end_time'))
            self.max_position_size.setText(str(settings.get('max_position_size')))
            self.max_profit_percentage.setText(str(settings.get('max_profit_percentage')))
            self.max_loss_percentage.setText(str(settings.get('max_loss_percentage')))
            self.max_contract_price.setText(str(settings.get('max_contract_price')))
            self.least_delta.setText(str(settings.get('least_delta')))
            
            strategies = settings.get('strategies', [])
            for strategy, checkbox in self.strategy_checkboxes.items():
                checkbox.setChecked(strategy in strategies or 'ALL' in strategies)
            
            self.log("Settings loaded successfully from settings.txt")
        except FileNotFoundError:
            self.log("settings.txt not found. Using default settings.")
        except json.JSONDecodeError:
            self.log("Error decoding settings.txt. Using default settings.")
        except Exception as e:
            self.log(f"An error occurred while loading settings: {str(e)}")


    def save_settings(self):
        settings = {
            'auto_start': self.auto_start_toggle.isChecked(),
            'auto_start_time': self.auto_start_time.text(),
            'auto_end_time': self.auto_end_time.text(),
            'max_position_size': int(self.max_position_size.text()),
            'max_profit_percentage': float(self.max_profit_percentage.text()),
            'max_loss_percentage': float(self.max_loss_percentage.text()),
            'max_contract_price': float(self.max_contract_price.text()),
            'least_delta': float(self.least_delta.text()),
            'strategies': [strategy for strategy, checkbox in self.strategy_checkboxes.items() if checkbox.isChecked()]
        }

        try:
            with open('/Users/josuecastellanos/Documents/Automated_Trading_System/setting/settings.txt', 'w') as f:
                json.dump(settings, f, indent=4)
            self.log("Settings saved successfully to settings.txt")
        except Exception as e:
            self.log(f"An error occurred while saving settings: {str(e)}")

        # Log the saved settings
        self.log(f"{json.dumps(settings, indent=2)}")
        self.apply_settings(settings)


    def apply_settings(self, settings):
        # Apply the settings to the current instance
        self.auto_start_toggle.setChecked(settings['auto_start'])
        self.auto_start_toggle.setText("On" if settings['auto_start'] else "Off")
        self.auto_start_time.setText(settings['auto_start_time'])
        self.auto_end_time.setText(settings['auto_end_time'])
        self.max_position_size.setText(str(settings['max_position_size']))
        self.max_profit_percentage.setText(str(settings['max_profit_percentage']))
        self.max_loss_percentage.setText(str(settings['max_loss_percentage']))
        self.max_contract_price.setText(str(settings['max_contract_price']))
        self.least_delta.setText(str(settings['least_delta']))

        for strategy, checkbox in self.strategy_checkboxes.items():
            checkbox.setChecked(strategy in settings['strategies'] or 'ALL' in settings['strategies'])

        # If the bot is running, update its settings
        if self.client and self.client.isRunning():
            self.client.set_settings(settings)


    def start_bot(self):
        if self.client is None:
            self.load_settings()  # Load settings before starting the bot
            self.status_label.setText("Running")
            self.start_stop_button.setText("Stop Robot")
            self.log("Robot started")
            
            self.client = Client(self)
            self.client.log_dict_signal.connect(self.log)
            self.client.log_signal.connect(self.log)
            self.client.position_update_signal.connect(self.update_positions)
            self.client.trade_update_signal.connect(self.update_trades)
            self.client.gmail.set_checker(True)
            self.client.gmail.check_email_automatic()

            current_settings = {
                'auto_start': self.auto_start_toggle.isChecked(),
                'auto_start_time': self.auto_start_time.text(),
                'auto_end_time': self.auto_end_time.text(),
                'max_position_size': int(self.max_position_size.text()),
                'max_profit_percentage': float(self.max_profit_percentage.text()),
                'max_loss_percentage': float(self.max_loss_percentage.text()),
                'max_contract_price': float(self.max_contract_price.text()),
                'least_delta': float(self.least_delta.text()),
                'strategies': [strategy for strategy, checkbox in self.strategy_checkboxes.items() if checkbox.isChecked()]
            }
            self.client.set_settings(current_settings)
        
        else: 
            self.status_label.setText("Running")
            self.start_stop_button.setText("Stop Robot")
            self.log("Robot started")
            self.client.gmail.set_checker(True)
            self.client.gmail.check_email_automatic()

        self.client.start()


    def stop_bot(self):
        # TODO: Stop Gmail from reading messages as well, FIXED 
        self.client.gmail.set_checker(False)

        self.status_label.setText("Stopped")
        self.start_stop_button.setText("Start Robot")
        self.log("Robot stopped")
        
        if self.client and self.client.isRunning():
            self.client.requestInterruption()
            self.client.wait()


    def log(self, message):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.log_text.append(f"[{timestamp}] {message}")


    def update_positions(self, symbol, price, quantity, profit_loss, alert):
        # Check if the symbol is already in the table
        if symbol in self.positions:
            row = self.positions[symbol]
            # Update price and quantity
            self.positions_table.setItem(row, 1, QTableWidgetItem(str(quantity)))
            self.positions_table.setItem(row, 2, QTableWidgetItem(str(price)))
            self.positions_table.setItem(row, 3, QTableWidgetItem(str(profit_loss)))
        else:
            # Add new row
            row = self.positions_table.rowCount()
            self.positions_table.insertRow(row)
            self.positions_table.setItem(row, 0, QTableWidgetItem(symbol))
            self.positions_table.setItem(row, 1, QTableWidgetItem(str(quantity)))
            self.positions_table.setItem(row, 2, QTableWidgetItem(str(price)))
            self.positions_table.setItem(row, 3, QTableWidgetItem(str(profit_loss)))
            self.positions_table.setItem(row, 4, QTableWidgetItem(str(alert)))

            self.positions[symbol] = row


    def update_trades(self, status, symbol, price, quantity, profit_loss=None, alert=None):
        # Add a new row to the trades table
        row = self.trades_table.rowCount()
        self.trades_table.insertRow(row)
        self.trades_table.setItem(row, 0, QTableWidgetItem(status))
        self.trades_table.setItem(row, 1, QTableWidgetItem(symbol))
        self.trades_table.setItem(row, 2, QTableWidgetItem(str(quantity)))
        self.trades_table.setItem(row, 3, QTableWidgetItem(str(price)))
        self.trades_table.setItem(row, 5, QTableWidgetItem(str(alert)))
             
        if status == "SOLD":            
            self.trades_table.setItem(row, 4, QTableWidgetItem(str(profit_loss)))
    

    # This is optional 
    # You can start the bot manually or comment out this out   
    # def _check_time(self):
    #     while True:
    #         now = datetime.now()
    #         if now.hour == 6 and now.minute == 29:
    #             self.start_bot()
    #             # Wait for 1 minute to avoid multiple starts
    #             # time.sleep(59)
    #         elif now.hour == 12 and now.minute == 59:
    #             self.stop_bot()
    #             # Wait for 1 minute to avoid multiple stops
    #             # time.sleep(60)
    #         time.sleep(30)  # Check every 30 seconds


