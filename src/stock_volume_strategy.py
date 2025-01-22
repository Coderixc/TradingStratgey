"""
Developer Name : Kamal Kumar Chanchal
EmailId : kchanchal78@gmail.com
Linkedin : linkedin.com/in/kamalchanchal
"""

import pandas as pd
import logging
from datetime import datetime, timedelta


class StockVolumeStrategy:

    def __init__(self, day_data_file, intraday_data_files):
        # Register Logger
        self.__logger_()


        if len(intraday_data_files) <2:
            raise ValueError("intraday data path is missing")

        # Load Day Data from CSV
        self.__load_daily_stocks_data(day_data_file)

        # Load Intraday Data from CSV
        # self.logger.info(f"Loading 19 April intraday data: {intraday_data_files[0]}")
        # self.intraday_data_19th = pd.read_csv(intraday_data_files[0])
        # self.intraday_data_19th['Date'] = pd.to_datetime(self.intraday_data_19th['Date'], format='%d-%m-%Y')
        # self.intraday_data_19th['Timestamp'] = pd.to_datetime(self.intraday_data_19th['Time'],
        #                                                       format='%H:%M:%S').dt.time

        self.__load_intrady_stocks_data1(intraday_data_files[0])

        # self.logger.info(f"Loading 22 April intraday data: {intraday_data_files[1]}")
        # self.intraday_data_22nd = pd.read_csv(intraday_data_files[1])
        # self.intraday_data_22nd['Date'] = pd.to_datetime(self.intraday_data_22nd['Date'], format='%d/%m/%y')
        # self.intraday_data_22nd['Timestamp'] = pd.to_datetime(self.intraday_data_22nd['Time'],
        #                                                       format='%H:%M:%S').dt.time
        self.__load_intrady_stocks_data2(intraday_data_files[1])

        self.logger.info('Data files loaded successfully.')

    def __load_daily_stocks_data(self,day_data_file):
        try:
            # Load Day Data from CSV
            self.logger.info(f"Loading daily stock data: {day_data_file}")
            self.day_data = pd.read_csv(day_data_file)
            self.day_data['Date'] = pd.to_datetime(self.day_data['Date'], format='%d/%m/%y')
        except AttributeError as e:
            raise AttributeError(f"Not found:{e}")
        except KeyError as e:
            raise KeyError(f"Missing or wrong key :{e}")
        except Exception as e:
            raise Exception(f"error occured while loading data:{day_data_file}")

    def __load_intrady_stocks_data1(self,day_data_file):
        try:
            # Load Day Data from CSV
            self.logger.info(f"Loading 19 April intraday data: {day_data_file}")
            self.intraday_data_19th = pd.read_csv(day_data_file)
            self.intraday_data_19th['Date'] = pd.to_datetime(self.intraday_data_19th['Date'], format='%d-%m-%Y')
            self.intraday_data_19th['Timestamp'] = pd.to_datetime(self.intraday_data_19th['Time'],
                                                                  format='%H:%M:%S').dt.time
        except AttributeError as e:
            raise AttributeError(f"Not found:{e}")
        except KeyError as e:
            raise KeyError(f"Missing or wrong key :{e}")
        except Exception as e:
            raise Exception(f"error occured while loading data:{day_data_file}")

    def __load_intrady_stocks_data2(self,day_data_file):
        try:
            self.logger.info(f"Loading 22 April intraday data: {day_data_file}")
            self.intraday_data_22nd = pd.read_csv(day_data_file)
            self.intraday_data_22nd['Date'] = pd.to_datetime(self.intraday_data_22nd['Date'], format='%d/%m/%y')
            self.intraday_data_22nd['Timestamp'] = pd.to_datetime(self.intraday_data_22nd['Time'],
                                                                  format='%H:%M:%S').dt.time

        except AttributeError as e:
            raise AttributeError(f"Not found:{e}")
        except KeyError as e:
            raise KeyError(f"Missing or wrong key :{e}")
        except Exception as e:
            raise Exception(f"error occured while loading data:{day_data_file}")

    def __logger_(self):
        """Set up logger to display messages in the console."""
        self.logger = logging.getLogger('StockVolumeStrategy')
        handler = logging.StreamHandler()  # Display logs in the console
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)
        self.logger.info('Initializing StockVolumeStrategy...')

    def calculate_30_day_avg(self):
        """Calculate the 30-day average volume for each stock."""
        self.logger.info("Calculating 30-day average volume...")
        self.stock_30_day_avg = {}

        for stock in self.day_data['Stock Name'].unique():
            stock_data = self.day_data[self.day_data['Stock Name'] == stock]

            # For 19th April: Only consider data before 19th
            recent_data = stock_data[stock_data['Date'] < datetime(2024, 4, 19)]
            if len(recent_data) >= 30:
                avg_volume = recent_data['Volume'].tail(30).mean()  # Last 30 days volume
                self.stock_30_day_avg[stock] = avg_volume
                self.logger.debug(f"30-day average for stock {stock}: {avg_volume}")

            # For 22nd April, include 19th April data
            recent_data_22nd = stock_data[stock_data['Date'] <= datetime(2024, 4, 22)]
            if len(recent_data_22nd) >= 30:
                avg_volume_22nd = recent_data_22nd['Volume'].tail(30).mean()
                self.stock_30_day_avg[stock] = avg_volume_22nd
                self.logger.debug(f"30-day average for stock {stock} (including 19th April): {avg_volume_22nd}")

    def calculate_crossover(self, date):
        """Calculate when the cumulative volume exceeds the 30-day average volume."""
        self.logger.info(f"Calculating crossover for {date}...")
        crossover_timestamp = {}

        for stock in self.intraday_data_19th['Stock Name'].unique():
            if stock in self.stock_30_day_avg:
                stock_30_day_avg = self.stock_30_day_avg[stock]

                # Select the intraday data for the given date
                if date == '2024-04-19':
                    intraday_data = self.intraday_data_19th
                elif date == '2024-04-22':
                    intraday_data = self.intraday_data_22nd
                else:
                    continue

                # Filter data for the stock
                stock_data = intraday_data[intraday_data['Stock Name'] == stock]
                cumulative_volume = 0
                start_time = datetime.strptime(f"{date} 09:15:00", '%Y-%m-%d %H:%M:%S')

                for idx, row in stock_data.iterrows():
                    timestamp = datetime.combine(datetime.today(), row['Timestamp'])
                    if timestamp < start_time:
                        continue

                    cumulative_volume += row['Last Traded Quantity']

                    # Check if cumulative volume exceeds the 30-day average
                    if cumulative_volume >= stock_30_day_avg:
                        crossover_timestamp[stock] = timestamp.strftime('%Y-%m-%d %H:%M:%S')
                        self.logger.info(f"Crossover for {stock} found at {timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
                        break

                if stock not in crossover_timestamp:
                    crossover_timestamp[stock] = None
                    self.logger.info(f"No crossover found for {stock} on {date}.")

        return crossover_timestamp

    def run(self):
        """Run the entire strategy."""
        self.logger.info("Running the StockVolumeStrategy...")

        # Step 1: Calculate 30-day average volumes
        self.calculate_30_day_avg()

        # Step 2: Run crossover calculations for both dates
        crossover_19th = self.calculate_crossover('2024-04-19')
        crossover_22nd = self.calculate_crossover('2024-04-22')

        # Print the results
        self.logger.info(f"Timestamp crossover for 19th April 2024: {crossover_19th}")
        self.logger.info(f"Timestamp crossover for 22nd April 2024: {crossover_22nd}")
