"""
Developer Name : Kamal Kumar Chanchal
EmailId : kchanchal78@gmail.com
Linkedin : linkedin.com/in/kamalchanchal
"""
""" 
Task 
For each stock on 19th April 2024 and 22nd April 2024, determine the timestamp when the
cumulative traded volume within a rolling 60-minute window first exceeds the stock’s 30-day
average volume.

This 30-day average is calculated from the 30 trading days preceding the
trading day. If no crossover occurs during the day, the output for that stock should be None.

"""


from src import StockVolumeStrategy
import pandas as pd
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

# Define file paths
path = './data/'
day_data_file = path+'SampleDayData.csv'

intraday_data_files = ['{}19thAprilSampleData.csv'.format(path),
                       '{}22ndAprilSampleData.csv'.format(path)]

# Instantiate the strategy and run it
strategy = StockVolumeStrategy(day_data_file, intraday_data_files)
strategy.run()
