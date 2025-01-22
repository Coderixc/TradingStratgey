"""
Developer Name : Kamal Kumar Chanchal
EmailId : kchanchal78@gmail.com
Linkedin : linkedin.com/in/kamalchanchal
"""

from src import StockVolumeStrategy
import pandas as pd


# Define file paths
path = './data/'
day_data_file = path+'SampleDayData.csv'

intraday_data_files = ['{}19thAprilSampleData.csv'.format(path),
                       '{}22ndAprilSampleData.csv'.format(path)]

# Instantiate the strategy and run it
strategy = StockVolumeStrategy(day_data_file, intraday_data_files)
strategy.run()
