import os
import pandas as pd
from datetime import timedelta

class DataProcessor:
    def __init__(self, start_date, end_date, extract_base_path, logger, data_type, pair, intervals=None):
        self.start_date = start_date
        self.end_date = end_date
        self.extract_base_path = extract_base_path
        self.logger = logger
        self.data_type = data_type
        self.pair = pair
        self.intervals = intervals

    def generate_urls(self):        
        current_date = self.start_date
        if self.data_type == 'klines':            
            while current_date <= self.end_date:
                for interval in self.intervals:
                    base_url = (f"https://data.binance.vision/data/futures/um/daily/klines/"
                                f"{self.pair}/{interval}/")
                    url = (f"{base_url}{self.pair}-{interval}-"
                           f"{current_date.strftime('%Y-%m-%d')}.zip")
                    yield url
                current_date += timedelta(days=1)
        
        # Obsługa innych typów (aggTrades, metrics, itp.)
        else:
            base_url = f"https://data.binance.vision/data/futures/um/daily/{self.data_type}/{self.pair}/"
            while current_date <= self.end_date:
                url = (f"{base_url}{self.pair}-{self.data_type}-"
                       f"{current_date.strftime('%Y-%m-%d')}.zip")
                yield url
                current_date += timedelta(days=1)

    def load_and_save_data(self, csv_files, output_file):
        is_first = not os.path.exists(output_file)  # Sprawdź, czy plik wynikowy już istnieje
        for file in csv_files:
            df = pd.read_csv(file)
            df.to_csv(output_file, mode='a', header=is_first, index=False)
            is_first = False
        self.logger.info(f"Saved all data to file {output_file}")
