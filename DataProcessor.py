import os
import pandas as pd
import DataDownloader
import LoggerSetup
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

class DataProcessor:
    def __init__(self, start_date, end_date, extract_base_path, logger):
        self.start_date = start_date
        self.end_date = end_date
        self.extract_base_path = extract_base_path
        self.logger = logger

    def generate_urls(self):
        base_url = "https://data.binance.vision/data/futures/um/daily/klines/ATOMUSDT/1m/"
        date = self.start_date
        while date <= self.end_date:
            url = f"{base_url}ATOMUSDT-1m-{date.strftime('%Y-%m-%d')}.zip"
            yield url
            date += timedelta(days=1)

    def load_and_save_data(self, csv_files, output_file):
        is_first = True
        for file in csv_files:
            df = pd.read_csv(file)
            df.to_csv(output_file, mode='a', header=is_first, index=False)
            is_first = False
        df.head
        self.logger.info(f"Zapisano wszystkie dane do pliku {output_file}")

if __name__ == '__main__':
    start_date = datetime.strptime('2020-01-01', '%Y-%m-%d')
    end_date = datetime.strptime('2024-06-03', '%Y-%m-%d')
    extract_base_path = r'F:\projekt binance\data\futures\um\daily\klines\ATOMUSDT\1m'
    os.makedirs(extract_base_path, exist_ok=True)

    logger_setup = LoggerSetup()
    logger = logger_setup.logger

    data_downloader = DataDownloader(logger)
    data_processor = DataProcessor(start_date, end_date, extract_base_path, logger)

    urls = list(data_processor.generate_urls())

    max_workers = 10
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(data_downloader.download_and_extract, url, extract_base_path) for url in urls]

    for future in futures:
        future.result()

    csv_files = [os.path.join(extract_base_path, f) for f in os.listdir(extract_base_path) if f.endswith('.csv')]
    output_file = 'all_atomusdt_1m_data.csv'
    data_processor.load_and_save_data(csv_files, output_file)