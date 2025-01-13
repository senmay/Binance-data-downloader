import os
from datetime import date, datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

from logger_setup import LoggerSetup
from data_downloader import DataDownloader
from data_processor import DataProcessor

if __name__ == '__main__':    

    start_date = datetime.strptime('2024-12-10', '%Y-%m-%d')
    end_date = datetime.strptime(
    (date.today() - timedelta(days=1)).strftime('%Y-%m-%d'),
    '%Y-%m-%d'
)
    base_path = r'F:\projekt binance\data\futures\um\daily'
    data_types = ['klines']
    pairs = ['ETHUSDT', 'BTCUSDT', 'ATOMUSDT']
    my_intervals = ['1m', '3m', '5m', '15m', '1h', '4h']

    logger_setup = LoggerSetup()
    logger = logger_setup.logger

    data_downloader = DataDownloader(logger)

for data_type in data_types:
    for pair in pairs:
        # Creating path for every data type and specific pair
        extract_base_path = os.path.join(base_path, data_type, pair)
        os.makedirs(extract_base_path, exist_ok=True)
        
        if data_type == 'klines':
            data_processor = DataProcessor(
                start_date, 
                end_date, 
                extract_base_path, 
                logger, 
                data_type, 
                pair, 
                intervals=my_intervals
            )
        else:
            data_processor = DataProcessor(
                start_date, 
                end_date, 
                extract_base_path, 
                logger, 
                data_type, 
                pair
            )
        
        urls = list(data_processor.generate_urls())

        unique_urls = set(urls)
        if len(unique_urls) != len(urls):
            logger.warning(f"Found duplicate URLs in the list for {data_type}")

        logger.info(f"Total URLs to download for {data_type}: {len(urls)}")

        max_workers = 10
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(data_downloader.download_and_extract, url, extract_base_path)
                for url in urls
            ]

        for future in futures:
            future.result()

        csv_files = [
            os.path.join(extract_base_path, f) 
            for f in os.listdir(extract_base_path) 
            if f.endswith('.csv')
        ]
        output_file = os.path.join(base_path, f'all_{data_type}_{pair}_data.csv')
        
        # Usuń istniejący plik wynikowy, jeśli istnieje
        if os.path.exists(output_file):
            os.remove(output_file)
            logger.info(f"Existing output file {output_file} removed")

        # data_processor.load_and_save_data(csv_files, output_file)

