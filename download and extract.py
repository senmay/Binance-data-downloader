import os
import requests
import zipfile
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import logging

# Konfiguracja logowania
logging.basicConfig(filename='data_download.log', filemode='a', level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Funkcja do pobierania i rozpakowywania pliku z mechanizmem ponawiania
def download_and_extract(url, extract_to='.', max_retries=3):
    local_filename = url.split('/')[-1]
    csv_filename = local_filename.replace('.zip', '.csv')
    csv_filepath = os.path.join(extract_to, csv_filename)
    
    # Sprawdzenie, czy plik już istnieje
    if os.path.exists(csv_filepath):
        logger.info(f"Plik już istnieje: {csv_filepath}")
        return
    
    for attempt in range(max_retries):
        try:
            # Pobieranie pliku
            with requests.get(url, stream=True, timeout=10) as r:
                r.raise_for_status()
                with open(local_filename, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            # Rozpakowywanie pliku
            with zipfile.ZipFile(local_filename, 'r') as zip_ref:
                zip_ref.extractall(extract_to)
            os.remove(local_filename)
            logger.info(f"Pobrano i rozpakowano: {url}")
            return
        except Exception as e:
            logger.error(f"Problem z pobraniem {url}: {e}")
            if attempt < max_retries - 1:
                logger.warning(f"Ponawianie próby {attempt + 1} z {max_retries}...")
            else:
                logger.critical("Wyczerpano limit prób")

# Funkcja do generowania URL-ów
def generate_urls(start_date, end_date):
    base_url = "https://data.binance.vision/data/futures/um/daily/klines/ATOMUSDT/1m/"
    date = start_date
    while date <= end_date:
        url = f"{base_url}ATOMUSDT-1m-{date.strftime('%Y-%m-%d')}.zip"
        yield url
        date += timedelta(days=1)

# Ustawienia daty początkowej i końcowej
start_date = datetime.strptime('2020-01-01', '%Y-%m-%d')
end_date = datetime.strptime('2024-06-03', '%Y-%m-%d')

# Ścieżka docelowa do zapisu danych
extract_base_path = r'F:\projekt binance\data\futures\um\daily\klines\ATOMUSDT\1m'

# Tworzenie folderów, jeśli nie istnieją
os.makedirs(extract_base_path, exist_ok=True)

# Lista URL-ów do pobrania
urls = list(generate_urls(start_date, end_date))

# Pobieranie i rozpakowywanie plików równolegle
max_workers = 10  # Liczba równoczesnych wątków
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    futures = [executor.submit(download_and_extract, url, extract_base_path) for url in urls]

# Czekanie na zakończenie wszystkich wątków
for future in futures:
    future.result()

# Znajdowanie wszystkich plików CSV w katalogu
csv_files = [os.path.join(extract_base_path, f) for f in os.listdir(extract_base_path) if f.endswith('.csv')]

# Ładowanie i zapisywanie danych partiami
output_file = 'all_atomusdt_1m_data.csv'
is_first = True
logger.info("Zaczynam ładować i zapisywać dane partiami")
for file in csv_files:
    df = pd.read_csv(file)
    df.to_csv(output_file, mode='a', header=is_first, index=False)
    is_first = False

logger.info("Zapisano wszystkie dane do pliku all_atomusdt_1m_data.csv")

