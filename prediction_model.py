import pandas as pd

# Ścieżka do pliku CSV
csv_file_path = r'F:\projekt binance\data\all_atomusdt_1m_data.csv'

# Wczytaj dane z pliku CSV, ignorując nagłówki
data = pd.read_csv(csv_file_path, header=None)

# Definicja poprawnych nagłówków
columns = [
    'open_time', 'open', 'high', 'low', 'close', 'volume',
    'close_time', 'quote_volume', 'count', 'taker_buy_volume',
    'taker_buy_quote_volume', 'ignore'
]

# Ustaw poprawne nagłówki
data.columns = columns

# Usuń wiersze, które zawierają nagłówki (opcjonalnie, na podstawie typów danych)
data = data[data['open_time'].apply(lambda x: str(x).isdigit())]

# Przekształć kolumnę timestamp na odpowiedni format, jeśli potrzebne
data['open_time'] = pd.to_datetime(data['open_time'], unit='ms')
data['close_time'] = pd.to_datetime(data['close_time'], unit='ms')

# Wyświetl pierwsze kilka wierszy danych
print(data.head())

# # Opcjonalnie: Zapisz poprawione dane do nowego pliku CSV
# data.to_csv(r'F:\projekt binance\data\cleaned_all_atomusdt_1m_data.csv', index=False)
