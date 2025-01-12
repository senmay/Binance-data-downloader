import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score

# Wczytaj dane
csv_file_path = r'F:\projekt binance\data\cleaned_all_atomusdt_1m_data_modified.csv'
data = pd.read_csv(csv_file_path)

# Konwertuj kolumny open_time i close_time na datetime
data['open_time'] = pd.to_datetime(data['open_time'])

# Ustaw 'open_time' jako indeks
data.set_index('open_time', inplace=True)

# Dodaj przesunięte (lagged) cechy
data['prev_close_1'] = data['close'].shift(1)
data['prev_close_2'] = data['close'].shift(2)
data['prev_close_3'] = data['close'].shift(3)
data['prev_volume_1'] = data['volume'].shift(1)

# Usuń wiersze z brakującymi wartościami
data.dropna(inplace=True)

# Wybierz cechy (features) oraz zmienną docelową (target)
features = ['prev_close_1', 'prev_close_2', 'prev_close_3', 'prev_volume_1']
X = data[features]
y = data['close']

# Podział danych na zbiór treningowy (80%) i testowy (20%)
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Standaryzacja cech
scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)

# Stwórz model regresji liniowej
model = LinearRegression()

# Trening modelu na danych treningowych
model.fit(X_train_scaled, y_train)

# Przewidywanie na zbiorze testowym
y_pred = model.predict(X_test_scaled)

# Obliczanie metryk oceny
mse = mean_squared_error(y_test, y_pred)
r2 = r2_score(y_test, y_pred)

print(f"Mean Squared Error: {mse}")
print(f"R^2 Score: {r2}")

# Przewidywanie ceny zamknięcia w kolejnych 10 minutach
# Weź ostatni wiersz danych jako dane wejściowe dla predykcji
last_row = data.iloc[-1]
input_features = np.array([[
    last_row['close'], 
    last_row['prev_close_1'], 
    last_row['prev_close_2'], 
    last_row['volume']
]])

# Przekształć dane wejściowe przy użyciu standardyzatora (scaler)
input_features_scaled = scaler.transform(input_features)

# Użyj modelu do przewidzenia ceny zamknięcia
predicted_close = model.predict(input_features_scaled)

# Wyświetl wynik
print(f"Przewidywana cena zamknięcia w kolejnych 10 minutach: {predicted_close[0]}")
