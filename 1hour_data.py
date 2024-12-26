import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import os

# Set the save directory
SAVE_DIR = "D:/Historic_prices/hour/"  # Replace with your desired path

# Ensure the directory exists
os.makedirs(SAVE_DIR, exist_ok=True)

# Fetch the top 500 coins by 24-hour trading volume
def fetch_top_500_coins():
    url = "https://api.binance.com/api/v3/ticker/24hr"
    response = requests.get(url).json()
    # Sort by 24-hour volume and get the top 500 symbols
    sorted_coins = sorted(response, key=lambda x: float(x["volume"]), reverse=True)
    top_500 = [coin["symbol"] for coin in sorted_coins[:500] if coin["symbol"].endswith("USDT")]
    return top_500

# Fetch historical 1-hour data for a single symbol
def fetch_historical_data(symbol, start_date, end_date, interval="1h"):
    url = "https://api.binance.com/api/v3/klines"
    start_time = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp() * 1000)
    end_time = int(datetime.strptime(end_date, "%Y-%m-%d").timestamp() * 1000)

    all_data = []
    while start_time < end_time:
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": start_time,
            "limit": 1000,
        }
        response = requests.get(url, params=params).json()
        if isinstance(response, list) and response:
            all_data.extend(response)
            start_time = response[-1][0] + 1  # Move to the next batch
        else:
            break  # Exit if there's no more data
        time.sleep(0.1)  # Avoid hitting rate limits

    # Convert to DataFrame
    df = pd.DataFrame(all_data, columns=[
        "Open time", "Open", "High", "Low", "Close", "Volume",
        "Close time", "Quote asset volume", "Number of trades",
        "Taker buy base asset volume", "Taker buy quote asset volume", "Ignore"
    ])
    df["Open time"] = pd.to_datetime(df["Open time"], unit="ms")
    return df

# Download historical data for top 500 coins
def download_data_for_top_coins():
    top_coins = fetch_top_500_coins()
    start_date = "2017-01-01"  # Start from Binance's earliest possible date
    end_date = datetime.now().strftime("%Y-%m-%d")  # Today

    for coin in top_coins:
        print(f"Downloading data for {coin} (1-hour timeframe)...")
        try:
            df = fetch_historical_data(symbol=coin, start_date=start_date, end_date=end_date, interval="1h")
            if not df.empty:
                # Save data to CSV in the specified directory
                file_path = os.path.join(SAVE_DIR, f"{coin}_1h_data.csv")
                df.to_csv(file_path, index=False)
                print(f"Data for {coin} saved to {file_path}.")
            else:
                print(f"No data available for {coin}.")
        except Exception as e:
            print(f"Error downloading data for {coin}: {e}")
        time.sleep(1)  # Pause to avoid rate limits

# Run the script
if __name__ == "__main__":
    print(f"Data will be saved to: {SAVE_DIR}")
    download_data_for_top_coins()
