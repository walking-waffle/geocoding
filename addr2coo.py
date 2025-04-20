import pandas as pd
import requests
import time

# Mapbox Token
MAPBOX_TOKEN = '!!! Mapbox TOKEN !!!'

# CONSTANT
INPUT_CSV = 'input.csv'
OUTPUT_CSV = 'output.csv'
ADDRESS_COLUMN = 'address'

# address to lng and lat
def geocode_address(address):
    # Mapbox API endpoint for geocoding. Requires a valid access token.
    # May fail if token is expired or rate limit is exceeded.
    url = f'https://api.mapbox.com/geocoding/v5/mapbox.places/{address}.json'
    params = {
        'access_token': MAPBOX_TOKEN,
        'limit': 1,
        'language': 'zh'
    }
    try:
        response = requests.get(url, params=params)
        data = response.json()
        features = data.get('features', [])
        if features:
            lng, lat = features[0]['center']
            return lng, lat
    except Exception as e:
        print(f"ERROR: {e}")
    return None, None

def main():
    df_input = pd.read_csv(INPUT_CSV)
    unique_addresses = set()
    
    try:
        df_output = pd.read_csv(OUTPUT_CSV)
        done_addresses = set(df_output[ADDRESS_COLUMN])
    except FileNotFoundError:
        df_output = pd.DataFrame(columns=[ADDRESS_COLUMN, 'longitude', 'latitude'])
        done_addresses = set()

    results = []

    for _, row in df_input.iterrows():
        addr = str(row[ADDRESS_COLUMN]).strip()

        if addr in done_addresses:
            print(f"Already exists in output, skipped: {addr}")
            continue
        if addr in unique_addresses:
            print(f"Duplicate address in input, skipped: {addr}")
            continue

        unique_addresses.add(addr)

        # transfer
        lon, lat = geocode_address(addr)
        if lat is not None and lon is not None:
            results.append({ADDRESS_COLUMN: addr, 'longitude': lon, 'latitude': lat})
        else:
            print(f"Address not found: {addr}")

        time.sleep(0.2)

    if results:
        df_new = pd.DataFrame(results)

        # 在合併之前，過濾掉全是NA的欄位
        df_new = df_new.dropna(axis=1, how='all')

        # 合併 df_output 和 df_new
        df_combined = pd.concat([df_output, df_new], ignore_index=True)
        
        # 輸出到 CSV
        df_combined.to_csv(OUTPUT_CSV, index=False)

        print(f"\nSuccessfully written to: {OUTPUT_CSV} (Added {len(results)} entries)")
    else:
        print("\nNo new data added.")

if __name__ == "__main__":
    main()
