import logging
from datetime import date, timedelta

import requests
from requests.exceptions import RequestException


class EnerginetDataClient:
    def __init__(self):
        self.link = "https://api.energidataservice.dk/dataset/DayAheadPrices"

    def load_data(self) -> list:
        # 1. Calculate the dynamic dates
        try:
            today = date.today()
            yesterday = today - timedelta(days=1)
            tomorrow = today + timedelta(days=1)
            day_after_tomorrow = today + timedelta(days=2)

            # 2. Format them into strings (YYYY-MM-DD)
            # .isoformat() is the key function here
            start_date = tomorrow.isoformat()
            end_date = day_after_tomorrow.isoformat()

            # 3. Build the dynamic URL
            price_area = "DK2"
            url = self.link

            params = {
                "start": f"{start_date}T00:00",
                "end": f"{end_date}T00:00",
                "filter": f'{{"PriceArea":["{price_area}"]}}',  # Note: Filter syntax might need JSON string
                "sort": "TimeUTC asc",
            }

            # 4. Make the call
            response = requests.get(url, params=params)
            response.raise_for_status()

            data = response.json()
            logging.info(
                f"Data fetched {len(data)} records for {start_date}"
            )
            return data["records"]
        except RequestException as e:
            # This catches network errors, timeouts, and bad status codes
            logging.error(f"API request failed: {e}")
            return {}
        except Exception as e:
            logging.error(f"Error fetching data: {e}")
            return {}


if __name__ == "__main__":
    client = EnerginetDataClient()
    data = client.load_data()
    print(data)
