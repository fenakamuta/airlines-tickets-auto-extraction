import pandas as pd
from os import environ
from dotenv import load_dotenv
from datetime import datetime
from latam.flights import extract_flights_data

load_dotenv()

flight_origin = environ.get("FLIGHT_ORIGIN", "GRU")
flight_destination = environ.get("FLIGHT_DESTINATION", "GIG")
flight_date_start = environ.get("FLIGHT_DATE_START", "2025-06-30")
flight_date_end = environ.get("FLIGHT_DATE_END", "2025-07-07")

GOOGLE_APPLICATION_CREDENTIALS = environ.get("GOOGLE_APPLICATION_CREDENTIALS")

if __name__ == "__main__":
    flights = extract_flights_data(
        flight_origin,
        flight_destination,
        flight_date_start,
        flight_date_end
    )

    df = pd.DataFrame(flights)
    df.to_csv('flights.csv', index=False)
    
    if GOOGLE_APPLICATION_CREDENTIALS:
        from google.cloud import storage
        client = storage.Client()
        bucket_name = "ticket-airlines-auto-extract"

        # Create bucket if it doesn't exist
        bucket = client.bucket(bucket_name)
        if not bucket.exists():
            bucket.create()
        
        # Create blob name with current date and origin/destination
        current_date = datetime.now().strftime("%Y-%m-%d")
        blob_name = f"latam/{current_date}/{flight_origin}_to_{flight_destination}.csv"
        
        blob = bucket.blob(blob_name)
        blob.upload_from_string(df.to_csv(index=False))