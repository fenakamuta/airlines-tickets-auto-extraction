import pandas as pd
from os import environ
from dotenv import load_dotenv
from datetime import datetime
from latam.flights import extract_flights_data

load_dotenv()

origin = environ.get("LATAM_FLIGHT_ORIGIN_NAME", "São Paulo")
destination = environ.get("LATAM_FLIGHT_DESTINATION_NAME", "Rio de Janeiro")
origin_airport = environ.get("LATAM_FLIGHT_ORIGIN_AIRPORT", "GRU")
destination_airport = environ.get("LATAM_FLIGHT_DESTINATION_AIRPORT", "GIG")
flight_date_start = environ.get("LATAM_FLIGHT_DATE_START", "2025-06-30")
flight_date_end = environ.get("LATAM_FLIGHT_DATE_END", "2025-07-07")
GOOGLE_APPLICATION_CREDENTIALS = environ.get("GOOGLE_APPLICATION_CREDENTIALS")


if __name__ == "__main__":
    flights = extract_flights_data(
        origin,
        destination,
        origin_airport,
        destination_airport,
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
        blob_name = f"latam/{current_date}/{origin_airport}_to_{destination_airport}.csv"
        
        blob = bucket.blob(blob_name)
        blob.upload_from_string(df.to_csv(index=False))