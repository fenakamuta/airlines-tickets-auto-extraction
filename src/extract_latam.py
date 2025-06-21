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
flight_date_start = environ.get("LATAM_FLIGHT_DATE_START_", "2025-06-30")
flight_date_end = environ.get("LATAM_FLIGHT_DATE_END_", "2026-01-01")
GOOGLE_APPLICATION_CREDENTIALS = environ.get("GOOGLE_APPLICATION_CREDENTIALS")


if __name__ == "__main__":
    from datetime import datetime, timedelta
    
    # Convert string dates to datetime objects
    start_date = datetime.strptime(flight_date_start, "%Y-%m-%d")
    end_date = datetime.strptime(flight_date_end, "%Y-%m-%d")
    
    # Iterate through date range
    current_date_iter = start_date
    while current_date_iter <= end_date:
        flight_date = current_date_iter.strftime("%Y-%m-%d")
        
        print(f"Processing date: {flight_date}")
        
        flights = extract_flights_data(
            origin,
            destination,
            origin_airport,
            destination_airport,
            flight_date
        )

        if flights:  # Only process if flights were found
            df = pd.DataFrame(flights)
            if GOOGLE_APPLICATION_CREDENTIALS:
                from google.cloud import storage
                client = storage.Client()
                bucket_name = "ticket-airlines-auto-extract"

                # Create bucket if it doesn't exist
                bucket = client.bucket(bucket_name)
                if not bucket.exists():
                    bucket.create()
                
                # Create blob name with current date, origin/destination, and flight date
                current_date = datetime.now().strftime("%Y-%m-%d")
                blob_name = f"latam/{current_date}/{origin_airport}_to_{destination_airport}_{flight_date}.csv"
                
                blob = bucket.blob(blob_name)
                blob.upload_from_string(df.to_csv(index=False))
                print(f"Uploaded {blob_name} to Google Cloud Storage")
        else:
            print(f"No flights found for {flight_date}")
        
        # Move to next date
        current_date_iter += timedelta(days=1)