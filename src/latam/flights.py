from datetime import datetime, timedelta
from playwright.sync_api import sync_playwright
from .utils import build_latam_url, extract_flight_cards

def click_if_present(page, selector, timeout=5000):
    """Clica em um seletor, se estiver presente."""
    try:
        page.wait_for_selector(selector, timeout=timeout)
        page.click(selector)
    except:
        pass


def date_range(start: datetime, end: datetime):
    """Gera datas de start até end, inclusivo."""
    while start <= end:
        yield start
        start += timedelta(days=1)


def extract_flights_data(
    origin: str, 
    destination: str, 
    origin_airport: str, 
    destination_airport: str, 
    flight_date_start: str, 
    flight_date_end: str
) -> list[dict]:
    """
    Extract flight data from LATAM Airlines website for a given date range.
    
    Args:
        origin: Origin city name (e.g., 'São Paulo')
        destination: Destination city name (e.g., 'Rio de Janeiro')
        origin_airport: Origin airport code (e.g., 'GRU')
        destination_airport: Destination airport code (e.g., 'GIG')
        flight_date_start: Start date in 'YYYY-MM-DD' format
        flight_date_end: End date in 'YYYY-MM-DD' format
        
    Returns:
        List of flight dictionaries containing flight information
    """
    all_flights = []

    # Convert date strings to datetime objects
    start_dt = datetime.strptime(flight_date_start, "%Y-%m-%d")
    end_dt = datetime.strptime(flight_date_end, "%Y-%m-%d")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        
        # Initial cookie and modal bypass
        page.goto("https://latamairlines.com/br/pt")
        click_if_present(page, '#cookies-politics-button')
        click_if_present(page, '#button-close-login-incentive')

        for dt in date_range(start_dt, end_dt):
            date_str = dt.strftime("%Y-%m-%d")
            print(f"Processing date: {date_str}")
            
            url = build_latam_url(origin_airport, destination_airport, date_str)
            page.goto(url)

            try:
                page.wait_for_selector(
                    'ol[aria-label="Voos disponíveis."]', 
                    timeout=30000
                )
                print("Page loaded successfully")
            except Exception as e:
                print(f"Warning: Flights list not found for date {date_str}: {e}")
                continue
            
            current_flights = extract_flight_cards(
                page, 
                date_str, 
                origin, 
                destination,
            )
            print(f"Found {len(current_flights)} flights for {date_str}")
            all_flights.extend(current_flights)
            
        browser.close()
        
    return all_flights