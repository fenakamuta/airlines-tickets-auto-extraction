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
    flight_date: str
) -> list[dict]:
    """
    Extract flight data from LATAM Airlines website for a single date.
    
    Args:
        origin: Origin city name (e.g., 'São Paulo')
        destination: Destination city name (e.g., 'Rio de Janeiro')
        origin_airport: Origin airport code (e.g., 'GRU')
        destination_airport: Destination airport code (e.g., 'GIG')
        flight_date: Date in 'YYYY-MM-DD' format
        
    Returns:
        List of flight dictionaries containing flight information
    """
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        
        # Initial cookie and modal bypass
        page.goto("https://latamairlines.com/br/pt")
        click_if_present(page, '#cookies-politics-button')
        click_if_present(page, '#button-close-login-incentive')

        print(f"Processing date: {flight_date}")
        
        url = build_latam_url(origin_airport, destination_airport, flight_date)
        
        try:
            page.goto(url)
        except Exception as e:
            print(f"Failed to navigate to {url}: {e}")
            browser.close()
            return []

        try:
            page.wait_for_selector(
                'ol[aria-label="Voos disponíveis."]', 
                timeout=15000
            )
            print("Page loaded successfully")
        except Exception as e:
            print(f"Warning: Flights list not found for date {flight_date}: {e}")
            browser.close()
            return []
        
        try:
            flights = extract_flight_cards(
                page, 
                flight_date, 
                origin, 
                destination,
            )
            print(f"Found {len(flights)} flights for {flight_date}")
        except Exception as e:
            print(f"Error extracting flights for {flight_date}: {e}")
            browser.close()
            return []
        
        browser.close()
        
    return flights