import re
from uuid import uuid4
from datetime import datetime
from typing import List, Dict, Optional
from urllib.parse import urlencode


def build_latam_url(
    origin: str,
    destination: str,
    date: Optional[str] = None,        # no formato 'YYYY-MM-DD', usa data atual se None
    adults: int = 1,
    children: int = 0,
    infants: int = 0,
    trip: str = "OW",        # "OW" ou "RT"
    cabin: str = "Economy"
) -> str:
    """
    Retorna a URL de busca de ofertas Latam com os parâmetros desejados.
    """
    # usa data atual se nenhuma data for fornecida
    if date is None:
        date = datetime.now().strftime("%Y-%m-%d")
    
    # converte '2025-07-03' → '2025-07-03T00:00:00.000Z'
    outbound_ts = datetime.strptime(date, "%Y-%m-%d") \
                        .strftime("%Y-%m-%dT00:00:00.000Z")

    params = {
        "origin": origin,
        "destination": destination,
        "outbound": outbound_ts,
        "adt": adults,
        "chd": children,
        "inf": infants,
        "trip": trip,
        "cabin": cabin,
        "redemption": "false",
        "sort": "RECOMMENDED",
        # exp_id não é obrigatório para carregar resultados,
        # mas ajuda a simular uma sessão real
        "exp_id": str(uuid4()),
    }

    return "https://www.latamairlines.com/br/pt/oferta-voos?" + urlencode(params)


from typing import List, Dict
from datetime import datetime
import re

from typing import List, Dict
from datetime import datetime, timedelta
import re

from typing import List, Dict
from datetime import datetime, timedelta
import re

def extract_flight_cards(page, flight_date: str) -> List[Dict]:
    """
    Extrai os cards de voo convertendo:
     - depart_time / arrive_time → datetime.datetime
     - duration                → float (horas)

    Corrige a extração do horário de chegada quando há "+1", "+2", etc.
    """
    flights = []
    cards = page.query_selector_all(
        'li.bodyFlightsstyle__ListItemAvailableFlights-sc__sc-1g00tx2-5'
    )

    for card in cards:
        try:
            # 1) saída
            dep_str = card.query_selector(
                'div[data-testid$="-origin"] span[class*="TextHourFlight"]'
            ).inner_text().strip()     # ex: "20:15"
            origin  = card.query_selector(
                'div[data-testid$="-origin"] span[class*="TextIATA"]'
            ).inner_text().strip()     # ex: "GRU"

            # 2) duração
            dur_str = card.query_selector(
                'div[data-testid$="-duration"] span[class*="Duration-sc"]'
            ).inner_text().strip()     # ex: "9 h 10 min."

            # 3) raw de chegada (pode vir "5:40", "5:40+1" ou até "5:40 +1")
            arr_raw = card.query_selector(
                'div[data-testid$="-destination"] span[class*="TextHourFlight"]'
            ).inner_text().strip()

            # isola "HH:MM"
            m_time = re.search(r'(\d{1,2}:\d{2})', arr_raw)
            if not m_time:
                continue
            time_part = m_time.group(1)

            # isola o número de dias a somar, se houver "+N"
            m_days = re.search(r'\+(\d+)', arr_raw)
            days_diff = int(m_days.group(1)) if m_days else 0

            dest_iata = card.query_selector(
                'div[data-testid$="-destination"] span[class*="TextIATA"]'
            ).inner_text().strip()

            # 4) preço
            price_txt = card.query_selector(
                'div[data-testid$="-amount"] span[class*="CurrencyAmount"]'
            ).inner_text()
            num = re.sub(r'[^\d,]', '', price_txt).replace('.', '').replace(',', '.')
            price = float(num)

            # 5) operadora
            operator = card.query_selector(
                'div.flightOperatorsstyles__OperatorName-sc__sc-ob3tfo-6'
            ).inner_text().strip()

            # --- conversão de datas ---
            depart_dt = datetime.strptime(f"{flight_date} {dep_str}", "%Y-%m-%d %H:%M")
            arrive_dt = (
                datetime.strptime(f"{flight_date} {time_part}", "%Y-%m-%d %H:%M")
                + timedelta(days=days_diff)
            )

            # parse da duração para horas decimais
            h_match = re.search(r'(\d+)\s*h', dur_str)
            m_match = re.search(r'(\d+)\s*m', dur_str)
            hours = int(h_match.group(1)) if h_match else 0
            mins  = int(m_match.group(1)) if m_match else 0
            duration = hours + mins / 60.0

            flights.append({
                "query_time":    datetime.now(),
                "depart_time":   depart_dt,
                "arrive_time":   arrive_dt,
                "origin":        origin,
                "destination":   dest_iata,
                "duration":      duration,
                "price":         price,
                "operator":      operator,
            })

        except Exception:
            continue

    return flights



if __name__ == "__main__":
  # exemplo de uso
  url = build_latam_url("GRU", "BPS", "2025-07-03")
  print(url)