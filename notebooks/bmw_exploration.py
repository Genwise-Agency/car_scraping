import re
from datetime import datetime

import pandas as pd
from playwright.sync_api import sync_playwright

url = "https://www.bmw.be/fr-be/sl/stocklocator_uc/results?filters=%257B%2522MARKETING_MODEL_RANGE%2522%253A%255B%2522i4_G26E%2522%252C%2522i5_G61E%2522%252C%2522i5_G60E%2522%255D%252C%2522PRICE%2522%253A%255Bnull%252C60000%255D%252C%2522REGISTRATION_YEAR%2522%253A%255B2024%252C-1%255D%252C%2522EQUIPMENT_GROUPS%2522%253A%257B%2522Default%2522%253A%255B%2522M%2520leather%2520steering%2520wheel%2522%255D%252C%2522favorites%2522%253A%255B%2522M%2520Sport%2520package%2522%255D%257D%257D"


def parse_price(price_str):
    """Convert price string like '59 950,00 €' to float like 59950.0"""
    if not price_str:
        return None
    # Remove currency symbol and spaces, replace comma with dot
    cleaned = price_str.replace('€', '').replace(' ', '').replace(',', '.').strip()
    try:
        return float(cleaned)
    except:
        return None


def parse_kilometers(km_str):
    """Convert kilometers string like '9500 km' to integer like 9500"""
    if not km_str:
        return None
    # Extract numbers only
    numbers = re.findall(r'\d+', km_str.replace(' ', ''))
    if numbers:
        try:
            return int(numbers[0])
        except:
            return None
    return None


def parse_car_id(car_id_str):
    """Convert car ID string to integer"""
    if not car_id_str:
        return None
    try:
        return int(car_id_str.strip())
    except:
        return None


def parse_horse_power(power_str):
    """Extract kW and PS from power string like '210 kW (286 PS)'"""
    if not power_str:
        return None, None
    # Extract kW value
    kw_match = re.search(r'(\d+)\s*kW', power_str)
    kw = int(kw_match.group(1)) if kw_match else None
    # Extract PS value
    ps_match = re.search(r'\((\d+)\s*PS\)', power_str)
    ps = int(ps_match.group(1)) if ps_match else None
    return kw, ps


def parse_registration_date(date_str):
    """Convert French date string like 'août 2025' to datetime object"""
    if not date_str:
        return None

    # French month names mapping
    french_months = {
        'janvier': 1, 'février': 2, 'mars': 3, 'avril': 4,
        'mai': 5, 'juin': 6, 'juillet': 7, 'août': 8,
        'septembre': 9, 'octobre': 10, 'novembre': 11, 'décembre': 12
    }

    try:
        # Extract month and year
        parts = date_str.strip().lower().split()
        if len(parts) >= 2:
            month_name = parts[0]
            year = int(parts[1])

            if month_name in french_months:
                month = french_months[month_name]
                # Create datetime object (using first day of month)
                return datetime(year, month, 1)
        return None
    except Exception as e:
        return None

print("=" * 60)
print("Starting BMW car scraping script")
print("=" * 60)

with sync_playwright() as p:
    print("\n[1/4] Launching browser...")
    browser = p.chromium.launch(headless=False)
    page = browser.new_page()

    print(f"[2/4] Navigating to URL...")
    print(f"      {url[:80]}...")
    page.goto(url)

    # Wait for and click the accept cookies button
    print("\n[3/4] Waiting for cookies popup...")
    accept_button = page.get_by_role("button", name="Tout accepter")
    accept_button.wait_for(state='visible', timeout=10000)
    print("      ✓ Cookies popup found, accepting...")
    accept_button.click()

    # Wait for page to load after accepting cookies
    page.wait_for_timeout(2000)
    print("      ✓ Cookies accepted, page loaded")

    # Scroll down and click "Montrer plus" button until it's no longer visible
    print("\n[4/4] Loading all car listings...")
    show_more_button = page.locator('[data-test="stolo-plp-show-more-button"]')
    click_count = 0

    while True:
        # Scroll down to load more content
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(2000)

        # Try to find and click the button
        try:
            show_more_button.wait_for(state='visible', timeout=5000)
            show_more_button.scroll_into_view_if_needed()
            click_count += 1
            print(f"      → Clicking 'Montrer plus' button (click #{click_count})...")
            show_more_button.click()
            # Wait for content to load after clicking
            page.wait_for_timeout(3000)
            print(f"      ✓ Content loaded (click #{click_count} completed)")
        except:
            # Button is no longer visible or doesn't exist, we're done
            print(f"      ✓ No more 'Montrer plus' buttons found. Total clicks: {click_count}")
            break

    # Extract all model card links
    print("\n[Extracting links] Finding all car detail links...")
    model_card_links = page.locator('a.model-card-link')
    links = []

    count = model_card_links.count()
    print(f"      Found {count} model card elements")

    for i in range(count):
        href = model_card_links.nth(i).get_attribute('href')
        if href:
            # Construct full URL if it's a relative path
            if href.startswith('/'):
                full_url = f"https://www.bmw.be{href}"
            else:
                full_url = href
            links.append(full_url)

        # Progress indicator every 10 links
        if (i + 1) % 10 == 0:
            print(f"      Processed {i + 1}/{count} links...")

    print("\n" + "=" * 60)
    print(f"SUMMARY: Found {len(links)} car detail links")
    print("=" * 60)
    for i, link in enumerate(links, 1):
        print(f"{i:3d}. {link}")

    # Navigate to a specific car detail page for exploration
    link = links[0]
    print("\n" + "=" * 60)
    print("Navigating to car detail page for exploration...")
    print("=" * 60)
    print(f"\n[Exploring] Opening car detail page...")
    print(f"      {link}")
    page.goto(link)
    page.wait_for_timeout(3000)
    print("      ✓ Car detail page loaded")

    # Check if cookies need to be accepted again
    try:
        accept_button = page.get_by_role("button", name="Tout accepter")
        if accept_button.is_visible(timeout=2000):
            print("      → Accepting cookies on detail page...")
            accept_button.click()
            page.wait_for_timeout(2000)
            print("      ✓ Cookies accepted")
    except:
        pass  # No cookies popup, continue

    # Extract car information
    print("\n[Extracting car data] Gathering information from detail page...")

    car_data = {}

    # Model name
    try:
        model_name = page.locator('h1#stock-locator__details-heading-1').inner_text()
        car_data['model_name'] = model_name.strip()
        print(f"      ✓ Model name: {car_data['model_name']}")
    except Exception as e:
        car_data['model_name'] = None
        print(f"      ✗ Model name: Not found")

    # Car ID
    try:
        car_id_element = page.locator('div.vehicle-intro__vin')
        car_id_text = car_id_element.inner_text()
        # Extract the ID number (e.g., "CAR-ID 39582" -> "39582")
        car_id_raw = car_id_text.replace('CAR-ID', '').strip()
        car_data['car_id'] = parse_car_id(car_id_raw)
        print(f"      ✓ Car ID: {car_id_raw} -> {car_data['car_id']} (integer)")
    except Exception as e:
        car_data['car_id'] = None
        print(f"      ✗ Car ID: Not found")

    # Price
    try:
        price_element = page.locator('div.subtitle-0.price strong')
        price_text = price_element.inner_text().strip()
        car_data['price_raw'] = price_text
        car_data['price'] = parse_price(price_text)
        print(f"      ✓ Price: {price_text} -> {car_data['price']} € (float)")
    except Exception as e:
        car_data['price_raw'] = None
        car_data['price'] = None
        print(f"      ✗ Price: Not found")

    # Link (already have it)
    car_data['link'] = link
    print(f"      ✓ Link: {link[:80]}...")

    # Kilometers
    try:
        mileage_key_fact = page.locator('div.key-fact[title="Kilomètres"]')
        mileage_value = mileage_key_fact.locator('div.value.caption').inner_text().strip()
        car_data['kilometers_raw'] = mileage_value
        car_data['kilometers'] = parse_kilometers(mileage_value)
        print(f"      ✓ Kilometers: {mileage_value} -> {car_data['kilometers']} km (integer)")
    except Exception as e:
        car_data['kilometers_raw'] = None
        car_data['kilometers'] = None
        print(f"      ✗ Kilometers: Not found")

    # Registration date
    try:
        registration_key_fact = page.locator('div.key-fact[title="Date d\'immatriculation"]')
        registration_value = registration_key_fact.locator('div.value.caption').inner_text().strip()
        car_data['registration_date_raw'] = registration_value
        car_data['registration_date'] = parse_registration_date(registration_value)
        if car_data['registration_date']:
            print(f"      ✓ Registration date: {registration_value} -> {car_data['registration_date'].strftime('%Y-%m')} (datetime)")
        else:
            print(f"      ✓ Registration date: {registration_value} (could not parse)")
    except Exception as e:
        car_data['registration_date_raw'] = None
        car_data['registration_date'] = None
        print(f"      ✗ Registration date: Not found")

    # Horse power
    try:
        power_key_fact = page.locator('div.key-fact[title="Power Based on Degree of Electrification"]')
        power_value = power_key_fact.locator('div.value.caption').inner_text().strip()
        car_data['horse_power_raw'] = power_value
        kw, ps = parse_horse_power(power_value)
        car_data['horse_power_kw'] = kw
        car_data['horse_power_ps'] = ps
        print(f"      ✓ Horse power: {power_value} -> {kw} kW, {ps} PS (integers)")
    except Exception as e:
        car_data['horse_power_raw'] = None
        car_data['horse_power_kw'] = None
        car_data['horse_power_ps'] = None
        print(f"      ✗ Horse power: Not found")

    # Create pandas DataFrame
    df = pd.DataFrame([car_data])

    # Reorder columns for better readability
    column_order = [
        'model_name', 'car_id', 'price', 'price_raw',
        'kilometers', 'kilometers_raw',
        'registration_date', 'registration_date_raw',
        'horse_power_kw', 'horse_power_ps', 'horse_power_raw',
        'link'
    ]
    # Only include columns that exist
    existing_columns = [col for col in column_order if col in df.columns]
    df = df[existing_columns]

    # Display extracted data
    print("\n" + "=" * 60)
    print("EXTRACTED CAR DATA (Raw values):")
    print("=" * 60)
    for key, value in car_data.items():
        if not key.endswith('_raw') and key not in ['price', 'kilometers', 'horse_power_kw', 'horse_power_ps', 'car_id', 'registration_date']:
            print(f"{key:20s}: {value}")
        elif key == 'registration_date' and value:
            print(f"{key:20s}: {value.strftime('%Y-%m-%d')} (datetime)")

    print("\n" + "=" * 60)
    print("PANDAS DATAFRAME:")
    print("=" * 60)
    print(df.to_string(index=False))
    print(f"\nDataFrame shape: {df.shape}")
    print(f"DataFrame dtypes:\n{df.dtypes}")

    input("\nPress Enter to close the browser...")
    browser.close()

