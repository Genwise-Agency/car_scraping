import json
import logging

from playwright.sync_api import sync_playwright

from .config import BROWSER_TIMEOUT, HEADLESS_MODE
from .parser import (
    parse_battery_range,
    parse_car_id,
    parse_horse_power,
    parse_kilometers,
    parse_price,
    parse_registration_date,
)

logger = logging.getLogger(__name__)


def extract_car_data(page, link):
    """Extract all car information from a detail page"""
    car_data = {}

    # Navigate to car detail page
    page.goto(link)
    page.wait_for_timeout(3000)

    # Check if cookies need to be accepted
    try:
        accept_button = page.get_by_role("button", name="Tout accepter")
        if accept_button.is_visible(timeout=2000):
            accept_button.click()
            page.wait_for_timeout(2000)
    except:
        pass  # No cookies popup, continue

    # Model name
    try:
        model_name = page.locator('h1#stock-locator__details-heading-1').inner_text()
        car_data['model_name'] = model_name.strip()
        logger.info(f"      → model_name: {car_data['model_name']}")
    except Exception as e:
        car_data['model_name'] = None
        logger.warning(f"      → model_name: Not found ({str(e)})")

    # Car ID
    try:
        car_id_element = page.locator('div.vehicle-intro__vin')
        car_id_text = car_id_element.inner_text()
        car_id_raw = car_id_text.replace('CAR-ID', '').strip()
        car_data['car_id'] = parse_car_id(car_id_raw)
        logger.info(f"      → car_id: {car_data['car_id']} (raw: {car_id_raw})")
    except Exception as e:
        car_data['car_id'] = None
        logger.warning(f"      → car_id: Not found ({str(e)})")

    # Price
    try:
        price_element = page.locator('div.subtitle-0.price strong')
        price_text = price_element.inner_text().strip()
        car_data['price_raw'] = price_text
        car_data['price'] = parse_price(price_text)
        logger.info(f"      → price: {car_data['price']} (raw: {car_data['price_raw']})")
    except Exception as e:
        car_data['price_raw'] = None
        car_data['price'] = None
        logger.warning(f"      → price: Not found ({str(e)})")

    # Link
    car_data['link'] = link
    logger.info(f"      → link: {link}")

    # Kilometers
    try:
        mileage_key_fact = page.locator('#stock-locator__key-facts-section div.key-fact[title="Kilomètres"]')
        mileage_key_fact.wait_for(state='visible', timeout=5000)
        mileage_value = mileage_key_fact.locator('div.value-disclaimer div.value.caption').inner_text().strip()
        if not mileage_value:
            mileage_value = mileage_key_fact.locator('div.value.caption').inner_text().strip()
        car_data['kilometers_raw'] = mileage_value
        car_data['kilometers'] = parse_kilometers(mileage_value)
        logger.info(f"      → kilometers: {car_data['kilometers']} (raw: {car_data['kilometers_raw']})")
    except Exception as e:
        car_data['kilometers_raw'] = None
        car_data['kilometers'] = None
        logger.warning(f"      → kilometers: Not found ({str(e)})")

    # Registration date
    try:
        registration_key_fact = page.locator('#stock-locator__key-facts-section div.key-fact[title="Date d\'immatriculation"]')
        registration_key_fact.wait_for(state='visible', timeout=5000)
        registration_value = registration_key_fact.locator('div.value-disclaimer div.value.caption').inner_text().strip()
        if not registration_value:
            registration_value = registration_key_fact.locator('div.value.caption').inner_text().strip()
        car_data['registration_date_raw'] = registration_value
        car_data['registration_date'] = parse_registration_date(registration_value)
        logger.info(f"      → registration_date: {car_data['registration_date']} (raw: {car_data['registration_date_raw']})")
    except Exception as e:
        car_data['registration_date_raw'] = None
        car_data['registration_date'] = None
        logger.warning(f"      → registration_date: Not found ({str(e)})")

    # Horse power
    try:
        power_key_fact = page.locator('#stock-locator__key-facts-section div.key-fact[title="Power Based on Degree of Electrification"]')
        power_key_fact.wait_for(state='visible', timeout=5000)
        power_value = power_key_fact.locator('div.value-disclaimer div.value.caption').inner_text().strip()
        if not power_value:
            power_value = power_key_fact.locator('div.value.caption').inner_text().strip()
        car_data['horse_power_raw'] = power_value
        kw, ps = parse_horse_power(power_value)
        car_data['horse_power_kw'] = kw
        car_data['horse_power_ps'] = ps
        logger.info(f"      → horse_power_kw: {car_data['horse_power_kw']}, horse_power_ps: {car_data['horse_power_ps']} (raw: {car_data['horse_power_raw']})")
    except Exception as e:
        car_data['horse_power_raw'] = None
        car_data['horse_power_kw'] = None
        car_data['horse_power_ps'] = None
        logger.warning(f"      → horse_power: Not found ({str(e)})")

    # Battery range
    try:
        battery_range_container = page.locator('div[data-technical-data-key="wltpPureElectricRangeCombinedKilometer"]').locator('xpath=ancestor::div[contains(@class, "technical-data_table")]')
        battery_range_container.wait_for(state='visible', timeout=5000)
        battery_range_value = battery_range_container.locator('div.headline-5 span').inner_text().strip()
        if not battery_range_value:
            battery_range_label = page.locator('div[data-technical-data-key="wltpPureElectricRangeCombinedKilometer"]')
            battery_range_value = battery_range_label.locator('xpath=following-sibling::div[contains(@class, "headline-5")]//span').inner_text().strip()
        car_data['battery_range_raw'] = battery_range_value
        car_data['battery_range_km'] = parse_battery_range(battery_range_value)
        logger.info(f"      → battery_range_km: {car_data['battery_range_km']} (raw: {car_data['battery_range_raw']})")
    except Exception as e:
        car_data['battery_range_raw'] = None
        car_data['battery_range_km'] = None
        logger.warning(f"      → battery_range: Not found ({str(e)})")

    # Extract equipment information
    equipment_data = {}
    try:
        equipment_sections = page.locator('section.equipment-section-container')
        section_count = equipment_sections.count()

        for section_idx in range(section_count):
            try:
                equipment_section = equipment_sections.nth(section_idx)
                accordion_panels = equipment_section.locator('neo-accordion-panel')
                panel_count = accordion_panels.count()

                for i in range(panel_count):
                    panel = accordion_panels.nth(i)
                    try:
                        header = panel.locator('.content-header')
                        category_name = header.locator('.header-label').inner_text().strip()
                        equipment_items = panel.locator('div.details-card')
                        item_count = equipment_items.count()

                        equipment_list = []
                        for j in range(item_count):
                            item = equipment_items.nth(j)
                            equipment_name = item.locator('div.headline-7.tw-mb-ng-300').inner_text().strip()
                            if equipment_name:
                                equipment_list.append(equipment_name)

                        if category_name and equipment_list:
                            if category_name in equipment_data:
                                existing_items = set(equipment_data[category_name])
                                new_items = [item for item in equipment_list if item not in existing_items]
                                equipment_data[category_name].extend(new_items)
                            else:
                                equipment_data[category_name] = equipment_list
                    except Exception as e:
                        continue
            except Exception as e:
                continue

        car_data['equipments'] = json.dumps(equipment_data, ensure_ascii=False, indent=2) if equipment_data else None
        if car_data['equipments']:
            equipment_count = sum(len(items) for items in equipment_data.values())
            logger.info(f"      → equipments: Found {len(equipment_data)} categories with {equipment_count} total items")
        else:
            logger.warning(f"      → equipments: Not found")
    except Exception as e:
        car_data['equipments'] = None
        logger.warning(f"      → equipments: Error extracting ({str(e)})")

    return car_data


def scrape_bmw_inventory(url, max_links=None):
    """Scrape BMW inventory and return list of car links and extracted data"""
    all_cars_data = []

    with sync_playwright() as p:
        logger.info("[1/4] Launching browser...")
        browser = p.chromium.launch(headless=HEADLESS_MODE)
        page = browser.new_page()

        logger.info(f"[2/4] Navigating to URL...")
        logger.info(f"      {url[:80]}...")
        page.goto(url, wait_until='networkidle', timeout=60000)

        # Wait for and click the accept cookies button
        logger.info("[3/4] Waiting for cookies popup...")
        try:
            accept_button = page.get_by_role("button", name="Tout accepter")
            accept_button.wait_for(state='visible', timeout=BROWSER_TIMEOUT)
            logger.info("      ✓ Cookies popup found, accepting...")
            accept_button.click()
            # Wait for page to reload/update after accepting cookies
            page.wait_for_timeout(2000)
            # Wait for network to be idle again after cookie acceptance
            try:
                page.wait_for_load_state('networkidle', timeout=10000)
            except Exception:
                pass  # Continue if networkidle times out
            page.wait_for_timeout(3000)
            logger.info("      ✓ Cookies accepted, page loaded")
        except Exception as e:
            logger.warning(f"      ⚠ Cookies popup not found or already accepted: {e}")
            # Wait a bit for page to stabilize
            page.wait_for_timeout(5000)
            # Ensure page is fully loaded
            try:
                page.wait_for_load_state('networkidle', timeout=10000)
            except Exception:
                pass

        # Wait for page content to load - wait for actual car listing container
        logger.info("[4/4] Loading all car listings...")

        # Wait for the results container or any car listing to appear
        try:
            # Try waiting for the results container
            results_container = page.locator('[class*="results"]').or_(page.locator('[class*="listing"]')).or_(page.locator('[class*="model-card"]'))
            results_container.first.wait_for(state='attached', timeout=15000)
            logger.info("      ✓ Results container detected")
        except Exception:
            logger.warning("      ⚠ Results container not found, continuing anyway...")

        # Additional wait for JavaScript to render content
        page.wait_for_timeout(5000)

        # Try to wait for any car listing links to appear using JavaScript
        try:
            # Wait for at least one link with "details" in href to appear
            page.wait_for_function(
                "document.querySelectorAll('a[href*=\"details\"]').length > 0",
                timeout=10000
            )
            logger.info("      ✓ Car detail links detected via JavaScript")
        except Exception:
            logger.warning("      ⚠ No car detail links detected via JavaScript, continuing...")

        # Additional wait after JavaScript detection
        page.wait_for_timeout(3000)

        # Scroll to trigger lazy loading
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(3000)
        page.evaluate("window.scrollTo(0, 0)")
        page.wait_for_timeout(2000)

        # Scroll down and click "Montrer plus" button until it's no longer visible
        show_more_button = page.locator('[data-test="stolo-plp-show-more-button"]')
        click_count = 0
        max_clicks = 50  # Safety limit

        while click_count < max_clicks:
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(2000)

            try:
                show_more_button.wait_for(state='visible', timeout=5000)
                show_more_button.scroll_into_view_if_needed()
                click_count += 1
                logger.info(f"      → Clicking 'Montrer plus' button (click #{click_count})...")
                show_more_button.click()
                page.wait_for_timeout(4000)  # Increased wait time
                logger.info(f"      ✓ Content loaded (click #{click_count} completed)")
            except Exception:
                logger.info(f"      ✓ No more 'Montrer plus' buttons found. Total clicks: {click_count}")
                break

        # Wait a bit more for any lazy-loaded content
        page.wait_for_timeout(3000)

        # Final scroll to ensure all content is loaded
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(2000)

        # Extract all model card links
        logger.info("[Extracting links] Finding all car detail links...")

        # Try multiple selectors in case the page structure is different
        selectors_to_try = [
            'a.model-card-link',
            'a[href*="/sl/stocklocator_uc/details"]',
            'a[href*="stocklocator_uc/details"]',
            'a[href*="/details"]',
            '[class*="model-card"] a',
            '[class*="vehicle-card"] a',
            '[class*="vehicle"] a[href*="details"]',
            '[class*="car"] a[href*="details"]',
            '[class*="listing"] a[href*="details"]',
            '[class*="result"] a[href*="details"]',
            'a[href*="stocklocator"][href*="details"]',
            'a[href*="/fr-be/sl/stocklocator_uc/details"]',
            'a[href*="bmw.be"][href*="details"]'
        ]

        model_card_links = None
        count = 0

        for selector in selectors_to_try:
            logger.info(f"      Trying selector: {selector}")
            model_card_links = page.locator(selector)
            count = model_card_links.count()
            if count > 0:
                logger.info(f"      ✓ Found {count} elements with selector: {selector}")
                break
            logger.info(f"      ✗ No elements found with selector: {selector}")

        if count == 0:
            # Debug: log page content to understand what's available
            logger.warning("      ⚠ No car listings found. Debugging page content...")
            try:
                page_title = page.title()
                page_url = page.url
                logger.info(f"      Page title: {page_title}")
                logger.info(f"      Page URL: {page_url}")

                # Try to find any links on the page
                all_links = page.locator('a[href]')
                all_links_count = all_links.count()
                logger.info(f"      Total links on page: {all_links_count}")

                # Sample some links to see their structure
                if all_links_count > 0:
                    logger.info("      Sampling first 10 links to understand structure:")
                    for i in range(min(10, all_links_count)):
                        try:
                            link = all_links.nth(i)
                            href = link.get_attribute('href')
                            text = link.inner_text()[:50] if link.inner_text() else ""
                            logger.info(f"        Link {i+1}: href='{href[:80] if href else 'None'}', text='{text}'")
                        except Exception:
                            pass

                # Check for common BMW page elements
                body_text = page.locator('body').inner_text()
                if 'Aucun résultat' in body_text or 'No results' in body_text:
                    logger.warning("      ⚠ Page indicates 'No results' - filters may be too restrictive")
                elif 'résultats' in body_text.lower() or 'results' in body_text.lower():
                    logger.info("      ✓ Page contains 'results' text")

                # Try to find links that contain "details" or "stocklocator" in href
                detail_links = page.locator('a[href*="details"]')
                detail_count = detail_links.count()
                logger.info(f"      Links containing 'details' in href: {detail_count}")

                stocklocator_links = page.locator('a[href*="stocklocator"]')
                stocklocator_count = stocklocator_links.count()
                logger.info(f"      Links containing 'stocklocator' in href: {stocklocator_count}")

                # Try to find any div or section that might contain car listings
                car_containers = page.locator('[class*="vehicle"], [class*="car"], [class*="listing"], [class*="result"], [id*="result"]')
                container_count = car_containers.count()
                logger.info(f"      Potential car containers found: {container_count}")

            except Exception as e:
                logger.warning(f"      ⚠ Could not debug page content: {e}")

        logger.info(f"      Found {count} model card elements")

        links = []

        # If still no links found, try JavaScript evaluation as last resort
        links_found_via_js = False
        if model_card_links is None or count == 0:
            logger.warning("      ⚠ No model card links found with selectors, trying JavaScript evaluation...")
            try:
                # Use JavaScript to find all links containing "details" and "stocklocator"
                js_links = page.evaluate("""
                    () => {
                        const links = Array.from(document.querySelectorAll('a[href]'));
                        return links
                            .filter(link => {
                                const href = link.getAttribute('href') || '';
                                return href.includes('details') && href.includes('stocklocator');
                            })
                            .map(link => link.getAttribute('href'));
                    }
                """)

                if js_links and len(js_links) > 0:
                    logger.info(f"      ✓ Found {len(js_links)} links via JavaScript evaluation")
                    # Convert relative URLs to absolute
                    for href in js_links:
                        if href:
                            if href.startswith('/'):
                                full_url = f"https://www.bmw.be{href}"
                            elif href.startswith('http'):
                                full_url = href
                            else:
                                full_url = f"https://www.bmw.be/{href}"
                            links.append(full_url)
                    count = len(links)
                    links_found_via_js = True
                    logger.info(f"      ✓ Extracted {count} car detail links via JavaScript")
                else:
                    logger.warning("      ⚠ No model card links found - cannot extract car listings")
            except Exception as e:
                logger.warning(f"      ⚠ JavaScript evaluation failed: {e}")
                logger.warning("      ⚠ No model card links found - cannot extract car listings")

        # Extract links using selectors if we didn't find them via JavaScript
        if not links_found_via_js and count > 0 and model_card_links is not None:
            for i in range(count):
                try:
                    href = model_card_links.nth(i).get_attribute('href')
                    if href:
                        if href.startswith('/'):
                            full_url = f"https://www.bmw.be{href}"
                        else:
                            full_url = href
                        links.append(full_url)

                    if (i + 1) % 10 == 0:
                        logger.info(f"      Processed {i + 1}/{count} links...")
                except Exception as e:
                    logger.warning(f"      ⚠ Error extracting link {i+1}: {e}")
                    continue

        logger.info("=" * 60)
        logger.info(f"SUMMARY: Found {len(links)} car detail links")
        logger.info("=" * 60)

        # Process car links
        logger.info("=" * 60)
        logger.info("PROCESSING CARS...")
        logger.info("=" * 60)

        test_links = links[:max_links] if max_links else links
        logger.info(f"Processing {len(test_links)} out of {len(links)} total links")

        for idx, link in enumerate(test_links, 1):
            logger.info(f"[{idx}/{len(test_links)}] Processing car {idx}...")
            logger.info(f"      Link: {link[:80]}...")

            try:
                car_data = extract_car_data(page, link)
                all_cars_data.append(car_data)
                logger.info(f"      ✓ Car {idx} data extracted successfully")
                if car_data.get('model_name'):
                    logger.info(f"      → Model: {car_data['model_name']}")
            except Exception as e:
                logger.error(f"      ✗ Error processing car {idx}: {str(e)}")
                error_data = {'link': link, 'error': str(e)}
                all_cars_data.append(error_data)

            page.wait_for_timeout(1000)

        logger.info(f"      ✓ Successfully processed {len(all_cars_data)} cars")

        browser.close()

    return all_cars_data
