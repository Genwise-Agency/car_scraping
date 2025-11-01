from playwright.sync_api import sync_playwright

url = "https://www.bmw.be/fr-be/sl/stocklocator_uc/results?filters=%257B%2522MARKETING_MODEL_RANGE%2522%253A%255B%2522i4_G26E%2522%252C%2522i5_G61E%2522%252C%2522i5_G60E%2522%255D%252C%2522PRICE%2522%253A%255Bnull%252C60000%255D%252C%2522REGISTRATION_YEAR%2522%253A%255B2024%252C-1%255D%252C%2522EQUIPMENT_GROUPS%2522%253A%257B%2522Default%2522%253A%255B%2522M%2520leather%2520steering%2520wheel%2522%255D%252C%2522favorites%2522%253A%255B%2522M%2520Sport%2520package%2522%255D%257D%257D"

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

    input("Press Enter to close the browser...")
    browser.close()

