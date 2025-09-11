import csv
import re
import time
import random
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from urllib.parse import urljoin, urlparse, urlsplit, urlunsplit
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException


# ----------------- CONFIG -----------------
SEARCH_URL = "https://www.apartments.com/apartments/seattle-wa/"
OUTPUT_CSV = "results.csv"
MIN_YEAR = 2023
CITY_FILTER = "Seattle"
WORKERS = 1         # <-- set to 1 while debugging so only one browser opens
HEADLESS = True   # <-- set False so Chrome opens visibly
MAX_LISTINGS = None
REQUEST_DELAY = (0.5, 1.0)
PAGE_LOAD_TIMEOUT = 25
# ------------------------------------------


csv_lock = Lock()


def normalize_url(u: str) -> str:
    """Normalize a URL for deduplication:
    - drop query and fragment
    - normalize scheme/netloc (lowercase netloc, drop leading www.)
    - collapse duplicate slashes in path and remove trailing slash
    - convert relative URLs against SEARCH_URL
    Returns empty string for unsupported schemes (javascript:, mailto:, etc.) or on failure.
    """
    try:
        if not u:
            return ""
        u = u.strip()
        if u.lower().startswith("javascript:") or u.lower().startswith("mailto:"):
            return ""
        p = urlsplit(u)
        scheme = p.scheme
        netloc = p.netloc
        # If no netloc, treat as relative and join with SEARCH_URL
        if not netloc:
            joined = urljoin(SEARCH_URL, u)
            p = urlsplit(joined)
            scheme = p.scheme
            netloc = p.netloc
        if not scheme:
            scheme = "https"
        netloc = netloc.lower()
        if netloc.startswith("www."):
            netloc = netloc[4:]
        # collapse multiple slashes in path
        path = re.sub(r"/+", "/", p.path or "/")
        path = path.rstrip("/")
        # ensure leading slash for non-empty path
        if path == "":
            path = ""
        # rebuild without query or fragment
        return urlunsplit((scheme, netloc, path, "", ""))
    except Exception:
        return ""


# Replace your old make_chrome_driver with this function:
def make_chrome_driver(headless=True):
    opts = Options()
    if headless:
        # use new headless flag; if your Chrome version doesn't support 'new', try "--headless"
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-dev-shm-usage")
    # avoid image loading to speed up
    prefs = {"profile.managed_default_content_settings.images": 2}
    opts.add_experimental_option("prefs", prefs)
    # try to reduce automation detection traces
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option('useAutomationExtension', False)
    # set page load strategy to 'eager' (DOMContentLoaded) for speed
    opts.set_capability("pageLoadStrategy", "eager")

    # Use Service(...) instead of passing the path as a positional argument
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)

    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
    driver.implicitly_wait(3)
    return driver


def collect_listing_links(driver, search_url, max_listings=None, max_pages=50):
    """
    Visit `search_url` then paginate through result pages until no Next button is found.
    Returns a deduped list of listing URLs (order preserved).
    - max_listings: stop early if we've collected this many links (None means unlimited).
    - max_pages: safety cap to avoid infinite loops.
    """
    print("Loading search page and collecting listing links (paginating)...")
    driver.get(search_url)
    wait = WebDriverWait(driver, 12)

    # candidate selectors for listing anchors (same as before)
    link_selectors = [
        "li.placard a[href]",
        "article.placard a[href]",
        ".placard a[href]",
        "a.placardTitle[href]",
        "a.property-link[href]",
        "a[href*='/apartments/']"
    ]

    collected = []
    seen = set()
    visited_page_urls = set()

    for page_index in range(1, max_pages + 1):
        current_url = driver.current_url
        print(f"--- page {page_index}: {current_url} ---")
        if current_url in visited_page_urls:
            print("Encountered previously visited page URL — stopping pagination to avoid loop.")
            break
        visited_page_urls.add(current_url)

        # gentle scrolling to encourage lazy load
        try:
            for i in range(10):
                driver.execute_script("window.scrollBy(0, window.innerHeight * 0.8);")
                time.sleep(0.25)
        except Exception:
            pass

        # try clicking "show more"/"load more" buttons on this page
        try:
            for attempt in range(3):
                buttons = driver.find_elements(By.XPATH,
                    "//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'show more') "
                    "or contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'load more') "
                    "or contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'view more')]")
                if not buttons:
                    break
                clicked_any = False
                for b in buttons:
                    try:
                        driver.execute_script("arguments[0].scrollIntoView(true);", b)
                        time.sleep(0.15)
                        b.click()
                        clicked_any = True
                        time.sleep(0.6)
                    except Exception:
                        continue
                if not clicked_any:
                    break
        except Exception:
            pass

        # short secondary scroll
        try:
            for i in range(5):
                driver.execute_script("window.scrollBy(0, 400);")
                time.sleep(0.12)
        except Exception:
            pass

        # gather links using selectors
        page_links = []
        for sel in link_selectors:
            try:
                elems = driver.find_elements(By.CSS_SELECTOR, sel)
                for e in elems:
                    try:
                        raw_href = e.get_attribute("href")
                        if not raw_href:
                            continue
                        href = normalize_url(raw_href)
                        if not href:
                            continue
                        if href not in seen:
                            seen.add(href)
                            page_links.append(href)
                            collected.append(href)
                    except Exception:
                        continue
                # if we've already collected a decent batch this page, try next page
                # (tunable)
                if len(page_links) >= 30:
                    break
            except Exception:
                continue

        # fallback: anchors containing "/apartments/"
        if not page_links:
            try:
                anchors = driver.find_elements(By.CSS_SELECTOR, "a[href*='/apartments/']")
                for a in anchors:
                    try:
                        raw_href = a.get_attribute("href")
                        if not raw_href:
                            continue
                        href = normalize_url(raw_href)
                        if not href:
                            continue
                        if href not in seen:
                            seen.add(href)
                            collected.append(href)
                            page_links.append(href)
                    except Exception:
                        continue
            except Exception:
                pass

        print(f"Collected {len(page_links)} new links on this page (total {len(collected)})")

        # stop early if we hit user's max_listings or global MAX_LISTINGS
        if max_listings and len(collected) >= max_listings:
            print(f"Reached max_listings={max_listings}; stopping pagination.")
            break
        if globals().get("MAX_LISTINGS") and globals()["MAX_LISTINGS"] and len(collected) >= globals()["MAX_LISTINGS"]:
            print(f"Reached global MAX_LISTINGS={globals()['MAX_LISTINGS']}; stopping pagination.")
            break

        # Try to find a "Next" control and click it
        next_xpaths = [
            "//a[@rel='next']",
            "//a[contains(translate(@aria-label,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'next')]",
            "//a[contains(normalize-space(.),'Next')]",
            "//a[contains(., '›') or contains(., '»') or contains(., '>')]",
            "//li[contains(@class,'next')]/a",
            "//a[contains(@class,'next')]"
        ]

        found_next = False
        for xp in next_xpaths:
            try:
                el = driver.find_element(By.XPATH, xp)
                if not el:
                    continue
                href = el.get_attribute("href")
                # scroll into view then click; if click fails, navigate to href
                try:
                    driver.execute_script("arguments[0].scrollIntoView(true);", el)
                    time.sleep(0.15)
                    el.click()
                except Exception:
                    if href:
                        driver.get(href)
                    else:
                        continue
                # wait for either URL change or page stale
                try:
                    wait.until(lambda d: d.current_url != current_url, message="waiting for URL to change")
                except Exception:
                    # fallback: wait a short moment for content to settle
                    time.sleep(1.2)
                found_next = True
                # small delay to let next page render
                time.sleep(0.6)
                break
            except Exception:
                continue

        if not found_next:
            print("No Next button found — finished paginating.")
            break

    print(f"Pagination complete. Collected {len(collected)} total links.")
    return collected


def _append_log_row(logfile, row):
    """Thread-safe append to a CSV logfile (uses csv_lock)."""
    with csv_lock:
        try:
            # if file doesn't exist, write header first
            mode = "a"
            write_header = False
            try:
                open(logfile, "r", encoding="utf-8").close()
            except FileNotFoundError:
                write_header = True
            with open(logfile, mode, newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                if write_header:
                    writer.writerow(["worker", "index", "total", "title", "address", "url", "built_year", "status", "note"])
                writer.writerow(row)
        except Exception as e:
            print("Failed to write log row:", e)


def process_link_batch(links, min_year, city_filter, output_csv, worker_id):
    """
    Improved per-visit logging and console output for verification.
    Writes two outputs:
     - results.csv (only saved rows that passed filters),
     - screened_log.csv (every visited link and its status).
    """
    driver = make_chrome_driver(headless=HEADLESS)
    results = []
    log_file = "screened_log.csv"

    for idx, link in enumerate(links, start=1):
        built_year = None
        status = "unknown"
        note = ""
        title = ""
        address = ""
        try:
            print(f"[W{worker_id}] ({idx}/{len(links)}) visiting: {link}")
            driver.get(link)
            # small random short delay to allow extra JS pieces to settle if needed
            time.sleep(random.uniform(0.2, 0.6))

            # Try to extract title and address *first* so we can log/print them regardless
            try:
                el_h1 = driver.find_elements(By.CSS_SELECTOR, "h1")
                if el_h1 and el_h1[0].text.strip():
                    title = el_h1[0].text.strip()
            except Exception:
                title = title

            try:
                addr_sel = driver.find_elements(By.CSS_SELECTOR, ".propertyAddress, .property-address, .js-address")
                if addr_sel and addr_sel[0].text.strip():
                    address = addr_sel[0].text.strip()
            except Exception:
                address = address

            # fallback: meta description
            if not address:
                try:
                    meta = driver.find_elements(By.CSS_SELECTOR, "meta[name='description']")
                    if meta:
                        content = meta[0].get_attribute("content")
                        if content:
                            address = content.strip()
                except Exception:
                    pass

            # Print immediate verification message (so you can watch names as they're checked)
            print(f"[W{worker_id}]  -> Title: {title or 'NO TITLE'} | Address: {address or 'NO ADDRESS'}")

            # page_text used for 'Built in' search
            page_text = driver.page_source

            # find "Built in ####"
            m = re.search(r'Built in\s*([0-9]{4})', page_text, flags=re.IGNORECASE)
            if not m:
                status = "no_built_in"
                note = "'Built in' phrase not found"
                print(f"[W{worker_id}]  -> {note}")
                _append_log_row(log_file, (worker_id, idx, len(links), title, address, link, "", status, note))
                continue

            built_year = int(m.group(1))
            if built_year < min_year:
                status = "too_old"
                note = f"Built in {built_year} < {min_year}"
                print(f"[W{worker_id}]  -> {note} => skip.")
                _append_log_row(log_file, (worker_id, idx, len(links), title, address, link, built_year, status, note))
                continue

            # CITY_FILTER check
            if city_filter:
                if not address or city_filter.lower() not in address.lower():
                    status = "city_mismatch"
                    note = f"Address does not contain '{city_filter}'"
                    print(f"[W{worker_id}]  -> {note} => skip.")
                    _append_log_row(log_file, (worker_id, idx, len(links), title, address, link, built_year, status, note))
                    continue

            # Passed all checks -> save
            results.append((title, address, built_year, link))
            status = "saved"
            note = f"Built in {built_year}"
            print(f"[W{worker_id}]  -> Saved: {title or 'no title'} | Built in {built_year}")

            _append_log_row(log_file, (worker_id, idx, len(links), title, address, link, built_year, status, note))

        except Exception as e:
            status = "error"
            note = str(e)
            print(f"[W{worker_id}]  -> Error visiting {link}: {e}")
            _append_log_row(log_file, (worker_id, idx, len(links), title, address, link, built_year or "", status, note))

        # polite short delay
        time.sleep(random.uniform(*REQUEST_DELAY))

    driver.quit()

    # dedupe results by normalized URL before writing
    if results:
        unique = []
        seen_res = set()
        for title, address, built_year, url in results:
            key = normalize_url(url)
            if not key or key in seen_res:
                continue
            seen_res.add(key)
            unique.append((title, address, built_year, url))
        results = unique

    # write results to CSV (thread-safe) - identical behavior as before but keep thread-safety
    if results:
        with csv_lock:
            try:
                # check if file exists; if not write header then rows
                try:
                    open(output_csv, "r", encoding="utf-8").close()
                    mode = "a"
                except FileNotFoundError:
                    mode = "w"
                with open(output_csv, mode, newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    if mode == "w":
                        writer.writerow(["title", "address", "built_year", "url"])
                    # inside the final CSV write loop in process_link_batch
                for row in results:
                    title, address, built_year, url = row
                    hyperlink_formula = f'=HYPERLINK("{url}", "{title or "Listing"}")'
                    writer.writerow([title, address, built_year, hyperlink_formula])
            except Exception as e:
                print("Failed to write results.csv:", e)


def main():
    print("Starting master driver to collect listing links...")
    master = make_chrome_driver(headless=HEADLESS)
    links = collect_listing_links(master, SEARCH_URL)
    master.quit()
    if not links:
        print("No listing links found. Double-check the SEARCH_URL or draw a map boundary and paste that URL.")
        return

    # prepare CSV file and write header (empty at start)
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["title", "address", "built_year", "url"])

    # split links into batches for workers
    n = len(links)
    workers = min(WORKERS, n)
    batch_size = math.ceil(n / workers)
    batches = [links[i*batch_size:(i+1)*batch_size] for i in range(workers)]

    print(f"Processing {n} links with {workers} workers (batch size ~= {batch_size})...")
    with ThreadPoolExecutor(max_workers=workers) as exe:
        futures = []
        for i, batch in enumerate(batches, start=1):
            if not batch:
                continue
            futures.append(exe.submit(process_link_batch, batch, MIN_YEAR, CITY_FILTER, OUTPUT_CSV, i))
        for f in as_completed(futures):
            try:
                f.result()
            except Exception as e:
                print("Worker error:", e)
    print("All done. Results (if any) saved to", OUTPUT_CSV)


if __name__ == "__main__":
    main()