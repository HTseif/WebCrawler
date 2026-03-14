"""
crawler.py
==========
Crawls Data.gov for the first 100 pages (20 datasets/page = 2000 datasets).

Strategy
--------
1. PRIMARY  – CKAN REST API  (catalog.data.gov/api/3/action/package_search)
   Returns rich JSON: dataset metadata, organization, resources, tags, groups, extras.
2. FALLBACK – BeautifulSoup HTML scraping of the listing pages + detail pages,
   used when the API is unreachable or returns an error.

Output
------
  ../output/raw_datasets.json   – list of normalized dataset dicts
  ../output/raw_orgs.json       – deduplicated organization dicts

Run
---
  pip install requests beautifulsoup4 lxml
  python crawler.py
"""

import json
import logging
import os
import sys
import time
from typing import Optional

import requests
from bs4 import BeautifulSoup

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join(os.path.dirname(__file__), "crawler.log")),
    ],
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
BASE_API    = "https://catalog.data.gov/api/3/action"
BASE_HTML   = "https://catalog.data.gov/dataset"
PAGE_SIZE   = 20
TOTAL_PAGES = 100          # 100 pages × 20 = 2 000 datasets
REQUEST_DELAY = 0.6        # seconds between requests (be polite)
TIMEOUT     = 20
HEADERS     = {"User-Agent": "DataPortalCrawler/1.0 (academic research)"}

OUT_DIR      = os.path.join(os.path.dirname(__file__), "../output")
RAW_DS_FILE  = os.path.join(OUT_DIR, "raw_datasets.json")
RAW_ORG_FILE = os.path.join(OUT_DIR, "raw_orgs.json")

# ── Session ───────────────────────────────────────────────────────────────────
session = requests.Session()
session.headers.update(HEADERS)

org_cache: dict[str, dict] = {}


# ─────────────────────────────────────────────────────────────────────────────
# CKAN API helpers
# ─────────────────────────────────────────────────────────────────────────────

def api_search(start: int, rows: int = PAGE_SIZE) -> Optional[list[dict]]:
    """
    Call CKAN package_search.
    Returns list of raw package dicts, or None on failure.
    """
    try:
        resp = session.get(
            f"{BASE_API}/package_search",
            params={
                "rows":            rows,
                "start":           start,
                "sort":            "metadata_created desc",
                "include_private": False,
            },
            timeout=TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("success"):
            log.info(f"  API: fetched {len(data['result']['results'])} datasets "
                     f"(start={start})")
            return data["result"]["results"]
    except Exception as exc:
        log.warning(f"  API search failed (start={start}): {exc}")
    return None


def api_get_org(org_id: str) -> dict:
    """
    Fetch full organization record from CKAN.
    Returns dict (possibly empty on failure). Results are cached.
    """
    if org_id in org_cache:
        return org_cache[org_id]
    try:
        resp = session.get(
            f"{BASE_API}/organization_show",
            params={"id": org_id, "include_datasets": False},
            timeout=TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("success"):
            org = data["result"]
            # Pull extras for contact info
            extras = {e["key"]: e.get("value", "")
                      for e in (org.get("extras") or [])}
            org["_contact_email"] = extras.get("contact_email",
                                    extras.get("email", ""))
            org["_contact_name"]  = extras.get("contact_name", "")
            org["_contact_phone"] = extras.get("contact_phone",
                                    extras.get("phone", ""))
            org_cache[org_id] = org
            time.sleep(REQUEST_DELAY)
            return org
    except Exception as exc:
        log.debug(f"  Org fetch failed ({org_id}): {exc}")
    org_cache[org_id] = {}
    return {}


# ─────────────────────────────────────────────────────────────────────────────
# BeautifulSoup HTML fallback
# ─────────────────────────────────────────────────────────────────────────────

def html_scrape_listing(page: int) -> list[dict]:
    """
    Scrape the dataset listing page (page number 1-based).
    Returns list of minimal dataset dicts.
    """
    url = f"{BASE_HTML}?page={page}"
    datasets = []
    try:
        resp = session.get(url, timeout=TIMEOUT)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")

        for card in soup.select("li.dataset-item"):
            ds: dict = {"source": "html"}

            title_a = card.select_one("h3.dataset-heading a")
            if title_a:
                ds["title"] = title_a.get_text(strip=True)
                ds["url"]   = "https://catalog.data.gov" + title_a.get("href", "")
                ds["name"]  = title_a.get("href", "").rstrip("/").rsplit("/", 1)[-1]

            desc = card.select_one(".notes")
            ds["notes"] = desc.get_text(strip=True) if desc else ""

            org = card.select_one(".organization-title")
            ds["org_title"] = org.get_text(strip=True) if org else ""

            ds["tags"] = [t.get_text(strip=True) for t in card.select(".tag")]
            ds["formats"] = [f.get_text(strip=True) for f in card.select(".format-label")]

            dt = card.select_one("time")
            ds["metadata_modified"] = dt.get("datetime", "") if dt else ""

            datasets.append(ds)

        log.info(f"  HTML: scraped {len(datasets)} cards from page {page}")
    except Exception as exc:
        log.warning(f"  HTML scrape failed (page={page}): {exc}")
    return datasets


def html_scrape_detail(url: str) -> dict:
    """
    Scrape individual dataset page for resources, license, extras, org contacts.
    """
    detail: dict = {"resources": [], "extras": {}}
    try:
        resp = session.get(url, timeout=TIMEOUT)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")

        # License
        lic = soup.select_one(".license a, #dataset-license")
        detail["license_title"] = lic.get_text(strip=True) if lic else ""

        # Resources
        for row in soup.select(".resource-list li"):
            a   = row.select_one("a.resource-url-analytics, a[href]")
            fmt = row.select_one(".format-label")
            detail["resources"].append({
                "url":    a["href"] if a else "",
                "name":   a.get_text(strip=True) if a else "",
                "format": fmt.get_text(strip=True) if fmt else "",
            })

        # Sidebar extras
        for row in soup.select(".additional-info tr"):
            cells = row.find_all(["th", "td"])
            if len(cells) >= 2:
                detail["extras"][cells[0].get_text(strip=True)] = \
                    cells[1].get_text(strip=True)

        # Organization about page link
        org_a = soup.select_one(".organization-title a, .publisher a")
        if org_a and org_a.get("href"):
            detail["org_url"] = "https://catalog.data.gov" + org_a["href"]

    except Exception as exc:
        log.debug(f"  Detail scrape failed ({url}): {exc}")
    return detail


def html_scrape_org(org_url: str) -> dict:
    """
    Scrape organization about page for description and contact info.
    """
    org: dict = {}
    try:
        resp = session.get(org_url, timeout=TIMEOUT)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")

        desc = soup.select_one(".organization-about, .notes")
        org["description"] = desc.get_text(strip=True) if desc else ""

        for row in soup.select(".additional-info tr"):
            cells = row.find_all(["th", "td"])
            if len(cells) >= 2:
                key = cells[0].get_text(strip=True).lower()
                val = cells[1].get_text(strip=True)
                if "email" in key:
                    org["contact_email"] = val
                elif "contact" in key or "name" in key:
                    org["contact_name"] = val
                elif "phone" in key:
                    org["contact_phone"] = val
    except Exception as exc:
        log.debug(f"  Org HTML scrape failed ({org_url}): {exc}")
    return org


# ─────────────────────────────────────────────────────────────────────────────
# Normalize → schema-aligned dict
# ─────────────────────────────────────────────────────────────────────────────

def normalize(raw: dict) -> dict:
    """
    Normalize a raw CKAN package dict into fields that map directly
    to the data_portal schema tables.
    """
    org       = raw.get("organization") or {}
    resources = raw.get("resources")    or []
    tags      = raw.get("tags")         or []
    groups    = raw.get("groups")       or []
    extras    = {e["key"]: e.get("value", "")
                 for e in (raw.get("extras") or [])}

    # Fetch full org record for contact details (may be cached)
    full_org: dict = {}
    if org.get("id"):
        full_org = api_get_org(org["id"])

    # ── Organization fields ────────────────────────────────────────────────
    organization = {
        "organization_uuid": org.get("id", ""),
        "name":              org.get("title", org.get("name", "")),
        "description":       (full_org.get("description") or
                              org.get("description", "")),
        "organization_type": org.get("type", ""),
        "contact_email":     (full_org.get("_contact_email") or
                              extras.get("contact_email", "")),
        "contact_name":      (full_org.get("_contact_name") or
                              extras.get("contact_name", "")),
        "contact_phone":     (full_org.get("_contact_phone") or
                              extras.get("contact_phone", "")),
    }

    # ── Dataset fields ─────────────────────────────────────────────────────
    dataset = {
        "dataset_uuid":      raw.get("id", ""),
        "organization_uuid": org.get("id", ""),
        "name":              raw.get("title", raw.get("name", "")),
        "description":       raw.get("notes", ""),
        "access_level":      extras.get("access_level", "public"),
        "license":           raw.get("license_title", raw.get("license_id", "")),
        "metadata_created":  raw.get("metadata_created", ""),
        "metadata_modified": raw.get("metadata_modified", ""),
        "maintainer_email":  (raw.get("maintainer_email") or
                              extras.get("contact_email", "")),
        "maintainer_name":   (raw.get("maintainer") or
                              extras.get("contact_name", "")),
        "identifier":        (extras.get("identifier") or
                              raw.get("name", raw.get("id", ""))),
    }

    # ── Tags ───────────────────────────────────────────────────────────────
    tag_names = list({
        t.get("display_name", t.get("name", "")).strip().lower()
        for t in tags
        if t.get("display_name") or t.get("name")
    })

    # ── Topics (from groups/categories) ───────────────────────────────────
    topic_list = []
    for g in groups:
        topic_name = (g.get("display_name") or g.get("title") or
                      g.get("name", "")).strip()
        if topic_name:
            topic_list.append({
                "topic_name": topic_name,
                "category":   g.get("title", topic_name),
            })
    # Also extract theme extra as topic
    theme = extras.get("theme", "")
    if theme and theme not in {t["topic_name"] for t in topic_list}:
        topic_list.append({"topic_name": theme, "category": theme})

    # ── Resources → Format table ───────────────────────────────────────────
    formats = []
    for r in resources:
        url = r.get("url", "").strip()
        fmt = r.get("format", "").strip()
        if url:
            formats.append({
                "file_url":     url,
                "dataset_uuid": raw.get("id", ""),
                "file_format":  fmt,
            })

    return {
        "organization": organization,
        "dataset":      dataset,
        "tags":         tag_names,
        "topics":       topic_list,
        "formats":      formats,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Main crawl loop
# ─────────────────────────────────────────────────────────────────────────────

def crawl() -> tuple[list[dict], dict[str, dict]]:
    """
    Crawl 100 pages. Returns (normalized_datasets, orgs_by_uuid).
    """
    all_normalized: list[dict] = []
    orgs_by_uuid:   dict[str, dict] = {}

    for page in range(1, TOTAL_PAGES + 1):
        start = (page - 1) * PAGE_SIZE
        log.info(f"Page {page}/{TOTAL_PAGES}  (offset {start})")

        # ── Try CKAN API first ─────────────────────────────────────────────
        raw_list = api_search(start)

        if raw_list is not None:
            for raw in raw_list:
                norm = normalize(raw)
                all_normalized.append(norm)
                org = norm["organization"]
                if org["organization_uuid"]:
                    orgs_by_uuid[org["organization_uuid"]] = org
        else:
            # ── HTML fallback ──────────────────────────────────────────────
            log.info(f"  Falling back to HTML scraping for page {page}")
            cards = html_scrape_listing(page)
            for card in cards:
                # Scrape detail page for extras & resources
                detail = {}
                if card.get("url"):
                    time.sleep(REQUEST_DELAY)
                    detail = html_scrape_detail(card["url"])

                # Try to get org info
                org_html = {}
                if detail.get("org_url"):
                    time.sleep(REQUEST_DELAY)
                    org_html = html_scrape_org(detail["org_url"])

                import uuid as _uuid
                ds_id = str(_uuid.uuid4())

                norm = {
                    "organization": {
                        "organization_uuid": str(_uuid.uuid4()),
                        "name":              card.get("org_title", ""),
                        "description":       org_html.get("description", ""),
                        "organization_type": "",
                        "contact_email":     org_html.get("contact_email", ""),
                        "contact_name":      org_html.get("contact_name", ""),
                        "contact_phone":     org_html.get("contact_phone", ""),
                    },
                    "dataset": {
                        "dataset_uuid":      ds_id,
                        "organization_uuid": "",  # linked below
                        "name":              card.get("title", ""),
                        "description":       card.get("notes", ""),
                        "access_level":      detail.get("extras", {}).get(
                                                 "Access Level", "public"),
                        "license":           detail.get("license_title", ""),
                        "metadata_created":  "",
                        "metadata_modified": card.get("metadata_modified", ""),
                        "maintainer_email":  detail.get("extras", {}).get(
                                                 "Maintainer Email", ""),
                        "maintainer_name":   detail.get("extras", {}).get(
                                                 "Maintainer", ""),
                        "identifier":        card.get("name", ds_id),
                    },
                    "tags":    card.get("tags", []),
                    "topics":  [],
                    "formats": [
                        {"file_url": r["url"],
                         "dataset_uuid": ds_id,
                         "file_format": r.get("format", "")}
                        for r in detail.get("resources", [])
                        if r.get("url")
                    ],
                }
                # Link org uuid
                norm["dataset"]["organization_uuid"] = \
                    norm["organization"]["organization_uuid"]

                all_normalized.append(norm)
                org = norm["organization"]
                if org["organization_uuid"]:
                    orgs_by_uuid[org["organization_uuid"]] = org

        time.sleep(REQUEST_DELAY)

    log.info(f"Crawl complete: {len(all_normalized)} datasets, "
             f"{len(orgs_by_uuid)} unique organizations")
    return all_normalized, orgs_by_uuid


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    os.makedirs(OUT_DIR, exist_ok=True)

    datasets, orgs = crawl()

    with open(RAW_DS_FILE,  "w", encoding="utf-8") as f:
        json.dump(datasets, f, ensure_ascii=False, indent=2)
    with open(RAW_ORG_FILE, "w", encoding="utf-8") as f:
        json.dump(list(orgs.values()), f, ensure_ascii=False, indent=2)

    log.info(f"Saved → {RAW_DS_FILE}")
    log.info(f"Saved → {RAW_ORG_FILE}")
