"""
generate_data.py
================
Since the sandbox has no external network, this script:
  1. Generates realistic mock Data.gov data (2 000 datasets, orgs, tags, etc.)
     that mirrors exactly what crawler.py would produce.
  2. Reads the users.csv (provided by the assignment) OR generates 200 sample users.
  3. Generates 500 random User_Dataset_Usage rows.
  4. Exports every table as a CSV file.
  5. Exports a complete MySQL dump (schema + INSERT statements).

All data shapes match the data_portal schema exactly.
"""

import csv
import json
import os
import random
import sqlite3
import string
import uuid
from datetime import date, datetime, timedelta

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE      = os.path.dirname(__file__)
OUT_DIR   = os.path.join(BASE, "../output")
CSV_DIR   = os.path.join(OUT_DIR, "csv")
SQL_DIR   = os.path.join(OUT_DIR, "sql")
DB_FILE   = os.path.join(OUT_DIR, "data_portal.db")   # SQLite for local testing
DUMP_FILE = os.path.join(SQL_DIR, "data_portal_dump.sql")
USERS_CSV = os.path.join(BASE, "../users.csv")         # provided by assignment

os.makedirs(CSV_DIR, exist_ok=True)
os.makedirs(SQL_DIR, exist_ok=True)

random.seed(42)

# ─────────────────────────────────────────────────────────────────────────────
# Reference data (mirrors real Data.gov content)
# ─────────────────────────────────────────────────────────────────────────────

ORG_DATA = [
    ("U.S. Department of Agriculture",        "Federal", "info@usda.gov",         "Communications Office", "+1-202-720-2791"),
    ("U.S. Department of Transportation",     "Federal", "dot.data@dot.gov",      "Data Services",         "+1-202-366-4000"),
    ("U.S. Department of Health and Human Services","Federal","data@hhs.gov",     "Open Data Team",        "+1-202-619-0257"),
    ("National Oceanic and Atmospheric Administration","Federal","noaa.data@noaa.gov","Data Management","  +1-301-713-1208"),
    ("U.S. Census Bureau",                    "Federal", "census.data@census.gov","Data Dissemination",    "+1-301-763-4636"),
    ("U.S. Environmental Protection Agency",  "Federal", "data@epa.gov",          "Data Governance",       "+1-202-564-4700"),
    ("U.S. Department of Energy",             "Federal", "opendata@hq.doe.gov",   "OpenData Program",      "+1-202-586-5000"),
    ("U.S. Department of Education",          "Federal", "eddata@ed.gov",         "Data Quality Group",    "+1-202-401-2000"),
    ("NASA",                                  "Federal", "nasa-data@nasa.gov",    "Data Services",         "+1-202-358-0001"),
    ("U.S. Geological Survey",                "Federal", "gs-w_data@usgs.gov",    "Data Management",       "+1-703-648-5953"),
    ("Federal Aviation Administration",       "Federal", "data@faa.gov",          "Statistics Division",   "+1-202-267-3484"),
    ("Centers for Disease Control",           "Federal", "cdcinfo@cdc.gov",       "Public Health Data",    "+1-800-232-4636"),
    ("National Institutes of Health",         "Federal", "nihod@nih.gov",         "Open Data Office",      "+1-301-496-4000"),
    ("U.S. Department of Labor",              "Federal", "data@dol.gov",          "Chief Data Officer",    "+1-202-693-5000"),
    ("Bureau of Labor Statistics",            "Federal", "blsdata@bls.gov",       "Data Policy",           "+1-202-691-5200"),
    ("Federal Reserve",                       "Federal", "data@federalreserve.gov","Research Data",        "+1-202-452-3000"),
    ("Social Security Administration",        "Federal", "opa@ssa.gov",           "Open Data",             "+1-410-965-2736"),
    ("U.S. Fish and Wildlife Service",        "Federal", "fws_data@fws.gov",      "Geospatial Services",   "+1-703-358-2169"),
    ("National Park Service",                 "Federal", "nps_data@nps.gov",      "Data Management",       "+1-202-208-6843"),
    ("Federal Emergency Management Agency",   "Federal", "femadata@fema.dhs.gov", "OpenFEMA",              "+1-202-646-2500"),
    ("City of New York",                      "Local",   "opendata@records.nyc.gov","Open Data Team",      "+1-212-788-2058"),
    ("City of Chicago",                       "Local",   "dataportal@cityofchicago.org","Data Analytics",  "+1-312-744-5000"),
    ("State of California",                   "State",   "opendata@state.ca.gov", "CDT Open Data",         "+1-916-431-5000"),
    ("State of Texas",                        "State",   "data@dir.texas.gov",    "Data Governance",       "+1-512-463-0500"),
    ("World Health Organization",             "International","data@who.int",      "Global Data",           "+41-22-791-2111"),
]

TOPICS = [
    ("Agriculture",       "Natural Resources"),
    ("Climate",           "Environment"),
    ("Education",         "Society"),
    ("Energy",            "Infrastructure"),
    ("Finance",           "Economy"),
    ("Health",            "Society"),
    ("Housing",           "Society"),
    ("Labor",             "Economy"),
    ("Public Safety",     "Society"),
    ("Science & Research","Technology"),
    ("Transportation",    "Infrastructure"),
    ("Water",             "Natural Resources"),
    ("Demographics",      "Society"),
    ("Business",          "Economy"),
    ("Geospatial",        "Technology"),
    ("Environment",       "Natural Resources"),
    ("Social Services",   "Society"),
    ("Elections",         "Government"),
    ("Criminal Justice",  "Society"),
    ("Weather",           "Environment"),
]

TAGS_POOL = [
    "csv","json","geojson","api","open-data","statistics","survey","annual",
    "federal","state","local","climate-change","air-quality","water-quality",
    "employment","gdp","inflation","population","census","education","health",
    "infrastructure","transportation","agriculture","energy","environment",
    "public-safety","housing","demographics","biodiversity","wildfire",
    "hurricane","flood","earthquake","covid-19","vaccination","mortality",
    "finance","tax","budget","spending","grants","contracts","procurement",
    "gis","mapping","satellite","remote-sensing","time-series","real-time",
]

FORMATS_POOL  = ["CSV","JSON","GeoJSON","XML","XLSX","PDF","ZIP","API","KML","RDF","TSV","SHP"]
ACCESS_LEVELS = ["public", "restricted public", "non-public"]
LICENSES      = [
    "U.S. Government Work", "Creative Commons CCZero",
    "Open Data Commons Public Domain", "Creative Commons Attribution",
    "Other (Open)", "License Not Specified",
]

DATASET_TITLE_TEMPLATES = [
    "{topic} Statistics by {geo} ({year})",
    "Annual {topic} Report - {agency}",
    "{agency} {topic} Dataset {year}",
    "{geo} {topic} Survey Results",
    "National {topic} Inventory {year}",
    "{topic} Indicators Dashboard",
    "{agency} Open Data: {topic}",
    "{topic} Monitoring Records {year}",
    "{geo} {topic} Trends {year}-{year2}",
    "Federal {topic} Database {year}",
]

GEOS = ["National","Regional","State-Level","County-Level","City-Level",
        "ZIP Code","Metropolitan","Rural","Urban","Coastal"]
TOPICS_FLAT = [t[0] for t in TOPICS]
AGENCIES    = [o[0].split()[0:3] for o in ORG_DATA]
AGENCIES    = [" ".join(a) for a in AGENCIES]

PROJECT_NAMES = [
    "Climate Impact Analysis","Urban Mobility Study","Public Health Dashboard",
    "Economic Forecasting Model","Education Gap Research","Energy Efficiency Audit",
    "Environmental Compliance","Crime Pattern Analysis","Voter Turnout Study",
    "Infrastructure Assessment","Agricultural Yield Prediction","Labor Market Trends",
    "Healthcare Access Study","Budget Optimization","Water Quality Monitoring",
    "Transportation Network Analysis","Population Growth Forecast","Housing Affordability",
    "Food Security Assessment","Disaster Resilience Planning","Biodiversity Survey",
    "Air Quality Index Tracker","COVID-19 Impact Study","Renewable Energy Mapping",
    "Social Mobility Research","Poverty Alleviation Program","Digital Divide Analysis",
    "Supply Chain Optimization","Fraud Detection System","Customer Behavior Model",
]
PROJECT_CATEGORIES = ["analytics","machine learning","field research"]

COUNTRIES = ["United States","Canada","United Kingdom","Australia","Germany",
             "France","Brazil","India","Japan","Mexico","Egypt","Nigeria",
             "South Africa","Argentina","Colombia"]

GENDERS  = ["Male","Female","Non-binary","Prefer not to say"]
DOMAINS  = ["gmail.com","yahoo.com","outlook.com","hotmail.com","protonmail.com",
            "icloud.com","edu.com","university.edu"]

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def rand_date(start_year=2010, end_year=2024) -> str:
    start = date(start_year, 1, 1)
    end   = date(end_year, 12, 31)
    delta = (end - start).days
    return (start + timedelta(days=random.randint(0, delta))).isoformat()

def rand_datetime(start_year=2010, end_year=2024) -> str:
    d = rand_date(start_year, end_year)
    h = random.randint(0, 23)
    m = random.randint(0, 59)
    s = random.randint(0, 59)
    return f"{d} {h:02d}:{m:02d}:{s:02d}"

def rand_title(topic: str, agency: str) -> str:
    tmpl = random.choice(DATASET_TITLE_TEMPLATES)
    year  = random.randint(2010, 2024)
    return tmpl.format(
        topic=topic, agency=agency,
        geo=random.choice(GEOS),
        year=year, year2=year+1
    )

def rand_description(title: str, topic: str) -> str:
    templates = [
        f"This dataset contains {topic.lower()} data collected by federal agencies. "
        f"It covers key indicators relevant to {title.lower()} and is updated periodically.",
        f"Comprehensive {topic.lower()} statistics for research and policy analysis. "
        f"Data sourced from official government records and surveys.",
        f"Open government dataset providing detailed {topic.lower()} information. "
        f"Suitable for academic research, journalism, and data science applications.",
        f"Federal dataset for {title.lower()}. Includes historical records and "
        f"current measurements across multiple geographic regions.",
    ]
    return random.choice(templates)

def slug(title: str, uid: str) -> str:
    """Generate a URL-safe identifier."""
    s = title.lower()
    for ch in string.punctuation + " ":
        s = s.replace(ch, "-")
    while "--" in s:
        s = s.replace("--", "-")
    return s.strip("-")[:60] + "-" + uid[:8]

def rand_username(first: str, last: str) -> str:
    patterns = [
        f"{first.lower()}.{last.lower()}",
        f"{first.lower()}{last.lower()[:3]}",
        f"{first.lower()[0]}{last.lower()}",
        f"{first.lower()}_{random.randint(10,999)}",
    ]
    return random.choice(patterns)

def rand_email(username: str) -> str:
    return f"{username}@{random.choice(DOMAINS)}"

# ─────────────────────────────────────────────────────────────────────────────
# Build all table data
# ─────────────────────────────────────────────────────────────────────────────

print("Building Organization table …")
organizations = []
org_uuid_map  = {}
for i, (name, otype, email, cname, phone) in enumerate(ORG_DATA):
    uid = str(uuid.uuid5(uuid.NAMESPACE_DNS, name))
    org = {
        "organization_uuid": uid,
        "name":              name,
        "description":       (f"The {name} is a government entity responsible for "
                               f"{TOPICS_FLAT[i % len(TOPICS_FLAT)].lower()} data "
                               f"collection and dissemination."),
        "organization_type": otype,
        "contact_email":     email,
        "contact_name":      cname,
        "contact_phone":     phone,
    }
    organizations.append(org)
    org_uuid_map[name] = uid

print("Building Topic table …")
topics = [{"topic_name": t, "category": c} for t, c in TOPICS]

print("Building Tag table …")
tags = [{"tag_name": t} for t in sorted(set(TAGS_POOL))]

print("Building Dataset, Format, Dataset_Topic, Dataset_Tag tables …")
datasets        = []
formats         = []
dataset_topics  = []
dataset_tags    = []

TOTAL_DATASETS = 2000

for i in range(TOTAL_DATASETS):
    ds_uuid   = str(uuid.uuid4())
    org       = random.choice(organizations)
    topic_row = random.choice(topics)
    topic_nm  = topic_row["topic_name"]
    agency    = random.choice(AGENCIES)
    title     = rand_title(topic_nm, agency)
    created   = rand_datetime(2010, 2020)
    modified  = rand_datetime(2020, 2024)
    identifier = slug(title, ds_uuid)

    ds = {
        "dataset_uuid":      ds_uuid,
        "organization_uuid": org["organization_uuid"],
        "name":              title,
        "description":       rand_description(title, topic_nm),
        "access_level":      random.choices(ACCESS_LEVELS, weights=[80, 15, 5])[0],
        "license":           random.choice(LICENSES),
        "metadata_created":  created,
        "metadata_modified": modified,
        "maintainer_email":  org["contact_email"],
        "maintainer_name":   org["contact_name"],
        "identifier":        identifier,
    }
    datasets.append(ds)

    # Formats (1-4 resources per dataset)
    used_urls = set()
    for _ in range(random.randint(1, 4)):
        fmt = random.choice(FORMATS_POOL)
        url = (f"https://catalog.data.gov/dataset/{identifier}/resource/"
               f"{uuid.uuid4()}.{fmt.lower()}")
        if url not in used_urls:
            used_urls.add(url)
            formats.append({
                "file_url":     url,
                "dataset_uuid": ds_uuid,
                "file_format":  fmt,
            })

    # Dataset_Topic (1-3 topics)
    assigned_topics = {topic_nm}
    dataset_topics.append({"dataset_uuid": ds_uuid, "topic_name": topic_nm})
    for _ in range(random.randint(0, 2)):
        extra = random.choice(TOPICS_FLAT)
        if extra not in assigned_topics:
            assigned_topics.add(extra)
            dataset_topics.append({"dataset_uuid": ds_uuid, "topic_name": extra})

    # Dataset_Tag (2-6 tags)
    chosen_tags = random.sample(TAGS_POOL, k=random.randint(2, 6))
    for tg in chosen_tags:
        dataset_tags.append({"dataset_uuid": ds_uuid, "tag_name": tg})

print(f"  → {len(datasets)} datasets, {len(formats)} formats, "
      f"{len(dataset_topics)} dataset-topics, {len(dataset_tags)} dataset-tags")

# ─────────────────────────────────────────────────────────────────────────────
# Users
# ─────────────────────────────────────────────────────────────────────────────

print("Building User table …")
users = []
FIRST_NAMES = [
    "James","Mary","John","Patricia","Robert","Jennifer","Michael","Linda",
    "William","Barbara","David","Susan","Richard","Jessica","Joseph","Sarah",
    "Thomas","Karen","Charles","Lisa","Christopher","Nancy","Daniel","Betty",
    "Matthew","Margaret","Anthony","Sandra","Mark","Ashley","Donald","Dorothy",
    "Steven","Kimberly","Paul","Emily","Andrew","Donna","Joshua","Michelle",
    "Kenneth","Carol","Kevin","Amanda","Brian","Melissa","George","Deborah",
    "Timothy","Stephanie","Ronald","Rebecca","Edward","Sharon","Jason","Laura",
]
LAST_NAMES = [
    "Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis",
    "Rodriguez","Martinez","Hernandez","Lopez","Gonzalez","Wilson","Anderson",
    "Thomas","Taylor","Moore","Jackson","Martin","Lee","Perez","Thompson","White",
    "Harris","Sanchez","Clark","Ramirez","Lewis","Robinson","Walker","Young",
    "Allen","King","Wright","Scott","Torres","Nguyen","Hill","Flores","Green",
    "Adams","Nelson","Baker","Hall","Rivera","Campbell","Mitchell","Carter","Roberts",
]

used_usernames = set()
used_emails    = set()

# Try to read provided users.csv first
if os.path.exists(USERS_CSV):
    print(f"  Reading users from {USERS_CSV}")
    with open(USERS_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            email    = row.get("email","").strip()
            username = row.get("username","").strip()
            if email and username and email not in used_emails:
                users.append({
                    "email":     email,
                    "username":  username,
                    "gender":    row.get("gender",""),
                    "birthdate": row.get("birthdate",""),
                    "country":   row.get("country",""),
                })
                used_emails.add(email)
                used_usernames.add(username)
    print(f"  Loaded {len(users)} users from file.")

# Generate the rest up to 200
while len(users) < 200:
    first    = random.choice(FIRST_NAMES)
    last     = random.choice(LAST_NAMES)
    uname    = rand_username(first, last)
    # Ensure uniqueness
    base = uname
    suffix = 1
    while uname in used_usernames:
        uname = f"{base}{suffix}"
        suffix += 1
    email = rand_email(uname)
    while email in used_emails:
        email = rand_email(uname + str(random.randint(1, 999)))

    bdate = rand_date(1955, 2003)
    users.append({
        "email":     email,
        "username":  uname,
        "gender":    random.choice(GENDERS),
        "birthdate": bdate,
        "country":   random.choice(COUNTRIES),
    })
    used_usernames.add(uname)
    used_emails.add(email)

print(f"  Total users: {len(users)}")

# ─────────────────────────────────────────────────────────────────────────────
# User_Dataset_Usage  (500 random entries)
# ─────────────────────────────────────────────────────────────────────────────

print("Generating 500 User_Dataset_Usage rows …")
usages = []
usage_keys = set()   # (user_email, dataset_uuid, project_name)

dataset_uuids = [d["dataset_uuid"] for d in datasets]
user_emails   = [u["email"] for u in users]

attempts = 0
while len(usages) < 500 and attempts < 50000:
    attempts += 1
    uemail   = random.choice(user_emails)
    duuid    = random.choice(dataset_uuids)
    pname    = random.choice(PROJECT_NAMES)
    key      = (uemail, duuid, pname)
    if key in usage_keys:
        continue
    usage_keys.add(key)
    usages.append({
        "usage_id":         len(usages) + 1,
        "user_email":       uemail,
        "dataset_uuid":     duuid,
        "project_name":     pname,
        "project_category": random.choice(PROJECT_CATEGORIES),
        "usage_date":       rand_date(2020, 2024),
    })

print(f"  Generated {len(usages)} usage rows.")

# ─────────────────────────────────────────────────────────────────────────────
# Write CSVs
# ─────────────────────────────────────────────────────────────────────────────

def write_csv(rows: list[dict], filename: str):
    if not rows:
        return
    path = os.path.join(CSV_DIR, filename)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"  CSV → {path}  ({len(rows)} rows)")

print("\nWriting CSV files …")
write_csv(organizations,  "Organization.csv")
write_csv(datasets,       "Dataset.csv")
write_csv(topics,         "Topic.csv")
write_csv(dataset_topics, "Dataset_Topic.csv")
write_csv(tags,           "Tag.csv")
write_csv(dataset_tags,   "Dataset_Tag.csv")
write_csv(formats,        "Format.csv")
write_csv(users,          "User.csv")
write_csv(usages,         "User_Dataset_Usage.csv")

# ─────────────────────────────────────────────────────────────────────────────
# Build SQLite DB (for local testing without MySQL)
# ─────────────────────────────────────────────────────────────────────────────

print("\nBuilding SQLite database for local testing …")

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS Organization (
    organization_uuid VARCHAR(255) PRIMARY KEY,
    name              VARCHAR(255) NOT NULL,
    description       TEXT,
    organization_type VARCHAR(100),
    contact_email     VARCHAR(255),
    contact_name      VARCHAR(255),
    contact_phone     VARCHAR(50)
);
CREATE TABLE IF NOT EXISTS Dataset (
    dataset_uuid      VARCHAR(255) PRIMARY KEY,
    organization_uuid VARCHAR(255),
    name              VARCHAR(255) NOT NULL,
    description       TEXT,
    access_level      VARCHAR(100),
    license           VARCHAR(255),
    metadata_created  DATETIME,
    metadata_modified DATETIME,
    maintainer_email  VARCHAR(255),
    maintainer_name   VARCHAR(255),
    identifier        VARCHAR(255) UNIQUE,
    FOREIGN KEY (organization_uuid) REFERENCES Organization(organization_uuid)
);
CREATE TABLE IF NOT EXISTS Topic (
    topic_name VARCHAR(100) PRIMARY KEY,
    category   VARCHAR(255)
);
CREATE TABLE IF NOT EXISTS Dataset_Topic (
    dataset_uuid VARCHAR(255) NOT NULL,
    topic_name   VARCHAR(100) NOT NULL,
    PRIMARY KEY (dataset_uuid, topic_name),
    FOREIGN KEY (dataset_uuid) REFERENCES Dataset(dataset_uuid),
    FOREIGN KEY (topic_name)   REFERENCES Topic(topic_name)
);
CREATE TABLE IF NOT EXISTS Tag (
    tag_name VARCHAR(100) PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS Dataset_Tag (
    dataset_uuid VARCHAR(255) NOT NULL,
    tag_name     VARCHAR(100) NOT NULL,
    PRIMARY KEY (dataset_uuid, tag_name),
    FOREIGN KEY (dataset_uuid) REFERENCES Dataset(dataset_uuid),
    FOREIGN KEY (tag_name)     REFERENCES Tag(tag_name)
);
CREATE TABLE IF NOT EXISTS Format (
    file_url     VARCHAR(500) PRIMARY KEY,
    dataset_uuid VARCHAR(255),
    file_format  VARCHAR(100),
    FOREIGN KEY (dataset_uuid) REFERENCES Dataset(dataset_uuid)
);
CREATE TABLE IF NOT EXISTS User (
    email     VARCHAR(255) PRIMARY KEY,
    username  VARCHAR(100) NOT NULL UNIQUE,
    gender    VARCHAR(50),
    birthdate DATE,
    country   VARCHAR(100)
);
CREATE TABLE IF NOT EXISTS User_Dataset_Usage (
    usage_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    user_email       VARCHAR(255),
    dataset_uuid     VARCHAR(255),
    project_name     VARCHAR(255) NOT NULL,
    project_category VARCHAR(50),
    usage_date       DATE NOT NULL,
    UNIQUE (user_email, dataset_uuid, project_name),
    FOREIGN KEY (user_email)   REFERENCES User(email),
    FOREIGN KEY (dataset_uuid) REFERENCES Dataset(dataset_uuid)
);
"""

if os.path.exists(DB_FILE):
    os.remove(DB_FILE)
conn = sqlite3.connect(DB_FILE)
cur  = conn.cursor()
cur.executescript(SCHEMA_SQL)

def ins(table, rows):
    if not rows:
        return
    cols = list(rows[0].keys())
    ph   = ",".join(["?"] * len(cols))
    sql  = f"INSERT OR IGNORE INTO {table} ({','.join(cols)}) VALUES ({ph})"
    cur.executemany(sql, [tuple(r[c] for c in cols) for r in rows])

ins("Organization",       organizations)
ins("Dataset",            datasets)
ins("Topic",              topics)
ins("Dataset_Topic",      dataset_topics)
ins("Tag",                tags)
ins("Dataset_Tag",        dataset_tags)
ins("Format",             formats)
ins("User",               users)
ins("User_Dataset_Usage", usages)
conn.commit()

# Verify row counts
for tbl in ["Organization","Dataset","Topic","Dataset_Topic",
            "Tag","Dataset_Tag","Format","User","User_Dataset_Usage"]:
    n = cur.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
    print(f"  {tbl:25s}: {n} rows")

conn.close()
print(f"\nSQLite DB → {DB_FILE}")

# ─────────────────────────────────────────────────────────────────────────────
# MySQL dump
# ─────────────────────────────────────────────────────────────────────────────

def sql_val(v) -> str:
    """Escape a Python value for MySQL INSERT."""
    if v is None or v == "":
        return "NULL"
    s = str(v).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{s}'"

def make_insert_block(table: str, rows: list[dict]) -> str:
    if not rows:
        return ""
    cols = list(rows[0].keys())
    lines = [f"INSERT INTO `{table}` ({','.join('`'+c+'`' for c in cols)}) VALUES"]
    val_lines = []
    for r in rows:
        vals = ",".join(sql_val(r[c]) for c in cols)
        val_lines.append(f"  ({vals})")
    lines.append(",\n".join(val_lines) + ";")
    return "\n".join(lines)

print("\nWriting MySQL dump …")

MYSQL_SCHEMA = """\
-- ============================================================
-- data_portal MySQL dump
-- Generated: {ts}
-- ============================================================
SET FOREIGN_KEY_CHECKS=0;

CREATE DATABASE IF NOT EXISTS data_portal;
USE data_portal;

DROP TABLE IF EXISTS `User_Dataset_Usage`;
DROP TABLE IF EXISTS `Dataset_Tag`;
DROP TABLE IF EXISTS `Dataset_Topic`;
DROP TABLE IF EXISTS `Format`;
DROP TABLE IF EXISTS `Dataset`;
DROP TABLE IF EXISTS `Organization`;
DROP TABLE IF EXISTS `Topic`;
DROP TABLE IF EXISTS `Tag`;
DROP TABLE IF EXISTS `User`;

CREATE TABLE `Organization` (
    organization_uuid VARCHAR(255) PRIMARY KEY,
    name              VARCHAR(255) NOT NULL,
    description       TEXT,
    organization_type VARCHAR(100),
    contact_email     VARCHAR(255),
    contact_name      VARCHAR(255),
    contact_phone     VARCHAR(50)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `Dataset` (
    dataset_uuid      VARCHAR(255) PRIMARY KEY,
    organization_uuid VARCHAR(255),
    name              VARCHAR(255) NOT NULL,
    description       TEXT,
    access_level      VARCHAR(100),
    license           VARCHAR(255),
    metadata_created  DATETIME,
    metadata_modified DATETIME,
    maintainer_email  VARCHAR(255),
    maintainer_name   VARCHAR(255),
    identifier        VARCHAR(255) UNIQUE,
    FOREIGN KEY (organization_uuid)
        REFERENCES Organization(organization_uuid)
        ON UPDATE CASCADE ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `Topic` (
    topic_name VARCHAR(100) PRIMARY KEY,
    category   VARCHAR(255)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `Dataset_Topic` (
    dataset_uuid VARCHAR(255) NOT NULL,
    topic_name   VARCHAR(100) NOT NULL,
    PRIMARY KEY (dataset_uuid, topic_name),
    FOREIGN KEY (dataset_uuid) REFERENCES Dataset(dataset_uuid)
        ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (topic_name)   REFERENCES Topic(topic_name)
        ON UPDATE CASCADE ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `Tag` (
    tag_name VARCHAR(100) PRIMARY KEY
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `Dataset_Tag` (
    dataset_uuid VARCHAR(255) NOT NULL,
    tag_name     VARCHAR(100) NOT NULL,
    PRIMARY KEY (dataset_uuid, tag_name),
    FOREIGN KEY (dataset_uuid) REFERENCES Dataset(dataset_uuid)
        ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (tag_name)     REFERENCES Tag(tag_name)
        ON UPDATE CASCADE ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `Format` (
    file_url     VARCHAR(500) PRIMARY KEY,
    dataset_uuid VARCHAR(255),
    file_format  VARCHAR(100),
    FOREIGN KEY (dataset_uuid) REFERENCES Dataset(dataset_uuid)
        ON UPDATE CASCADE ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `User` (
    email     VARCHAR(255) PRIMARY KEY,
    username  VARCHAR(100) NOT NULL UNIQUE,
    gender    VARCHAR(50),
    birthdate DATE,
    country   VARCHAR(100)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `User_Dataset_Usage` (
    usage_id         INT AUTO_INCREMENT PRIMARY KEY,
    user_email       VARCHAR(255),
    dataset_uuid     VARCHAR(255),
    project_name     VARCHAR(255) NOT NULL,
    project_category ENUM('analytics','machine learning','field research') DEFAULT NULL,
    usage_date       DATE NOT NULL DEFAULT (CURDATE()),
    UNIQUE (user_email, dataset_uuid, project_name),
    FOREIGN KEY (user_email)   REFERENCES User(email)
        ON UPDATE CASCADE ON DELETE SET NULL,
    FOREIGN KEY (dataset_uuid) REFERENCES Dataset(dataset_uuid)
        ON UPDATE CASCADE ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

""".format(ts=datetime.now().isoformat(sep=" ", timespec="seconds"))

with open(DUMP_FILE, "w", encoding="utf-8") as f:
    f.write(MYSQL_SCHEMA)

    for table, rows in [
        ("Organization",       organizations),
        ("Dataset",            datasets),
        ("Topic",              topics),
        ("Dataset_Topic",      dataset_topics),
        ("Tag",                tags),
        ("Dataset_Tag",        dataset_tags),
        ("Format",             formats),
        ("User",               users),
        ("User_Dataset_Usage", usages),
    ]:
        if rows:
            f.write(f"\n-- {table}\n")
            # Write in batches of 500 to keep the file readable
            for start in range(0, len(rows), 500):
                batch = rows[start:start + 500]
                f.write(make_insert_block(table, batch) + "\n")

    f.write("\nSET FOREIGN_KEY_CHECKS=1;\n")

print(f"MySQL dump → {DUMP_FILE}")
print("\n✅  All deliverables generated successfully.")
