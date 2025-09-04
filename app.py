import re, time
import pandas as pd
import requests
from bs4 import BeautifulSoup
import streamlit as st

# ---------- PAGE SETTINGS ----------
st.set_page_config(page_title="Mass Tort Radar", layout="centered")

# ---------- HEADER ----------
st.markdown("<h1 style='text-align: center;'>🎯 Mass Tort Radar – Law Firm Scraper</h1>", unsafe_allow_html=True)
st.markdown("<p style='text-align: center; font-size: 1.1em;'>Upload a list of law firm URLs and (optionally) your own keyword list. I’ll extract firm info, detect mass tort terms, and return a clean CSV.</p>", unsafe_allow_html=True)
st.markdown("---")

# ---------- FILE UPLOADERS ----------
uploaded = st.file_uploader("📥 Step 1: Upload CSV of law firm URLs", type=["csv"])
keyword_file = st.file_uploader("🧠 Step 2 (Optional): Upload custom keyword list (.txt or .csv)", type=["txt", "csv"])

# ---------- SCRAPE BUTTON ----------
run = st.button("🚀 Run Scrape", disabled=not uploaded)

# ---------- DEFAULT KEYWORDS ----------
DEFAULT_MASS_TORT_TERMS = [t.strip() for t in """
afff, firefighting foam, pfas, camp lejeune, 3m earplug, earplugs, paraquat, roundup, glyphosate, talc,
talcum powder, baby powder, elmiron, hernia mesh, mesh implant, cpap, philips respironics, hair relaxer,
ozempic, wegovy, mounjaro, glp-1, suboxone tooth decay, zantac, valsartan, exactech, juul, vaping,
nec infant formula, nec, tylenol pregnancy, apap, acetaminophen autism, insulin pump recall, hip implant,
benzene sunscreen, silica, silicosis, social media harm, snapchat addiction, tiktok addiction, meta addiction,
uber assault, clergy abuse, boy scouts abuse, sexual abuse, paraquat parkinson
""".split(",")]

keyword_list = DEFAULT_MASS_TORT_TERMS

# ---------- LOAD CUSTOM KEYWORDS ----------
if keyword_file:
    try:
        if keyword_file.name.endswith(".txt"):
            lines = keyword_file.read().decode("utf-8").splitlines()
        else:
            df_keywords = pd.read_csv(keyword_file)
            lines = df_keywords.iloc[:, 0].dropna().astype(str).tolist()
        keyword_list = [line.strip() for line in lines if line.strip()]
        st.success(f"✅ Loaded {len(keyword_list)} custom keywords.")
        st.caption("Preview: " + ", ".join(keyword_list[:5]) + ("..." if len(keyword_list) > 5 else ""))
    except Exception as e:
        st.warning(f"⚠️ Failed to parse keyword file: {e}")
        keyword_list = DEFAULT_MASS_TORT_TERMS

# ---------- SCRAPE SETTINGS ----------
HEADERS = {"User-Agent": "Mozilla/5.0"}
PHONE_RE = re.compile(r'(\+?1[\s\-.]?)?\(?\d{3}\)?[\s\-.]?\d{3}[\s\-.]?\d{4}')
ADDR_HINT = re.compile(r'\b(Suite|Ste\.|Floor|FL|Ave|Avenue|St\.|Street|Blvd|Boulevard|Rd\.|Road|TX|CA|NY|FL|IL|WA|CO|GA|OH|NV|AZ|NM|NC|SC|VA|PA|MA|NJ|LA|MI)\b', re.I)

AGENCY_PATTERNS = {
    "Scorpion": [r"cdn\.scorpion\.co", r"scorpion.*\.js", r'meta[^>]+generator[^>]+Scorpion'],
    "FindLaw/Thomson Reuters": [r"findlaw", r"lawtracdn", r"thomsonreuters"],
    "Justia": [r"justia"],
    "LawRank": [r"lawrank"],
    "Juris Digital": [r"jurisdigital"],
    "iLawyerMarketing": [r"ilawyermarketing"],
    "On The Map": [r"onthemap"],
    "Nifty": [r"niftymarketing", r"nifty\."]
}

# ---------- CORE FUNCTIONS ----------
def get_html(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        if r.status_code == 200 and "text/html" in r.headers.get("Content-Type", ""):
            return r.text
    except Exception:
        return None
    return None

def extract_text(soup):
    for tag in soup(["script", "style", "noscript"]): tag.extract()
    return " ".join(soup.get_text(" ").split())

def find_firm_name(soup):
    og = soup.find("meta", property="og:site_name")
    if og and og.get("content"): return og["content"].strip()
    logo = soup.select_one("img[alt]")
    if logo and len(logo.get("alt", "").strip()) > 2:
        return logo.get("alt").strip()[:140]
    title = (soup.title.string if soup.title else "") or ""
    return title.strip()[:140]

def find_phone(text, soup):
    tel = soup.select_one('a[href^="tel:"]')
    if tel:
        m = PHONE_RE.search(tel.get("href", "") + " " + (tel.get_text() or ""))
        if m: return m.group(0)
    m = PHONE_RE.search(text)
    return m.group(0) if m else ""

def find_practice_areas(soup, text):
    areas = set()
    for selector in ["nav", "footer"]:
        for c in soup.select(selector):
            for a in c.find_all("a"):
                label = (a.get_text() or "").strip()
                if 2 <= len(label.split()) <= 5 and len(label) <= 40:
                    if any(k in label.lower() for k in ["injury", "accident", "divorce", "family", "criminal", "dui", "bankruptcy", "mass", "class", "abuse", "mesh", "cpap", "roundup", "talc", "earplug", "paraquat", "pfas"]):
                        areas.add(label)
    for kw in ["personal injury", "car accident", "divorce", "family law", "criminal defense", "mass tort", "class action", "dui", "truck accident", "motorcycle accident"]:
        if kw in text.lower(): areas.add(kw.title())
    return sorted(areas)

def find_locations(soup, text):
    locs = set()
    for addr in soup.find_all(["address"]):
        t = " ".join(addr.get_text(" ").split())
        if ADDR_HINT.search(t): locs.add(t)
    for chunk in re.split(r'\s{2,}', text):
        if ADDR_HINT.search(chunk) and 10 < len(chunk) < 120:
            locs.add(chunk.strip())

