import re, time
import pandas as pd
import requests
from bs4 import BeautifulSoup
import streamlit as st

# ---------- PAGE SETTINGS ----------
st.set_page_config(page_title="Mass Tort Radar", layout="centered")

# ---------- HEADER WITH LOGO + STYLING ----------
st.markdown("""
    <div style="background-color:#111; padding: 1.2rem 1rem; border-radius: 12px; margin-bottom: 2rem;">
        <div style="text-align: center;">
            <img src="https://cdn-copja.nitrocdn.com/JqMfseSnYjDVxZAOIJWXvOlZnIGyBMST/assets/images/optimized/rev-abff178/lawrank.com/wp-content/uploads/2024/04/TSEG.png" width="180">
            <h1 style="margin-top: 0.5rem; color: white;">🎯 Mass Tort Radar – Law Firm Scraper</h1>
            <p style="color: #ccc; font-size: 1.05rem; max-width: 700px; margin: 0 auto;">
                Upload a CSV of law firm URLs and (optionally) a keyword list. I’ll extract firm info, check for mass tort terms, and return a clean, downloadable CSV.
            </p>
        </div>
    </div>
""", unsafe_allow_html=True)

# ---------- FILE UPLOADERS ----------
uploaded = st.file_uploader("📥 Step 1: Upload CSV of law firm URLs", type=["csv"])
keyword_file = st.file_uploader("🧠 Step 2 (Optional): Upload custom keyword list (.txt or .csv)", type=["txt", "csv"])
run = st.button("🚀 Run Scrape", disabled=not uploaded)

# ---------- DEFAULT KEYWORDS ----------
DEFAULT_MASS_TORT_TERMS = [t.strip() for t in """
afff, firefighting foam, pfas, camp lejeune, gambling addiction, gambling, 3m earplug, earplugs, paraquat, roundup, glyphosate, talc,
talcum powder, baby powder, elmiron, hernia mesh, mesh implant, cpap, philips respironics, hair relaxer,
ozempic, wegovy, mounjaro, glp-1, suboxone tooth decay, zantac, valsartan, exactech, juul, vaping,
nec infant formula, Tylenol, tylenol pregnancy, apap, acetaminophen autism, insulin pump recall, hip implant,
benzene sunscreen, silica, silicosis, social media harm, snapchat addiction, tiktok addiction, meta addiction,
uber assault, clergy abuse, boy scouts abuse, sexual abuse, paraquat parkinson, dupixent
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

# ---------- REGEX AND HEADERS ----------
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

# ---------- UTIL FUNCTIONS ----------
def get_html(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        if r.status_code == 200 and "text/html" in r.headers.get("Content-Type", ""):
            return r.text
    except Exception as e:
        print(f"[ERROR] Failed to fetch {url}: {e}")
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
    return list(locs)

def find_agency(html):
    for agency, patterns in AGENCY_PATTERNS.items():
        for pat in patterns:
            if re.search(pat, html, re.I): return agency
    return ""

# ---------- MAIN SCRAPE ----------
if run:
    try:
        df = pd.read_csv(uploaded)
        urls = df.iloc[:, 0].dropna().tolist()
        results = []

        with st.spinner(f"Scraping {len(urls)} sites..."):
            for url in urls:
                html = get_html(url)
                if not html:
                    results.append({"URL": url, "Status": "Failed to load"})
                    continue
                soup = BeautifulSoup(html, "html.parser")
                text = extract_text(soup)
                results.append({
                    "URL": url,
                    "Firm Name": find_firm_name(soup),
                    "Phone": find_phone(text, soup),
                    "Practice Areas": ", ".join(find_practice_areas(soup, text)),
                    "Mass Tort Terms": ", ".join([kw for kw in keyword_list if kw.lower() in text.lower()]),
                    "Mass Tort Detected": "Y" if any(kw.lower() in text.lower() for kw in keyword_list) else "N",
                    "Locations": ", ".join(find_locations(soup, text)),
                    "Agency": find_agency(html)
                })

        st.success(f"✅ Scraped {len(results)} sites.")

        df_out = pd.DataFrame(results)
        st.dataframe(df_out)
        st.download_button("📄 Download CSV", df_out.to_csv(index=False), file_name="scrape_results.csv", mime="text/csv")

    except Exception as e:
        st.error(f"❌ Error during scraping: {e}")
