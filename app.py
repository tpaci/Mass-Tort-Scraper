import re, time
import pandas as pd
import requests
from bs4 import BeautifulSoup
import streamlit as st

# ---------- PAGE CONFIG ----------
st.set_page_config(page_title="Mass Tort Radar", layout="centered")

# ---------- TSEG HEADER ----------
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

# ---------- FILE UPLOAD ----------
uploaded = st.file_uploader("📥 Step 1: Upload CSV of law firm URLs", type=["csv"])
keyword_file = st.file_uploader("🧠 Step 2 (Optional): Upload custom keyword list (.txt or .csv)", type=["txt", "csv"])
run = st.button("🚀 Run Scrape", disabled=not uploaded)

# ---------- DEFAULT KEYWORDS ----------
DEFAULT_MASS_TORT_TERMS = [t.strip() for t in """
afff, firefighting foam, pfas, camp lejeune, gambling addiction, gambling, 3m earplug, earplugs, paraquat, roundup, glyphosate, talc,
talcum powder, baby powder, elmiron, hernia mesh, mesh implant, cpap, philips respironics, hair relaxer,
ozempic, wegovy, mounjaro, glp-1, suboxone tooth decay, zantac, valsartan, exactech, juul, vaping, roblox, tylenol pregnancy, apap, acetaminophen autism, insulin pump recall, hip implant,
benzene sunscreen, silica, silicosis, social media harm, snapchat addiction, tiktok addiction, meta addiction,
uber assault, clergy abuse, boy scouts abuse, sexual abuse, paraquat parkinson
""".split(",")]

keyword_list = DEFAULT_MASS_TORT_TERMS

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

# ---------- REGEX & AGENCY TAGS ----------
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

# ---------- HELPERS ----------
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

def find_locations(soup, text):
    locs = set()
    for addr in soup.find_all(["address"]):
        t = " ".join(addr.get_text(" ").split())
        if ADDR_HINT.search(t): locs.add(t)
    for chunk in re.split(r'\s{2,}', text):
        if ADDR_HINT.search(chunk) and 10 < len(chunk) < 120:
            locs.add(chunk.strip())
    return list(locs)

def detect_agency(html):
    for agency, patterns in AGENCY_PATTERNS.items():
        if any(re.search(pat, html, re.I) for pat in patterns):
            return agency
    return ""

def detect_mass_tort(text, keywords):
    found = [kw for kw in keywords if kw.lower() in text.lower()]
    return ", ".join(sorted(set(found))) if found else ""

# ---------- MAIN SCRAPER ----------
if run:
    df = pd.read_csv(uploaded)
    urls = df.iloc[:, 0].dropna().astype(str).tolist()
    batch_size = 50
    results = []

    progress = st.progress(0)
    status = st.empty()

    for i in range(0, len(urls), batch_size):
        batch = urls[i:i+batch_size]
        for idx, url in enumerate(batch):
            try:
                html = get_html(url)
                if not html:
                    results.append({"URL": url, "Firm Name": "", "Phone": "", "Locations": "", "Practice Areas": "", "Agency": "", "Mass Tort Terms": ""})
                    continue
                soup = BeautifulSoup(html, "html.parser")
                text = extract_text(soup)
                results.append({
                    "URL": url,
                    "Firm Name": find_firm_name(soup),
                    "Phone": find_phone(text, soup),
                    "Locations": "; ".join(find_locations(soup, text)),
                    "Practice Areas": "; ".join(find_locations(soup, text)),
                    "Agency": detect_agency(html),
                    "Mass Tort Terms": detect_mass_tort(text, keyword_list)
                })
            except Exception:
                results.append({"URL": url, "Firm Name": "", "Phone": "", "Locations": "", "Practice Areas": "", "Agency": "", "Mass Tort Terms": ""})

        progress.progress(min((i + batch_size) / len(urls), 1.0))
        status.text(f"✅ Scanned {min(i+batch_size, len(urls))} of {len(urls)} URLs...")

    st.success("🎉 Done scanning!")
    out_df = pd.DataFrame(results)
    st.dataframe(out_df)
    st.download_button("📥 Download Results", out_df.to_csv(index=False).encode("utf-8"), file_name="mass_tort_radar_results.csv", mime="text/csv")
