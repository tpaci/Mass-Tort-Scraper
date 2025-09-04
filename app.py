# app.py
import re, time, io
import pandas as pd
import requests
from bs4 import BeautifulSoup
import streamlit as st

st.set_page_config(page_title="Mass Tort Radar – Law Firm Scraper", layout="wide")

HEADERS = {"User-Agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119 Safari/537.36"}
PHONE_RE = re.compile(r'(\+?1[\s\-.]?)?\(?\d{3}\)?[\s\-.]?\d{3}[\s\-.]?\d{4}')
ADDR_HINT = re.compile(r'\b(Suite|Ste\.|Floor|FL|Ave|Avenue|St\.|Street|Blvd|Boulevard|Rd\.|Road|TX|CA|NY|FL|IL|WA|CO|GA|OH|NV|AZ|NM|NC|SC|VA|PA|MA|NJ|LA|MI)\b', re.I)
MASS_TORT_TERMS = [t.strip() for t in """
afff, firefighting foam, pfas, camp lejeune, 3m earplug, earplugs, paraquat, roundup, glyphosate, talc,
talcum powder, baby powder, elmiron, hernia mesh, mesh implant, cpap, philips respironics, hair relaxer,
ozempic, wegovy, mounjaro, glp-1, suboxone tooth decay, zantac, valsartan, exactech, juul, vaping,
nec infant formula, nec, tylenol pregnancy, apap, acetaminophen autism, insulin pump recall, hip implant,
benzene sunscreen, silica, silicosis, social media harm, snapchat addiction, tiktok addiction, meta addiction,
uber assault, clergy abuse, boy scouts abuse, sexual abuse, paraquat parkinson
""".split(",")]

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

def get_html(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        if r.status_code == 200 and "text/html" in r.headers.get("Content-Type",""):
            return r.text
    except Exception:
        return None
    return None

def extract_text(soup):
    for tag in soup(["script","style","noscript"]): tag.extract()
    return " ".join(soup.get_text(" ").split())

def find_firm_name(soup):
    og = soup.find("meta", property="og:site_name")
    if og and og.get("content"): return og["content"].strip()
    # try logo alt
    logo = soup.select_one("img[alt]")
    if logo and len(logo.get("alt","").strip()) > 2:
        return logo.get("alt").strip()[:140]
    title = (soup.title.string if soup.title else "") or ""
    return title.strip()[:140]

def find_phone(text, soup):
    tel = soup.select_one('a[href^="tel:"]')
    if tel:
        m = PHONE_RE.search(tel.get("href","") + " " + (tel.get_text() or ""))
        if m: return m.group(0)
    m = PHONE_RE.search(text)
    return m.group(0) if m else ""

def find_practice_areas(soup, text):
    areas = set()
    # common sections
    for selector in ["nav", "footer"]:
        for c in soup.select(selector):
            for a in c.find_all("a"):
                label = (a.get_text() or "").strip()
                if 2 <= len(label.split()) <= 5 and len(label) <= 40:
                    if any(k in label.lower() for k in ["injury","accident","divorce","family","criminal","dui","bankruptcy","mass","class","abuse","mesh","cpap","roundup","talc","earplug","paraquat","pfas"]):
                        areas.add(label)
    # keyword fallback
    for kw in ["personal injury","car accident","divorce","family law","criminal defense","mass tort","class action","dui","truck accident","motorcycle accident"]:
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
    return list(locs)[:6]

def detect_agency(html):
    for name, pats in AGENCY_PATTERNS.items():
        for p in pats:
            if re.search(p, html, re.I):
                return name, re.search(p, html, re.I).group(0)
    return "", ""

def mass_tort_hits(text):
    return sorted({t for t in MASS_TORT_TERMS if t and t.lower() in text.lower()})

def process_url(url):
    row = {"url":url, "firm_name":"", "phone":"", "practice_areas":"", "mass_tort_detected":"N",
           "mass_tort_terms":"", "locations":"", "agency_detected":"", "agency_evidence":"", "latest_mass_tort_update":"",
           "page_title":""}
    html = get_html(url)
    if not html: return row
    soup = BeautifulSoup(html, "html.parser")
    text = extract_text(soup)
    row["page_title"] = (soup.title.string.strip() if soup.title and soup.title.string else "")
    row["firm_name"] = find_firm_name(soup)
    row["phone"] = find_phone(text, soup)
    areas = find_practice_areas(soup, text)
    row["practice_areas"] = " | ".join(areas)
    row["locations"] = " | ".join(find_locations(soup, text))
    agency, evidence = detect_agency(html)
    row["agency_detected"], row["agency_evidence"] = agency, evidence
    hits = mass_tort_hits(text + " " + " ".join(areas))
    if hits:
        row["mass_tort_detected"] = "Y"
        row["mass_tort_terms"] = " | ".join(hits)
        # Leave latest_mass_tort_update blank here; you can add a separate step or GPT for news.
    return row

st.title("Mass Tort Radar – Law Firm Scraper")
st.caption("Upload a CSV with a 'url' column. I'll extract firm info, practice areas, locations, phone, vendor fingerprints, and flag mass-tort terms.")

uploaded = st.file_uploader("Upload CSV of URLs", type=["csv"])
run = st.button("Run Scrape", disabled=not uploaded)

if run and uploaded:
    df_in = pd.read_csv(uploaded)
    if "url" not in df_in.columns:
        st.error("Your CSV needs a 'url' column.")
    else:
        urls = df_in["url"].dropna().astype(str).tolist()
        out_rows = []
        prog = st.progress(0)
        status = st.empty()
        for i, u in enumerate(urls, start=1):
            status.markdown(f"Scraping **{u}** ({i}/{len(urls)})")
            try:
                out_rows.append(process_url(u))
            except Exception as e:
                out_rows.append({"url":u, "firm_name":"", "phone":"", "practice_areas":"", "mass_tort_detected":"N",
                                 "mass_tort_terms":"", "locations":"", "agency_detected":"", "agency_evidence":"",
                                 "latest_mass_tort_update":f"error: {e}", "page_title":""})
            prog.progress(int(i/len(urls)*100))
            time.sleep(0.5)
        status.empty(); prog.empty()

        df_out = pd.DataFrame(out_rows, columns=[
            "url","firm_name","phone","practice_areas","mass_tort_detected",
            "mass_tort_terms","locations","agency_detected","agency_evidence",
            "latest_mass_tort_update","page_title"
        ])
        st.subheader("Preview")
        st.dataframe(df_out, use_container_width=True)

        csv_bytes = df_out.to_csv(index=False).encode("utf-8")
        st.download_button("Download CSV", data=csv_bytes, file_name="scrape_output.csv", mime="text/csv")

st.markdown("---")
st.caption("Tip: For the 1-paragraph 'latest mass-tort update', run this CSV through your Custom GPT with Browsing enabled.")
