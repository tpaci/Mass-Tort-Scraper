import os
import re
import math
import time
from datetime import datetime
from urllib.parse import urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup
import streamlit as st

# -----------------------------
# PAGE CONFIG
# -----------------------------
st.set_page_config(page_title="Mass Tort Radar", layout="centered")

# -----------------------------
# BRAND HEADER
# -----------------------------
st.markdown("""
    <div style="background-color:#111; padding: 1.2rem 1rem; border-radius: 12px; margin-bottom: 2rem;">
        <div style="text-align: center;">
            <img src="https://cdn-copja.nitrocdn.com/JqMfseSnYjDVxZAOIJWXvOlZnIGyBMST/assets/images/optimized/rev-abff178/lawrank.com/wp-content/uploads/2024/04/TSEG.png" width="180">
            <h1 style="margin-top: 0.5rem; color: white;">🎯 Mass Tort Radar – Law Firm Scraper</h1>
            <p style="color: #ccc; font-size: 1.05rem; max-width: 700px; margin: 0 auto;">
                Upload a CSV of law firm URLs and (optionally) a keyword list. This version supports batching, resume mode,
                per-batch downloads, failed URL exports, URL validation, and timeout/load-time tracking.
            </p>
        </div>
    </div>
""", unsafe_allow_html=True)

# -----------------------------
# SETTINGS / CONSTANTS
# -----------------------------
RUNS_DIR = "runs"
os.makedirs(RUNS_DIR, exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125 Safari/537.36"
}

PHONE_RE = re.compile(r'(\+?1[\s\-.]?)?\(?\d{3}\)?[\s\-.]?\d{3}[\s\-.]?\d{4}')
ADDR_HINT = re.compile(
    r'\b(Suite|Ste\.|Floor|FL|Ave|Avenue|St\.|Street|Blvd|Boulevard|Rd\.|Road|Drive|Dr\.|Ln\.|Lane|Way|Parkway|Pkwy|TX|CA|NY|FL|IL|WA|CO|GA|OH|NV|AZ|NM|NC|SC|VA|PA|MA|NJ|LA|MI)\b',
    re.I
)

AGENCY_PATTERNS = {
    "Scorpion": [r"cdn\.scorpion\.co", r"scorpion.*\.js", r'meta[^>]+generator[^>]+Scorpion'],
    "FindLaw/Thomson Reuters": [r"findlaw", r"lawtracdn", r"thomsonreuters"],
    "Justia": [r"justia"],
    "LawRank": [r"lawrank"],
    "Juris Digital": [r"jurisdigital"],
    "iLawyerMarketing": [r"ilawyermarketing"],
    "On The Map": [r"onthemap"],
    "Nifty": [r"niftymarketing", r"nifty\."],
}

DEFAULT_MASS_TORT_TERMS = [t.strip() for t in """
afff, firefighting foam, pfas, camp lejeune, gambling addiction, gambling, 3m earplug, earplugs, paraquat, roundup, glyphosate, talc,
talcum powder, baby powder, elmiron, hernia mesh, mesh implant, cpap, philips respironics, hair relaxer,
ozempic, wegovy, mounjaro, glp-1, suboxone tooth decay, zantac, valsartan, exactech, juul, vaping,
nec infant formula, nec, tylenol pregnancy, apap, acetaminophen autism, insulin pump recall, hip implant,
benzene sunscreen, silica, silicosis, social media harm, snapchat addiction, tiktok addiction, meta addiction,
uber assault, clergy abuse, boy scouts abuse, sexual abuse, paraquat parkinson
""".split(",")]

# -----------------------------
# SESSION STATE
# -----------------------------
if "input_df" not in st.session_state:
    st.session_state["input_df"] = None

if "invalid_df" not in st.session_state:
    st.session_state["invalid_df"] = None

if "uploaded_file_name" not in st.session_state:
    st.session_state["uploaded_file_name"] = None

# -----------------------------
# HELPERS
# -----------------------------
def sanitize_run_name(name: str) -> str:
    name = name.strip()
    name = re.sub(r"[^a-zA-Z0-9_\-]", "_", name)
    return name or f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

def get_run_paths(run_name: str) -> dict:
    base = os.path.join(RUNS_DIR, run_name)
    os.makedirs(base, exist_ok=True)
    return {
        "dir": base,
        "master_csv": os.path.join(base, "master_results.csv"),
        "log_csv": os.path.join(base, "processed_urls.csv"),
        "failed_csv": os.path.join(base, "failed_urls.csv"),
        "meta_txt": os.path.join(base, "run_info.txt"),
    }

def normalize_url(url: str) -> str:
    url = str(url).strip()
    if not url:
        return ""
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    return url

def is_valid_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
        return bool(parsed.scheme in ("http", "https") and parsed.netloc and "." in parsed.netloc)
    except Exception:
        return False

def clean_and_validate_urls(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    if "url" in [c.lower() for c in df.columns]:
        actual_col = next(c for c in df.columns if c.lower() == "url")
        df = df.rename(columns={actual_col: "url"})
    else:
        first_col = df.columns[0]
        df = df.rename(columns={first_col: "url"})

    cleaned = df.copy()
    cleaned["original_url"] = cleaned["url"].astype(str)
    cleaned["url"] = cleaned["url"].astype(str).str.strip()
    cleaned = cleaned[cleaned["url"] != ""].copy()
    cleaned["url"] = cleaned["url"].apply(normalize_url)

    invalid_mask = ~cleaned["url"].apply(is_valid_url)
    invalid_df = cleaned.loc[invalid_mask, ["original_url", "url"]].copy()
    if not invalid_df.empty:
        invalid_df["reason"] = "Invalid URL format"

    valid_df = cleaned.loc[~invalid_mask].copy()
    valid_df = valid_df.drop_duplicates(subset=["url"]).reset_index(drop=True)

    return valid_df, invalid_df

def parse_uploaded_file(uploaded_file):
    if uploaded_file is None:
        return None, None

    file_name = uploaded_file.name

    if st.session_state["uploaded_file_name"] != file_name:
        raw_df = pd.read_csv(uploaded_file)
        input_df, invalid_df = clean_and_validate_urls(raw_df)

        st.session_state["input_df"] = input_df
        st.session_state["invalid_df"] = invalid_df
        st.session_state["uploaded_file_name"] = file_name

    return st.session_state["input_df"], st.session_state["invalid_df"]

def load_keywords(keyword_file):
    if keyword_file is None:
        return DEFAULT_MASS_TORT_TERMS

    try:
        if keyword_file.name.endswith(".txt"):
            lines = keyword_file.read().decode("utf-8").splitlines()
        else:
            df_keywords = pd.read_csv(keyword_file)
            lines = df_keywords.iloc[:, 0].dropna().astype(str).tolist()

        cleaned = [line.strip() for line in lines if str(line).strip()]
        return cleaned if cleaned else DEFAULT_MASS_TORT_TERMS
    except Exception:
        return DEFAULT_MASS_TORT_TERMS

def get_html(url: str, timeout_seconds: int):
    start = time.time()
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout_seconds, allow_redirects=True)
        load_time = round(time.time() - start, 2)

        if r.status_code == 200 and "text/html" in r.headers.get("Content-Type", ""):
            return r.text, "", load_time, "No"
        return None, f"HTTP {r.status_code}", load_time, "No"

    except requests.exceptions.Timeout:
        load_time = round(time.time() - start, 2)
        return None, "Request timed out", load_time, "Yes"
    except Exception as e:
        load_time = round(time.time() - start, 2)
        return None, str(e), load_time, "No"

def extract_text(soup: BeautifulSoup) -> str:
    for tag in soup(["script", "style", "noscript"]):
        tag.extract()
    return " ".join(soup.get_text(" ").split())

def find_firm_name(soup: BeautifulSoup) -> str:
    og = soup.find("meta", property="og:site_name")
    if og and og.get("content"):
        return og["content"].strip()

    logo = soup.select_one("img[alt]")
    if logo and len(logo.get("alt", "").strip()) > 2:
        return logo.get("alt", "").strip()[:140]

    title = soup.title.string.strip() if soup.title and soup.title.string else ""
    return title[:140]

def find_phone(text: str, soup: BeautifulSoup) -> str:
    tel = soup.select_one('a[href^="tel:"]')
    if tel:
        m = PHONE_RE.search(tel.get("href", "") + " " + (tel.get_text() or ""))
        if m:
            return m.group(0)

    m = PHONE_RE.search(text)
    return m.group(0) if m else ""

def find_locations(soup: BeautifulSoup, text: str):
    locs = set()

    for addr in soup.find_all(["address"]):
        t = " ".join(addr.get_text(" ").split())
        if ADDR_HINT.search(t):
            locs.add(t)

    for chunk in re.split(r"\s{2,}", text):
        chunk = chunk.strip()
        if ADDR_HINT.search(chunk) and 10 < len(chunk) < 120:
            locs.add(chunk)

    return list(locs)[:6]

def find_practice_areas(soup: BeautifulSoup, text: str):
    areas = set()

    keywords_for_links = [
        "injury", "accident", "divorce", "family", "criminal", "dui", "bankruptcy",
        "mass", "class", "abuse", "mesh", "cpap", "roundup", "talc", "earplug",
        "paraquat", "pfas", "employment", "wage", "social media", "gambling"
    ]

    for selector in ["nav", "footer"]:
        for c in soup.select(selector):
            for a in c.find_all("a"):
                label = (a.get_text() or "").strip()
                if 2 <= len(label.split()) <= 6 and len(label) <= 50:
                    if any(k in label.lower() for k in keywords_for_links):
                        areas.add(label)

    fallback_terms = [
        "personal injury", "car accident", "truck accident", "motorcycle accident",
        "family law", "criminal defense", "mass tort", "class action",
        "employment law", "wage and hour", "medical malpractice", "product liability"
    ]
    for kw in fallback_terms:
        if kw in text.lower():
            areas.add(kw.title())

    return sorted(areas)

def detect_agency(html: str) -> str:
    for agency, patterns in AGENCY_PATTERNS.items():
        for pat in patterns:
            if re.search(pat, html, re.I):
                return agency
    return ""

def detect_mass_tort(text: str, keywords) -> str:
    found = []
    lower_text = text.lower()
    for kw in keywords:
        kw_clean = str(kw).strip()
        if kw_clean and kw_clean.lower() in lower_text:
            found.append(kw_clean)
    return ", ".join(sorted(set(found)))

def process_url(url: str, keywords, timeout_seconds: int) -> dict:
    html, error_msg, load_time, timed_out = get_html(url, timeout_seconds)

    row = {
        "URL": url,
        "Firm Name": "",
        "Phone": "",
        "Locations": "",
        "Practice Areas": "",
        "Agency": "",
        "Mass Tort Terms": "",
        "Status": "",
        "Error": "",
        "Load Time (s)": load_time,
        "Timed Out": timed_out,
    }

    if not html:
        row["Status"] = "Failed"
        row["Error"] = error_msg
        return row

    try:
        soup = BeautifulSoup(html, "html.parser")
        text = extract_text(soup)

        row["Firm Name"] = find_firm_name(soup)
        row["Phone"] = find_phone(text, soup)
        row["Locations"] = "; ".join(find_locations(soup, text))
        row["Practice Areas"] = "; ".join(find_practice_areas(soup, text))
        row["Agency"] = detect_agency(html)
        row["Mass Tort Terms"] = detect_mass_tort(text, keywords)
        row["Status"] = "Success"
        return row
    except Exception as e:
        row["Status"] = "Failed"
        row["Error"] = str(e)
        return row

def load_processed_urls(log_csv_path: str) -> set:
    if not os.path.exists(log_csv_path):
        return set()
    try:
        df = pd.read_csv(log_csv_path)
        if "URL" in df.columns:
            return set(df["URL"].dropna().astype(str).tolist())
    except Exception:
        pass
    return set()

def append_results(batch_df: pd.DataFrame, master_csv_path: str, log_csv_path: str, failed_csv_path: str):
    header_needed_master = not os.path.exists(master_csv_path)
    header_needed_log = not os.path.exists(log_csv_path)

    batch_df.to_csv(master_csv_path, mode="a", header=header_needed_master, index=False)
    batch_df[["URL"]].to_csv(log_csv_path, mode="a", header=header_needed_log, index=False)

    failed_batch = batch_df[batch_df["Status"] == "Failed"].copy()
    if not failed_batch.empty:
        if os.path.exists(failed_csv_path):
            try:
                existing_failed = pd.read_csv(failed_csv_path)
                combined_failed = pd.concat([existing_failed, failed_batch], ignore_index=True)
                combined_failed = combined_failed.drop_duplicates(subset=["URL", "Error"])
                combined_failed.to_csv(failed_csv_path, index=False)
            except Exception:
                failed_batch.to_csv(failed_csv_path, mode="a", header=False, index=False)
        else:
            failed_batch.to_csv(failed_csv_path, index=False)

def write_run_info(meta_txt_path: str, text: str):
    with open(meta_txt_path, "w", encoding="utf-8") as f:
        f.write(text)

# -----------------------------
# UI
# -----------------------------
left, right = st.columns(2)

with left:
    uploaded = st.file_uploader("📥 Step 1: Upload CSV of law firm URLs", type=["csv"])

with right:
    keyword_file = st.file_uploader("🧠 Step 2 (Optional): Upload custom keyword list (.txt or .csv)", type=["txt", "csv"])

run_name_default = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
run_name_input = st.text_input(
    "🏷️ Run name",
    value=run_name_default,
    help="Use the same run name later if you want to resume where a previous scrape stopped."
)

c1, c2 = st.columns(2)
with c1:
    batch_size = st.number_input("Batch size", min_value=10, max_value=200, value=50, step=10)
with c2:
    delay_between_requests = st.number_input("Delay between requests (seconds)", min_value=0.0, max_value=5.0, value=0.25, step=0.25)

c3, c4 = st.columns(2)
with c3:
    timeout_seconds = st.number_input("Per-URL timeout (seconds)", min_value=3, max_value=60, value=15, step=1)
with c4:
    stop_after_batches = st.number_input("Stop after X batches (0 = all)", min_value=0, max_value=500, value=0, step=1)

resume_mode = st.checkbox("Resume mode (skip URLs already processed for this run)", value=True)

run = st.button("🚀 Run Scrape", disabled=not uploaded)

# -----------------------------
# PREVIEW + ONE-TIME PARSE
# -----------------------------
input_df = None
invalid_df = None

if uploaded:
    try:
        input_df, invalid_df = parse_uploaded_file(uploaded)

        st.success(f"Loaded {len(input_df)} valid unique URLs.")
        if invalid_df is not None and not invalid_df.empty:
            st.warning(f"{len(invalid_df)} invalid URL rows were found and will be skipped.")

        with st.expander("Preview valid uploaded URLs"):
            st.dataframe(input_df[["url"]].head(10), use_container_width=True)

        if invalid_df is not None and not invalid_df.empty:
            with st.expander("Preview invalid URLs that will be skipped"):
                st.dataframe(invalid_df, use_container_width=True)

    except Exception as e:
        st.error(f"Could not read uploaded CSV: {e}")

# -----------------------------
# MAIN
# -----------------------------
if run:
    try:
        if uploaded is None or input_df is None:
            st.error("Please upload a valid CSV before running the scraper.")
            st.stop()

        keywords = load_keywords(keyword_file)

        if keyword_file is not None:
            st.info(f"Using {len(keywords)} custom keywords.")
        else:
            st.info(f"Using default keyword list ({len(keywords)} keywords).")

        run_name = sanitize_run_name(run_name_input)
        paths = get_run_paths(run_name)

        processed_urls = load_processed_urls(paths["log_csv"]) if resume_mode else set()

        total_urls = len(input_df)
        urls_to_process = [u for u in input_df["url"].tolist() if u not in processed_urls]
        already_done = total_urls - len(urls_to_process)

        invalid_count = 0 if invalid_df is None else len(invalid_df)

        write_run_info(
            paths["meta_txt"],
            f"Run name: {run_name}\n"
            f"Started: {datetime.now()}\n"
            f"Total valid uploaded URLs: {total_urls}\n"
            f"Invalid URL rows skipped: {invalid_count}\n"
            f"Already processed (before this run): {already_done}\n"
            f"Batch size: {batch_size}\n"
            f"Resume mode: {resume_mode}\n"
            f"Per-URL timeout: {timeout_seconds}\n"
            f"Stop after X batches: {stop_after_batches}\n"
        )

        st.markdown("---")
        st.subheader("Run status")
        st.write(f"**Run name:** `{run_name}`")
        st.write(f"**Total valid uploaded URLs:** {total_urls}")
        st.write(f"**Invalid URL rows skipped:** {invalid_count}")
        st.write(f"**Already processed:** {already_done}")
        st.write(f"**Remaining this run:** {len(urls_to_process)}")

        if invalid_df is not None and not invalid_df.empty:
            st.download_button(
                "📥 Download Invalid URLs CSV",
                data=invalid_df.to_csv(index=False).encode("utf-8"),
                file_name=f"{run_name}_invalid_urls.csv",
                mime="text/csv"
            )

        if len(urls_to_process) == 0:
            st.success("Nothing left to process for this run. Everything in this file has already been scraped.")
            if os.path.exists(paths["master_csv"]):
                final_df = pd.read_csv(paths["master_csv"])
                st.dataframe(final_df.tail(20), use_container_width=True)
                st.download_button(
                    "📥 Download Existing Results",
                    data=final_df.to_csv(index=False).encode("utf-8"),
                    file_name=f"{run_name}_results.csv",
                    mime="text/csv"
                )
            if os.path.exists(paths["failed_csv"]):
                failed_existing_df = pd.read_csv(paths["failed_csv"])
                st.download_button(
                    "📥 Download Existing Failed URLs",
                    data=failed_existing_df.to_csv(index=False).encode("utf-8"),
                    file_name=f"{run_name}_failed_urls.csv",
                    mime="text/csv"
                )
            st.stop()

        total_batches = math.ceil(len(urls_to_process) / batch_size)
        batches_to_run = total_batches if stop_after_batches == 0 else min(total_batches, stop_after_batches)

        overall_progress = st.progress(0.0)
        batch_status = st.empty()
        live_status = st.empty()
        live_table = st.empty()
        download_section = st.container()

        processed_this_session = 0

        for batch_num in range(batches_to_run):
            start_idx = batch_num * batch_size
            end_idx = start_idx + batch_size
            batch_urls = urls_to_process[start_idx:end_idx]

            batch_status.info(
                f"Processing batch {batch_num + 1} of {batches_to_run} "
                f"({len(batch_urls)} URLs in this batch)"
            )

            batch_rows = []
            for i, url in enumerate(batch_urls, start=1):
                live_status.write(
                    f"Scraping URL {processed_this_session + i} of {len(urls_to_process)} remaining this run: {url}"
                )
                row = process_url(url, keywords, timeout_seconds)
                batch_rows.append(row)

                if delay_between_requests > 0:
                    time.sleep(delay_between_requests)

            batch_df = pd.DataFrame(batch_rows)
            append_results(batch_df, paths["master_csv"], paths["log_csv"], paths["failed_csv"])

            failed_batch = batch_df[batch_df["Status"] == "Failed"].copy()

            processed_this_session += len(batch_urls)
            overall_progress.progress(processed_this_session / max(len(urls_to_process), 1))

            live_table.dataframe(batch_df, use_container_width=True)

            success_count = (batch_df["Status"] == "Success").sum()
            fail_count = (batch_df["Status"] == "Failed").sum()
            timed_out_count = (batch_df["Timed Out"] == "Yes").sum()

            st.write(
                f"Batch {batch_num + 1} finished: "
                f"{success_count} success, {fail_count} failed, {timed_out_count} timed out."
            )

            if os.path.exists(paths["master_csv"]):
                temp_df = pd.read_csv(paths["master_csv"])
                with download_section:
                    st.download_button(
                        f"⬇️ Download Progress (after batch {batch_num + 1})",
                        data=temp_df.to_csv(index=False).encode("utf-8"),
                        file_name=f"{run_name}_progress_batch_{batch_num + 1}.csv",
                        mime="text/csv",
                        key=f"download_progress_{batch_num}"
                    )

            with download_section:
                st.download_button(
                    f"⬇️ Download Batch {batch_num + 1} Only",
                    data=batch_df.to_csv(index=False).encode("utf-8"),
                    file_name=f"{run_name}_batch_{batch_num + 1}.csv",
                    mime="text/csv",
                    key=f"download_batch_only_{batch_num}"
                )

            if not failed_batch.empty:
                with download_section:
                    st.download_button(
                        f"⬇️ Download Failed URLs (batch {batch_num + 1})",
                        data=failed_batch.to_csv(index=False).encode("utf-8"),
                        file_name=f"{run_name}_failed_batch_{batch_num + 1}.csv",
                        mime="text/csv",
                        key=f"download_failed_batch_{batch_num}"
                    )

        if stop_after_batches != 0 and batches_to_run < total_batches:
            st.warning(
                f"Stopped after {batches_to_run} batches on purpose. "
                f"Re-run with the same run name and Resume mode checked to continue."
            )
        else:
            st.success("🎉 Scrape complete.")

        if os.path.exists(paths["master_csv"]):
            final_df = pd.read_csv(paths["master_csv"])
            st.subheader("Final / Current results preview")
            st.dataframe(final_df.tail(50), use_container_width=True)

            st.download_button(
                "📥 Download Full Results CSV",
                data=final_df.to_csv(index=False).encode("utf-8"),
                file_name=f"{run_name}_results.csv",
                mime="text/csv"
            )

        if os.path.exists(paths["failed_csv"]):
            failed_df = pd.read_csv(paths["failed_csv"])
            st.download_button(
                "📥 Download All Failed URLs CSV",
                data=failed_df.to_csv(index=False).encode("utf-8"),
                file_name=f"{run_name}_failed_urls.csv",
                mime="text/csv"
            )

        st.caption(f"Saved files are temporarily stored in: {paths['dir']}")

    except Exception as e:
        st.error(f"Something broke during the scrape: {e}")
