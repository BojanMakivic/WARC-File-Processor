import streamlit as st
import gzip
import os
import requests
import glob
import warcio
from warcio.archiveiterator import ArchiveIterator

# Constants
CC_BASE = "https://data.commoncrawl.org/"

st.set_page_config(page_title="WARC Processor", layout="wide")
st.title("📦 WARC File Processor")

# Initialize session state for stopping downloads
if 'stop_download' not in st.session_state:
    st.session_state.stop_download = False

# --- Step 1: Upload .gz of segment paths ---
st.header("1. Upload robotstxt.paths.gz file")
uploaded_file = st.file_uploader("Upload .gz file containing WARC segment paths", type=['gz'])
segment_paths = []
if uploaded_file:
    try:
        with gzip.open(uploaded_file, "rt", encoding="utf-8") as f:
            for line in f:
                segment_paths.append(line.strip())
        
        #with gzip.GzipFile(fileobj=uploaded_file) as f:
            #for raw in f:
                #line = raw.decode('utf-8').strip()
                #if line:
                   # segment_paths.append(line)
        st.success(f"Loaded file with {len(segment_paths)} segment paths.")
    except Exception as e:
        st.error(f"Failed to read uploaded file: {e}")

# --- Step 2: Download segments ---
st.header("2. Download WARC segments")
download_dir = st.text_input("Local directory to store downloaded WARC segments", value=os.getcwd())
col1, col2 = st.columns(2)
with col1:
    start_dl = st.button("Download segments")
with col2:
    if st.button("Stop Download"):
        st.session_state.stop_download = True

if start_dl and segment_paths and download_dir:
    os.makedirs(download_dir, exist_ok=True)
    download_progress = st.progress(0)
    status_text = st.empty()
    total = len(segment_paths)
    for idx, rel_path in enumerate(segment_paths):
        if st.session_state.stop_download:
            status_text.warning("Download stopped by user.")
            break
        percent = int((idx + 1) / total * 100)
        status_text.text(f"Downloading segment {idx+1}/{total} ({percent}%)")
        url = CC_BASE + rel_path
        local_fname = os.path.basename(rel_path)
        local_path = os.path.join(download_dir, local_fname)
        try:
            with requests.get(url, stream=True) as r:
                r.raise_for_status()
                with open(local_path, 'wb') as out_f:
                    for chunk in r.iter_content(chunk_size=8192):
                        out_f.write(chunk)
        except Exception as e:
            st.warning(f"Error downloading {url}: {e}")
        download_progress.progress((idx + 1) / total)
    if not st.session_state.stop_download:
        status_text.success("All segments downloaded.")
    download_progress.empty()
    st.session_state.stop_download = False

# --- Step 3: Process downloaded WARCs ---
st.header("3. Process downloaded WARC files into a single text file")
warc_folder = st.text_input("Directory where WARC files are stored", value=download_dir)
output_path = st.text_input("Output .txt file path", value="output.txt")
if st.button("Process WARCs"):
    if not os.path.isdir(warc_folder):
        st.error("Invalid WARC folder path.")
    else:
        warc_files = glob.glob(os.path.join(warc_folder, "*.warc.gz"))#[f for f in os.listdir(warc_folder) if f.endswith('.warc.gz')]
        if not warc_files:
            st.warning("No .warc.gz files found in the specified directory.")
        else:
            try:
                total = len(warc_files)
                process_progress = st.progress(0)
                status_text = st.empty()
                with open(output_path, 'w', encoding='utf-8') as out_f:
                    for idx, fname in enumerate(warc_files):
                        percent = int((idx + 1) / total * 100)
                        status_text.text(f"Processing {idx+1}/{total} ({percent}%) - {fname}")
                        # Find the matching robot path for this basename
                        basename = os.path.basename(fname)
                        matches = [p for p in segment_paths if basename in p]
                        if matches:
                            chosen = matches[0]
                        else:
                            chosen = basename
                        remote_url = CC_BASE + chosen

                        # Write header info
                        out_f.write("\n" + "#" * 80 + "\n")
                        out_f.write(f"Processing file: {fname}\n")
                        out_f.write("#" * 80 + "\n")
                        # Iterate WARC records
                        full_path = os.path.join(warc_folder, fname)
                        with gzip.open(full_path, 'rb') as gz:
                            for record in ArchiveIterator(gz):
                                url = record.rec_headers.get_header("WARC-Target-URI")
                                if url and ".at/" in url and record.rec_type == 'response':
                                    content = record.content_stream().read().decode("utf-8", errors="ignore")
                                    out_f.write("=" * 80 + "\n")
                                    out_f.write(f"URL: {url}\n")
                                    out_f.write(f"File: {remote_url}\n")
                                    out_f.write(f"Date: {record.rec_headers.get_header('WARC-Date')}\n")
                                    server = (record.http_headers.get_header('Server') if record.http_headers else 'N/A')
                                    out_f.write(f"Server: {server}\n")
                                    ip = record.rec_headers.get_header('WARC-IP-Address') or 'N/A'
                                    out_f.write(f"IP: {ip}\n")
                                    content_length = (record.http_headers.get_header('Content-Length') if record.http_headers else 'N/A')
                                    out_f.write(f"Content Length: {content_length}\n")
                                    content_type = (record.http_headers.get_header('Content-Type') if record.http_headers else 'N/A')
                                    out_f.write(f"Content Type: {content_type}\n")
                                    status_code = (record.http_headers.get_statuscode() if record.http_headers else 'N/A')
                                    out_f.write(f"Status code: {status_code}\n")
                                    if status_code == '200':
                                        out_f.write("Content:\n")
                                        out_f.write(content + "\n")
                                    out_f.write("\n" + "=" * 80 + "\n\n")
                        process_progress.progress((idx + 1) / total)
                status_text.success(f"Processing complete. Output saved to {output_path}")
                process_progress.empty()
                with open(output_path, 'r', encoding='utf-8') as f:
                    st.download_button("Download Output File", f, file_name=os.path.basename(output_path), mime='text/plain')
            except Exception as e:
                st.error(f"Error during processing: {e}")
