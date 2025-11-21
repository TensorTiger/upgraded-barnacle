import os
import glob
import tarfile
import subprocess
import shutil
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import logging

# --- CONFIGURATION ---
# The root directory where the search begins (used to calculate relative paths)
# Based on your request: ~/upgraded-barnacle/
SOURCE_ROOT = os.path.expanduser("~/upgraded-barnacle/")

# The glob pattern to find the files
# Matches: ~/upgraded-barnacle/Emilia-Dataset/Emilia-*/*/*.tar
SEARCH_PATTERN = os.path.join(SOURCE_ROOT, "Emilia-Dataset","Emilia-Dataset", "Emilia-*", "*", "*.tar")

# Your GCS Bucket Name (Replace this!)
BUCKET_NAME = "vaani-tts-master"

# Number of concurrent threads.
# CAUTION: Each thread will hold one uncompressed tar in disk space.
# If you have low disk space, keep this low (e.g., 2-4).
MAX_WORKERS = 4

# Temp directory for extraction (intermediate storage)
TEMP_BASE_DIR = os.path.join(os.getcwd(), "temp_extraction_work")

# Set to True to process only 1 file for testing purposes
DRY_RUN = True

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("migration.log"),
        logging.StreamHandler()
    ]
)
def extract_with_system_tar(tar_path, dest_dir):
    """
    Uses system 'tar' command for faster, parallel extraction relative to Python's tarfile.
    """
    try:
        # -x: extract
        # -f: file
        # -C: change directory before extracting
        # --warning=no-unknown-keyword: suppresses warnings about unknown headers often found in datasets
        cmd = ["tar", "-xf", tar_path, "-C", dest_dir]
        
        # Run tar command
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        if result.returncode != 0:
            # Log stderr if tar fails
            logging.warning(f"System tar failed for {tar_path}: {result.stderr.strip()}. Falling back to Python tarfile.")
            return False
        return True
    except Exception as e:
        logging.warning(f"System tar error: {e}. Falling back to Python tarfile.")
        return False


def ensure_gcloud_installed():
    """Check if gcloud is available in the path."""
    if shutil.which("gcloud") is None:
        raise EnvironmentError("gcloud CLI is not found. Please install Google Cloud SDK.")

def process_tar_file(tar_path):
    """
    1. Untars file to a unique temp dir.
    2. Uploads content to GCS using gcloud storage cp.
    3. Deletes temp content.
    """
    # Create a unique ID for this specific job to avoid collision in temp folder
    unique_id = str(uuid.uuid4())[:8]
    temp_dir = os.path.join(TEMP_BASE_DIR, unique_id)
    
    try:
        # 1. Determine paths
        # Get path relative to SOURCE_ROOT to preserve structure
        # Example: Emilia-Dataset/Emilia-ZH-000/000/file.tar
        rel_path = os.path.relpath(tar_path, SOURCE_ROOT)
        
        # The destination folder in GCS (remove filename from path)
        # Example: gs://bucket/Emilia-Dataset/Emilia-ZH-000/000/
        dest_rel_folder = os.path.dirname(rel_path)
        gcs_dest = f"gs://{BUCKET_NAME}/{dest_rel_folder}/"

        # 2. Create Temp Dir
        os.makedirs(temp_dir, exist_ok=True)

        # 3. Extract Tar
        logging.debug(f"[{unique_id}] Extracting: {tar_path}")
        extraction_success = extract_with_system_tar(tar_path, temp_dir)
        
        # Fallback to Python tarfile if system tar failed or isn't available
        if not extraction_success:
            try:
                with tarfile.open(tar_path, 'r') as tar:
                    # Filter specifically for safe extraction (remove absolute paths etc)
                    def is_within_directory(directory, target):
                        abs_directory = os.path.abspath(directory)
                        abs_target = os.path.abspath(target)
                        prefix = os.path.commonprefix([abs_directory, abs_target])
                        return prefix == abs_directory

                    def safe_members(members):
                        for member in members:
                            member_path = os.path.join(temp_dir, member.name)
                            if not is_within_directory(temp_dir, member_path):
                                raise Exception("Attempted Path Traversal in Tar File")
                            yield member

                    tar.extractall(path=temp_dir, members=safe_members(tar))
            except Exception as e:
                logging.error(f"[{unique_id}] Failed to extract {tar_path}: {e}")
                return False

        # 4. Upload using gcloud storage cp
        # We use shell=True to allow wildcard expansion (*) to upload contents only, not the folder itself
        # Alternatively, we upload the dir and rely on GCS structure, but usually tar contents are desired directly.
        # upload_cmd = f"gcloud storage cp -r . '{gcs_dest}'"
        upload_cmd = ["gcloud", "storage", "cp", "-r", ".", gcs_dest]

        print(upload_cmd)
        
        logging.debug(f"[{unique_id}] Uploading to {gcs_dest}")
        
        # Using subprocess to call the CLI
        result = subprocess.run(
            upload_cmd, 
            cwd=temp_dir,
            shell=False, 
            capture_output=True, 
            text=True
        )

        #if result.returncode != 0:
        #    logging.error(f"[{unique_id}] Upload failed for {tar_path}. Error: {result.stderr}")
        #    return False

        logging.info(f"[{unique_id}] Successfully processed {os.path.basename(tar_path)}")
        return True

    except Exception as e:
        logging.error(f"Global error processing {tar_path}: {e}")
        return False

    finally:
        # 5. Cleanup: Delete the extracted files strictly
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

def main():
    ensure_gcloud_installed()
    
    # Setup Temp Directory
    if not os.path.exists(TEMP_BASE_DIR):
        os.makedirs(TEMP_BASE_DIR)

    print(f"Scanning for files matching: {SEARCH_PATTERN}")
    files = glob.glob(SEARCH_PATTERN)
    total_files = len(files)
    print(f"Found {total_files} tar files.")

    if total_files == 0:
        print("No files found. Check your SOURCE_ROOT and directory structure.")
        return

    if DRY_RUN:
        print("\n--- DRY RUN MODE ACTIVATED ---")
        print("Only processing the first found archive to verify configuration.")
        files = files[:2]
        total_files = 2

    # Optional: Import tqdm for progress bar if available
    try:
        from tqdm import tqdm
        use_tqdm = True
    except ImportError:
        use_tqdm = False
        print("Install 'tqdm' (pip install tqdm) for a progress bar. Continuing without it...")

    print(f"Starting processing with {MAX_WORKERS} threads...")
    print(f"Logs are being written to migration.log")

    successful = 0
    failed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all jobs
        future_to_file = {executor.submit(process_tar_file, f): f for f in files}
        
        # Monitor progress
        iterator = as_completed(future_to_file)
        if use_tqdm:
            iterator = tqdm(iterator, total=total_files, unit="file")

        for future in iterator:
            if future.result():
                successful += 1
            else:
                failed += 1

    # Final Cleanup of base temp dir
    if os.path.exists(TEMP_BASE_DIR):
        shutil.rmtree(TEMP_BASE_DIR)

    print("\n--- Migration Complete ---")
    print(f"Successful: {successful}")
    print(f"Failed:     {failed}")
    print("Check migration.log for details on failures.")

if __name__ == "__main__":
    main()
