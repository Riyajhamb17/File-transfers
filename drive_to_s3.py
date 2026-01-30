
import os
import time
import boto3
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.exceptions import ClientError
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from tqdm import tqdm

# ---------------- CONFIG ----------------
ROOT_DRIVE_FOLDER_ID = "10j7Z3ySuFNgzVwEXE16TDXWBWHNh8UO2"

S3_BUCKET = "kinetics-600"
S3_BASE_PREFIX = "labels"

VIDEO_EXTENSIONS = (".mp4", ".mov", ".avi", ".mkv")

TEMP_DIR = "temp_download"

MAX_RETRIES = 5
CHUNK_SIZE = 1024 * 1024      # 1MB (Drive-safe)
MAX_WORKERS = 2               # Drive-safe
# --------------------------------------


# ---------- AUTH ----------
def authenticate_drive():
    gauth = GoogleAuth()
    gauth.settings["http_timeout"] = 120
    gauth.LocalWebserverAuth()
    return GoogleDrive(gauth)


# ---------- DRIVE HELPERS ----------
def list_children(drive, folder_id):
    return drive.ListFile({
        "q": f"'{folder_id}' in parents and trashed=false"
    }).GetList()


def download_with_restart(file_obj, final_path):
    """
    Reliable restart-based download:
    - Deletes partial file on failure
    - Retries from byte 0
    - Shows progress + ETA
    """
    file_size = int(file_obj.get("fileSize", 0))

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # Always start clean
            if os.path.exists(final_path):
                os.remove(final_path)

            file_obj.http = None  # force fresh connection

            with tqdm(
                total=file_size,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                desc=f"⬇️ {file_obj['title']}",
                leave=False
            ) as pbar:

                def progress_cb(current, total):
                    pbar.update(current - pbar.n)

                file_obj.GetContentFile(
                    final_path,
                    chunksize=CHUNK_SIZE,
                    callback=progress_cb
                )

            return True

        except Exception as e:
            print(f"\n⚠️ Download failed (attempt {attempt}): {e}")
            if os.path.exists(final_path):
                os.remove(final_path)

            time.sleep(2 ** attempt)

    return False


# ---------- S3 HELPERS ----------
def s3_object_exists(s3, key):
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise


def upload_with_retry(local_path, s3_key, s3):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            s3.upload_file(local_path, S3_BUCKET, s3_key)
            print(f"☁️ Uploaded → {s3_key}")
            return True
        except Exception as e:
            print(f"⚠️ Upload failed (attempt {attempt}): {e}")
            time.sleep(2 ** attempt)
    return False


# ---------- PIPELINE ----------
def process_single_video(video, label_name, s3):
    filename = video["title"]
    s3_key = f"{S3_BASE_PREFIX}/{label_name}/{filename}"

    # Skip if already uploaded
    try:
        if s3_object_exists(s3, s3_key):
            print(f"⏭ Skipped: {filename}")
            return
    except Exception as e:
        print(f"❌ S3 check failed: {e}")
        return

    safe_name = f"{video['id']}_{filename}"
    local_path = os.path.join(TEMP_DIR, safe_name)

    print(f"\n⬇️ Starting: {filename}")

    if not download_with_restart(video, local_path):
        print(f"❌ Permanent download failure: {filename}")
        return

    if not upload_with_retry(local_path, s3_key, s3):
        print(f"❌ Upload failed permanently: {filename}")

    try:
        os.remove(local_path)
    except:
        pass


def process_drive_structure(drive):
    os.makedirs(TEMP_DIR, exist_ok=True)

    s3 = boto3.client(
        "s3",
        config=boto3.session.Config(
            retries={"max_attempts": 5},
            max_pool_connections=10
        )
    )

    resolution_folders = list_children(drive, ROOT_DRIVE_FOLDER_ID)

    for res in resolution_folders:
        if res["mimeType"] != "application/vnd.google-apps.folder":
            continue

        print(f"\n📂 Resolution: {res['title']}")

        label_folders = list_children(drive, res["id"])

        for label in label_folders:
            if label["mimeType"] != "application/vnd.google-apps.folder":
                continue

            label_name = label["title"]
            print(f"   🏷 Label: {label_name}")

            videos = list_children(drive, label["id"])

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = [
                    executor.submit(process_single_video, v, label_name, s3)
                    for v in videos
                    if v["title"].lower().endswith(VIDEO_EXTENSIONS)
                ]

                for f in as_completed(futures):
                    try:
                        f.result()
                    except Exception as e:
                        print(f"❌ Worker crashed, skipping item: {e}")


# ---------- MAIN ----------
def main():
    print("🔐 Authenticating Google Drive...")
    drive = authenticate_drive()

    print("🚀 Drive → S3 (restart-based resume, production-safe)...")
    process_drive_structure(drive)

    print("\n✅ Pipeline completed safely!")


if __name__ == "__main__":
    main()
