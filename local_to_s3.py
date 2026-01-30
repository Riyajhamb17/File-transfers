# aws s3 sync dataset_by_label s3://kinetics-600/labels

import os
import shutil

# ===================== CONFIG =====================
EXTRACTED_ROOT = "Kinetics600_Dataset"     # folder where zip was extracted
OUTPUT_ROOT = "restructured"         # final clean dataset

VIDEO_EXTENSIONS = (".mp4", ".mov", ".avi", ".mkv")
# ==================================================


def ensure_unique_path(dst_path):
    """
    If file exists, append _1, _2, etc.
    """
    if not os.path.exists(dst_path):
        return dst_path

    base, ext = os.path.splitext(dst_path)
    counter = 1

    while True:
        new_path = f"{base}_{counter}{ext}"
        if not os.path.exists(new_path):
            return new_path
        counter += 1


def restructure_dataset():
    os.makedirs(OUTPUT_ROOT, exist_ok=True)

    moved = 0
    skipped = 0

    # Walk resolution folders
    for resolution in os.listdir(EXTRACTED_ROOT):
        res_path = os.path.join(EXTRACTED_ROOT, resolution)

        if not os.path.isdir(res_path):
            continue

        print(f"\n📂 Resolution: {resolution}")

        # Walk label folders
        for label in os.listdir(res_path):
            label_src = os.path.join(res_path, label)

            if not os.path.isdir(label_src):
                continue

            label_dst = os.path.join(OUTPUT_ROOT, label)
            os.makedirs(label_dst, exist_ok=True)

            print(f"   🏷 Label: {label}")

            # Walk videos
            for file in os.listdir(label_src):
                if not file.lower().endswith(VIDEO_EXTENSIONS):
                    skipped += 1
                    continue

                src_path = os.path.join(label_src, file)
                dst_path = os.path.join(label_dst, file)
                dst_path = ensure_unique_path(dst_path)

                shutil.move(src_path, dst_path)
                moved += 1

    print("\n✅ Restructuring complete!")
    print(f"📦 Videos moved: {moved}")
    print(f"⏭ Files skipped: {skipped}")


if __name__ == "__main__":
    restructure_dataset()
