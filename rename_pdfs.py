import os
import sys


def rename_pdfs(folder_path):
    if not os.path.isdir(folder_path):
        print(f"Error: '{folder_path}' is not a valid directory.")
        sys.exit(1)

    pdf_files = [f for f in os.listdir(folder_path) if f.lower().endswith(".pdf")]

    if not pdf_files:
        print(f"No PDF files found in '{folder_path}'.")
        return

    renamed_count = 0
    skipped_count = 0

    for filename in pdf_files:
        if " " not in filename:
            skipped_count += 1
            continue

        new_filename = filename.replace(" ", "_")
        old_path = os.path.join(folder_path, filename)
        new_path = os.path.join(folder_path, new_filename)

        if os.path.exists(new_path):
            print(f"Skipped: '{filename}' -> '{new_filename}' (target already exists)")
            skipped_count += 1
            continue

        os.rename(old_path, new_path)
        print(f"Renamed: '{filename}' -> '{new_filename}'")
        renamed_count += 1

    print(f"\nDone. {renamed_count} file(s) renamed, {skipped_count} file(s) skipped.")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python rename_pdfs.py <folder_path>")
        sys.exit(1)

    rename_pdfs(sys.argv[1])
