import os
import glob
import time

# Keep logs from the past 7 days
cutoff_days = 7
cutoff_time = time.time() - (cutoff_days * 86400)  # 86400 seconds/day

# Find all .log files in the root folder
log_files = glob.glob("*.log")

deleted = []

for log_file in log_files:
    file_mtime = os.path.getmtime(log_file)
    if file_mtime < cutoff_time:
        try:
            os.remove(log_file)
            deleted.append(log_file)
        except Exception as e:
            print(f"Error deleting {log_file}: {e}")

# Summary
if deleted:
    print(f"Deleted {len(deleted)} old log(s):")
    for f in deleted:
        print(f"  - {f}")
else:
    print("No logs older than one week were found.")
