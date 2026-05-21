
# line 139 -- list all statuses (both files and dirs)
stats = fs.listStatus(s3_path)

# line 140 -- existing code -- filters only directories
dirs = [st.getPath().getName() for st in stats if st.isDirectory()]

# ADD THIS -- filters only files (not directories)
files = [st.getPath().getName() for st in stats if not st.isDirectory()]

# print both
print("Directories:", dirs)
print("Files:", files)

sys.exit(0)
