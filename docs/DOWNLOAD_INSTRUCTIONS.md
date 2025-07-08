# Downloading Output Directory from SSH

## Quick Commands

### 1. **Using rsync (Recommended - Preserves structure and resumes)**
```bash
# From your local machine:
rsync -avz --progress qasseml@cpm1.emp.vumc.io:~/Metamap-Cpm1/output_csvs/ ./output_csvs/

# With compression for faster transfer:
rsync -avz --compress-level=9 --progress qasseml@cpm1.emp.vumc.io:~/Metamap-Cpm1/output_csvs/ ./output_csvs/
```

### 2. **Using scp (Simple but no resume)**
```bash
# Download entire directory:
scp -r qasseml@cpm1.emp.vumc.io:~/Metamap-Cpm1/output_csvs ./

# With compression:
scp -C -r qasseml@cpm1.emp.vumc.io:~/Metamap-Cpm1/output_csvs ./
```

### 3. **Create a tar archive first (Good for large directories)**
```bash
# On the server (cpm1):
cd ~/Metamap-Cpm1
tar -czf output_csvs.tar.gz output_csvs/

# Then download from your local machine:
scp qasseml@cpm1.emp.vumc.io:~/Metamap-Cpm1/output_csvs.tar.gz ./

# Extract locally:
tar -xzf output_csvs.tar.gz
```

### 4. **Using zip (If you prefer zip format)**
```bash
# On the server (cpm1):
cd ~/Metamap-Cpm1
zip -r output_csvs.zip output_csvs/

# Download from local:
scp qasseml@cpm1.emp.vumc.io:~/Metamap-Cpm1/output_csvs.zip ./

# Extract locally:
unzip output_csvs.zip
```

## Advanced Options

### Download only CSV files (exclude logs)
```bash
rsync -avz --progress --include="*.csv" --include="*/" --exclude="*" \
  qasseml@cpm1.emp.vumc.io:~/Metamap-Cpm1/output_csvs/ ./output_csvs/
```

### Download with bandwidth limit (if network is slow)
```bash
rsync -avz --progress --bwlimit=1000 \
  qasseml@cpm1.emp.vumc.io:~/Metamap-Cpm1/output_csvs/ ./output_csvs/
```

### Check size before downloading
```bash
ssh qasseml@cpm1.emp.vumc.io "du -sh ~/Metamap-Cpm1/output_csvs/"
```

### Count files before downloading
```bash
ssh qasseml@cpm1.emp.vumc.io "find ~/Metamap-Cpm1/output_csvs -name '*.csv' | wc -l"
```

## Windows Users

### Using WinSCP (GUI)
1. Download WinSCP from https://winscp.net/
2. Connect to: `cpm1.emp.vumc.io`
3. Username: `qasseml`
4. Navigate to: `/home/qasseml/Metamap-Cpm1/output_csvs/`
5. Drag and drop to download

### Using PowerShell with scp
```powershell
# In PowerShell:
scp -r qasseml@cpm1.emp.vumc.io:~/Metamap-Cpm1/output_csvs C:\Users\YourName\Desktop\
```

## Tips

1. **For large transfers**, use `screen` or `tmux` on the server side if creating archives
2. **rsync is best** because it can resume interrupted transfers
3. **Add `-v` for verbose output** to see what's being transferred
4. **Use compression** (`-z` for rsync, `-C` for scp) for faster transfers
5. **Check available space** on your local machine first

## Troubleshooting

If connection times out:
```bash
# Add SSH options for keepalive:
rsync -avz -e "ssh -o ServerAliveInterval=60" --progress \
  qasseml@cpm1.emp.vumc.io:~/Metamap-Cpm1/output_csvs/ ./output_csvs/
```

If permission denied:
```bash
# Make sure you have read permissions on server:
ssh qasseml@cpm1.emp.vumc.io "chmod -R 755 ~/Metamap-Cpm1/output_csvs/"
``` 