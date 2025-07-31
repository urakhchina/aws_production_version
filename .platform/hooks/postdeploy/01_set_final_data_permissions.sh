#!/bin/bash
# .platform/hooks/postdeploy/01_set_final_data_permissions.sh

LOG_FILE="/tmp/postdeploy_final_permissions.log" # Use a different log file name
DATA_DIR="/var/app/current/data"
UPLOAD_DIR="$DATA_DIR/uploads"
RAW_DIR="$DATA_DIR/raw"
PROCESSED_DIR="$DATA_DIR/processed"

echo "--- Starting POSTDEPLOY permission fix at $(date) ---" | tee -a $LOG_FILE

# Function to set ownership and permissions
set_perms() {
  local TARGET_DIR="$1"
  echo "Processing directory: $TARGET_DIR" | tee -a $LOG_FILE
  if [ ! -d "$TARGET_DIR" ]; then
    echo "Creating directory: $TARGET_DIR" | tee -a $LOG_FILE
    mkdir -p "$TARGET_DIR" >> $LOG_FILE 2>&1
  fi
  echo "Setting ownership to webapp:webapp for $TARGET_DIR" | tee -a $LOG_FILE
  chown -R webapp:webapp "$TARGET_DIR" >> $LOG_FILE 2>&1
  echo "Setting permissions to 775 for $TARGET_DIR" | tee -a $LOG_FILE
  chmod -R 775 "$TARGET_DIR" >> $LOG_FILE 2>&1 # Recursive for subdirs too
  ls -ld "$TARGET_DIR" >> $LOG_FILE 2>&1
}

# Apply to parent data directory first (not recursive for chown initially if subdirs handled separately)
echo "Processing parent directory: $DATA_DIR" | tee -a $LOG_FILE
if [ ! -d "$DATA_DIR" ]; then
    mkdir -p "$DATA_DIR" >> $LOG_FILE 2>&1
fi
chown webapp:webapp "$DATA_DIR" >> $LOG_FILE 2>&1 # Just the data dir itself
chmod 775 "$DATA_DIR" >> $LOG_FILE 2>&1
ls -ld "$DATA_DIR" >> $LOG_FILE 2>&1


# Apply to subdirectories
set_perms "$UPLOAD_DIR"
set_perms "$RAW_DIR"
set_perms "$PROCESSED_DIR"

echo "--- Finished POSTDEPLOY permission fix at $(date) ---" | tee -a $LOG_FILE
exit 0 # IMPORTANT: Hooks must exit with 0 for success