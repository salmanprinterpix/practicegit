#!/bin/bash

#################################################################################
# SQL Server to PostgreSQL Migration Automation Script
# This script automates the migration process using sqlserver2pgsql utility
# with Microsoft Teams notification on completion
#################################################################################

set -e  # Exit on any error

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored messages
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to format duration
format_duration() {
    local total_seconds=$1
    local hours=$((total_seconds / 3600))
    local minutes=$(((total_seconds % 3600) / 60))
    local seconds=$((total_seconds % 60))
    printf "%02d:%02d:%02d" "$hours" "$minutes" "$seconds"
}

# Function to send Microsoft Teams notification
send_teams_notification() {
    local status=$1
    local duration=$2
    local sqlserver_db=$3
    local postgres_db=$4
    local error_msg=${5:-""}

    if [ -z "$TEAMS_WEBHOOK_URL" ]; then
        print_warning "TEAMS_WEBHOOK_URL not set. Skipping Teams notification."
        return 0
    fi

    local color
    local status_text
    local title

    if [ "$status" = "success" ]; then
        color="28a745"  # Green
        status_text="✅ Success"
        title="Migration Completed Successfully"
    else
        color="dc3545"  # Red
        status_text="❌ Failed"
        title="Migration Failed"
    fi

    local timestamp=$(date '+%Y-%m-%d %H:%M:%S %Z')
    local hostname=$(hostname)

    # Create JSON payload for Teams adaptive card
    local json_payload=$(cat <<EOF
{
    "@type": "MessageCard",
    "@context": "https://schema.org/extensions",
    "themeColor": "${color}",
    "summary": "Database Migration ${status_text}",
    "sections": [{
        "activityTitle": "${title}",
        "activitySubtitle": "SQL Server → PostgreSQL Migration",
        "activityImage": "https://img.icons8.com/color/96/000000/database.png",
        "facts": [
            {
                "name": "Status:",
                "value": "${status_text}"
            },
            {
                "name": "Duration:",
                "value": "${duration}"
            },
            {
                "name": "Source Database:",
                "value": "${sqlserver_db}"
            },
            {
                "name": "Target Database:",
                "value": "${postgres_db}"
            },
            {
                "name": "Server:",
                "value": "${hostname}"
            },
            {
                "name": "Completed At:",
                "value": "${timestamp}"
            }
EOF
)

    # Add error message if migration failed
    if [ "$status" = "failed" ] && [ -n "$error_msg" ]; then
        json_payload="${json_payload},"$(cat <<EOF
            {
                "name": "Error:",
                "value": "${error_msg}"
            }
EOF
)
    fi

    # Close the JSON
    json_payload="${json_payload}"$(cat <<'EOF'
        ],
        "markdown": true
    }]
}
EOF
)

    # Send notification to Teams
    local response
    response=$(curl -s -w "\n%{http_code}" -X POST "$TEAMS_WEBHOOK_URL" \
        -H "Content-Type: application/json" \
        -d "$json_payload")

    local http_code=$(echo "$response" | tail -n1)

    if [ "$http_code" = "200" ]; then
        print_info "Teams notification sent successfully"
    else
        print_warning "Failed to send Teams notification. HTTP code: $http_code"
    fi
}

# Enable full script logging
set -o pipefail
TIMESTAMP="$(date '+%Y%m%d-%H%M%S')"
SCRIPT_LOG_DIR="/tmp"
SCRIPT_LOG="$SCRIPT_LOG_DIR/migration-${TIMESTAMP}.log"
mkdir -p "$SCRIPT_LOG_DIR"
exec > >(tee -a "$SCRIPT_LOG") 2>&1
print_info "All output is being logged to: $SCRIPT_LOG"

# Record start time for duration calculation
MIGRATION_START_TIME=$(date +%s)

# Live top-right timer display (writes to /dev/tty to avoid logging)
TIMER_PID=""
start_timer_display() {
    # Skip if no terminal is attached
    if [ ! -w /dev/tty ]; then
        return 0
    fi
    (
        local start_ts=$(date +%s)
        local tty="/dev/tty"
        while true; do
            local now cols elapsed hours minutes seconds text pos_row pos_col
            now=$(date +%s)
            cols=$(tput cols 2>/dev/null || echo 80)
            elapsed=$((now - start_ts))
            hours=$((elapsed / 3600))
            minutes=$(((elapsed % 3600) / 60))
            seconds=$((elapsed % 60))
            printf -v text "⏱ %02d:%02d:%02d" "$hours" "$minutes" "$seconds"
            pos_row=0
            pos_col=$((cols - ${#text} - 1))
            if [ "$pos_col" -lt 0 ]; then pos_col=0; fi
            # Save cursor, move to top-right, draw, restore cursor
            printf "\0337" > "$tty"
            printf "\033[%d;%dH\033[1m%s\033[0m" $((pos_row + 1)) $((pos_col + 1)) "$text" > "$tty"
            printf "\0338" > "$tty"
            sleep 1
        done
    ) &
    TIMER_PID=$!
}

stop_timer_display() {
    if [ -n "$TIMER_PID" ] && kill -0 "$TIMER_PID" 2>/dev/null; then
        kill "$TIMER_PID" >/dev/null 2>&1 || true
        wait "$TIMER_PID" 2>/dev/null || true
    fi
    if [ -w /dev/tty ]; then
        local cols text pos_row pos_col
        cols=$(tput cols 2>/dev/null || echo 80)
        text="⏱ 00:00:00"
        pos_row=0
        pos_col=$((cols - ${#text} - 1))
        if [ "$pos_col" -lt 0 ]; then pos_col=0; fi
        # Clear the timer area
        printf "\0337" > /dev/tty
        printf "\033[%d;%dH%*s" $((pos_row + 1)) $((pos_col + 1)) ${#text} "" > /dev/tty
        printf "\0338" > /dev/tty
    fi
}

# Cleanup function that sends notification on exit
cleanup_and_notify() {
    local exit_code=$?
    stop_timer_display

    MIGRATION_END_TIME=$(date +%s)
    MIGRATION_DURATION=$((MIGRATION_END_TIME - MIGRATION_START_TIME))
    FORMATTED_DURATION=$(format_duration $MIGRATION_DURATION)

    if [ $exit_code -eq 0 ]; then
        send_teams_notification "success" "$FORMATTED_DURATION" "$SQLSERVER_DATABASE" "$DATABASE_NAME"
    else
        local error_msg="Migration script exited with code $exit_code"
        send_teams_notification "failed" "$FORMATTED_DURATION" "$SQLSERVER_DATABASE" "$DATABASE_NAME" "$error_msg"
    fi
}

# Start timer immediately and ensure cleanup on exit
start_timer_display
trap cleanup_and_notify EXIT INT TERM

# Check for required commands
print_info "Checking required commands..."
for cmd in psql perl curl; do
    if ! command_exists "$cmd"; then
        print_error "$cmd is not installed. Please install it first."
        exit 1
    fi
done

#################################################################################
# Configuration - Modify these values as needed
#################################################################################

# Microsoft Teams Webhook URL (REQUIRED for notifications)
# Get this from Teams: Channel → Connectors → Incoming Webhook
TEAMS_WEBHOOK_URL="${TEAMS_WEBHOOK_URL:-https://syncoms.webhook.office.com/webhookb2/842b4711-48e1-4480-898f-35e053fb0f4f@49099e21-d9d9-45dc-b95b-f5f4afb3b3ff/JenkinsCI/c71ef45016064b3e8ad3335c5f55ddbe/27298b69-d954-44d3-b95b-6ff3939a423e/V2aUYTAJKMTx67C3FSAvk2R9Sok4ZlSVAYH9S2kl6iUTU1}"

# SQL Server Configuration
SQLSERVER_HOST="10.20.2.6"
SQLSERVER_PORT="1433"
SQLSERVER_USER="salman.k"
SQLSERVER_PASSWORD="salman123"

# PostgreSQL Configuration
POSTGRES_HOST="10.20.26.231"
POSTGRES_PORT="5432"
POSTGRES_USER="postgres"
POSTGRES_PASSWORD="pixROOT@789"

# Directory Paths
MASTER_MIGRATION_DIR="/opt/migration/master"
OPT_BASE_DIR="/opt"
PGSQL_BASE_DIR="/var/lib/pgsql"

# Kettle Configuration
KETTLE_LOG_DIR="/tmp"

#################################################################################
# Get Database Name from User Input
#################################################################################

if [ -z "$1" ]; then
    print_error "Usage: $0 <database_name> [sqlserver_database_name] [--recreate-db]"
    print_info "Example: $0 printerpix_nl_customer PrinterPixNL --recreate-db"
    exit 1
fi

DATABASE_NAME="$1"
if [ -n "$2" ]; then
    SQLSERVER_DATABASE="$2"
else
    read -p "Enter SQL Server source database name [default: $DATABASE_NAME]: " sqlserver_db_input
    SQLSERVER_DATABASE="${sqlserver_db_input:-$DATABASE_NAME}"
fi

# Parse additional optional flags
RECREATE_DB=""
# Shift off the first two positional args if both present
if [ -n "$2" ]; then
    shift 2
else
    shift 1
fi
while [ "$#" -gt 0 ]; do
    case "$1" in
        -r|--recreate-db)
            RECREATE_DB="yes"
            ;;
        *)
            print_warning "Unknown argument: $1"
            ;;
    esac
    shift
done

print_info "Starting migration for database: $DATABASE_NAME"
print_info "SQL Server source database: $SQLSERVER_DATABASE"

if [ -z "$TEAMS_WEBHOOK_URL" ]; then
    print_warning "TEAMS_WEBHOOK_URL is not set. Teams notifications will be skipped."
    print_info "To enable notifications, set the TEAMS_WEBHOOK_URL environment variable."
fi

#################################################################################
# Step 0: Create Required Directories
#################################################################################

print_info "Step 0: Creating required directories..."

PGSQL_DB_DIR="$PGSQL_BASE_DIR/$DATABASE_NAME" #/var/lib/pgsql/printerpix_nl
OPT_DB_DIR="$OPT_BASE_DIR/$DATABASE_NAME"  #/opt/printerpix_nl

# Create PostgreSQL directory
if [ ! -d "$PGSQL_DB_DIR" ]; then
    mkdir -p "$PGSQL_DB_DIR"
    print_info "Created directory: $PGSQL_DB_DIR"
else
    print_warning "Directory already exists: $PGSQL_DB_DIR"
fi

# Create /opt directory
if [ ! -d "$OPT_DB_DIR" ]; then
    mkdir -p "$OPT_DB_DIR"
    print_info "Created directory: $OPT_DB_DIR"
else
    print_warning "Directory already exists: $OPT_DB_DIR"
fi

# Copy master migration files
print_info "Copying files from $MASTER_MIGRATION_DIR to $OPT_DB_DIR (keeping only ${SQLSERVER_DATABASE}.sql)..."
if [ -d "$MASTER_MIGRATION_DIR" ]; then
    cp -r "$MASTER_MIGRATION_DIR"/* "$OPT_DB_DIR/"
    # Remove all .sql files except the one matching the SQL Server database name
    shopt -s nullglob
    for sql_file in "$OPT_DB_DIR"/*.sql; do
        base_name="$(basename "$sql_file")"
        if [ "$base_name" != "${SQLSERVER_DATABASE}.sql" ]; then
            rm -f "$sql_file"
        fi
    done
    # Ensure the matching .sql exists; copy it from master if missing
    if [ ! -f "$OPT_DB_DIR/${SQLSERVER_DATABASE}.sql" ]; then
        if [ -f "$MASTER_MIGRATION_DIR/${SQLSERVER_DATABASE}.sql" ]; then
            cp "$MASTER_MIGRATION_DIR/${SQLSERVER_DATABASE}.sql" "$OPT_DB_DIR/"
            print_info "Copied ${SQLSERVER_DATABASE}.sql to $OPT_DB_DIR"
        else
            print_warning "No matching SQL file found: ${SQLSERVER_DATABASE}.sql in $MASTER_MIGRATION_DIR"
        fi
    fi
    print_info "Files copied successfully"
else
    print_error "Master migration directory not found: $MASTER_MIGRATION_DIR"
    exit 1
fi

#################################################################################
# Pre-step: Cleanup Kettle files in /opt/printerpix_es and $OPT_DB_DIR
#################################################################################

print_info "Pre-step: Cleaning up Kettle *.ktr and *.kjb files..."

# Clean /opt/printerpix_es if present
if [ -d "/opt/printerpix_es" ]; then
    if compgen -G "/opt/printerpix_es/*.ktr" > /dev/null || compgen -G "/opt/printerpix_es/*.kjb" > /dev/null; then
        rm -f "/opt/printerpix_es"/*.ktr "/opt/printerpix_es"/*.kjb 2>/dev/null || true
        print_info "Deleted .ktr and .kjb files from /opt/printerpix_es"
    else
        print_info "No .ktr or .kjb files found in /opt/printerpix_es"
    fi
fi

# Clean $OPT_DB_DIR if present
if [ -d "$OPT_DB_DIR" ]; then
    if compgen -G "$OPT_DB_DIR/*.ktr" > /dev/null || compgen -G "$OPT_DB_DIR/*.kjb" > /dev/null; then
        rm -f "$OPT_DB_DIR"/*.ktr "$OPT_DB_DIR"/*.kjb 2>/dev/null || true
        print_info "Deleted .ktr and .kjb files from $OPT_DB_DIR"
    else
        print_info "No .ktr or .kjb files found in $OPT_DB_DIR"
    fi
fi

#################################################################################
# Step 1: Run sqlserver2pgsql.pl
#################################################################################

print_info "Step 1: Running sqlserver2pgsql.pl..."

BEFORE_SQL="$PGSQL_DB_DIR/before.sql"
AFTER_SQL="$PGSQL_DB_DIR/after.sql"
UNSURE_SQL="$PGSQL_DB_DIR/unsure.sql"
OUTPUT_SQL="$OPT_DB_DIR/${DATABASE_NAME}.sql" #/opt/printerpix_nl/printerpix_nl.sql

# Offer to delete previously generated SQL files if they exist
if [ -f "$BEFORE_SQL" ] || [ -f "$AFTER_SQL" ] || [ -f "$UNSURE_SQL" ]; then
    print_warning "Existing generated SQL files detected in $PGSQL_DB_DIR"
    read -p "Delete before.sql, after.sql, and unsure.sql before proceeding? [y/N]: " delete_choice
    case "$delete_choice" in
        [yY]|[yY][eE][sS])
            rm -f "$BEFORE_SQL" "$AFTER_SQL" "$UNSURE_SQL"
            print_info "Old SQL files deleted"
            ;;
        *)
            print_info "Keeping existing SQL files"
            ;;
    esac
fi

cd "$OPT_DB_DIR"

./sqlserver2pgsql.pl \
    -b "$BEFORE_SQL" \
    -a "$AFTER_SQL" \
    -u "$UNSURE_SQL" \
    -k "$OPT_DB_DIR" \
    -sd "$SQLSERVER_DATABASE" \
    -sh "$SQLSERVER_HOST" \
    -sp "$SQLSERVER_PORT" \
    -su "$SQLSERVER_USER" \
    -sw "$SQLSERVER_PASSWORD" \
    -pd "$DATABASE_NAME" \
    -ph "$POSTGRES_HOST" \
    -pp "$POSTGRES_PORT" \
    -pu "$POSTGRES_USER" \
    -pw "$POSTGRES_PASSWORD" \
    -f "$OUTPUT_SQL"

print_info "sqlserver2pgsql.pl completed successfully"

#################################################################################
# Step 1.5: Post-process generated SQL scripts (after.sql, unsure.sql)
#################################################################################

print_info "Step 1.5: Post-processing generated SQL scripts..."

for target in "$UNSURE_SQL" "$AFTER_SQL"; do
    if [ -f "$target" ]; then
        sed -i.bak 's/((isactive=(1)))/isactive = TRUE/g' "$target"
        sed -i.bak 's/((isarchived=(1)))/isarchived = TRUE/g' "$target"
        sed -i.bak 's/((isarchived=(0)))/isarchived = TRUE/g' "$target"
        sed -i.bak 's/newid()/uuid_generate_v4()/g' "$target"
        sed -i.bak 's/newsequentialid()/uuid_generate_v4()/g' "$target"
        sed -i.bak 's/((defaultshipping=(1)))/defaultshipping = TRUE/g' "$target"
        sed -i.bak 's/isanonymous=(0)/isanonymous=TRUE/g' "$target"
        sed -i.bak 's/suser_sname()/CURRENT_USER/g' "$target"
        sed -i.bak 's/getutcdate()/CURRENT_TIMESTAMP/g' "$target"
        # Strip unwanted psql meta and transaction wrappers
        sed -i.bak '/^[[:space:]]*\\set[[:space:]][[:space:]]*ON_ERROR_STOP.*$/d' "$target"
        sed -i.bak '/^[[:space:]]*\\set[[:space:]][[:space:]]*ECHO[[:space:]][[:space:]]*all.*$/d' "$target"
        sed -i.bak '/^[[:space:]]*BEGIN;[[:space:]]*$/d' "$target"
        sed -i.bak '${/^[[:space:]]*COMMIT;[[:space:]]*$/d;}' "$target"
    else
        print_warning "File not found for post-processing: $target"
    fi
done

#################################################################################
# Step 1.8: Optionally drop and recreate PostgreSQL database, ensure extensions
#################################################################################

# Ask user if they want to drop and recreate when flag not provided
if [ -z "$RECREATE_DB" ]; then
    read -p "Drop and recreate PostgreSQL database '$DATABASE_NAME'? This will erase existing data. [y/N]: " recreate_choice
    case "$recreate_choice" in
        [yY]|[yY][eE][sS])
            RECREATE_DB="yes"
            ;;
        *)
            RECREATE_DB=""
            ;;
    esac
fi

if [ -n "$RECREATE_DB" ]; then
    print_info "Dropping and recreating database '$DATABASE_NAME'..."
    PGPASSWORD="$POSTGRES_PASSWORD" psql -v ON_ERROR_STOP=1 -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -p "$POSTGRES_PORT" -d postgres <<EOF
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = '$DATABASE_NAME' AND pid <> pg_backend_pid();
DROP DATABASE IF EXISTS "$DATABASE_NAME";
CREATE DATABASE "$DATABASE_NAME";
EOF
    print_info "Ensuring extension uuid-ossp exists in '$DATABASE_NAME'..."
    PGPASSWORD="$POSTGRES_PASSWORD" psql -v ON_ERROR_STOP=1 -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -p "$POSTGRES_PORT" -d "$DATABASE_NAME" -c "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";"
else
    # Best effort to ensure extension (may fail if DB doesn't exist yet)
    print_info "Ensuring extension uuid-ossp exists in '$DATABASE_NAME'..."
    PGPASSWORD="$POSTGRES_PASSWORD" psql -v ON_ERROR_STOP=1 -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -p "$POSTGRES_PORT" -d "$DATABASE_NAME" -c "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";" || \
        print_warning "Could not create extension now (database may not exist yet)."
fi

#################################################################################
# Step 2: Create Tables using before.sql
#################################################################################

print_info "Step 2: Creating tables using before.sql..."

export PGPASSWORD="$POSTGRES_PASSWORD"

psql -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -p "$POSTGRES_PORT" -d "$DATABASE_NAME" < "$BEFORE_SQL"

print_info "Tables created successfully"

#################################################################################
# Step 3: Backup NOT NULL Constraints
#################################################################################

print_info "Step 3: Backing up NOT NULL constraints..."

NOT_NULL_BACKUP="$OPT_DB_DIR/not_null_constraints_backup_${DATABASE_NAME}.csv" #

# Use server-side COPY to STDOUT to avoid psql meta-command parsing issues
psql -v ON_ERROR_STOP=1 -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -p "$POSTGRES_PORT" -d "$DATABASE_NAME" \
    -c "COPY (
        SELECT table_schema, table_name, column_name, is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'public' AND is_nullable = 'NO'
    ) TO STDOUT WITH CSV HEADER" > "$NOT_NULL_BACKUP"

# Basic sanity check that the file was created and is not empty
if [ ! -s "$NOT_NULL_BACKUP" ]; then
    print_error "Failed to back up NOT NULL constraints to: $NOT_NULL_BACKUP"
    exit 1
fi

print_info "NOT NULL constraints backed up to: $NOT_NULL_BACKUP"

#################################################################################
# Step 4: Drop NOT NULL Constraints
#################################################################################

print_info "Step 4: Dropping NOT NULL constraints..."

psql -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -p "$POSTGRES_PORT" -d "$DATABASE_NAME" <<'EOF'
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT
            table_schema,
            table_name,
            column_name
        FROM
            information_schema.columns
        WHERE
            table_schema = 'public'
            AND is_nullable = 'NO'
    LOOP
        EXECUTE format(
            'ALTER TABLE %I.%I ALTER COLUMN %I DROP NOT NULL;',
            r.table_schema,
            r.table_name,
            r.column_name
        );
    END LOOP;
END $$;
EOF

print_info "NOT NULL constraints dropped successfully"

#################################################################################
# Step 5: Transfer Data using Kettle
#################################################################################

print_info "Step 5: Transferring data using Kettle..."

 KETTLE_LOG="$KETTLE_LOG_DIR/kettle-${DATABASE_NAME}.log"

 echo "-------------------------------------------------" >> "$KETTLE_LOG"
 echo "Kettle Job Started at: $(date '+%Y-%m-%d %H:%M:%S')" >> "$KETTLE_LOG"
 echo "-------------------------------------------------" >> "$KETTLE_LOG"

 nohup "$OPT_DB_DIR/data-integration/kitchen.sh" \
     -file="$OPT_DB_DIR/migration.kjb" \
     -level=Debug >> "$KETTLE_LOG" 2>&1 &

 JOB_PID=$!
 print_info "Kettle job started with PID: $JOB_PID"
 print_info "Log file: $KETTLE_LOG"

 wait $JOB_PID
 KETTLE_EXIT_CODE=$?

 echo "-------------------------------------------------" >> "$KETTLE_LOG"
 echo "Kettle Job Ended at: $(date '+%Y-%m-%d %H:%M:%S')" >> "$KETTLE_LOG"
 echo "-------------------------------------------------" >> "$KETTLE_LOG"

 if [ $KETTLE_EXIT_CODE -eq 0 ]; then
     print_info "Data transfer completed successfully"
 else
     print_error "Kettle job failed with exit code: $KETTLE_EXIT_CODE"
     print_info "Check log file: $KETTLE_LOG"
     exit 1
 fi

#################################################################################
# Step 6: Create NOT NULL Constraints Table
#################################################################################

print_info "Step 6: Creating not_null_constraints table..."

psql -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -p "$POSTGRES_PORT" -d "$DATABASE_NAME" <<EOF
DROP TABLE IF EXISTS not_null_constraints;
CREATE TABLE not_null_constraints (
    table_schema TEXT,
    table_name TEXT,
    column_name TEXT,
    is_nullable TEXT
);
EOF

print_info "not_null_constraints table created"

#################################################################################
# Step 7: Load NOT NULL Constraints from CSV
#################################################################################

print_info "Step 7: Loading NOT NULL constraints from CSV..."

# Use server-side COPY FROM STDIN to avoid psql meta-command parsing issues
psql -v ON_ERROR_STOP=1 -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -p "$POSTGRES_PORT" -d "$DATABASE_NAME" \
    -c "COPY not_null_constraints FROM STDIN WITH CSV HEADER" < "$NOT_NULL_BACKUP"

print_info "NOT NULL constraints loaded successfully"

#################################################################################
# Step 8: Create and Execute Restore Procedure
#################################################################################

print_info "Step 8: Creating restore procedure..."

psql -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -p "$POSTGRES_PORT" -d "$DATABASE_NAME" <<'EOF'
CREATE OR REPLACE PROCEDURE restore_not_null_constraints()
LANGUAGE plpgsql
AS $$
DECLARE
    rec RECORD;
BEGIN
    FOR rec IN
        SELECT table_schema, table_name, column_name
        FROM not_null_constraints
        WHERE is_nullable = 'NO'
    LOOP
        BEGIN
            EXECUTE format(
                'ALTER TABLE %I.%I ALTER COLUMN %I SET NOT NULL;',
                rec.table_schema,
                rec.table_name,
                rec.column_name
            );
            RAISE NOTICE 'Restored NOT NULL on %.%.%', rec.table_schema, rec.table_name, rec.column_name;
        EXCEPTION
            WHEN OTHERS THEN
                RAISE WARNING 'Failed to set NOT NULL on %.%.%: %', rec.table_schema, rec.table_name, rec.column_name, SQLERRM;
        END;
    END LOOP;
END;
$$;
EOF

print_info "Restore procedure created"

print_info "Executing restore_not_null_constraints procedure..."

psql -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -p "$POSTGRES_PORT" -d "$DATABASE_NAME" <<EOF
CALL restore_not_null_constraints();
EOF

print_info "NOT NULL constraints restored"

#################################################################################
# Step 9: Apply after.sql (Indexes and Sequences)
#################################################################################

print_info "Step 9: Applying indexes and sequences from after.sql..."

psql -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -p "$POSTGRES_PORT" -d "$DATABASE_NAME" < "$AFTER_SQL"

print_info "Indexes and sequences applied successfully"

#################################################################################
# Step 10: Apply unsure.sql
#################################################################################

print_info "Step 10: Applying unsure.sql..."

psql -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -p "$POSTGRES_PORT" -d "$DATABASE_NAME" < "$UNSURE_SQL"

print_info "unsure.sql applied successfully"

#################################################################################
# Step 11: Delete NOT NULL backup CSV
#################################################################################

print_info "Step 11: Deleting NOT NULL backup CSV..."

if [ -f "$NOT_NULL_BACKUP" ]; then
    rm -f "$NOT_NULL_BACKUP"
    print_info "Deleted NOT NULL backup file: $NOT_NULL_BACKUP"
else
    print_warning "NOT NULL backup file not found (already deleted?): $NOT_NULL_BACKUP"
fi

#################################################################################
# Step 12: Cleanup Kettle files
#################################################################################

print_info "Step 12: Cleaning up Kettle *.ktr and *.kjb files..."

if compgen -G "$OPT_DB_DIR/*.ktr" > /dev/null || compgen -G "$OPT_DB_DIR/*.kjb" > /dev/null; then
        rm -f "$OPT_DB_DIR"/*.ktr "$OPT_DB_DIR"/*.kjb 2>/dev/null || true
        print_info "Deleted .ktr and .kjb files from $OPT_DB_DIR"
else
        print_info "No .ktr or .kjb files found in $OPT_DB_DIR"
fi

#################################################################################
# Migration Complete
#################################################################################

unset PGPASSWORD

# Calculate total migration time
MIGRATION_END_TIME=$(date +%s)
MIGRATION_DURATION=$((MIGRATION_END_TIME - MIGRATION_START_TIME))
FORMATTED_DURATION=$(format_duration $MIGRATION_DURATION)

print_info "=========================================="
print_info "Migration completed successfully!"
print_info "Database: $DATABASE_NAME"
print_info "Total Migration Time: $FORMATTED_DURATION"
print_info "=========================================="
print_info "Summary:"
print_info "- SQL Server Database: $SQLSERVER_DATABASE"
print_info "- PostgreSQL Database: $DATABASE_NAME"
print_info "- PostgreSQL directory: $PGSQL_DB_DIR"
print_info "- Migration files: $OPT_DB_DIR"
print_info "- Script log: $SCRIPT_LOG"
print_info "=========================================="

exit 0
