#!/bin/bash
# Enhanced PostgreSQL Database Operations Tool
# Features: Backup/Restore, Schema Management, Advanced User Permissions, Connection Pooling Stats
# Usage: chmod +x postgres_enhanced.sh && ./postgres_enhanced.sh

PG_HOST="10.20.26.250"
PG_PORT="5000"
PG_USER="postgres"
PG_PASSWORD="pixROOT@789"

# Directory and filenames
VIEW_SCRIPTS_DIR="/var/lib/pgsql/sqlscripts"
BACKUP_DIR="/var/lib/pgsql/backups"
CUSTOMER_VIEWS_FILE="customerviews.sql"
PRODUCT_VIEWS_FILE="productviews.sql"
FUNCTIONS_FILE="functions.sql"

VERBOSE=0
LOG_FILE="/tmp/postgres_ops.log"  # Changed to /tmp for universal write access

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
NC='\033[0m'
BOLD='\033[1m'
BRIGHT_RED='\033[1;31m'
BRIGHT_GREEN='\033[1;32m'
BRIGHT_YELLOW='\033[1;33m'
BRIGHT_BLUE='\033[1;34m'
BRIGHT_MAGENTA='\033[1;35m'
BRIGHT_CYAN='\033[1;36m'
BRIGHT_WHITE='\033[1;37m'

# ---- Logging ----
log_action() {
    local msg="$1"
    # Try to write to log file, silently fail if no permissions
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $msg" >> "$LOG_FILE" 2>/dev/null || true
}

# ---- Helpers ----
_trim() { printf "%s" "$1" | tr -d '\r' | awk '{$1=$1;print}'; }

strip_outer_quotes() {
    local s="$1"
    s=$(_trim "$s")
    if [[ "$s" =~ ^\".*\"$ || "$s" =~ ^\'.*\'$ ]]; then
        s="${s:1:${#s}-2}"
    fi
    echo "$s"
}

sql_quote_identifier() {
    local id="$1"
    id="${id//\"/\"\"}"
    printf '"%s"' "$id"
}

sql_quote_literal() {
    local s="$1"
    s="${s//\'/\'\'}"
    printf "'%s'" "$s"
}

contains_unquoted_dot() {
    local s="$1"
    if [[ "$s" =~ ^\" ]]; then return 1; fi
    [[ "$s" == *.* ]]
}

validate_identifier() {
    local id="$1"
    # Check for SQL injection patterns
    if [[ "$id" =~ [\;\'\"\`\$\(\)] ]]; then
        return 1
    fi
    return 0
}

execute_psql() {
    local query="$1"
    local database="${2:-postgres}"
    PGPASSWORD="$PG_PASSWORD" psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$database" -c "$query" 2>&1
}

execute_psql_quiet() {
    local query="$1"
    local database="${2:-postgres}"
    local out
    out=$(PGPASSWORD="$PG_PASSWORD" psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$database" -t -A -c "$query" 2>/dev/null || true)
    printf "%s" "$out" | tr -d '\r' | awk '{$1=$1;print}'
}

execute_psql_file() {
    local sql_file="$1"
    local database="${2:-postgres}"
    local schema_name="${3:-public}"
    if [[ -z "$sql_file" || ! -f "$sql_file" ]]; then
        echo -e "${RED}SQL file not found: $sql_file${NC}"
        return 1
    fi
    if PGPASSWORD="$PG_PASSWORD" psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$database" -v ON_ERROR_STOP=1 -c "SET search_path TO ${schema_name}, public" -1 -f "$sql_file" 2>&1; then
        return 0
    else
        echo -e "${RED}Failed applying file: $sql_file${NC}"
        return 1
    fi
}

execute_psql_file_continue() {
    local sql_file="$1"
    local database="${2:-postgres}"
    local schema_name="${3:-public}"
    if [[ -z "$sql_file" || ! -f "$sql_file" ]]; then
        echo -e "${RED}SQL file not found: $sql_file${NC}"
        return 1
    fi
    if PGPASSWORD="$PG_PASSWORD" psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$database" -v ON_ERROR_STOP=0 -c "SET search_path TO ${schema_name}, public" -f "$sql_file" 2>&1; then
        return 0
    else
        echo -e "${YELLOW}Completed with some errors while applying: $sql_file${NC}"
        return 0
    fi
}

# ---- Enhanced Listing Functions ----
list_databases_display() {
    echo -e "${YELLOW}Databases:${NC}"
    execute_psql "SELECT 
        datname as \"Database Name\", 
        pg_size_pretty(pg_database_size(datname)) as \"Size\",
        (SELECT count(*) FROM pg_stat_activity WHERE datname = d.datname) as \"Connections\",
        pg_encoding_to_char(encoding) as \"Encoding\"
    FROM pg_database d 
    WHERE datistemplate = false 
    ORDER BY datname;" "postgres"
}

list_tables_display() {
    local db="$1"
    echo -e "${YELLOW}Tables in database '$db':${NC}"
    # Use a more robust query that handles table names with special characters
    execute_psql "SELECT 
        CASE WHEN t.schemaname != 'public' 
            THEN t.schemaname || '.' || t.tablename 
            ELSE t.tablename 
        END as \"Table\",
        pg_size_pretty(pg_total_relation_size((quote_ident(t.schemaname) || '.' || quote_ident(t.tablename))::regclass)) as \"Size\",
        (SELECT count(*) FROM information_schema.columns WHERE table_schema = t.schemaname AND table_name = t.tablename) as \"Columns\"
    FROM pg_tables t
    WHERE t.schemaname NOT IN ('information_schema','pg_catalog','pg_toast') 
    ORDER BY t.schemaname, t.tablename;" "$db"
}

list_schemas_display() {
    local db="$1"
    echo -e "${YELLOW}Schemas in database '$db':${NC}"
    execute_psql "SELECT 
        schema_name as \"Schema\",
        (SELECT count(*) FROM information_schema.tables WHERE table_schema = schema_name) as \"Tables\",
        schema_owner as \"Owner\"
    FROM information_schema.schemata 
    WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast', 'pg_temp_1', 'pg_toast_temp_1')
    ORDER BY schema_name;" "$db"
}

database_exists() {
    local db="$1"
    local r
    r=$(execute_psql_quiet "SELECT 1 FROM pg_database WHERE datname = $(sql_quote_literal "$db") AND datistemplate = false LIMIT 1;" "postgres")
    [[ -n "$r" ]]
}

schema_exists() {
    local db="$1"
    local schema="$2"
    local r
    r=$(execute_psql_quiet "SELECT 1 FROM information_schema.schemata WHERE schema_name = $(sql_quote_literal "$schema") LIMIT 1;" "$db")
    [[ -n "$r" ]]
}

# ---- Table Resolution ----
resolve_table() {
    local db_name="$1"
    local user_input="$2"
    RESOLVED_SCHEMA=""
    RESOLVED_TABLE=""

    if [[ -z "$db_name" || -z "$user_input" ]]; then
        return 1
    fi

    user_input=$(_trim "$user_input")
    user_input=$(strip_outer_quotes "$user_input")
    [[ $VERBOSE -eq 1 ]] && echo "DEBUG resolve_table raw input=[$user_input]" >&2

    # Strategy A: Try exact match first (for complex names with dots)
    [[ $VERBOSE -eq 1 ]] && echo "DEBUG trying exact schema+table match" >&2
    local exact_match=$(execute_psql_quiet "SELECT table_schema, table_name 
        FROM information_schema.tables 
        WHERE table_schema NOT IN ('information_schema','pg_catalog','pg_toast')
        AND (table_schema || '.' || table_name) = $(sql_quote_literal "$user_input")
        LIMIT 1;" "$db_name")
    
    if [[ -n "$exact_match" ]]; then
        RESOLVED_SCHEMA=$(echo "$exact_match" | cut -d'|' -f1)
        RESOLVED_TABLE=$(echo "$exact_match" | cut -d'|' -f2)
        [[ $VERBOSE -eq 1 ]] && echo "DEBUG resolved by exact match => $RESOLVED_SCHEMA . $RESOLVED_TABLE" >&2
        return 0
    fi

    # Strategy B: if there's at least one dot, try last-dot split
    if contains_unquoted_dot "$user_input"; then
        local schema_last="${user_input%.*}"
        local table_last="${user_input##*.}"
        [[ $VERBOSE -eq 1 ]] && echo "DEBUG trying last-dot: schema='$schema_last' table='$table_last'" >&2
        local found_last=$(execute_psql_quiet "SELECT 1 FROM information_schema.tables WHERE table_schema = $(sql_quote_literal "$schema_last") AND table_name = $(sql_quote_literal "$table_last") LIMIT 1;" "$db_name")
        if [[ -n "$found_last" ]]; then
            RESOLVED_SCHEMA="$schema_last"
            RESOLVED_TABLE="$table_last"
            [[ $VERBOSE -eq 1 ]] && echo "DEBUG resolved by last-dot" >&2
            return 0
        fi

        # Strategy C: try first-dot split
        local schema_first="${user_input%%.*}"
        local table_first="${user_input#*.}"
        [[ $VERBOSE -eq 1 ]] && echo "DEBUG trying first-dot: schema='$schema_first' table='$table_first'" >&2
        local found_first=$(execute_psql_quiet "SELECT 1 FROM information_schema.tables WHERE table_schema = $(sql_quote_literal "$schema_first") AND table_name = $(sql_quote_literal "$table_first") LIMIT 1;" "$db_name")
        if [[ -n "$found_first" ]]; then
            RESOLVED_SCHEMA="$schema_first"
            RESOLVED_TABLE="$table_first"
            [[ $VERBOSE -eq 1 ]] && echo "DEBUG resolved by first-dot" >&2
            return 0
        fi
    fi

    # Strategy D: unqualified table name
    [[ $VERBOSE -eq 1 ]] && echo "DEBUG trying unqualified lookup for table_name='$user_input'" >&2
    local found_unq=$(execute_psql_quiet "SELECT table_schema FROM information_schema.tables WHERE table_name = $(sql_quote_literal "$user_input") AND table_schema NOT IN ('information_schema','pg_catalog','pg_toast') ORDER BY table_schema LIMIT 1;" "$db_name")
    if [[ -n "$found_unq" ]]; then
        RESOLVED_SCHEMA="$found_unq"
        RESOLVED_TABLE="$user_input"
        [[ $VERBOSE -eq 1 ]] && echo "DEBUG resolved by unqualified lookup => $RESOLVED_SCHEMA . $RESOLVED_TABLE" >&2
        return 0
    fi

    # Strategy E: pattern match fallback
    [[ $VERBOSE -eq 1 ]] && echo "DEBUG trying pattern-match fallback" >&2
    local found_pattern=$(execute_psql_quiet "SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema NOT IN ('information_schema','pg_catalog','pg_toast') AND (table_schema || '.' || table_name) LIKE '%' || $(sql_quote_literal "$user_input") || '%' LIMIT 1;" "$db_name")
    if [[ -n "$found_pattern" ]]; then
        RESOLVED_SCHEMA=$(echo "$found_pattern" | cut -d'|' -f1)
        RESOLVED_TABLE=$(echo "$found_pattern" | cut -d'|' -f2)
        [[ $VERBOSE -eq 1 ]] && echo "DEBUG resolved by pattern-match => $RESOLVED_SCHEMA . $RESOLVED_TABLE" >&2
        return 0
    fi

    return 1
}

table_exists() {
    local db="$1"
    local input="$2"
    if resolve_table "$db" "$input"; then
        return 0
    fi
    return 1
}

# ---- Sequence Helpers ----
make_regclass_literal() {
    local raw="$1"
    local db="$2"
    raw=$(_trim "$raw")
    raw=$(strip_outer_quotes "$raw")
    local dot_count
    dot_count=$(awk -F'.' '{print NF-1}' <<< "$raw")
    if [[ "$dot_count" -gt 1 ]]; then
        local schema="${raw%%.*}"
        local remainder="${raw#*.}"
        remainder="${remainder//\"/\"\"}"
        printf "%s" "$(sql_quote_literal "${schema}.\"${remainder}\"")"
        return 0
    fi
    printf "%s" "$(sql_quote_literal "$raw")"
    return 0
}

get_sequence_current_value() {
    local seqname="$1"; local db_name="$2"
    seqname=$(_trim "$seqname"); seqname=$(strip_outer_quotes "$seqname")
    if contains_unquoted_dot "$seqname"; then
        local schema="${seqname%%.*}"; local remainder="${seqname#*.}"
        local val
        val=$(execute_psql_quiet "SELECT last_value FROM pg_catalog.pg_sequences WHERE schemaname = $(sql_quote_literal "$schema") AND sequencename = $(sql_quote_literal "$remainder") LIMIT 1;" "$db_name")
        [[ -n "$val" ]] && { echo "$val"; return; }
        val=$(execute_psql_quiet "SELECT last_value FROM \"${schema}\".\"${remainder}\" LIMIT 1;" "$db_name")
        [[ -n "$val" ]] && { echo "$val"; return; }
        val=$(execute_psql_quiet "SELECT last_value FROM ${schema}.${remainder} LIMIT 1;" "$db_name")
        [[ -n "$val" ]] && { echo "$val"; return; }
        echo "N/A"; return
    else
        local val
        val=$(execute_psql_quiet "SELECT last_value FROM pg_catalog.pg_sequences WHERE sequencename = $(sql_quote_literal "$seqname") LIMIT 1;" "$db_name")
        [[ -n "$val" ]] && { echo "$val"; return; }
        val=$(execute_psql_quiet "SELECT last_value FROM \"${seqname}\" LIMIT 1;" "$db_name")
        [[ -n "$val" ]] && { echo "$val"; return; }
        val=$(execute_psql_quiet "SELECT last_value FROM ${seqname} LIMIT 1;" "$db_name")
        [[ -n "$val" ]] && { echo "$val"; return; }
        echo "N/A"
    fi
}

resolve_sequences_for_input() {
    local raw="$1"; local db_name="$2"
    raw=$(_trim "$raw"); raw=$(strip_outer_quotes "$raw")
    local lit
    lit=$(make_regclass_literal "$raw" "$db_name")
    local seq_match
    seq_match=$(execute_psql_quiet "SELECT n.nspname || '.' || c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.oid = $lit::regclass AND c.relkind = 'S' LIMIT 1;" "$db_name")
    if [[ -n "$seq_match" ]]; then
        echo "$seq_match"
        return 0
    fi

    if resolve_table "$db_name" "$raw"; then
        local t_lit
        t_lit=$(make_regclass_literal "$RESOLVED_SCHEMA.$RESOLVED_TABLE" "$db_name")
        local seqs
        seqs=$(execute_psql_quiet "SELECT n.nspname || '.' || c.relname FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid JOIN pg_depend d ON d.objid = c.oid AND d.deptype = 'a' WHERE c.relkind = 'S' AND d.refobjid = $t_lit::regclass ORDER BY 1;" "$db_name")
        if [[ -n "$seqs" ]]; then
            echo "$seqs"
            return 0
        fi
    fi

    return 1
}

# ---- Display Functions ----
show_header() {
    clear
    echo -e "${BRIGHT_BLUE}════════════════════════════════════════════════════════════${NC}"
    echo -e "${BRIGHT_BLUE}   ╔═══════════════════════════════════════════════════╗${NC}"
    echo -e "${BRIGHT_BLUE}   ║  ${BRIGHT_WHITE}PostgreSQL Enhanced Operations Tool${BRIGHT_BLUE}          ║${NC}"
    echo -e "${BRIGHT_BLUE}   ╚═══════════════════════════════════════════════════╝${NC}"
    echo -e "${BRIGHT_CYAN}   Host: ${BRIGHT_WHITE}$PG_HOST:$PG_PORT${NC}"
    echo -e "${BRIGHT_CYAN}   User: ${BRIGHT_WHITE}$PG_USER${NC}"
    echo -e "${BRIGHT_BLUE}════════════════════════════════════════════════════════════${NC}"
    echo ""
}

# ---- NEW FEATURES ----

# 1. Backup Database
backup_database() {
    list_databases_display
    read -p "Enter database name to backup: " db_name
    db_name=$(_trim "$db_name")
    if ! database_exists "$db_name"; then
        echo -e "${RED}Database '$db_name' does not exist${NC}"
        read -p "Press Enter..."
        return 1
    fi

    # Create backup directory if it doesn't exist
    mkdir -p "$BACKUP_DIR" 2>/dev/null

    local timestamp=$(date +%Y%m%d_%H%M%S)
    local backup_file="${BACKUP_DIR}/${db_name}_${timestamp}.sql"
    
    echo -e "${YELLOW}Backing up database '$db_name'...${NC}"
    
    if PGPASSWORD="$PG_PASSWORD" pg_dump -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$db_name" -F p -f "$backup_file" 2>&1; then
        local size=$(du -h "$backup_file" | cut -f1)
        echo -e "${GREEN}Backup created successfully: $backup_file ($size)${NC}"
        log_action "Backup created: $db_name -> $backup_file"
        
        # Compress the backup
        read -p "Compress backup? (y/n): " compress
        if [[ "$compress" == "y" || "$compress" == "Y" ]]; then
            echo -e "${YELLOW}Compressing backup...${NC}"
            if gzip "$backup_file" 2>&1; then
                echo -e "${GREEN}Compressed to: ${backup_file}.gz${NC}"
                log_action "Backup compressed: ${backup_file}.gz"
            fi
        fi
    else
        echo -e "${RED}Backup failed${NC}"
        log_action "Backup failed: $db_name"
    fi
    
    read -p "Press Enter to continue..."
}

# 2. Restore Database
restore_database() {
    echo -e "${YELLOW}Available backups in $BACKUP_DIR:${NC}"
    if [[ -d "$BACKUP_DIR" ]]; then
        ls -lh "$BACKUP_DIR" | grep -E '\.(sql|gz)$' || echo "No backups found"
    else
        echo "Backup directory does not exist"
        read -p "Press Enter..."
        return 1
    fi
    
    echo ""
    read -p "Enter full path to backup file: " backup_file
    backup_file=$(_trim "$backup_file")
    
    if [[ ! -f "$backup_file" ]]; then
        echo -e "${RED}Backup file not found: $backup_file${NC}"
        read -p "Press Enter..."
        return 1
    fi
    
    list_databases_display
    read -p "Enter target database name (will be created if doesn't exist): " db_name
    db_name=$(_trim "$db_name")
    
    if [[ -z "$db_name" ]]; then
        echo -e "${RED}Invalid database name${NC}"
        read -p "Press Enter..."
        return 1
    fi
    
    # Create database if it doesn't exist
    if ! database_exists "$db_name"; then
        echo -e "${YELLOW}Database doesn't exist. Creating...${NC}"
        execute_psql "CREATE DATABASE $(sql_quote_identifier "$db_name");" "postgres"
    else
        echo -e "${YELLOW}WARNING: Database '$db_name' already exists. This will overwrite existing data.${NC}"
        read -p "Type 'yes' to confirm: " confirm
        if [[ "$confirm" != "yes" ]]; then
            echo "Cancelled"
            read -p "Press Enter..."
            return 0
        fi
    fi
    
    echo -e "${YELLOW}Restoring database...${NC}"
    
    # Handle compressed backups
    if [[ "$backup_file" == *.gz ]]; then
        if gunzip -c "$backup_file" | PGPASSWORD="$PG_PASSWORD" psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$db_name" 2>&1; then
            echo -e "${GREEN}Database restored successfully${NC}"
            log_action "Database restored: $db_name from $backup_file"
        else
            echo -e "${RED}Restore failed${NC}"
            log_action "Restore failed: $db_name from $backup_file"
        fi
    else
        if PGPASSWORD="$PG_PASSWORD" psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$db_name" -f "$backup_file" 2>&1; then
            echo -e "${GREEN}Database restored successfully${NC}"
            log_action "Database restored: $db_name from $backup_file"
        else
            echo -e "${RED}Restore failed${NC}"
            log_action "Restore failed: $db_name from $backup_file"
        fi
    fi
    
    read -p "Press Enter to continue..."
}

# 3. Create Schema
create_schema() {
    list_databases_display
    read -p "Enter database name: " db_name
    db_name=$(_trim "$db_name")
    if ! database_exists "$db_name"; then
        echo -e "${RED}Database not found${NC}"
        read -p "Press Enter..."
        return 1
    fi
    
    list_schemas_display "$db_name"
    
    read -p "Enter new schema name: " schema_name
    schema_name=$(_trim "$schema_name")
    
    if [[ -z "$schema_name" ]]; then
        echo -e "${RED}Invalid schema name${NC}"
        read -p "Press Enter..."
        return 1
    fi
    
    if ! validate_identifier "$schema_name"; then
        echo -e "${RED}Invalid characters in schema name${NC}"
        read -p "Press Enter..."
        return 1
    fi
    
    if schema_exists "$db_name" "$schema_name"; then
        echo -e "${RED}Schema '$schema_name' already exists${NC}"
        read -p "Press Enter..."
        return 1
    fi
    
    if execute_psql "CREATE SCHEMA $(sql_quote_identifier "$schema_name");" "$db_name" >/dev/null 2>&1; then
        echo -e "${GREEN}Schema '$schema_name' created successfully${NC}"
        log_action "Schema created: $schema_name in $db_name"
        
        # Optional: Set schema owner
        read -p "Set schema owner? (y/n): " set_owner
        if [[ "$set_owner" == "y" || "$set_owner" == "Y" ]]; then
            read -p "Enter owner username: " owner
            owner=$(_trim "$owner")
            if [[ -n "$owner" ]]; then
                execute_psql "ALTER SCHEMA $(sql_quote_identifier "$schema_name") OWNER TO $(sql_quote_identifier "$owner");" "$db_name"
                echo -e "${GREEN}Owner set to '$owner'${NC}"
            fi
        fi
    else
        echo -e "${RED}Failed to create schema${NC}"
    fi
    
    read -p "Press Enter to continue..."
}

# 4. Drop Schema
drop_schema() {
    list_databases_display
    read -p "Enter database name: " db_name
    db_name=$(_trim "$db_name")
    if ! database_exists "$db_name"; then
        echo -e "${RED}Database not found${NC}"
        read -p "Press Enter..."
        return 1
    fi
    
    list_schemas_display "$db_name"
    
    read -p "Enter schema name to DELETE: " schema_name
    schema_name=$(_trim "$schema_name")
    
    if [[ "$schema_name" == "public" || "$schema_name" == "information_schema" || "$schema_name" == "pg_catalog" ]]; then
        echo -e "${RED}Cannot delete system schema '$schema_name'${NC}"
        read -p "Press Enter..."
        return 1
    fi
    
    if ! schema_exists "$db_name" "$schema_name"; then
        echo -e "${RED}Schema '$schema_name' does not exist${NC}"
        read -p "Press Enter..."
        return 1
    fi
    
    echo -e "${YELLOW}WARNING: This will permanently delete schema '$schema_name' and ALL its contents${NC}"
    read -p "Type 'yes' to confirm: " confirm
    if [[ "$confirm" != "yes" ]]; then
        echo "Cancelled"
        read -p "Press Enter..."
        return 0
    fi
    
    if execute_psql "DROP SCHEMA $(sql_quote_identifier "$schema_name") CASCADE;" "$db_name" >/dev/null 2>&1; then
        echo -e "${GREEN}Schema '$schema_name' deleted successfully${NC}"
        log_action "Schema deleted: $schema_name from $db_name"
    else
        echo -e "${RED}Failed to delete schema${NC}"
    fi
    
    read -p "Press Enter to continue..."
}

# 5. Analyze Table Statistics
analyze_table() {
    list_databases_display
    read -p "Enter database name: " db_name
    db_name=$(_trim "$db_name")
    if ! database_exists "$db_name"; then
        echo -e "${RED}Database not found${NC}"
        read -p "Press Enter..."
        return 1
    fi
    
    list_tables_display "$db_name"
    read -p "Enter table name (schema.table OR unqualified): " table_input
    table_input=$(_trim "$table_input")
    table_input=$(strip_outer_quotes "$table_input")
    
    if ! resolve_table "$db_name" "$table_input"; then
        echo -e "${RED}Table not found${NC}"
        read -p "Press Enter..."
        return 1
    fi
    
    echo -e "${YELLOW}Analyzing table '$RESOLVED_SCHEMA.$RESOLVED_TABLE'...${NC}"
    
    # Build properly quoted table reference
    local schema_quoted=$(sql_quote_identifier "$RESOLVED_SCHEMA")
    local table_quoted=$(sql_quote_identifier "$RESOLVED_TABLE")
    local full_table_ref="${schema_quoted}.${table_quoted}"
    
    # For counting rows, use the regclass method
    local lit
    lit=$(make_regclass_literal "$RESOLVED_SCHEMA.$RESOLVED_TABLE" "$db_name")
    
    # Row count
    local row_count
    row_count=$(execute_psql_quiet "SELECT count(*) FROM ${full_table_ref};" "$db_name")
    if [[ -z "$row_count" ]]; then
        row_count=$(execute_psql_quiet "SELECT reltuples::bigint FROM pg_class WHERE oid = ${lit}::regclass;" "$db_name")
        echo -e "${CYAN}Estimated Rows: ${BRIGHT_WHITE}$row_count${NC} (from statistics)"
    else
        echo -e "${CYAN}Total Rows: ${BRIGHT_WHITE}$row_count${NC}"
    fi
    
    # Table size
    local table_size
    table_size=$(execute_psql_quiet "SELECT pg_size_pretty(pg_total_relation_size(${lit}::regclass));" "$db_name")
    echo -e "${CYAN}Table Size: ${BRIGHT_WHITE}$table_size${NC}"
    
    # Column statistics
    echo -e "\n${YELLOW}Column Statistics:${NC}"
    execute_psql "SELECT 
        a.attname as \"Column\",
        format_type(a.atttypid, a.atttypmod) as \"Type\",
        CASE WHEN s.null_frac IS NOT NULL THEN round((s.null_frac * 100)::numeric, 2) || '%' ELSE 'N/A' END as \"Null %\",
        CASE WHEN s.n_distinct >= 0 THEN s.n_distinct::text ELSE 'N/A' END as \"Distinct Values\",
        CASE WHEN s.avg_width IS NOT NULL THEN s.avg_width || ' bytes' ELSE 'N/A' END as \"Avg Width\"
    FROM pg_attribute a
    LEFT JOIN pg_stats s ON s.schemaname = $(sql_quote_literal "$RESOLVED_SCHEMA") AND s.tablename = $(sql_quote_literal "$RESOLVED_TABLE") AND s.attname = a.attname
    WHERE a.attrelid = ${lit}::regclass 
    AND a.attnum > 0 
    AND NOT a.attisdropped
    ORDER BY a.attnum;" "$db_name"
    
    # Run ANALYZE
    read -p "Run ANALYZE to update statistics? (y/n): " run_analyze
    if [[ "$run_analyze" == "y" || "$run_analyze" == "Y" ]]; then
        echo -e "${YELLOW}Running ANALYZE on ${full_table_ref}...${NC}"
        if execute_psql "ANALYZE ${full_table_ref};" "$db_name"; then
            echo -e "${GREEN}ANALYZE completed successfully${NC}"
            log_action "ANALYZE run on: $RESOLVED_SCHEMA.$RESOLVED_TABLE in $db_name"
        else
            echo -e "${RED}ANALYZE failed${NC}"
            echo -e "${YELLOW}Trying with VERBOSE for more details...${NC}"
            execute_psql "ANALYZE VERBOSE ${full_table_ref};" "$db_name"
        fi
    fi
    
    read -p "Press Enter to continue..."
}

# 6. Vacuum Table
vacuum_table() {
    list_databases_display
    read -p "Enter database name: " db_name
    db_name=$(_trim "$db_name")
    if ! database_exists "$db_name"; then
        echo -e "${RED}Database not found${NC}"
        read -p "Press Enter..."
        return 1
    fi
    
    echo "Vacuum Options:"
    echo "1) VACUUM specific table"
    echo "2) VACUUM ANALYZE specific table"
    echo "3) VACUUM FULL specific table (locks table)"
    echo "4) VACUUM entire database"
    read -p "Choice [1-4]: " vchoice
    
    case "$vchoice" in
        1|2|3)
            list_tables_display "$db_name"
            read -p "Enter table name (schema.table OR unqualified): " table_input
            table_input=$(_trim "$table_input")
            table_input=$(strip_outer_quotes "$table_input")
            
            if ! resolve_table "$db_name" "$table_input"; then
                echo -e "${RED}Table not found${NC}"
                read -p "Press Enter..."
                return 1
            fi
            
            # Build properly quoted table reference
            local schema_quoted=$(sql_quote_identifier "$RESOLVED_SCHEMA")
            local table_quoted=$(sql_quote_identifier "$RESOLVED_TABLE")
            local full_table_ref="${schema_quoted}.${table_quoted}"
            
            echo -e "${YELLOW}Processing table: $RESOLVED_SCHEMA.$RESOLVED_TABLE${NC}"
            
            case "$vchoice" in
                1)
                    if execute_psql "VACUUM ${full_table_ref};" "$db_name" >/dev/null 2>&1; then
                        echo -e "${GREEN}VACUUM completed${NC}"
                        log_action "VACUUM: $RESOLVED_SCHEMA.$RESOLVED_TABLE in $db_name"
                    else
                        echo -e "${RED}VACUUM failed${NC}"
                        echo -e "${YELLOW}Trying alternative method...${NC}"
                        # Alternative: use regclass casting
                        local regclass_ref=$(make_regclass_literal "$RESOLVED_SCHEMA.$RESOLVED_TABLE" "$db_name")
                        if execute_psql "VACUUM (SELECT $regclass_ref::regclass);" "$db_name" >/dev/null 2>&1; then
                            echo -e "${GREEN}VACUUM completed with alternative method${NC}"
                        else
                            echo -e "${RED}VACUUM failed with both methods${NC}"
                        fi
                    fi
                    ;;
                2)
                    if execute_psql "VACUUM ANALYZE ${full_table_ref};" "$db_name" >/dev/null 2>&1; then
                        echo -e "${GREEN}VACUUM ANALYZE completed${NC}"
                        log_action "VACUUM ANALYZE: $RESOLVED_SCHEMA.$RESOLVED_TABLE in $db_name"
                    else
                        echo -e "${RED}VACUUM ANALYZE failed${NC}"
                        echo -e "${YELLOW}Trying alternative method...${NC}"
                        local regclass_ref=$(make_regclass_literal "$RESOLVED_SCHEMA.$RESOLVED_TABLE" "$db_name")
                        if execute_psql "VACUUM ANALYZE (SELECT $regclass_ref::regclass);" "$db_name" >/dev/null 2>&1; then
                            echo -e "${GREEN}VACUUM ANALYZE completed with alternative method${NC}"
                        else
                            echo -e "${RED}VACUUM ANALYZE failed with both methods${NC}"
                        fi
                    fi
                    ;;
                3)
                    echo -e "${RED}WARNING: VACUUM FULL will lock the table and may take a long time${NC}"
                    read -p "Type 'yes' to confirm: " confirm
                    if [[ "$confirm" != "yes" ]]; then
                        echo "Cancelled"
                        read -p "Press Enter..."
                        return 0
                    fi
                    
                    echo -e "${YELLOW}Starting VACUUM FULL on $RESOLVED_SCHEMA.$RESOLVED_TABLE...${NC}"
                    echo -e "${CYAN}This may take several minutes for large tables...${NC}"
                    
                    if execute_psql "VACUUM FULL ${full_table_ref};" "$db_name"; then
                        echo -e "${GREEN}VACUUM FULL completed successfully${NC}"
                        log_action "VACUUM FULL: $RESOLVED_SCHEMA.$RESOLVED_TABLE in $db_name"
                    else
                        echo -e "${RED}VACUUM FULL failed${NC}"
                        echo -e "${YELLOW}Error details shown above. Common issues:${NC}"
                        echo -e "${CYAN}- Insufficient disk space (needs space equal to table size)${NC}"
                        echo -e "${CYAN}- Active connections holding locks${NC}"
                        echo -e "${CYAN}- Permissions issues${NC}"
                        
                        read -p "Try with VERBOSE option to see details? (y/n): " try_verbose
                        if [[ "$try_verbose" == "y" || "$try_verbose" == "Y" ]]; then
                            echo -e "${YELLOW}Running VACUUM FULL VERBOSE...${NC}"
                            execute_psql "VACUUM FULL VERBOSE ${full_table_ref};" "$db_name"
                        fi
                    fi
                    ;;
            esac
            ;;
        4)
            echo -e "${YELLOW}Running VACUUM on entire database '$db_name'...${NC}"
            if execute_psql "VACUUM;" "$db_name" >/dev/null 2>&1; then
                echo -e "${GREEN}Database VACUUM completed${NC}"
                log_action "VACUUM database: $db_name"
            else
                echo -e "${RED}Database VACUUM failed${NC}"
            fi
            ;;
        *)
            echo "Invalid selection"
            read -p "Press Enter..."
            return 1
            ;;
    esac
    
    read -p "Press Enter to continue..."
}

# 7. Show Database Connections
show_connections() {
    list_databases_display
    read -p "Enter database name (or 'all' for all databases): " db_name
    db_name=$(_trim "$db_name")
    
    echo -e "${YELLOW}Active Database Connections:${NC}"
    
    if [[ "$db_name" == "all" ]]; then
        execute_psql "SELECT 
            datname as \"Database\",
            usename as \"User\",
            application_name as \"Application\",
            client_addr as \"Client IP\",
            state as \"State\",
            query_start as \"Query Start\",
            state_change as \"State Change\"
        FROM pg_stat_activity 
        WHERE datname IS NOT NULL
        ORDER BY datname, query_start DESC;" "postgres"
    else
        if ! database_exists "$db_name"; then
            echo -e "${RED}Database not found${NC}"
            read -p "Press Enter..."
            return 1
        fi
        
        execute_psql "SELECT 
            pid as \"PID\",
            usename as \"User\",
            application_name as \"Application\",
            client_addr as \"Client IP\",
            state as \"State\",
            query as \"Current Query\",
            query_start as \"Query Start\"
        FROM pg_stat_activity 
        WHERE datname = $(sql_quote_literal "$db_name")
        ORDER BY query_start DESC;" "postgres"
    fi
    
    read -p "Press Enter to continue..."
}

# 8. Kill Database Connections
kill_connections() {
    list_databases_display
    read -p "Enter database name: " db_name
    db_name=$(_trim "$db_name")
    if ! database_exists "$db_name"; then
        echo -e "${RED}Database not found${NC}"
        read -p "Press Enter..."
        return 1
    fi
    
    # Show current connections
    local conn_count
    conn_count=$(execute_psql_quiet "SELECT count(*) FROM pg_stat_activity WHERE datname = $(sql_quote_literal "$db_name") AND pid <> pg_backend_pid();" "postgres")
    
    echo -e "${YELLOW}Active connections to '$db_name': $conn_count${NC}"
    
    if [[ "$conn_count" == "0" ]]; then
        echo "No active connections to terminate"
        read -p "Press Enter..."
        return 0
    fi
    
    execute_psql "SELECT 
        pid as \"PID\",
        usename as \"User\",
        application_name as \"Application\",
        state as \"State\"
    FROM pg_stat_activity 
    WHERE datname = $(sql_quote_literal "$db_name") 
    AND pid <> pg_backend_pid();" "postgres"
    
    echo -e "${RED}WARNING: This will terminate all connections to '$db_name'${NC}"
    read -p "Type 'yes' to confirm: " confirm
    if [[ "$confirm" != "yes" ]]; then
        echo "Cancelled"
        read -p "Press Enter..."
        return 0
    fi
    
    local killed
    killed=$(execute_psql_quiet "SELECT count(*) FROM pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $(sql_quote_literal "$db_name") AND pid <> pg_backend_pid();" "postgres")
    
    echo -e "${GREEN}Terminated $killed connections${NC}"
    log_action "Killed $killed connections to: $db_name"
    
    read -p "Press Enter to continue..."
}

# 9. Show Table Constraints
show_constraints() {
    list_databases_display
    read -p "Enter database name: " db_name
    db_name=$(_trim "$db_name")
    if ! database_exists "$db_name"; then
        echo -e "${RED}Database not found${NC}"
        read -p "Press Enter..."
        return 1
    fi
    
    list_tables_display "$db_name"
    read -p "Enter table name (schema.table OR unqualified): " table_input
    table_input=$(_trim "$table_input")
    table_input=$(strip_outer_quotes "$table_input")
    
    if ! resolve_table "$db_name" "$table_input"; then
        echo -e "${RED}Table not found${NC}"
        read -p "Press Enter..."
        return 1
    fi
    
    local lit
    lit=$(make_regclass_literal "$RESOLVED_SCHEMA.$RESOLVED_TABLE" "$db_name")
    
    echo -e "${YELLOW}Constraints for table '$RESOLVED_SCHEMA.$RESOLVED_TABLE':${NC}"
    
    execute_psql "SELECT
        c.conname as \"Constraint Name\",
        CASE c.contype
            WHEN 'p' THEN 'PRIMARY KEY'
            WHEN 'f' THEN 'FOREIGN KEY'
            WHEN 'u' THEN 'UNIQUE'
            WHEN 'c' THEN 'CHECK'
            WHEN 'x' THEN 'EXCLUSION'
        END as \"Type\",
        pg_get_constraintdef(c.oid) as \"Definition\"
    FROM pg_constraint c
    WHERE c.conrelid = $lit::regclass
    ORDER BY c.contype, c.conname;" "$db_name"
    
    read -p "Press Enter to continue..."
}

# 10. Grant Schema Permissions
grant_schema_permissions() {
    list_databases_display
    read -p "Enter database name: " db_name
    db_name=$(_trim "$db_name")
    if ! database_exists "$db_name"; then
        echo -e "${RED}Database not found${NC}"
        read -p "Press Enter..."
        return 1
    fi
    
    list_schemas_display "$db_name"
    read -p "Enter schema name: " schema_name
    schema_name=$(_trim "$schema_name")
    
    if ! schema_exists "$db_name" "$schema_name"; then
        echo -e "${RED}Schema not found${NC}"
        read -p "Press Enter..."
        return 1
    fi
    
    read -p "Enter username: " username
    username=$(_trim "$username")
    
    # Resolve actual role name
    local actual_role
    actual_role=$(execute_psql_quiet "SELECT rolname FROM pg_catalog.pg_roles WHERE LOWER(rolname) = LOWER($(sql_quote_literal "$username")) LIMIT 1;" "postgres")
    if [[ -z "$actual_role" ]]; then
        echo -e "${RED}User '$username' does not exist${NC}"
        read -p "Press Enter..."
        return 1
    fi
    username="$actual_role"
    
    echo "Permission Options:"
    echo "1) USAGE (access schema)"
    echo "2) CREATE (create objects in schema)"
    echo "3) ALL (all schema privileges)"
    read -p "Choice [1-3]: " pchoice
    
    case "$pchoice" in
        1)
            execute_psql "GRANT USAGE ON SCHEMA $(sql_quote_identifier "$schema_name") TO $(sql_quote_identifier "$username");" "$db_name"
            echo -e "${GREEN}Granted USAGE on schema '$schema_name' to '$username'${NC}"
            log_action "Granted USAGE on schema $schema_name to $username in $db_name"
            ;;
        2)
            execute_psql "GRANT CREATE ON SCHEMA $(sql_quote_identifier "$schema_name") TO $(sql_quote_identifier "$username");" "$db_name"
            echo -e "${GREEN}Granted CREATE on schema '$schema_name' to '$username'${NC}"
            log_action "Granted CREATE on schema $schema_name to $username in $db_name"
            ;;
        3)
            execute_psql "GRANT ALL ON SCHEMA $(sql_quote_identifier "$schema_name") TO $(sql_quote_identifier "$username");" "$db_name"
            echo -e "${GREEN}Granted ALL on schema '$schema_name' to '$username'${NC}"
            log_action "Granted ALL on schema $schema_name to $username in $db_name"
            ;;
        *)
            echo "Invalid selection"
            read -p "Press Enter..."
            return 1
            ;;
    esac
    
    read -p "Press Enter to continue..."
}

# 11. Show Database Statistics
show_db_statistics() {
    list_databases_display
    read -p "Enter database name: " db_name
    db_name=$(_trim "$db_name")
    if ! database_exists "$db_name"; then
        echo -e "${RED}Database not found${NC}"
        read -p "Press Enter..."
        return 1
    fi
    
    echo -e "${YELLOW}Database Statistics for '$db_name':${NC}\n"
    
    # General stats
    echo -e "${CYAN}General Information:${NC}"
    execute_psql "SELECT 
        pg_database_size($(sql_quote_literal "$db_name")) as size_bytes,
        pg_size_pretty(pg_database_size($(sql_quote_literal "$db_name"))) as \"Size\",
        (SELECT count(*) FROM pg_stat_activity WHERE datname = $(sql_quote_literal "$db_name")) as \"Active Connections\",
        (SELECT count(*) FROM information_schema.tables WHERE table_catalog = $(sql_quote_literal "$db_name") AND table_schema NOT IN ('information_schema','pg_catalog')) as \"Tables\",
        (SELECT count(*) FROM pg_views WHERE schemaname NOT IN ('information_schema','pg_catalog')) as \"Views\",
        (SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid WHERE n.nspname NOT IN ('pg_catalog','information_schema')) as \"Functions\"
    FROM pg_database WHERE datname = $(sql_quote_literal "$db_name");" "postgres"
    
    echo -e "\n${CYAN}Top 10 Largest Tables:${NC}"
    execute_psql "SELECT 
        t.schemaname || '.' || t.tablename as \"Table\",
        pg_size_pretty(pg_total_relation_size((quote_ident(t.schemaname) || '.' || quote_ident(t.tablename))::regclass)) as \"Total Size\",
        pg_size_pretty(pg_relation_size((quote_ident(t.schemaname) || '.' || quote_ident(t.tablename))::regclass)) as \"Table Size\",
        pg_size_pretty(pg_total_relation_size((quote_ident(t.schemaname) || '.' || quote_ident(t.tablename))::regclass) - 
                       pg_relation_size((quote_ident(t.schemaname) || '.' || quote_ident(t.tablename))::regclass)) as \"Indexes Size\"
    FROM pg_tables t
    WHERE t.schemaname NOT IN ('information_schema','pg_catalog')
    ORDER BY pg_total_relation_size((quote_ident(t.schemaname) || '.' || quote_ident(t.tablename))::regclass) DESC
    LIMIT 10;" "$db_name"
    
    echo -e "\n${CYAN}Cache Hit Ratio:${NC}"
    execute_psql "SELECT 
        sum(heap_blks_read) as heap_read,
        sum(heap_blks_hit) as heap_hit,
        CASE WHEN sum(heap_blks_hit) + sum(heap_blks_read) > 0 
            THEN round(100.0 * sum(heap_blks_hit) / (sum(heap_blks_hit) + sum(heap_blks_read)), 2) 
            ELSE 0 
        END as \"Cache Hit Ratio %\"
    FROM pg_statio_user_tables;" "$db_name"
    
    read -p "Press Enter to continue..."
}

# 12. Export Table to CSV
export_table_csv() {
    list_databases_display
    read -p "Enter database name: " db_name
    db_name=$(_trim "$db_name")
    if ! database_exists "$db_name"; then
        echo -e "${RED}Database not found${NC}"
        read -p "Press Enter..."
        return 1
    fi
    
    list_tables_display "$db_name"
    read -p "Enter table name (schema.table OR unqualified): " table_input
    table_input=$(_trim "$table_input")
    table_input=$(strip_outer_quotes "$table_input")
    
    if ! resolve_table "$db_name" "$table_input"; then
        echo -e "${RED}Table not found${NC}"
        read -p "Press Enter..."
        return 1
    fi
    
    local export_dir="/var/lib/pgsql/exports"
    mkdir -p "$export_dir" 2>/dev/null
    
    local timestamp=$(date +%Y%m%d_%H%M%S)
    local csv_file="${export_dir}/${RESOLVED_TABLE}_${timestamp}.csv"
    
    read -p "Enter output file path (or press Enter for: $csv_file): " custom_path
    if [[ -n "$custom_path" ]]; then
        csv_file="$custom_path"
    fi
    
    echo -e "${YELLOW}Exporting table to CSV...${NC}"
    
    local schema_quoted=$(sql_quote_identifier "$RESOLVED_SCHEMA")
    local table_quoted=$(sql_quote_identifier "$RESOLVED_TABLE")
    
    if execute_psql "COPY ${schema_quoted}.${table_quoted} TO STDOUT WITH CSV HEADER;" "$db_name" > "$csv_file" 2>&1; then
        local size=$(du -h "$csv_file" | cut -f1)
        echo -e "${GREEN}Export successful: $csv_file ($size)${NC}"
        log_action "Exported table: $RESOLVED_SCHEMA.$RESOLVED_TABLE to $csv_file"
    else
        echo -e "${RED}Export failed${NC}"
    fi
    
    read -p "Press Enter to continue..."
}

# 13. Show Slow Queries
show_slow_queries() {
    echo -e "${YELLOW}Long Running Queries (> 5 seconds):${NC}"
    
    execute_psql "SELECT 
        pid,
        usename as \"User\",
        datname as \"Database\",
        state,
        now() - query_start as \"Duration\",
        substring(query, 1, 100) as \"Query (first 100 chars)\"
    FROM pg_stat_activity
    WHERE state = 'active' 
    AND query NOT LIKE '%pg_stat_activity%'
    AND (now() - query_start) > interval '5 seconds'
    ORDER BY (now() - query_start) DESC;" "postgres"
    
    read -p "Kill a specific query? (y/n): " kill_query
    if [[ "$kill_query" == "y" || "$kill_query" == "Y" ]]; then
        read -p "Enter PID to kill: " pid
        if [[ "$pid" =~ ^[0-9]+$ ]]; then
            if execute_psql "SELECT pg_cancel_backend($pid);" "postgres" >/dev/null 2>&1; then
                echo -e "${GREEN}Query cancelled for PID $pid${NC}"
                log_action "Cancelled query PID: $pid"
            else
                echo -e "${RED}Failed to cancel query${NC}"
            fi
        fi
    fi
    
    read -p "Press Enter to continue..."
}

# 14. Duplicate Table Structure
duplicate_table_structure() {
    list_databases_display
    read -p "Enter database name: " db_name
    db_name=$(_trim "$db_name")
    if ! database_exists "$db_name"; then
        echo -e "${RED}Database not found${NC}"
        read -p "Press Enter..."
        return 1
    fi
    
    list_tables_display "$db_name"
    read -p "Enter source table name (schema.table OR unqualified): " table_input
    table_input=$(_trim "$table_input")
    table_input=$(strip_outer_quotes "$table_input")
    
    if ! resolve_table "$db_name" "$table_input"; then
        echo -e "${RED}Table not found${NC}"
        read -p "Press Enter..."
        return 1
    fi
    
    read -p "Enter new table name: " new_table
    new_table=$(_trim "$new_table")
    
    if [[ -z "$new_table" ]]; then
        echo -e "${RED}Invalid table name${NC}"
        read -p "Press Enter..."
        return 1
    fi
    
    echo "Options:"
    echo "1) Structure only (no data)"
    echo "2) Structure and data"
    read -p "Choice [1-2]: " dup_choice
    
    local schema_quoted=$(sql_quote_identifier "$RESOLVED_SCHEMA")
    local table_quoted=$(sql_quote_identifier "$RESOLVED_TABLE")
    local new_table_quoted=$(sql_quote_identifier "$new_table")
    
    case "$dup_choice" in
        1)
            if execute_psql "CREATE TABLE ${schema_quoted}.${new_table_quoted} (LIKE ${schema_quoted}.${table_quoted} INCLUDING ALL);" "$db_name" >/dev/null 2>&1; then
                echo -e "${GREEN}Table structure duplicated successfully${NC}"
                log_action "Duplicated structure: $RESOLVED_SCHEMA.$RESOLVED_TABLE -> $new_table in $db_name"
            else
                echo -e "${RED}Failed to duplicate table structure${NC}"
            fi
            ;;
        2)
            if execute_psql "CREATE TABLE ${schema_quoted}.${new_table_quoted} AS TABLE ${schema_quoted}.${table_quoted};" "$db_name" >/dev/null 2>&1; then
                echo -e "${GREEN}Table duplicated with data successfully${NC}"
                log_action "Duplicated table with data: $RESOLVED_SCHEMA.$RESOLVED_TABLE -> $new_table in $db_name"
            else
                echo -e "${RED}Failed to duplicate table${NC}"
            fi
            ;;
        *)
            echo "Invalid selection"
            read -p "Press Enter..."
            return 1
            ;;
    esac
    
    read -p "Press Enter to continue..."
}

# ---- ORIGINAL FUNCTIONS ----

# NEW FUNCTION: Explain Analyze Query
explain_analyze_query() {
    list_databases_display
    read -p "Enter database name: " db_name
    db_name=$(_trim "$db_name")
    if ! database_exists "$db_name"; then
        echo -e "${RED}Database not found${NC}"
        read -p "Press Enter..."
        return 1
    fi

    echo ""
    list_tables_display "$db_name"
    echo ""

    echo -e "${YELLOW}Enter your SQL query to analyze (without quotes):${NC}"
    echo -e "${BLUE}Examples:${NC}"
    echo "  select * from syncoms.photos.photo1 where id > 1000"
    echo "  select count(*) from logapalooza.eventlog"
    echo ""
    echo -e "${CYAN}Note: This will run EXPLAIN ANALYZE to show query cost and execution plan${NC}"
    echo ""

    read -p "SQL> " user_query
    user_query=$(_trim "$user_query")

    if [[ -z "$user_query" ]]; then
        echo -e "${RED}No query provided${NC}"
        read -p "Press Enter..."
        return 1
    fi

    if [[ "${user_query,,}" == "cancel" ]]; then
        echo "Cancelled"
        read -p "Press Enter..."
        return 0
    fi

    echo ""
    echo -e "${YELLOW}Processing query for analysis...${NC}"
    
    # Get ALL tables from database (sorted by length, longest first)
    local all_tables=$(execute_psql_quiet "SELECT table_schema || '.' || table_name FROM information_schema.tables WHERE table_schema NOT IN ('information_schema','pg_catalog','pg_toast') ORDER BY LENGTH(table_schema || '.' || table_name) DESC;" "$db_name")
    
    # Build the final query with auto-quoting
    local final_query="$user_query"
    local quoted_count=0
    
    # Process each table for auto-quoting
    while IFS= read -r table_full_name; do
        [[ -z "$table_full_name" ]] && continue
        
        if [[ "$table_full_name" == *.* ]]; then
            local lower_query="${final_query,,}"
            local lower_table="${table_full_name,,}"
            
            if [[ "$lower_query" == *"$lower_table"* ]]; then
                if [[ "$final_query" != *"\"$table_full_name\""* ]]; then
                    local pos=0
                    local query_len=${#final_query}
                    local table_len=${#table_full_name}
                    local new_query=""
                    local replaced=0
                    
                    while [[ $pos -lt $query_len ]]; do
                        local remaining="${final_query:$pos}"
                        local chunk="${remaining:0:$table_len}"
                        
                        if [[ "${chunk,,}" == "$lower_table" ]] && [[ $replaced -eq 0 ]]; then
                            new_query="${new_query}\"${table_full_name}\""
                            pos=$((pos + table_len))
                            replaced=1
                            quoted_count=$((quoted_count + 1))
                            echo -e "${GREEN}  ✓ Auto-quoted: \"$table_full_name\"${NC}"
                        else
                            new_query="${new_query}${final_query:$pos:1}"
                            pos=$((pos + 1))
                        fi
                    done
                    
                    if [[ $replaced -eq 1 ]]; then
                        final_query="$new_query"
                    fi
                fi
            fi
        fi
    done <<< "$all_tables"

    echo ""
    
    # Add EXPLAIN ANALYZE prefix
    local explain_query="EXPLAIN (ANALYZE, BUFFERS, VERBOSE, FORMAT TEXT) $final_query"
    
    echo -e "${BRIGHT_CYAN}╔════════════════════════════════════════════════╗${NC}"
    echo -e "${BRIGHT_CYAN}║${NC} ${BRIGHT_WHITE}EXPLAIN ANALYZE${NC}                                ${BRIGHT_CYAN}║${NC}"
    echo -e "${BRIGHT_CYAN}╠════════════════════════════════════════════════╣${NC}"
    echo -e "${BRIGHT_CYAN}║${NC} ${BRIGHT_YELLOW}Query:${NC}"
    echo -e "${BRIGHT_CYAN}║${NC}   $final_query"
    echo -e "${BRIGHT_CYAN}╚════════════════════════════════════════════════╝${NC}"
    echo ""

    echo -e "${YELLOW}Running EXPLAIN ANALYZE...${NC}"
    echo -e "${CYAN}(This will execute the query to gather real statistics)${NC}"
    echo ""

    # Execute EXPLAIN ANALYZE
    local result
    result=$(PGPASSWORD="$PG_PASSWORD" psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$db_name" -c "$explain_query" 2>&1)
    local exit_code=$?
    
    if [[ $exit_code -eq 0 ]]; then
        echo -e "${GREEN}═══════════════════════════════════════════════════════${NC}"
        echo -e "${GREEN}                 EXECUTION PLAN & COST                  ${NC}"
        echo -e "${GREEN}═══════════════════════════════════════════════════════${NC}"
        echo ""
        echo "$result"
        echo ""
        echo -e "${GREEN}═══════════════════════════════════════════════════════${NC}"
        echo ""
        
        # Extract key metrics
        local planning_time=$(echo "$result" | grep "Planning Time:" | awk '{print $3, $4}')
        local execution_time=$(echo "$result" | grep "Execution Time:" | awk '{print $3, $4}')
        local total_cost=$(echo "$result" | grep -m1 "cost=" | sed 's/.*cost=\([0-9.]*\)\.\.\([0-9.]*\).*/\2/')
        
        echo -e "${BRIGHT_WHITE}Key Metrics:${NC}"
        [[ -n "$planning_time" ]] && echo -e "${CYAN}  Planning Time:  ${BRIGHT_WHITE}$planning_time${NC}"
        [[ -n "$execution_time" ]] && echo -e "${CYAN}  Execution Time: ${BRIGHT_WHITE}$execution_time${NC}"
        [[ -n "$total_cost" ]] && echo -e "${CYAN}  Total Cost:     ${BRIGHT_WHITE}$total_cost${NC}"
        echo ""
        
        log_action "EXPLAIN ANALYZE executed on $db_name"
    else
        echo -e "${RED}✗ EXPLAIN ANALYZE failed${NC}"
        echo ""
        echo "$result"
        echo ""
    fi

    read -p "Press Enter to continue..."
}

list_databases() {
    list_databases_display
    read -p "Press Enter to continue..."
}

create_database() {
    list_databases_display
    read -p "Enter new database name: " db_name
    db_name=$(_trim "$db_name")
    if [[ -z "$db_name" ]]; then
        echo -e "${RED}Invalid name${NC}"
        read -p "Press Enter..."
        return 1
    fi
    
    if ! validate_identifier "$db_name"; then
        echo -e "${RED}Invalid characters in database name${NC}"
        read -p "Press Enter..."
        return 1
    fi
    
    if database_exists "$db_name"; then
        echo -e "${RED}Database '$db_name' already exists${NC}"
        read -p "Press Enter..."
        return 1
    fi
    
    execute_psql "CREATE DATABASE $(sql_quote_identifier "$db_name");" "postgres"
    echo -e "${GREEN}Database '$db_name' created${NC}"
    log_action "Database created: $db_name"
    read -p "Press Enter to continue..."
}

alter_database_name() {
    list_databases_display
    read -p "Enter current database name: " old_db_name
    old_db_name=$(_trim "$old_db_name")
    if ! database_exists "$old_db_name"; then
        echo -e "${RED}Database '$old_db_name' does not exist${NC}"
        read -p "Press Enter..."
        return 1
    fi
    read -p "Enter new database name: " new_db_name
    new_db_name=$(_trim "$new_db_name")
    
    if ! validate_identifier "$new_db_name"; then
        echo -e "${RED}Invalid characters in database name${NC}"
        read -p "Press Enter..."
        return 1
    fi
    
    execute_psql "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $(sql_quote_literal "$old_db_name") AND pid <> pg_backend_pid();" "postgres"
    execute_psql "ALTER DATABASE $(sql_quote_identifier "$old_db_name") RENAME TO $(sql_quote_identifier "$new_db_name");" "postgres"
    log_action "Database renamed: $old_db_name -> $new_db_name"
    read -p "Press Enter to continue..."
}

drop_database() {
    list_databases_display
    read -p "Enter database name to DELETE: " db_name
    db_name=$(_trim "$db_name")
    if [[ -z "$db_name" ]]; then
        echo -e "${RED}Invalid name${NC}"
        read -p "Press Enter..."
        return 1
    fi
    if ! database_exists "$db_name"; then
        echo -e "${RED}Database '$db_name' does not exist${NC}"
        read -p "Press Enter..."
        return 1
    fi
    if [[ "$db_name" == "postgres" || "$db_name" == "template0" || "$db_name" == "template1" ]]; then
        echo -e "${RED}Refusing to delete critical database '$db_name'${NC}"
        read -p "Press Enter..."
        return 1
    fi
    echo -e "${YELLOW}WARNING:${NC} This will permanently delete database '"$db_name"'."
    read -p "Type 'yes' to confirm: " conf
    if [[ "$conf" != "yes" ]]; then
        echo "Cancelled"
        read -p "Press Enter..."
        return 1
    fi
    execute_psql "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $(sql_quote_literal "$db_name") AND pid <> pg_backend_pid();" "postgres"
    if execute_psql "DROP DATABASE $(sql_quote_identifier "$db_name");" "postgres" >/dev/null 2>&1; then
        echo -e "${GREEN}Database '$db_name' deleted${NC}"
        log_action "Database deleted: $db_name"
    else
        echo -e "${RED}Failed to delete database '$db_name'${NC}"
    fi
    read -p "Press Enter to continue..."
}

create_user() {
    list_databases_display
    read -p "Enter username: " username
    username=$(_trim "$username")
    
    if ! validate_identifier "$username"; then
        echo -e "${RED}Invalid characters in username${NC}"
        read -p "Press Enter..."
        return 1
    fi
    
    read -s -p "Enter password: " password
    echo ""
    read -p "Enter database to grant read-only access: " db_name
    db_name=$(_trim "$db_name")
    if ! database_exists "$db_name"; then
        echo -e "${RED}Database does not exist${NC}"
        read -p "Press Enter..."
        return 1
    fi

    execute_psql "DO \$\$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = $(sql_quote_literal "$username")) THEN
      CREATE ROLE $(sql_quote_identifier "$username") LOGIN PASSWORD $(sql_quote_literal "$password");
   END IF;
END
\$\$;" "postgres"

    execute_psql "GRANT CONNECT ON DATABASE $(sql_quote_identifier "$db_name") TO $(sql_quote_identifier "$username");" "postgres"
    execute_psql "GRANT USAGE ON SCHEMA public TO $(sql_quote_identifier "$username");" "$db_name"
    execute_psql "GRANT SELECT ON ALL TABLES IN SCHEMA public TO $(sql_quote_identifier "$username");" "$db_name"
    execute_psql "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO $(sql_quote_identifier "$username");" "$db_name"
    log_action "User created: $username with read-only access to $db_name"
    read -p "Press Enter to continue..."
}

list_users() {
    echo -e "${YELLOW}Login Users:${NC}"
    execute_psql "SELECT 
        rolname AS \"Role\", 
        CASE WHEN rolcanlogin THEN 'LOGIN' ELSE '' END AS \"Attr\", 
        CASE WHEN rolsuper THEN 'SUPERUSER' ELSE '' END AS \"Super\",
        CASE WHEN rolcreatedb THEN 'CREATEDB' ELSE '' END AS \"CreateDB\",
        CASE WHEN rolcreaterole THEN 'CREATEROLE' ELSE '' END AS \"CreateRole\"
    FROM pg_catalog.pg_roles 
    WHERE rolcanlogin 
    ORDER BY rolname;" "postgres"
    read -p "Press Enter to continue..."
}

create_login_user() {
    read -p "Enter username: " username
    username=$(_trim "$username")
    if [[ -z "$username" ]]; then
        echo -e "${RED}Invalid username${NC}"
        read -p "Press Enter..."
        return 1
    fi
    
    if ! validate_identifier "$username"; then
        echo -e "${RED}Invalid characters in username${NC}"
        read -p "Press Enter..."
        return 1
    fi
    
    read -s -p "Enter password: " password
    echo ""
    if [[ -z "$password" ]]; then
        echo -e "${RED}Password cannot be empty${NC}"
        read -p "Press Enter..."
        return 1
    fi
    read -s -p "Confirm password: " password2
    echo ""
    if [[ "$password" != "$password2" ]]; then
        echo -e "${RED}Passwords do not match${NC}"
        read -p "Press Enter..."
        return 1
    fi

    execute_psql "DO \$\$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = $(sql_quote_literal "$username")) THEN
      CREATE ROLE $(sql_quote_identifier "$username") LOGIN PASSWORD $(sql_quote_literal "$password");
   ELSE
      RAISE NOTICE 'Role already exists: %', $(sql_quote_literal "$username");
   END IF;
END
\$\$;" "postgres"
    echo -e "${GREEN}Processed user '$username'${NC}"
    log_action "Login user created: $username"
    read -p "Press Enter to continue..."
}

update_user_credentials() {
    read -p "Enter username to update: " username
    username=$(_trim "$username")
    if [[ -z "$username" ]]; then
        echo -e "${RED}Invalid username${NC}"
        read -p "Press Enter..."
        return 1
    fi

    local actual_role
    actual_role=$(execute_psql_quiet "SELECT rolname FROM pg_catalog.pg_roles WHERE LOWER(rolname) = LOWER($(sql_quote_literal "$username")) LIMIT 1;" "postgres")
    if [[ -z "$actual_role" ]]; then
        echo -e "${RED}User '$username' does not exist${NC}"
        read -p "Press Enter..."
        return 1
    fi
    username="$actual_role"

    read -s -p "Enter new password: " password
    echo ""
    if [[ -z "$password" ]]; then
        echo -e "${RED}Password cannot be empty${NC}"
        read -p "Press Enter..."
        return 1
    fi
    read -s -p "Confirm new password: " password2
    echo ""
    if [[ "$password" != "$password2" ]]; then
        echo -e "${RED}Passwords do not match${NC}"
        read -p "Press Enter..."
        return 1
    fi

    if execute_psql "ALTER ROLE $(sql_quote_identifier "$username") WITH PASSWORD $(sql_quote_literal "$password");" "postgres" >/dev/null 2>&1; then
        echo -e "${GREEN}Password updated for user '$username'${NC}"
        log_action "Password updated for user: $username"
    else
        echo -e "${RED}Failed to update password${NC}"
    fi
    read -p "Press Enter to continue..."
}

grant_user_db_access() {
    list_databases_display
    read -p "Enter existing username: " username
    username=$(_trim "$username")
    if [[ -z "$username" ]]; then
        echo -e "${RED}Invalid username${NC}"
        read -p "Press Enter..."
        return 1
    fi

    local actual_role
    actual_role=$(execute_psql_quiet "SELECT rolname FROM pg_catalog.pg_roles WHERE LOWER(rolname) = LOWER($(sql_quote_literal "$username")) LIMIT 1;" "postgres")
    if [[ -z "$actual_role" ]]; then
        echo -e "${RED}User '$username' does not exist${NC}"
        read -p "Press Enter..."
        return 1
    fi
    username="$actual_role"

    read -p "Enter database to grant access: " db_name
    db_name=$(_trim "$db_name")
    if ! database_exists "$db_name"; then
        echo -e "${RED}Database does not exist${NC}"
        read -p "Press Enter..."
        return 1
    fi

    execute_psql "GRANT CONNECT ON DATABASE $(sql_quote_identifier "$db_name") TO $(sql_quote_identifier "$username");" "postgres"
    execute_psql "GRANT USAGE ON SCHEMA public TO $(sql_quote_identifier "$username");" "$db_name"
    execute_psql "GRANT SELECT ON ALL TABLES IN SCHEMA public TO $(sql_quote_identifier "$username");" "$db_name"
    execute_psql "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO $(sql_quote_identifier "$username");" "$db_name"

    echo -e "${GREEN}Granted read-only access on '$db_name' to '$username'${NC}"
    log_action "Granted read-only access: $username on $db_name"
    read -p "Press Enter to continue..."
}

grant_user_db_rw_access() {
    list_databases_display
    read -p "Enter existing username: " username
    username=$(_trim "$username")
    if [[ -z "$username" ]]; then
        echo -e "${RED}Invalid username${NC}"
        read -p "Press Enter..."
        return 1
    fi

    local actual_role
    actual_role=$(execute_psql_quiet "SELECT rolname FROM pg_catalog.pg_roles WHERE LOWER(rolname) = LOWER($(sql_quote_literal "$username")) LIMIT 1;" "postgres")
    if [[ -z "$actual_role" ]]; then
        echo -e "${RED}User '$username' does not exist${NC}"
        read -p "Press Enter..."
        return 1
    fi
    username="$actual_role"

    read -p "Enter database to grant access: " db_name
    db_name=$(_trim "$db_name")
    if ! database_exists "$db_name"; then
        echo -e "${RED}Database does not exist${NC}"
        read -p "Press Enter..."
        return 1
    fi

    echo "Choose permission type on schema 'public':"
    echo "1) Read-Write (SELECT, INSERT, UPDATE, DELETE)"
    echo "2) INSERT only"
    echo "3) UPDATE only"
    echo "4) DELETE only"
    echo "5) ALL on tables and sequences"
    read -p "Choice [1-5]: " pchoice

    execute_psql "GRANT CONNECT ON DATABASE $(sql_quote_identifier "$db_name") TO $(sql_quote_identifier "$username");" "postgres"
    execute_psql "GRANT USAGE ON SCHEMA public TO $(sql_quote_identifier "$username");" "$db_name"

    case "$pchoice" in
        1)
            execute_psql "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO $(sql_quote_identifier "$username");" "$db_name"
            execute_psql "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO $(sql_quote_identifier "$username");" "$db_name"
            execute_psql "GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA public TO $(sql_quote_identifier "$username");" "$db_name"
            execute_psql "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO $(sql_quote_identifier "$username");" "$db_name"
            echo -e "${GREEN}Granted read-write on tables and sequences in 'public'${NC}"
            log_action "Granted read-write access: $username on $db_name"
            ;;
        2)
            execute_psql "GRANT INSERT ON ALL TABLES IN SCHEMA public TO $(sql_quote_identifier "$username");" "$db_name"
            execute_psql "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT INSERT ON TABLES TO $(sql_quote_identifier "$username");" "$db_name"
            execute_psql "GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA public TO $(sql_quote_identifier "$username");" "$db_name"
            execute_psql "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO $(sql_quote_identifier "$username");" "$db_name"
            echo -e "${GREEN}Granted INSERT and sequence usage in 'public'${NC}"
            log_action "Granted INSERT access: $username on $db_name"
            ;;
        3)
            execute_psql "GRANT UPDATE ON ALL TABLES IN SCHEMA public TO $(sql_quote_identifier "$username");" "$db_name"
            execute_psql "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT UPDATE ON TABLES TO $(sql_quote_identifier "$username");" "$db_name"
            echo -e "${GREEN}Granted UPDATE on tables in 'public'${NC}"
            log_action "Granted UPDATE access: $username on $db_name"
            ;;
        4)
            execute_psql "GRANT DELETE ON ALL TABLES IN SCHEMA public TO $(sql_quote_identifier "$username");" "$db_name"
            execute_psql "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT DELETE ON TABLES TO $(sql_quote_identifier "$username");" "$db_name"
            echo -e "${GREEN}Granted DELETE on tables in 'public'${NC}"
            log_action "Granted DELETE access: $username on $db_name"
            ;;
        5)
            execute_psql "GRANT ALL PRIVILEGES ON SCHEMA public TO $(sql_quote_identifier "$username");" "$db_name"
            execute_psql "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO $(sql_quote_identifier "$username");" "$db_name"
            execute_psql "GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO $(sql_quote_identifier "$username");" "$db_name"
            execute_psql "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON TABLES TO $(sql_quote_identifier "$username");" "$db_name"
            execute_psql "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON SEQUENCES TO $(sql_quote_identifier "$username");" "$db_name"
            echo -e "${GREEN}Granted ALL privileges on schema, tables, sequences in 'public'${NC}"
            log_action "Granted ALL privileges: $username on $db_name"
            ;;
        *)
            echo "Invalid selection"
            read -p "Press Enter..."
            return 1
            ;;
    esac

    read -p "Press Enter to continue..."
}

check_indexes() {
    list_databases_display
    read -p "Enter database name: " db_name
    db_name=$(_trim "$db_name")
    if ! database_exists "$db_name"; then
        echo -e "${RED}DB not found${NC}"
        read -p "Press Enter..."
        return 1
    fi

    list_tables_display "$db_name"
    read -p "Enter table name (schema.table OR unqualified): " table_input
    table_input=$(_trim "$table_input")
    table_input=$(strip_outer_quotes "$table_input")

    if ! resolve_table "$db_name" "$table_input"; then
        echo -e "${RED}Table not found${NC}"
        read -p "Press Enter..."
        return 1
    fi

    local lit
    lit=$(make_regclass_literal "$RESOLVED_SCHEMA.$RESOLVED_TABLE" "$db_name")
    local idx
    idx=$(execute_psql_quiet "SELECT '    \"' || i.relname || '\" ' ||
        CASE WHEN ix.indisprimary THEN 'PRIMARY KEY, '
             WHEN ix.indisunique THEN 'UNIQUE, '
             ELSE '' END ||
        regexp_replace(pg_get_indexdef(ix.indexrelid), '^.* USING ', '')
      FROM pg_index ix
      JOIN pg_class i ON i.oid = ix.indexrelid
      WHERE ix.indrelid = $lit::regclass
      ORDER BY i.relname;" "$db_name")
    if [[ -n "$idx" ]]; then
        echo "Indexes:"
        echo "$idx"
    else
        echo "No indexes found"
    fi
    read -p "Press Enter to continue..."
}

check_table_structure() {
    list_databases_display
    read -p "Enter database name: " db_name
    db_name=$(_trim "$db_name")
    if ! database_exists "$db_name"; then
        echo -e "${RED}DB not found${NC}"
        read -p "Press Enter..."
        return 1
    fi

    list_tables_display "$db_name"
    read -p "Enter table name (schema.table OR unqualified): " table_input
    table_input=$(_trim "$table_input")
    table_input=$(strip_outer_quotes "$table_input")

    if ! resolve_table "$db_name" "$table_input"; then
        echo -e "${RED}Table not found${NC}"
        read -p "Press Enter..."
        return 1
    fi

    local lit
    lit=$(make_regclass_literal "$RESOLVED_SCHEMA.$RESOLVED_TABLE" "$db_name")

    echo "Table \"$RESOLVED_SCHEMA.$RESOLVED_TABLE\""

    execute_psql "SELECT a.attname AS \"Column\",
       format_type(a.atttypid, a.atttypmod) AS \"Type\",
       CASE WHEN a.attcollation <> 0 THEN (SELECT coll.collname FROM pg_collation coll WHERE coll.oid = a.attcollation) ELSE NULL END AS \"Collation\",
       CASE WHEN a.attnotnull THEN 'not null' ELSE '' END AS \"Nullable\",
       pg_get_expr(ad.adbin, ad.adrelid) AS \"Default\",
       CASE a.attstorage WHEN 'p' THEN 'plain' WHEN 'm' THEN 'main' WHEN 'x' THEN 'external' WHEN 'e' THEN 'extended' ELSE NULL END AS \"Storage\",
       CASE a.attcompression WHEN 'p' THEN 'pglz' WHEN 'l' THEN 'lz4' ELSE NULL END AS \"Compression\",
       CASE WHEN a.attstattarget = -1 THEN NULL ELSE a.attstattarget END AS \"Stats target\",
       col_description(a.attrelid, a.attnum) AS \"Description\"
     FROM pg_attribute a
     LEFT JOIN pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
     WHERE a.attrelid = $lit::regclass AND a.attnum > 0 AND NOT a.attisdropped
     ORDER BY a.attnum;" "$db_name"

    echo ""

    local idx
    idx=$(execute_psql_quiet "SELECT '    \"' || i.relname || '\" ' ||
        CASE WHEN ix.indisprimary THEN 'PRIMARY KEY, '
             WHEN ix.indisunique THEN 'UNIQUE, '
             ELSE '' END ||
        regexp_replace(pg_get_indexdef(ix.indexrelid), '^.* USING ', '')
      FROM pg_index ix
      JOIN pg_class i ON i.oid = ix.indexrelid
      WHERE ix.indrelid = $lit::regclass
      ORDER BY i.relname;" "$db_name")
    if [[ -n "$idx" ]]; then
        echo "Indexes:"
        echo "$idx"
    fi

    local fks
    fks=$(execute_psql_quiet "SELECT '    \"' || c.conname || '\" ' || pg_get_constraintdef(c.oid)
      FROM pg_constraint c
      WHERE c.conrelid = $lit::regclass AND c.contype = 'f'
      ORDER BY c.conname;" "$db_name")
    if [[ -n "$fks" ]]; then
        echo "Foreign-key constraints:"
        echo "$fks"
    fi

    local am
    am=$(execute_psql_quiet "SELECT 'Access method: ' || am.amname FROM pg_class c JOIN pg_am am ON am.oid = c.relam WHERE c.oid = $lit::regclass;" "$db_name")
    if [[ -n "$am" ]]; then
        echo "$am"
    fi
    read -p "Press Enter to continue..."
}

check_sequences() {
    list_databases_display
    read -p "Enter database name: " db_name
    db_name=$(_trim "$db_name")
    if ! database_exists "$db_name"; then
        echo -e "${RED}DB not found${NC}"
        read -p "Press Enter..."
        return 1
    fi

    list_tables_display "$db_name"
    read -p "Enter table name (schema.table OR unqualified): " table_input
    table_input=$(_trim "$table_input")
    table_input=$(strip_outer_quotes "$table_input")

    if ! resolve_table "$db_name" "$table_input"; then
        echo -e "${RED}Table not found${NC}"
        read -p "Press Enter..."
        return 1
    fi

    local lit
    lit=$(make_regclass_literal "$RESOLVED_SCHEMA.$RESOLVED_TABLE" "$db_name")
    seqs=$(execute_psql_quiet "SELECT n.nspname || '.' || c.relname FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid JOIN pg_depend d ON d.objid = c.oid AND d.deptype = 'a' JOIN pg_class t ON d.refobjid = t.oid WHERE c.relkind = 'S' AND d.refobjid = $lit::regclass ORDER BY 1;" "$db_name")

    if [[ -z "$seqs" ]]; then
        echo "No sequences found for table."
        read -p "Press Enter to continue..."
        return 0
    fi

    while IFS= read -r s; do
        s=$(_trim "$s")
        [[ -z "$s" ]] && continue
        val=$(get_sequence_current_value "$s" "$db_name")
        echo "Sequence: $s, Current Value: $val"
    done <<< "$seqs"
    read -p "Press Enter to continue..."
}

reset_sequences() {
    list_databases_display
    read -p "Enter database name: " db_name
    db_name=$(_trim "$db_name")
    if ! database_exists "$db_name"; then
        echo -e "${RED}DB not found${NC}"
        read -p "Press Enter..."
        return 1
    fi

    echo "1) Reset sequences for ALL sequences in the DB"
    echo "2) Reset a specific sequence"
    read -p "Choice [1-2]: " rchoice

    if [[ "$rchoice" == "1" ]]; then
        read -p "Enter numeric value to set for ALL sequences: " new_value
        if ! [[ "$new_value" =~ ^[0-9]+$ ]]; then
            echo -e "${RED}Invalid number${NC}"
            read -p "Press Enter..."
            return 1
        fi
        read -p "Type 'yes' to proceed: " conf
        if [[ "$conf" != "yes" ]]; then
            echo "Cancelled"
            read -p "Press Enter..."
            return 1
        fi
        sequences=$(execute_psql_quiet "SELECT schemaname || '.' || sequencename FROM pg_catalog.pg_sequences ORDER BY 1;" "$db_name")
        local total=0
        local ok=0
        while IFS= read -r seq; do
            seq=$(_trim "$seq")
            [[ -z "$seq" ]] && continue
            total=$((total+1))
            lit=$(make_regclass_literal "$seq" "$db_name")
            if execute_psql "SELECT setval($lit::regclass, $new_value, false);" "$db_name" >/dev/null 2>&1; then
                ok=$((ok+1))
                echo "Updated $seq"
            else
                echo "Failed $seq"
            fi
        done <<< "$sequences"
        echo "Completed: $ok/$total"
        log_action "Reset all sequences in $db_name to $new_value"
        read -p "Press Enter to continue..."
        return 0

    elif [[ "$rchoice" == "2" ]]; then
        list_tables_display "$db_name"
        read -p "Enter sequence OR table name (schema.name OR name) WITHOUT quotes: " seq_or_table
        seq_or_table=$(_trim "$seq_or_table")
        seq_or_table=$(strip_outer_quotes "$seq_or_table")
        read -p "Enter numeric value to set: " v
        if ! [[ "$v" =~ ^[0-9]+$ ]]; then
            echo -e "${RED}Invalid number${NC}"
            read -p "Press Enter..."
            return 1
        fi
        seqs=$(resolve_sequences_for_input "$seq_or_table" "$db_name")
        if [[ -z "$seqs" ]]; then
            echo -e "${RED}No matching sequences found${NC}"
            read -p "Press Enter..."
            return 1
        fi
        echo "About to set the following sequences to $v:"
        echo "$seqs"
        read -p "Type 'yes' to proceed: " c2
        if [[ "$c2" != "yes" ]]; then
            echo "Cancelled"
            read -p "Press Enter..."
            return 1
        fi
        local updated=0
        local total=0
        while IFS= read -r one; do
            one=$(_trim "$one")
            [[ -z "$one" ]] && continue
            total=$((total+1))
            lit=$(make_regclass_literal "$one" "$db_name")
            if execute_psql "SELECT setval($lit::regclass, $v, false);" "$db_name" >/dev/null 2>&1; then
                updated=$((updated+1))
                echo "Updated $one"
            else
                echo "Failed $one"
            fi
        done <<< "$seqs"
        echo "Completed: $updated/$total"
        log_action "Reset sequences for $seq_or_table in $db_name to $v"
        read -p "Press Enter to continue..."
        return 0
    else
        echo "Invalid selection"
        read -p "Press Enter..."
        return 1
    fi
}

create_index() {
    list_databases_display
    read -p "Enter database name: " db_name
    db_name=$(_trim "$db_name")
    if ! database_exists "$db_name"; then
        echo -e "${RED}DB not found${NC}"
        read -p "Press Enter..."
        return 1
    fi

    list_tables_display "$db_name"
    read -p "Enter table name (schema.table OR unqualified): " table_input
    table_input=$(_trim "$table_input")
    table_input=$(strip_outer_quotes "$table_input")

    if ! resolve_table "$db_name" "$table_input"; then
        echo -e "${RED}Table not found${NC}"
        read -p "Press Enter..."
        return 1
    fi

    local lit
    lit=$(make_regclass_literal "$RESOLVED_SCHEMA.$RESOLVED_TABLE" "$db_name")

    echo -e "${YELLOW}Columns in \"$RESOLVED_SCHEMA.$RESOLVED_TABLE\":${NC}"
    execute_psql "SELECT a.attname AS \"Column\" FROM pg_attribute a WHERE a.attrelid = $lit::regclass AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum;" "$db_name"

    read -p "Enter column name to index: " col_input
    col_input=$(_trim "$col_input")
    col_input=$(strip_outer_quotes "$col_input")
    if [[ -z "$col_input" ]]; then
        echo -e "${RED}Invalid column${NC}"
        read -p "Press Enter..."
        return 1
    fi

    local actual_col
    actual_col=$(execute_psql_quiet "SELECT attname FROM pg_attribute WHERE attrelid = $lit::regclass AND LOWER(attname) = LOWER($(sql_quote_literal "$col_input")) AND attnum > 0 AND NOT attisdropped LIMIT 1;" "$db_name")
    if [[ -z "$actual_col" ]]; then
        echo -e "${RED}Column '$col_input' not found on table${NC}"
        read -p "Press Enter..."
        return 1
    fi
    col_input="$actual_col"

    local sanitized_table="${RESOLVED_TABLE//[^a-zA-Z0-9_]/_}"
    local sanitized_col="${col_input//[^a-zA-Z0-9_]/_}"
    local default_index="idx_${sanitized_table}_${sanitized_col}"

    echo "Suggested index name: $default_index"
    read -p "Enter index name (or press Enter to use suggested): " idx_name
    idx_name=$(_trim "$idx_name")
    if [[ -z "$idx_name" ]]; then
        idx_name="$default_index"
    fi

    local idx_exists
    idx_exists=$(execute_psql_quiet "SELECT 1 FROM pg_class WHERE relkind = 'i' AND relname = $(sql_quote_literal "$idx_name") LIMIT 1;" "$db_name")
    if [[ -n "$idx_exists" ]]; then
        echo -e "${YELLOW}Index '$idx_name' already exists${NC}"
        read -p "Press Enter to continue..."
        return 0
    fi

    local schema_quoted
    local table_quoted
    local col_quoted
    schema_quoted=$(sql_quote_identifier "$RESOLVED_SCHEMA")
    table_quoted=$(sql_quote_identifier "$RESOLVED_TABLE")
    col_quoted=$(sql_quote_identifier "$col_input")

    if execute_psql "CREATE INDEX IF NOT EXISTS $(sql_quote_identifier "$idx_name") ON ${schema_quoted}.${table_quoted} USING btree (${col_quoted});" "$db_name" >/dev/null 2>&1; then
        echo -e "${GREEN}Index '$idx_name' created on \"$RESOLVED_SCHEMA.$RESOLVED_TABLE\" ($col_input)${NC}"
        log_action "Index created: $idx_name on $RESOLVED_SCHEMA.$RESOLVED_TABLE($col_input) in $db_name"
    else
        echo -e "${RED}Failed to create index${NC}"
    fi
    read -p "Press Enter to continue..."
}

apply_views() {
    list_databases_display
    read -p "Enter database name: " db_name
    db_name=$(_trim "$db_name")
    if ! database_exists "$db_name"; then
        echo -e "${RED}DB not found${NC}"
        read -p "Press Enter..."
        return 1
    fi

    read -p "Enter target schema (default: public): " schema_name
    schema_name=$(_trim "$schema_name")
    if [[ -z "$schema_name" ]]; then
        schema_name="public"
    fi

    local customer_path="${VIEW_SCRIPTS_DIR}/${CUSTOMER_VIEWS_FILE}"
    local product_path="${VIEW_SCRIPTS_DIR}/${PRODUCT_VIEWS_FILE}"

    echo "Apply Views:"
    echo "1) Customer Views (${CUSTOMER_VIEWS_FILE})"
    echo "2) Product Views (${PRODUCT_VIEWS_FILE})"
    echo "3) Both"
    read -p "Choice [1-3]: " vchoice

    local any_applied=0
    case "$vchoice" in
        1)
            if execute_psql_file_continue "$customer_path" "$db_name" "$schema_name"; then
                echo -e "${GREEN}Customer views applied${NC}"
                log_action "Applied customer views to $db_name.$schema_name"
                any_applied=1
            fi
            ;;
        2)
            if execute_psql_file_continue "$product_path" "$db_name" "$schema_name"; then
                echo -e "${GREEN}Product views applied${NC}"
                log_action "Applied product views to $db_name.$schema_name"
                any_applied=1
            fi
            ;;
        3)
            local ok1=0
            local ok2=0
            if execute_psql_file_continue "$customer_path" "$db_name" "$schema_name"; then
                ok1=1
                echo -e "${GREEN}Customer views applied${NC}"
            fi
            if execute_psql_file_continue "$product_path" "$db_name" "$schema_name"; then
                ok2=1
                echo -e "${GREEN}Product views applied${NC}"
            fi
            if [[ $ok1 -eq 1 || $ok2 -eq 1 ]]; then
                any_applied=1
                log_action "Applied views to $db_name.$schema_name"
            fi
            ;;
        *)
            echo "Invalid selection"
            read -p "Press Enter..."
            return 1
            ;;
    esac

    if [[ $any_applied -eq 0 ]]; then
        echo -e "${YELLOW}No changes applied${NC}"
    fi
    read -p "Press Enter to continue..."
}

apply_functions() {
    list_databases_display
    read -p "Enter database name: " db_name
    db_name=$(_trim "$db_name")
    if ! database_exists "$db_name"; then
        echo -e "${RED}DB not found${NC}"
        read -p "Press Enter..."
        return 1
    fi

    read -p "Enter target schema (default: public): " schema_name
    schema_name=$(_trim "$schema_name")
    if [[ -z "$schema_name" ]]; then
        schema_name="public"
    fi

    local functions_path="${VIEW_SCRIPTS_DIR}/${FUNCTIONS_FILE}"

    if execute_psql_file_continue "$functions_path" "$db_name" "$schema_name"; then
        echo -e "${GREEN}Functions applied from ${FUNCTIONS_FILE}${NC}"
        log_action "Applied functions to $db_name.$schema_name"
    else
        echo -e "${RED}Failed applying functions from ${FUNCTIONS_FILE}${NC}"
    fi
    read -p "Press Enter to continue..."
}

list_views() {
    list_databases_display
    read -p "Enter database name: " db_name
    db_name=$(_trim "$db_name")
    if ! database_exists "$db_name"; then
        echo -e "${RED}DB not found${NC}"
        read -p "Press Enter..."
        return 1
    fi

    echo -e "${YELLOW}Views in database '$db_name':${NC}"
    execute_psql "SELECT
        schemaname as \"Schema\",
        viewname as \"View Name\",
        pg_size_pretty(pg_total_relation_size(quote_ident(schemaname) || '.' || quote_ident(viewname))) as \"Size\"
    FROM pg_views
    WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
    ORDER BY schemaname, viewname;" "$db_name"

    read -p "Press Enter to continue..."
}

list_functions() {
    list_databases_display
    read -p "Enter database name: " db_name
    db_name=$(_trim "$db_name")
    if ! database_exists "$db_name"; then
        echo -e "${RED}DB not found${NC}"
        read -p "Press Enter..."
        return 1
    fi

    echo -e "${YELLOW}Functions in database '$db_name':${NC}"
    execute_psql "SELECT
        n.nspname as \"Schema\",
        p.proname as \"Function Name\",
        pg_get_function_arguments(p.oid) as \"Arguments\",
        pg_get_function_result(p.oid) as \"Return Type\"
    FROM pg_proc p
    JOIN pg_namespace n ON p.pronamespace = n.oid
    WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
    ORDER BY n.nspname, p.proname;" "$db_name"

    read -p "Press Enter to continue..."
}

execute_custom_query() {
    list_databases_display
    read -p "Enter database name: " db_name
    db_name=$(_trim "$db_name")
    if ! database_exists "$db_name"; then
        echo -e "${RED}DB not found${NC}"
        read -p "Press Enter..."
        return 1
    fi

    echo ""
    list_tables_display "$db_name"
    echo ""

    echo -e "${YELLOW}Enter your SQL query (or type 'cancel' to abort):${NC}"
    echo -e "${BLUE}Examples:${NC}"
    echo "  SELECT * FROM table_name LIMIT 10;"
    echo "  SELECT * FROM \"schema.name\".\"table.name\" LIMIT 10;"
    echo "  SELECT COUNT(*) FROM table_name;"
    echo "  SELECT column1, column2 FROM table_name WHERE condition;"
    echo ""
    echo -e "${CYAN}Note: For tables with dots in names, use quotes like:${NC}"
    echo -e "${CYAN}  SELECT * FROM \"syncoms.photos\".\"album.duplicate\" LIMIT 10;${NC}"
    echo ""

    read -p "SQL> " query
    query=$(_trim "$query")

    if [[ -z "$query" ]]; then
        echo -e "${RED}No query provided${NC}"
        read -p "Press Enter..."
        return 1
    fi

    if [[ "${query,,}" == "cancel" ]]; then
        echo "Cancelled"
        read -p "Press Enter..."
        return 0
    fi

    # Smart table name fixing for queries
    # Only apply auto-quoting if the table reference is NOT already quoted
    # Pattern: Find FROM/JOIN/INTO/UPDATE/TABLE followed by unquoted identifiers with 2+ dots
    # This regex looks for keywords followed by identifiers with multiple dots that aren't already quoted
    local fixed_query="$query"
    
    # Check if query has unquoted multi-dot table references
    if [[ "$fixed_query" =~ (FROM|JOIN|INTO|UPDATE|TABLE)[[:space:]]+([a-zA-Z0-9_]+\.[a-zA-Z0-9_]+\.[a-zA-Z0-9_\.]+) ]]; then
        echo -e "${YELLOW}Detected table names with multiple dots. Attempting to fix...${NC}"
        
        # Extract all table names from the displayed list to match against
        local table_list=$(execute_psql_quiet "SELECT table_schema || '.' || table_name FROM information_schema.tables WHERE table_schema NOT IN ('information_schema','pg_catalog','pg_toast') ORDER BY 1;" "$db_name")
        
        # For each table in the list, check if it appears unquoted in the query and quote it
        while IFS= read -r full_table; do
            [[ -z "$full_table" ]] && continue
            
            # Count dots in the full table name
            local dot_count=$(echo "$full_table" | tr -cd '.' | wc -c)
            
            if [[ $dot_count -ge 2 ]]; then
                # This table has 2+ dots, need to handle it carefully
                local schema="${full_table%.*}"
                local table="${full_table##*.}"
                
                # Replace unquoted references with properly quoted ones
                # Look for the table reference that's NOT already in quotes
                fixed_query=$(echo "$fixed_query" | sed -E "s/(FROM|JOIN|INTO|UPDATE|TABLE)([[:space:]]+)${full_table}([[:space:],;]|$)/\1\2\"${schema}\".\"${table}\"\3/gi")
            fi
        done <<< "$table_list"
    fi

    # Warn about dangerous operations
    if [[ "${fixed_query,,}" =~ (drop|truncate|delete)[[:space:]]+ ]]; then
        echo -e "${RED}WARNING: This query contains potentially destructive operations!${NC}"
        echo "Query: $fixed_query"
        read -p "Type 'yes' to confirm execution: " confirm
        if [[ "$confirm" != "yes" ]]; then
            echo "Cancelled"
            read -p "Press Enter..."
            return 0
        fi
    fi

    echo -e "${YELLOW}Executing query...${NC}"
    if [[ "$fixed_query" != "$query" ]]; then
        echo -e "${CYAN}Auto-fixed query: $fixed_query${NC}"
    fi
    echo ""

    if execute_psql "$fixed_query" "$db_name"; then
        echo ""
        echo -e "${GREEN}Query executed successfully${NC}"
        log_action "Custom query executed on $db_name"
    else
        echo ""
        echo -e "${RED}Query execution failed${NC}"
        echo -e "${YELLOW}Tip: For tables with dots in their names, use double quotes:${NC}"
        echo -e "${CYAN}  SELECT * FROM \"schema.with.dots\".\"table.with.dots\" LIMIT 10;${NC}"
    fi

    read -p "Press Enter to continue..."
}

show_menu() {
    local border_top="${BRIGHT_CYAN}┏━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓${NC}"
    local border_mid="${BRIGHT_CYAN}┣━━━━━━━━╋━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┫${NC}"
    local border_bot="${BRIGHT_CYAN}┗━━━━━━━━┻━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛${NC}"

    print_row() {
        printf "${BRIGHT_CYAN}┃${NC} ${BRIGHT_YELLOW}%-6s${NC} ${BRIGHT_CYAN}┃${NC} ${BRIGHT_GREEN}%-44s${NC} ${BRIGHT_CYAN}┃${NC}\n" "$1" "$2"
    }

    echo -e "$border_top"
    printf "${BRIGHT_CYAN}┃${NC} ${BRIGHT_WHITE}%-6s${NC} ${BRIGHT_CYAN}┃${NC} ${BRIGHT_WHITE}%-44s${NC} ${BRIGHT_CYAN}┃${NC}\n" "No" "Action"
    echo -e "$border_mid"

    print_row "1"  "Alter Database Name"
    print_row "2"  "Analyze Table Statistics"
    print_row "3"  "Apply Functions"
    print_row "4"  "Apply Views"
    print_row "5"  "Backup Database"
    print_row "6"  "Check Indexes"
    print_row "7"  "Check Sequences for Table"
    print_row "8"  "Check Table Structure"
    print_row "9"  "Create Database"
    print_row "10" "Create Index"
    print_row "11" "Create Schema"
    print_row "12" "Create User (read-only)"
    print_row "13" "Create User (username + password)"
    print_row "14" "Delete Database"
    print_row "15" "Execute Custom Query"
    print_row "16" "Explain Analyze Query (Cost Analysis)"
    print_row "17" "Export Table to CSV"
    print_row "18" "Grant DB Access to User"
    print_row "19" "Grant DB RW/Custom Access"
    print_row "20" "Grant Schema Permissions"
    print_row "21" "Kill Database Connections"
    print_row "22" "List Databases"
    print_row "23" "List Functions in Database"
    print_row "24" "List Schemas"
    print_row "25" "List Users"
    print_row "26" "List Views in Database"
    print_row "27" "Reset Sequence(s)"
    print_row "28" "Restore Database"
    print_row "29" "Show Constraints"
    print_row "30" "Show Database Connections"
    print_row "31" "Show Database Statistics"
    print_row "32" "Show Slow Queries"
    print_row "33" "Update User Credentials"
    print_row "34" "Vacuum Table/Database"
    print_row "0"  "Exit"

    echo -e "$border_bot"
    echo -e -n "${BRIGHT_CYAN}Enter choice:${NC} "
    read choice
}

list_schemas() {
    list_databases_display
    read -p "Enter database name: " db_name
    db_name=$(_trim "$db_name")
    if ! database_exists "$db_name"; then
        echo -e "${RED}DB not found${NC}"
        read -p "Press Enter..."
        return 1
    fi
    
    list_schemas_display "$db_name"
    read -p "Press Enter to continue..."
}

test_connection() {
    if ! command -v psql >/dev/null 2>&1; then
        echo -e "${RED}psql not found in PATH${NC}"
        return 1
    fi
    if execute_psql_quiet "SELECT 1;" "postgres" >/dev/null 2>&1; then
        return 0
    else
        echo -e "${RED}DB connection failed${NC}"
        return 1
    fi
}

# ---- Main ----
main() {
    # Create necessary directories
    mkdir -p "$BACKUP_DIR" 2>/dev/null || true
    
    # Set up logging - try multiple locations
    if [[ -w "/var/log" ]]; then
        LOG_FILE="/var/log/postgres_ops.log"
    elif [[ -w "$HOME" ]]; then
        LOG_FILE="$HOME/postgres_ops.log"
    else
        LOG_FILE="/tmp/postgres_ops.log"
    fi
    
    # Create log file if it doesn't exist
    touch "$LOG_FILE" 2>/dev/null || LOG_FILE="/dev/null"
    
    show_header
    if ! test_connection; then
        read -p "Press Enter to exit..."
        exit 1
    fi
    
    log_action "Script started by user: $USER"
    
    local last_choice=""
    while true; do
        show_header
        show_menu

        # If user just presses Enter and there was a previous choice, repeat it
        if [[ -z "$choice" && -n "$last_choice" ]]; then
            choice="$last_choice"
            echo "Repeating last action: $choice"
        fi

        # Store the current choice for next iteration
        if [[ -n "$choice" && "$choice" != "0" ]]; then
            last_choice="$choice"
        fi

        case $choice in
            1) alter_database_name ;;
            2) analyze_table ;;
            3) apply_functions ;;
            4) apply_views ;;
            5) backup_database ;;
            6) check_indexes ;;
            7) check_sequences ;;
            8) check_table_structure ;;
            9) create_database ;;
            10) create_index ;;
            11) create_schema ;;
            12) create_user ;;
            13) create_login_user ;;
            14) drop_database ;;
            15) execute_custom_query ;;
            16) explain_analyze_query ;;
            17) export_table_csv ;;
            18) grant_user_db_access ;;
            19) grant_user_db_rw_access ;;
            20) grant_schema_permissions ;;
            21) kill_connections ;;
            22) list_databases ;;
            23) list_functions ;;
            24) list_schemas ;;
            25) list_users ;;
            26) list_views ;;
            27) reset_sequences ;;
            28) restore_database ;;
            29) show_constraints ;;
            30) show_connections ;;
            31) show_db_statistics ;;
            32) show_slow_queries ;;
            33) update_user_credentials ;;
            34) vacuum_table ;;
            0) 
                echo -e "${BRIGHT_GREEN}Thank you for using PostgreSQL Enhanced Operations Tool${NC}"
                log_action "Script exited normally by user: $USER"
                exit 0
                ;;
            *) 
                echo -e "${RED}Invalid selection${NC}"
                read -p "Press Enter..."
                ;;
        esac
    done
}

main
