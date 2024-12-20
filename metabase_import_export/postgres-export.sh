#!/bin/bash

# Prompt for PostgreSQL connection details
read -p "Enter PostgreSQL host [default: metabasedb]: " PGHOST
PGHOST=${PGHOST:-metabasedb}

# Prompt for PostgreSQL username with a default value
read -p "Enter PostgreSQL username [default: metabase-user]: " PGUSER
PGUSER=${PGUSER:-metabase-user}

# Prompt for PostgreSQL password
echo -n "Enter metabase user password: "
read -s PGPASSWORD
echo

# Define database name (make sure this is set correctly)
DBNAME="metabase"  # Set the actual database name

id=$(docker exec bahmni-lite-metabasedb-1 sh -c "PGPASSWORD=$PGPASSWORD psql -h $PGHOST -U $PGUSER -d $DBNAME -t -c \"SELECT id FROM metabase_database WHERE name = 'mart' LIMIT 1;\"" | xargs)

# Check if ID was fetched successfully
if [ -z "$id" ]; then
  echo "Analytics Database not found :("
  exit 1
fi

current_date=$(date +%Y-%m-%d)

# Define the backup directory
backup_dir="metabase-backup-$current_date"

# Create the backup directory and handle errors
if ! mkdir "$backup_dir"; then
  echo "Failed to create directory '$backup_dir'."
  exit 1
fi
cd "$backup_dir"

# Run the first command
docker exec bahmni-lite-metabasedb-1 sh -c "PGPASSWORD=$PGPASSWORD psql -h $PGHOST -U $PGUSER -d $DBNAME -t -c \"\COPY (SELECT * FROM setting WHERE key IN ('custom-geojson')) TO 'setting.csv' WITH CSV DELIMITER ',' HEADER;\""
docker exec bahmni-lite-metabasedb-1 sh -c "PGPASSWORD=$PGPASSWORD psql -h $PGHOST -U $PGUSER -d $DBNAME -t -c \"\COPY (select id,email,first_name,last_name,password,password_salt,date_joined,last_login,is_superuser,is_active,reset_token,reset_triggered,is_qbnewb,login_attributes,updated_at,sso_source,locale,is_datasetnewb from core_user) TO 'core_user.csv' With CSV DELIMITER',' HEADER;\""
docker exec bahmni-lite-metabasedb-1 sh -c "PGPASSWORD=$PGPASSWORD psql -h $PGHOST -U $PGUSER -d $DBNAME -t -c \"\COPY (select * from collection) TO 'collection.csv' With CSV DELIMITER',' HEADER;\""
docker exec bahmni-lite-metabasedb-1 sh -c "PGPASSWORD=$PGPASSWORD psql -h $PGHOST -U $PGUSER -d $DBNAME -t -c \"\COPY (select * from report_card) TO 'report_card.csv' With CSV DELIMITER',' HEADER;\""
docker exec bahmni-lite-metabasedb-1 sh -c "PGPASSWORD=$PGPASSWORD psql -h $PGHOST -U $PGUSER -d $DBNAME -t -c \"\COPY (select * from metabase_table where db_id = $id) TO 'metabase_table.csv' With CSV DELIMITER ',' HEADER;\""
docker exec bahmni-lite-metabasedb-1 sh -c "PGPASSWORD=$PGPASSWORD psql -h $PGHOST -U $PGUSER -d $DBNAME -t -c \"\COPY (select metabase_field.* from metabase_field inner join metabase_table on metabase_field.table_id = metabase_table.id where metabase_table.db_id = $id) TO 'metabase_field.csv' With CSV DELIMITER ',' HEADER;\""
# Dashboard
docker exec bahmni-lite-metabasedb-1 sh -c "PGPASSWORD=$PGPASSWORD psql -h $PGHOST -U $PGUSER -d $DBNAME -t -c \"\COPY (select * from report_dashboard) TO 'report_dashboard.csv' With CSV DELIMITER',' HEADER;\""
docker exec bahmni-lite-metabasedb-1 sh -c "PGPASSWORD=$PGPASSWORD psql -h $PGHOST -U $PGUSER -d $DBNAME -t -c \"\COPY (select * from report_dashboardcard) TO 'report_dashboardcard.csv' With CSV DELIMITER',' HEADER;\""
docker exec bahmni-lite-metabasedb-1 sh -c "PGPASSWORD=$PGPASSWORD psql -h $PGHOST -U $PGUSER -d $DBNAME -t -c \"\COPY (select * from dashboardcard_series) TO 'dashboardcard_series.csv' With CSV DELIMITER',' HEADER;\""
# Permissions
docker exec bahmni-lite-metabasedb-1 sh -c "PGPASSWORD=$PGPASSWORD psql -h $PGHOST -U $PGUSER -d $DBNAME -t -c \"\COPY (select * from permissions_group) TO 'permissions_group.csv' With CSV DELIMITER',' HEADER;\""
docker exec bahmni-lite-metabasedb-1 sh -c "PGPASSWORD=$PGPASSWORD psql -h $PGHOST -U $PGUSER -d $DBNAME -t -c \"\COPY (select * from permissions) TO 'permissions.csv' With CSV DELIMITER',' HEADER;\""
docker exec bahmni-lite-metabasedb-1 sh -c "PGPASSWORD=$PGPASSWORD psql -h $PGHOST -U $PGUSER -d $DBNAME -t -c \"\COPY (select * from permissions_group_membership) TO 'permissions_group_membership.csv' With CSV DELIMITER',' HEADER;\""

required_vars=("setting.csv" "core_user.csv" "collection.csv" "report_card.csv" "metabase_table.csv" "metabase_field.csv" "report_dashboard.csv" "report_dashboardcard.csv" "dashboardcard_series.csv" "permissions_group.csv" "permissions.csv" "permissions_group_membership.csv")
for var in "${required_vars[@]}"; do
    docker cp bahmni-lite-metabasedb-1:/${var} .
done

# Clear the password variable
unset PGPASSWORD

cd ..
zip -r "$backup_dir.zip" "$backup_dir"
rm -r "$backup_dir"

echo "Data exported successfully."