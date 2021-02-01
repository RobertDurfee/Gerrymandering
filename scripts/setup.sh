#!/bin/bash

# Create admin user
createuser -U postgres -d -r -s gm_admin;

# Create database 
createdb -U gm_admin gm;

# Revoke public
psql -U gm_admin -d gm -c "$(cat << END
  REVOKE ALL ON SCHEMA public FROM PUBLIC;
  REVOKE ALL ON DATABASE gm FROM PUBLIC;
END
)";

# Create schema
psql -U gm_admin -d gm -c "CREATE SCHEMA gm;";

# Create readonly role
psql -U gm_admin -d gm -c "$(cat << END
  CREATE ROLE gm_readonly;
  GRANT CONNECT ON DATABASE gm TO gm_readonly;
  GRANT USAGE ON SCHEMA gm TO gm_readonly;
  GRANT SELECT ON ALL TABLES IN SCHEMA gm TO gm_readonly;
  ALTER DEFAULT PRIVILEGES IN SCHEMA gm GRANT SELECT ON TABLES TO gm_readonly;
END
)";

# Create readwrite role
psql -U gm_admin -d gm -c "$(cat << END
  CREATE ROLE gm_readwrite;
  GRANT CONNECT ON DATABASE gm TO gm_readwrite;
  GRANT USAGE, CREATE ON SCHEMA gm TO gm_readwrite;
  GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA gm TO gm_readwrite;
  ALTER DEFAULT PRIVILEGES IN SCHEMA gm GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO gm_readwrite;
  GRANT USAGE ON ALL SEQUENCES IN SCHEMA gm TO gm_readwrite;
  ALTER DEFAULT PRIVILEGES IN SCHEMA gm GRANT USAGE ON SEQUENCES TO gm_readwrite;
END
)";

# Create ingest user
psql -U gm_admin -d gm -c "$(cat << END
  CREATE USER gm_ingest;
  GRANT gm_readwrite TO gm_ingest;
END
)";

# Create API user
psql -U gm_admin -d gm -c "$(cat << END
  CREATE USER gm_api;
  GRANT gm_readonly TO gm_api;
END
)";

# Allow external, unauthenticated, readonly connections using the `gm_api` user
# Change `listen_addresses = 'localhost'` to `listen_addresses = '*'` in `postgresql.conf`
# Add `host gm gm_api 0.0.0.0/0 trust` to `pg_hba.conf`
# Add `host gm gm_api ::0/0 trust` to `pg_hba.conf`

# Enable PostGIS extension
psql -U gm_admin -d gm -c "CREATE EXTENSION postgis;";
