# AGENTS.md

- Repo: https://github.com/jacobfvanzyl/pgkv
- pgkv is a PostgreSQL Trusted Language Extension (TLE) packaged to follow pg_tle conventions: https://github.com/aws/pg_tle
- Keep versioned install scripts (`pgkv--X.Y.Z.sql`) and upgrade paths (`pgkv--old--new.sql`) in sync with `pgkv.control` `default_version`; `relocatable = false` because SQL hardcodes the `pgkv` schema.
- Before release, verify both direct install and upgrade path:
  - `CREATE EXTENSION pgkv;`
  - `CREATE EXTENSION pgkv VERSION 'old'; ALTER EXTENSION pgkv UPDATE TO 'new';`
- Run `./stress/run-stress.sh` and pgTAP tests before committing.
