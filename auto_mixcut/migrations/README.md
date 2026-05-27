# Migrations

`001_sqlite_init.sql` is the executable local/dev schema used by tests.

`001_mysql_init.sql` is currently a production placeholder showing where the RDS migration should live. The column set mirrors the local schema; before applying to MySQL, translate `TEXT` JSON columns to native `JSON`, `INTEGER PRIMARY KEY AUTOINCREMENT` to `BIGINT PRIMARY KEY AUTO_INCREMENT`, and boolean integers to `BOOLEAN`.
