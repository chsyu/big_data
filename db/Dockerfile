# 使用官方 PostgreSQL 基礎映像
FROM postgres:14

# 複製自定義的 postgresql.conf
COPY postgresql.conf /usr/share/postgresql/postgresql.conf

# 設定環境變數以指定自定義配置文件
ENV POSTGRESQL_CONF_DIR=/usr/share/postgresql
ENV PGDATA=/var/lib/postgresql/data

# 告訴 PostgreSQL 使用自定義的配置文件
CMD ["postgres", "-c", "config_file=/usr/share/postgresql/postgresql.conf"]