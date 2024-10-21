# 基於官方的 PostgreSQL 14 映像
FROM postgres:14

# 複製自定義的 postgresql.conf 到容器內的指定位置
COPY postgresql.conf /etc/postgresql/postgresql.conf

# 設定環境變數以指定 PGDATA 和使用的配置文件路徑
ENV PGDATA=/var/lib/postgresql/data

# 使用自定義的 postgresql.conf 啟動 PostgreSQL
CMD ["postgres", "-c", "config_file=/etc/postgresql/postgresql.conf"]