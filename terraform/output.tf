output "external_url_address_clickhouse" {
  value = "https://c-${yandex_mdb_clickhouse_cluster.mch-cluster.id}.rw.mdb.yandexcloud.net:8443/"
}