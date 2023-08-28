resource "yandex_mdb_clickhouse_cluster" "mch-cluster" {
  description        = "Managed Service for ClickHouse cluster"
  name               = "mch-cluster"
  environment        = "PRODUCTION"
  network_id         = yandex_vpc_network.dataproc-ch-network.id
  security_group_ids = [yandex_vpc_security_group.mch_security_group.id]

  clickhouse {
    resources {
      resource_preset_id = "s2.micro" # 2 vCPU, 8 GB RAM
      disk_type_id       = "network-ssd"
      disk_size          = 10 # GB
    }
  }

  host {
    type             = "CLICKHOUSE"
    zone             = "ru-central1-a"
    subnet_id        = yandex_vpc_subnet.dataproc-ch-subnet-a.id
    assign_public_ip = true # Required for connection from the Internet
  }

  database {
    name = "db1"
  }

  user {
    name     = "user1"
    password = var.pass_clickhouse
    permission {
      database_name = "db1"
    }
  }
}