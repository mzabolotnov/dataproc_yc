# Infrastructure for the Yandex Cloud Managed Service for ClickHouse, Data Proc, and Object Storage
#
# RU: https://cloud.yandex.ru/docs/data-proc/tutorials/s3-dataproc-ch
# EN: https://cloud.yandex.com/en/docs/data-proc/tutorials/s3-dataproc-ch
#
# Set the configuration of the Managed Service for ClickHouse cluster, Data Proc cluster, and Object Storage

resource "yandex_dataproc_cluster" "dataproc-cluster" {
  description        = "Data Proc cluster"
  depends_on         = [yandex_resourcemanager_folder_iam_binding.dataproc-agent]
  bucket             = yandex_storage_bucket.output-bucket.id
  name               = "dataproc-cluster"
  service_account_id = yandex_iam_service_account.dataproc-sa.id
  zone_id            = "ru-central1-a"
  ui_proxy           = true
  security_group_ids = ["${yandex_vpc_security_group.dataproc-security-group.id}"]

  cluster_config {
    version_id = "2.0"

    hadoop {
      services        = ["HDFS", "SPARK", "YARN"]
      ssh_public_keys = [file(var.public_key_path)]
    }

    subcluster_spec {
      name = "main"
      role = "MASTERNODE"
      resources {
        resource_preset_id = "s2.micro" # 2 vCPU, 8 GB RAM
        disk_type_id       = "network-hdd"
        disk_size          = 20 # GB
      }
      subnet_id   = yandex_vpc_subnet.dataproc-ch-subnet-a.id
      hosts_count = 1
      assign_public_ip = true
    }

    subcluster_spec {
      name = "data"
      role = "DATANODE"
      resources {
        resource_preset_id = "s2.micro" # 2 vCPU, 8 GB RAM
        disk_type_id       = "network-hdd"
        disk_size          = 20 # GB
      }
      subnet_id   = yandex_vpc_subnet.dataproc-ch-subnet-a.id
      hosts_count = 1
    }
  }
}

