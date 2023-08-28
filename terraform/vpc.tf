resource "yandex_vpc_network" "dataproc-ch-network" {
  description = "Network for Data Proc and Managed Service for ClickHouse"
  name        = "dataproc_ch_network"
}

# # NAT gateway for Data Proc
resource "yandex_vpc_gateway" "dataproc-nat" {
  name = "dataproc-nat"
  shared_egress_gateway {}
}

# # Route table for Data Proc
resource "yandex_vpc_route_table" "dataproc-rt" {
  network_id = yandex_vpc_network.dataproc-ch-network.id

  static_route {
    destination_prefix = "0.0.0.0/0"
    gateway_id         = yandex_vpc_gateway.dataproc-nat.id
  }
}

resource "yandex_vpc_subnet" "dataproc-ch-subnet-a" {
  description    = "Subnet ru-central1-a availability zone for Data Proc and Managed Service for ClickHouse"
  name           = "dataproc_ch_subnet_a"
  zone           = "ru-central1-a"
  network_id     = yandex_vpc_network.dataproc-ch-network.id
  v4_cidr_blocks = ["10.140.0.0/24"]
  route_table_id = yandex_vpc_route_table.dataproc-rt.id
}