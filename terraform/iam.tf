
resource "yandex_iam_service_account" "dataproc-sa" {
  description = "Service account to manage the Data Proc cluster"
  name        = "dataproc-sa"
}

# Assign the `dataproc.agent` role to the Data Proc service account
resource "yandex_resourcemanager_folder_iam_binding" "dataproc-agent" {
  folder_id = var.folder_id
  role      = "dataproc.agent"
  members   = ["serviceAccount:${yandex_iam_service_account.dataproc-sa.id}"]
}

# Yandex Object Storage bucket

# Create a service account for Object Storage creation
resource "yandex_iam_service_account" "sa-for-obj-storage" {
  folder_id = var.folder_id
  name      = "sa-for-obj-storage"
}

# Grant the service account storage.admin role to create storages and grant bucket ACLs
resource "yandex_resourcemanager_folder_iam_binding" "s3-editor" {
  folder_id = var.folder_id
  role      = "storage.admin"
  members   = ["serviceAccount:${yandex_iam_service_account.sa-for-obj-storage.id}"]
}

resource "yandex_iam_service_account_static_access_key" "sa-static-key" {
  description        = "Static access key for Object Storage"
  service_account_id = yandex_iam_service_account.sa-for-obj-storage.id
}