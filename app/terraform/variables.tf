variable "credentials" {
  description = "My Credentials"
  default     = "/creds/keys.json"
}

variable "project" {
  description = "Project"
  default     = "fhir-omop"
}

variable "region" {
  description = "Region"
  default     = "us-central1"
}

variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "fhir_omop_dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "fhir_omop_data_lake"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}