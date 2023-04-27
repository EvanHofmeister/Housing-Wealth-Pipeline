locals {
  data_lake_bucket = "avm-staging-bucket"
}

variable "project" {
  default = "<ENTER PROJECT NAME>"
}

variable "region" {
  default = "europe-west3"
  type = string
}

variable "storage_class" {
  default = "STANDARD"
}

variable "BQ_DATASET" {
  type = string
  default = "avm_data"
}
