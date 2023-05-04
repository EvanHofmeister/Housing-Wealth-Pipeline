locals {
  data_lake_bucket = "housing_dl"
}

variable "project" {
  default = "housing-wealth"
}

variable "region" {
  default = "us-central1"
  type = string
}

variable "storage_class" {
  default = "STANDARD"
}

variable "BQ_DATASET" {
  type = string
  default = "housing_bq"
}
