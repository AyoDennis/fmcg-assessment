terraform {
  backend "s3" {
    bucket = "fmcg-de-assessment"
    key    = "key/terraform.tfstate"
    region = "eu-central-1"
    profile = "mayor"
  }
}
