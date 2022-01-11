terraform {
  backend "s3" {
  }
}

data "terraform_remote_state" "vpc" {
  backend = "s3"
  config = {
    bucket = var.remote_state_bucket
    key    = "${var.base_stack}/terraform.tfstate"
  }
}

provider "aws" {
  region = var.aws_deploy_region
  assume_role {
    role_arn = var.aws_deploy_role_arn
  }
}