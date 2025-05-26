# Create VPC named "fmcg_vpc"
resource "aws_vpc" "fmcg_vpc" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = {
    Name = "fmcg_vpc"
  }
}

# Create public subnet
resource "aws_subnet" "public_subnet" {
  vpc_id                  = aws_vpc.fmcg_vpc.id
  cidr_block              = "10.0.1.0/24"
  availability_zone       = "eu-central-1a"
  map_public_ip_on_launch = true

  tags = {
    Name = "public_subnet"
  }
}

# Create private subnet
resource "aws_subnet" "private_subnet" {
  vpc_id            = aws_vpc.fmcg_vpc.id
  cidr_block        = "10.0.2.0/24"
  availability_zone = "eu-central-1b"

  tags = {
    Name = "private_subnet"
  }
}

# Create Internet Gateway for public access
resource "aws_internet_gateway" "fmcg_igw" {
  vpc_id = aws_vpc.fmcg_vpc.id

  tags = {
    Name = "fmcg_igw"
  }
}

# Route Table for public subnet
resource "aws_route_table" "public_route_table" {
  vpc_id = aws_vpc.fmcg_vpc.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.fmcg_igw.id
  }

  tags = {
    Name = "public_route_table"
  }
}

# Associate the public subnet with the public route table
resource "aws_route_table_association" "public_subnet_association" {
  subnet_id      = aws_subnet.public_subnet.id
  route_table_id = aws_route_table.public_route_table.id
}

# Redshift subnet group with both subnets
resource "aws_redshift_subnet_group" "redshift_subnet_group" {
  name = "redshift-subnet-group"
  subnet_ids = [
    aws_subnet.public_subnet.id,
    aws_subnet.private_subnet.id
  ]
  description = "Subnet group for Redshift cluster"
}

# Security group for Redshift
resource "aws_security_group" "redshift_sg" {
  name        = "redshift-sg"
  description = "Allow Redshift access"
  vpc_id      = aws_vpc.fmcg_vpc.id

  ingress {
    from_port   = 5439
    to_port     = 5439
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"] # ⚠️ Insecure — restrict in production!
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "redshift_sg"
  }
}

# Random password for Redshift
resource "random_password" "password" {
  length           = 16
  special          = true
  override_special = "[#$%&*()-_=+[03o:?"
}

# Random suffix for secret name
resource "random_string" "unique_suffix" {
  length  = 6
  special = false
}

# Redshift cluster
resource "aws_redshift_cluster" "redshift_cluster" {
  cluster_identifier        = "forecast-redshift-cluster"
  database_name             = "fmcg_db"
  master_username           = "admin"
  master_password           = random_password.password.result
  node_type                 = "dc2.large"
  cluster_type              = "single-node"
  port                      = 5439
  skip_final_snapshot       = true

  vpc_security_group_ids    = [aws_security_group.redshift_sg.id]
  cluster_subnet_group_name = aws_redshift_subnet_group.redshift_subnet_group.name
}

# Secrets Manager secret to store Redshift credentials
resource "aws_secretsmanager_secret" "redshift_connection" {
  description = "fmcg Redshift connect details"
  name        = "redshift_secret_${random_string.unique_suffix.result}"
}

# Store Redshift connection string in Secrets Manager
resource "aws_secretsmanager_secret_version" "redshift_connection" {
  secret_id = aws_secretsmanager_secret.redshift_connection.id

  secret_string = jsonencode({
    username            = aws_redshift_cluster.redshift_cluster.master_username
    password            = aws_redshift_cluster.redshift_cluster.master_password
    engine              = "redshift"
    host                = aws_redshift_cluster.redshift_cluster.endpoint
    port                = 5439
    dbClusterIdentifier = aws_redshift_cluster.redshift_cluster.cluster_identifier
  })
}
