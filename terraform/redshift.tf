resource "random_password" "password" {
    length = 16
    special = true
    override_special = "[#$%&*()-_=+[0o:?"
}

resource "random_string" "unique_suffix" {
    length = 6
    special = false
}


# Resources

resource "aws_redshift_cluster" "redshift_cluster" {
    cluster_identifier = "forecast-redshift-cluster"
    database_name = "mydb"
    master_username = "admin"
    master_password= random_password.password.result
    node_type = "dc2.large"
    cluster_type = "single-node"
    
    skip_final_snapshot = true
}

resource "aws_secretsmanager_secret" "redshift_connection" {
description = "Redshift connect details"
name
= "redshiff[secret_s(random_string.unique_suffix.result}"
}
resource "aws_secretsmanager _secret_version" "redshift_connection" {
secret_id = aws_secretsmanager_secret.redshift_connection.id
secret_string = jsonencode(K
username
= aws_redshift_cluster.redshift_cluster.master_username
password
= aws_redshift_cluster.redshift_cluster.master_password
engine
= "redshift"
host
= aws_redshift_cluster.redshift_cluster.endpoint
port
= "5439"
dblusterIdentifier = aws_redshift_cluster.redshift_cluster.cluster_identifier
})
}