# resource "aws_iam_user" "airflow_user" {
#   name = "ayodeji_airflow"

#   tags = {
#     service     = "airflow"
#     environment = "dev"
#   }
# }

# resource "aws_iam_access_key" "airflow_credentials" {
#   user = aws_iam_user.airflow_user.name
# }

# resource "aws_ssm_parameter" "airflow_access_key" {
#   name  = "/dev/airflow/ayodeji_airflow_user_access_key"
#   type  = "String"
#   value = aws_iam_access_key.airflow_credentials.id
# }

# resource "aws_ssm_parameter" "airflow_secret_key" {
#   name  = "/dev/airflow/ayodeji_airflow_user_secret_key"
#   type  = "String"
#   value = aws_iam_access_key.airflow_credentials.secret
# }

# resource "aws_iam_policy" "airflow_policy" {
#   name        = "airflow-policy"
#   description = "Dedicated policy for airflow instance "

#   policy = jsonencode({
#     Version = "2012-10-17"
#     Statement = [
#       {
#         Effect = "Allow",
#         Action = [
#           "s3:ListBucket",
#           "s3:*Object*",
#         ]
#         Resource = [
#           "arn:aws:s3:::fmcg-de-assessment*",
#         ]
#       },
#     ]
#   })
# }

# resource "aws_iam_user_policy_attachment" "test-attach" {
#   user       = aws_iam_user.airflow_user.name
#   policy_arn = aws_iam_policy.airflow_policy.arn
# }
