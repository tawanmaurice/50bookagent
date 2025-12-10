#############################################
# IAM role + policies for all 50 agents
#############################################

# Execution role for all Lambda functions
resource "aws_iam_role" "lambda_exec" {
  name = "speaking-agent-lambda-role-v3-multi"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

# Basic Lambda logging to CloudWatch
resource "aws_iam_role_policy_attachment" "lambda_basic_logs" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# App-specific permissions: DynamoDB + SES
resource "aws_iam_role_policy" "lambda_dynamodb_policy" {
  name = "speaking-agent-lambda-app-policy-v3-multi"
  role = aws_iam_role.lambda_exec.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      # DynamoDB access for speaking-leads-v3-multi
      {
        Effect = "Allow"
        Action = [
          "dynamodb:PutItem",
          "dynamodb:UpdateItem",
          "dynamodb:GetItem",
          "dynamodb:Scan",
          "dynamodb:Query"
        ]
        Resource = "arn:aws:dynamodb:us-east-1:276671279137:table/speaking-leads-v3-multi"
      },

      # SES send permissions for your verified identity
      {
        Effect = "Allow"
        Action = [
          "ses:SendEmail",
          "ses:SendRawEmail"
        ]
        Resource = "arn:aws:ses:us-east-1:276671279137:identity/tawanmaurice@gmail.com"
      }
    ]
  })
}
