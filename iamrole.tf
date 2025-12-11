#############################################
# IAM role + policies for Book Agents
#############################################

# Execution role for all book-related Lambda functions
resource "aws_iam_role" "lambda_exec" {
  name = "book-agents-lambda-role-v1"

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
  name = "book-agents-lambda-app-policy-v1"
  role = aws_iam_role.lambda_exec.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      # DynamoDB access for book-leads-v1
      {
        Effect = "Allow"
        Action = [
          "dynamodb:PutItem",
          "dynamodb:UpdateItem",
          "dynamodb:GetItem",
          "dynamodb:Scan",
          "dynamodb:Query"
        ]
        Resource = aws_dynamodb_table.book_leads.arn
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
