#############################################
# DynamoDB table for Book Agents
#############################################

resource "aws_dynamodb_table" "book_leads" {
  name         = "book-leads-v1"
  billing_mode = "PAY_PER_REQUEST"

  hash_key = "id"

  attribute {
    name = "id"
    type = "S"
  }

  tags = {
    Project     = "book-agents-50"
    Environment = "prod"
    Owner       = "Tawan"
  }
}
