#############################################
# LAMBDA FUNCTIONS for 50 Book Agents System
#############################################

# -------------------------------------------------------------------
# 1) Main Book Agents Scraper (runs all 50 agents’ Google search)
# -------------------------------------------------------------------
resource "aws_lambda_function" "book_agents_scraper" {
  function_name = "book-agents-scraper"
  role          = aws_iam_role.lambda_exec.arn
  handler       = "lambda.book_scraper_handler"
  runtime       = "python3.12"

  filename         = "lambda.zip"
  source_code_hash = filebase64sha256("lambda.zip")

  timeout     = 900
  memory_size = 512

  environment {
    variables = {
      # DynamoDB
      TABLE_NAME = "book-leads-v1"

      # Google Custom Search
      GOOGLE_API_KEY = var.google_api_key
      GOOGLE_CX      = var.google_cx

      # SES / email basics (not really used by scraper, but harmless)
      FROM_EMAIL   = "tawanmaurice@gmail.com"
      REPORT_EMAIL = "tawanmaurice@gmail.com"

      # Campaign label (optional, matches lambda.py default)
      CAMPAIGN_LABEL = "BookAgents50"

      # OpenAI key (Terraform still asks for it; lambda.py can ignore it)
      OPENAI_API_KEY = var.openai_api_key
    }
  }

  tags = {
    Owner = "Tawan"
  }
}

# -------------------------------------------------------------------
# 2) Daily Outreach Lambda (emails decision makers with PDF link)
#     NOTE: name is book_daily_outreach to match cloudwatch.tf
# -------------------------------------------------------------------
resource "aws_lambda_function" "book_daily_outreach" {
  function_name = "book-daily-outreach"
  role          = aws_iam_role.lambda_exec.arn
  handler       = "lambda.book_daily_outreach_handler"
  runtime       = "python3.12"

  filename         = "lambda.zip"
  source_code_hash = filebase64sha256("lambda.zip")

  timeout     = 900
  memory_size = 512

  environment {
    variables = {
      TABLE_NAME = "book-leads-v1"

      # SES config
      FROM_EMAIL   = "tawanmaurice@gmail.com"
      REPORT_EMAIL = "tawanmaurice@gmail.com"
      SES_REGION   = "us-east-1"

      # Outreach behavior
      TEST_MODE           = "false"         # set "true" if you only want test emails
      GO_LIVE_DATE        = "2026-01-06"    # matches lambda.py default
      DAILY_TOTAL_LIMIT   = "50"
      MAX_PER_DOMAIN_PER_DAY = "3"
      ONLY_EDU_EMAILS     = "false"         # set "true" if you want .edu only

      # PDF download link (change this when you have the real URL)
      PDF_URL = "https://YOURDOMAIN.com/path/to/5-student-success-shifts.pdf"

      CAMPAIGN_LABEL = "BookAgents50"
    }
  }

  tags = {
    Owner = "Tawan"
  }
}

# -------------------------------------------------------------------
# 3) Weekly/Monthly Reply Stats Report Lambda
# -------------------------------------------------------------------
resource "aws_lambda_function" "book_reply_stats_report" {
  function_name = "book-reply-stats-report"
  role          = aws_iam_role.lambda_exec.arn
  handler       = "lambda.book_reply_stats_report_handler"
  runtime       = "python3.12"

  filename         = "lambda.zip"
  source_code_hash = filebase64sha256("lambda.zip")

  timeout     = 900
  memory_size = 512

  environment {
    variables = {
      TABLE_NAME = "book-leads-v1"

      FROM_EMAIL   = "tawanmaurice@gmail.com"
      REPORT_EMAIL = "tawanmaurice@gmail.com"
      SES_REGION   = "us-east-1"

      CAMPAIGN_LABEL    = "BookAgents50"
      REPLY_REPORT_PERIOD = "weekly"   # or "monthly"
    }
  }

  tags = {
    Owner = "Tawan"
  }
}
