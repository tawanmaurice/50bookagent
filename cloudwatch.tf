############################################
# CloudWatch Schedules – Book Agents Project
############################################

# Assumes you have these Lambda functions defined in lambda.tf:
#
# resource "aws_lambda_function" "book_agents_scraper" { ... }
# resource "aws_lambda_function" "book_daily_outreach" { ... }
# resource "aws_lambda_function" "book_reply_stats_report" { ... }
#
# If your names differ, update the references below.

############################################
# 1) DAILY SCRAPER – run ALL 50 book agents
############################################

# Runs once per day at 13:00 UTC
#   - 08:00 AM Eastern (standard time)
#   - 09:00 AM Eastern (during daylight savings)
#
# book_scraper_handler(event, context) will see NO agent_name
# and will automatically loop through every entry in AGENTS.
resource "aws_cloudwatch_event_rule" "book_agents_scrape_daily" {
  name                = "book-agents-scrape-daily"
  description         = "Run all 50 book scraper agents once per day"
  schedule_expression = "cron(0 13 * * ? *)"
}

resource "aws_cloudwatch_event_target" "book_agents_scrape_target" {
  rule      = aws_cloudwatch_event_rule.book_agents_scrape_daily.name
  target_id = "book-agents-scraper-lambda"
  arn       = aws_lambda_function.book_agents_scraper.arn

  # No input -> handler runs ALL agents
}

resource "aws_lambda_permission" "book_agents_scrape_permission" {
  statement_id  = "AllowExecutionFromCloudWatchBookAgentsScrape"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.book_agents_scraper.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.book_agents_scrape_daily.arn
}

###################################################
# 2) DAILY OUTREACH – 3-step email sequence runner
###################################################

# Trigger once per day at 13:30 UTC.
# The Lambda itself enforces:
#   - Weekdays only
#   - Skip US federal holidays
#   - Daily caps, per-domain caps, GO_LIVE_DATE, etc.
resource "aws_cloudwatch_event_rule" "book_daily_outreach" {
  name                = "book-daily-outreach"
  description         = "Run daily book outreach email sequence"
  schedule_expression = "cron(30 13 * * ? *)"
}

resource "aws_cloudwatch_event_target" "book_daily_outreach_target" {
  rule      = aws_cloudwatch_event_rule.book_daily_outreach.name
  target_id = "book-daily-outreach-lambda"
  arn       = aws_lambda_function.book_daily_outreach.arn

  # No special input needed; handler uses DynamoDB data + GO_LIVE_DATE
}

resource "aws_lambda_permission" "book_daily_outreach_permission" {
  statement_id  = "AllowExecutionFromCloudWatchBookDailyOutreach"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.book_daily_outreach.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.book_daily_outreach.arn
}

###########################################################
# 3) REPLY STATS REPORT – weekly (or monthly) summary email
###########################################################

# Trigger once per week – Monday at 14:00 UTC.
# The Lambda itself decides if it is "weekly" vs "monthly"
# based on REPLY_REPORT_PERIOD env var.
resource "aws_cloudwatch_event_rule" "book_reply_stats_weekly" {
  name                = "book-reply-stats-weekly"
  description         = "Generate weekly/monthly reply stats report for book outreach"
  # cron(Minutes Hours Day-of-month Month Day-of-week Year)
  # Here: 0 14 ? * MON *  -> Every Monday at 14:00 UTC
  schedule_expression = "cron(0 14 ? * MON *)"
}

resource "aws_cloudwatch_event_target" "book_reply_stats_weekly_target" {
  rule      = aws_cloudwatch_event_rule.book_reply_stats_weekly.name
  target_id = "book-reply-stats-lambda"
  arn       = aws_lambda_function.book_reply_stats_report.arn
}

resource "aws_lambda_permission" "book_reply_stats_weekly_permission" {
  statement_id  = "AllowExecutionFromCloudWatchBookReplyStatsWeekly"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.book_reply_stats_report.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.book_reply_stats_weekly.arn
}
