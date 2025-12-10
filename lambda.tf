#############################################
# Lambda functions for multi-agent scraper  #
# (Only Lambda resources – no terraform{},  #
#  no provider, no variables, no DynamoDB,  #
#  no IAM role definitions.)               #
#############################################

# Uses variables defined in variables.tf and IAM role in iamrole.tf

locals {
  common_env = {
    # ----- Existing Google Custom Search config -----
    GOOGLE_API_KEY = var.google_api_key
    GOOGLE_CX      = var.google_cx

    # ----- SES / email settings -----
    # Region where SES is configured (must match where your verified email lives)
    SES_REGION = "us-east-1"

    # The email address you send *from* (must be SES-verified in SES)
    FROM_EMAIL           = "tawanmaurice@gmail.com"

    # Where daily/weekly reports get sent (can be same as FROM_EMAIL)
    REPORT_EMAIL         = "tawanmaurice@gmail.com"

    # Test mode:
    # - "true"  => only sends ONE test email to TEST_RECIPIENT_EMAIL
    # - "false" => sends real outreach emails to leads (after GO_LIVE_DATE)
    TEST_MODE            = "true"
    TEST_RECIPIENT_EMAIL = "tawanmaurice@gmail.com"

    # ----- Outreach behavior controls -----

    # Do NOT send real outreach emails before this date (Eastern time)
    GO_LIVE_DATE = "2026-01-06"

    # Max total outreach emails per day across all agents
    DAILY_TOTAL_LIMIT = "50"

    # Max outreach emails per day to the SAME domain (e.g. mycollege.edu)
    MAX_PER_DOMAIN_PER_DAY = "3"

    # Total emails in the sequence per contact (1 initial + follow-ups)
    MAX_SEQUENCE_STEPS = "5"

    # Days after initial email to send the first follow-up
    INITIAL_FOLLOWUP_DAYS = "4"

    # Days between subsequent follow-ups (email #2 → #3 → #4 → #5)
    FOLLOWUP_INTERVAL_DAYS = "7"

    # Only send to .edu addresses if "true"
    ONLY_EDU_EMAILS = "true"

    # How often you get the reply / stats report: "daily" or "weekly"
    REPLY_REPORT_PERIOD = "weekly"
  }
}

#############################################
# Student Athlete Leadership Agent
#############################################
resource "aws_lambda_function" "student_athlete_leadership_agent" {
  function_name = "student-athlete-leadership-agent"
  handler       = "lambda.student_athlete_handler"
  runtime       = "python3.12"

  filename         = "lambda.zip"
  source_code_hash = filebase64sha256("lambda.zip")

  role        = aws_iam_role.lambda_exec.arn
  timeout     = 900
  memory_size = 512

  environment {
    variables = local.common_env
  }
}

#############################################
# Men of Color Initiative Agent
#############################################
resource "aws_lambda_function" "men_of_color_initiative_agent" {
  function_name = "men-of-color-initiative-agent"
  handler       = "lambda.men_of_color_handler"
  runtime       = "python3.12"

  filename         = "lambda.zip"
  source_code_hash = filebase64sha256("lambda.zip")

  role        = aws_iam_role.lambda_exec.arn
  timeout     = 900
  memory_size = 512

  environment {
    variables = local.common_env
  }
}

#############################################
# First-Gen Student Success Agent
#############################################
resource "aws_lambda_function" "first_gen_student_success_agent" {
  function_name = "first-gen-student-success-agent"
  handler       = "lambda.first_gen_handler"
  runtime       = "python3.12"

  filename         = "lambda.zip"
  source_code_hash = filebase64sha256("lambda.zip")

  role        = aws_iam_role.lambda_exec.arn
  timeout     = 900
  memory_size = 512

  environment {
    variables = local.common_env
  }
}

#############################################
# Multicultural Center Leadership Agent
#############################################
resource "aws_lambda_function" "multicultural_center_leadership_agent" {
  function_name = "multicultural-center-leadership-agent"
  handler       = "lambda.multicultural_center_handler"
  runtime       = "python3.12"

  filename         = "lambda.zip"
  source_code_hash = filebase64sha256("lambda.zip")

  role        = aws_iam_role.lambda_exec.arn
  timeout     = 900
  memory_size = 512

  environment {
    variables = local.common_env
  }
}

#############################################
# Service Learning / Civic Engagement Agent
#############################################
resource "aws_lambda_function" "service_learning_civic_engagement_agent" {
  function_name = "service-learning-civic-engagement-agent"
  handler       = "lambda.service_learning_handler"
  runtime       = "python3.12"

  filename         = "lambda.zip"
  source_code_hash = filebase64sha256("lambda.zip")

  role        = aws_iam_role.lambda_exec.arn
  timeout     = 900
  memory_size = 512

  environment {
    variables = local.common_env
  }
}

#############################################
# High School Student Council Leadership Agent
#############################################
resource "aws_lambda_function" "hs_student_council_leadership_agent" {
  function_name = "hs-student-council-leadership-agent"
  handler       = "lambda.hs_student_council_handler"
  runtime       = "python3.12"

  filename         = "lambda.zip"
  source_code_hash = filebase64sha256("lambda.zip")

  role        = aws_iam_role.lambda_exec.arn
  timeout     = 900
  memory_size = 512

  environment {
    variables = local.common_env
  }
}

#############################################
# Summer Bridge / Orientation Agent
#############################################
resource "aws_lambda_function" "summer_bridge_orientation_agent" {
  function_name = "summer-bridge-orientation-agent"
  handler       = "lambda.summer_bridge_handler"
  runtime       = "python3.12"

  filename         = "lambda.zip"
  source_code_hash = filebase64sha256("lambda.zip")

  role        = aws_iam_role.lambda_exec.arn
  timeout     = 900
  memory_size = 512

  environment {
    variables = local.common_env
  }
}

#####################################################
# Student Leadership / Community College Agents
#####################################################

#############################################
# SGA Leadership Agent
#############################################
resource "aws_lambda_function" "sga_leadership_agent" {
  function_name = "sga-leadership-agent"
  handler       = "lambda.sga_leadership_handler"
  runtime       = "python3.12"

  filename         = "lambda.zip"
  source_code_hash = filebase64sha256("lambda.zip")

  role        = aws_iam_role.lambda_exec.arn
  timeout     = 900
  memory_size = 512

  environment {
    variables = local.common_env
  }
}

#############################################
# Student Leadership Retreat Agent
#############################################
resource "aws_lambda_function" "student_leadership_retreat_agent" {
  function_name = "student-leadership-retreat-agent"
  handler       = "lambda.student_leadership_retreat_handler"
  runtime       = "python3.12"

  filename         = "lambda.zip"
  source_code_hash = filebase64sha256("lambda.zip")

  role        = aws_iam_role.lambda_exec.arn
  timeout     = 900
  memory_size = 512

  environment {
    variables = local.common_env
  }
}

#############################################
# Student Leadership Conference Agent
#############################################
resource "aws_lambda_function" "student_leadership_conference_agent" {
  function_name = "student-leadership-conference-agent"
  handler       = "lambda.student_leadership_conference_handler"
  runtime       = "python3.12"

  filename         = "lambda.zip"
  source_code_hash = filebase64sha256("lambda.zip")

  role        = aws_iam_role.lambda_exec.arn
  timeout     = 900
  memory_size = 512

  environment {
    variables = local.common_env
  }
}

#############################################
# Leadership Summit Agent
#############################################
resource "aws_lambda_function" "leadership_summit_agent" {
  function_name = "leadership-summit-agent"
  handler       = "lambda.leadership_summit_handler"
  runtime       = "python3.12"

  filename         = "lambda.zip"
  source_code_hash = filebase64sha256("lambda.zip")

  role        = aws_iam_role.lambda_exec.arn
  timeout     = 900
  memory_size = 512

  environment {
    variables = local.common_env
  }
}

#############################################
# Officer Training Agent
#############################################
resource "aws_lambda_function" "officer_training_agent" {
  function_name = "officer-training-agent"
  handler       = "lambda.officer_training_handler"
  runtime       = "python3.12"

  filename         = "lambda.zip"
  source_code_hash = filebase64sha256("lambda.zip")

  role        = aws_iam_role.lambda_exec.arn
  timeout     = 900
  memory_size = 512

  environment {
    variables = local.common_env
  }
}

#############################################
# Speaker Series / Lyceum Agent
#############################################
resource "aws_lambda_function" "speaker_series_lyceum_agent" {
  function_name = "speaker-series-lyceum-agent"
  handler       = "lambda.speaker_series_lyceum_handler"
  runtime       = "python3.12"

  filename         = "lambda.zip"
  source_code_hash = filebase64sha256("lambda.zip")

  role        = aws_iam_role.lambda_exec.arn
  timeout     = 900
  memory_size = 512

  environment {
    variables = local.common_env
  }
}

#############################################
# Orientation Leader Agent
#############################################
resource "aws_lambda_function" "orientation_leader_agent" {
  function_name = "orientation-leader-agent"
  handler       = "lambda.orientation_leader_handler"
  runtime       = "python3.12"

  filename         = "lambda.zip"
  source_code_hash = filebase64sha256("lambda.zip")

  role        = aws_iam_role.lambda_exec.arn
  timeout     = 900
  memory_size = 512

  environment {
    variables = local.common_env
  }
}

#############################################
# Res Life / RA Leadership Agent (broad)
#############################################
resource "aws_lambda_function" "res_life_ra_leadership_agent" {
  function_name = "res-life-ra-leadership-agent"
  handler       = "lambda.res_life_ra_leadership_handler"
  runtime       = "python3.12"

  filename         = "lambda.zip"
  source_code_hash = filebase64sha256("lambda.zip")

  role        = aws_iam_role.lambda_exec.arn
  timeout     = 900
  memory_size = 512

  environment {
    variables = local.common_env
  }
}

#####################################################
# RA / Ambassador / Sophomore focused
#####################################################

#############################################
# Resident Assistant Leadership Agent
#############################################
resource "aws_lambda_function" "resident_assistant_leadership_agent" {
  function_name = "resident-assistant-leadership-agent"
  handler       = "lambda.resident_assistant_leadership_handler"
  runtime       = "python3.12"

  filename         = "lambda.zip"
  source_code_hash = filebase64sha256("lambda.zip")

  role        = aws_iam_role.lambda_exec.arn
  timeout     = 900
  memory_size = 512

  environment {
    variables = local.common_env
  }
}

#############################################
# Campus Ambassador Events Agent
#############################################
resource "aws_lambda_function" "campus_ambassador_events_agent" {
  function_name = "campus-ambassador-events-agent"
  handler       = "lambda.campus_ambassador_events_handler"
  runtime       = "python3.12"

  filename         = "lambda.zip"
  source_code_hash = filebase64sha256("lambda.zip")

  role        = aws_iam_role.lambda_exec.arn
  timeout     = 900
  memory_size = 512

  environment {
    variables = local.common_env
  }
}

#############################################
# Sophomore Leadership Agent
#############################################
resource "aws_lambda_function" "sophomore_leadership_agent" {
  function_name = "sophomore-leadership-agent"
  handler       = "lambda.sophomore_leadership_handler"
  runtime       = "python3.12"

  filename         = "lambda.zip"
  source_code_hash = filebase64sha256("lambda.zip")

  role        = aws_iam_role.lambda_exec.arn
  timeout     = 900
  memory_size = 512

  environment {
    variables = local.common_env
  }
}

#############################################
# Honors Program Leadership Agent
#############################################
resource "aws_lambda_function" "honors_program_leadership_agent" {
  function_name = "honors-program-leadership-agent"
  handler       = "lambda.honors_program_leadership_handler"
  runtime       = "python3.12"

  filename         = "lambda.zip"
  source_code_hash = filebase64sha256("lambda.zip")

  role        = aws_iam_role.lambda_exec.arn
  timeout     = 900
  memory_size = 512

  environment {
    variables = local.common_env
  }
}

#############################################
# Leadership Certificate Program Agent
#############################################
resource "aws_lambda_function" "leadership_certificate_program_agent" {
  function_name = "leadership-certificate-program-agent"
  handler       = "lambda.leadership_certificate_program_handler"
  runtime       = "python3.12"

  filename         = "lambda.zip"
  source_code_hash = filebase64sha256("lambda.zip")

  role        = aws_iam_role.lambda_exec.arn
  timeout     = 900
  memory_size = 512

  environment {
    variables = local.common_env
  }
}

#############################################
# Leadership Academy Agent
#############################################
resource "aws_lambda_function" "leadership_academy_agent" {
  function_name = "leadership-academy-agent"
  handler       = "lambda.leadership_academy_handler"
  runtime       = "python3.12"

  filename         = "lambda.zip"
  source_code_hash = filebase64sha256("lambda.zip")

  role        = aws_iam_role.lambda_exec.arn
  timeout     = 900
  memory_size = 512

  environment {
    variables = local.common_env
  }
}

#############################################
# Student Activities Leadership Agent
#############################################
resource "aws_lambda_function" "student_activities_leadership_agent" {
  function_name = "student-activities-leadership-agent"
  handler       = "lambda.student_activities_leadership_handler"
  runtime       = "python3.12"

  filename         = "lambda.zip"
  source_code_hash = filebase64sha256("lambda.zip")

  role        = aws_iam_role.lambda_exec.arn
  timeout     = 900
  memory_size = 512

  environment {
    variables = local.common_env
  }
}

#############################################
# College Success Leadership Agent
#############################################
resource "aws_lambda_function" "college_success_leadership_agent" {
  function_name = "college-success-leadership-agent"
  handler       = "lambda.college_success_leadership_handler"
  runtime       = "python3.12"

  filename         = "lambda.zip"
  source_code_hash = filebase64sha256("lambda.zip")

  role        = aws_iam_role.lambda_exec.arn
  timeout     = 900
  memory_size = 512

  environment {
    variables = local.common_env
  }
}

#############################################
# Career Success Leadership Agent
#############################################
resource "aws_lambda_function" "career_success_leadership_agent" {
  function_name = "career-success-leadership-agent"
  handler       = "lambda.career_success_leadership_handler"
  runtime       = "python3.12"

  filename         = "lambda.zip"
  source_code_hash = filebase64sha256("lambda.zip")

  role        = aws_iam_role.lambda_exec.arn
  timeout     = 900
  memory_size = 512

  environment {
    variables = local.common_env
  }
}

#############################################
# Social Justice / DEI Leadership Agent
#############################################
resource "aws_lambda_function" "social_justice_leadership_agent" {
  function_name = "social-justice-leadership-agent"
  handler       = "lambda.social_justice_leadership_handler"
  runtime       = "python3.12"

  filename         = "lambda.zip"
  source_code_hash = filebase64sha256("lambda.zip")

  role        = aws_iam_role.lambda_exec.arn
  timeout     = 900
  memory_size = 512

  environment {
    variables = local.common_env
  }
}

#############################################
# Community College Student Leadership Agent
#############################################
resource "aws_lambda_function" "cc_student_leadership_agent" {
  function_name = "cc-student-leadership-agent"
  handler       = "lambda.cc_student_leadership_handler"
  runtime       = "python3.12"

  filename         = "lambda.zip"
  source_code_hash = filebase64sha256("lambda.zip")

  role        = aws_iam_role.lambda_exec.arn
  timeout     = 900
  memory_size = 512

  environment {
    variables = local.common_env
  }
}

#############################################
# CC Success & Retention Agent
#############################################
resource "aws_lambda_function" "cc_success_and_retention_agent" {
  function_name = "cc-success-and-retention-agent"
  handler       = "lambda.cc_success_and_retention_handler"
  runtime       = "python3.12"

  filename         = "lambda.zip"
  source_code_hash = filebase64sha256("lambda.zip")

  role        = aws_iam_role.lambda_exec.arn
  timeout     = 900
  memory_size = 512

  environment {
    variables = local.common_env
  }
}

##########################################################
# NEW 10 AGENTS – HS / TRIO / Greek / Peer / Belonging
##########################################################

#############################################
# HS Student Leadership Conferences Agent
#############################################
resource "aws_lambda_function" "hs_student_leadership_conferences_agent" {
  function_name = "hs-student-leadership-conferences-agent"
  handler       = "lambda.hs_student_leadership_conferences_handler"
  runtime       = "python3.12"

  filename         = "lambda.zip"
  source_code_hash = filebase64sha256("lambda.zip")

  role        = aws_iam_role.lambda_exec.arn
  timeout     = 900
  memory_size = 512

  environment {
    variables = local.common_env
  }
}

#############################################
# HS Faculty / Staff Training Agent
#############################################
resource "aws_lambda_function" "hs_faculty_staff_training_agent" {
  function_name = "hs-faculty-staff-training-agent"
  handler       = "lambda.hs_faculty_staff_training_handler"
  runtime       = "python3.12"

  filename         = "lambda.zip"
  source_code_hash = filebase64sha256("lambda.zip")

  role        = aws_iam_role.lambda_exec.arn
  timeout     = 900
  memory_size = 512

  environment {
    variables = local.common_env
  }
}

#############################################
# Transfer Student Leadership Agent
#############################################
resource "aws_lambda_function" "transfer_student_leadership_agent" {
  function_name = "transfer-student-leadership-agent"
  handler       = "lambda.transfer_student_leadership_handler"
  runtime       = "python3.12"

  filename         = "lambda.zip"
  source_code_hash = filebase64sha256("lambda.zip")

  role        = aws_iam_role.lambda_exec.arn
  timeout     = 900
  memory_size = 512

  environment {
    variables = local.common_env
  }
}

#############################################
# TRIO Leadership Agent
#############################################
resource "aws_lambda_function" "trio_leadership_agent" {
  function_name = "trio-leadership-agent"
  handler       = "lambda.trio_leadership_handler"
  runtime       = "python3.12"

  filename         = "lambda.zip"
  source_code_hash = filebase64sha256("lambda.zip")

  role        = aws_iam_role.lambda_exec.arn
  timeout     = 900
  memory_size = 512

  environment {
    variables = local.common_env
  }
}

#############################################
# Greek Life Leadership Agent
#############################################
resource "aws_lambda_function" "greek_life_leadership_agent" {
  function_name = "greek-life-leadership-agent"
  handler       = "lambda.greek_life_leadership_handler"
  runtime       = "python3.12"

  filename         = "lambda.zip"
  source_code_hash = filebase64sha256("lambda.zip")

  role        = aws_iam_role.lambda_exec.arn
  timeout     = 900
  memory_size = 512

  environment {
    variables = local.common_env
  }
}

#############################################
# Peer Mentor Leadership Agent
#############################################
resource "aws_lambda_function" "peer_mentor_leadership_agent" {
  function_name = "peer-mentor-leadership-agent"
  handler       = "lambda.peer_mentor_leadership_handler"
  runtime       = "python3.12"

  filename         = "lambda.zip"
  source_code_hash = filebase64sha256("lambda.zip")

  role        = aws_iam_role.lambda_exec.arn
  timeout     = 900
  memory_size = 512

  environment {
    variables = local.common_env
  }
}

#############################################
# Women in Leadership Agent
#############################################
resource "aws_lambda_function" "women_in_leadership_agent" {
  function_name = "women-in-leadership-agent"
  handler       = "lambda.women_in_leadership_handler"
  runtime       = "python3.12"

  filename         = "lambda.zip"
  source_code_hash = filebase64sha256("lambda.zip")

  role        = aws_iam_role.lambda_exec.arn
  timeout     = 900
  memory_size = 512

  environment {
    variables = local.common_env
  }
}

#############################################
# Leadership Honor Society Agent
#############################################
resource "aws_lambda_function" "leadership_honor_society_agent" {
  function_name = "leadership-honor-society-agent"
  handler       = "lambda.leadership_honor_society_handler"
  runtime       = "python3.12"

  filename         = "lambda.zip"
  source_code_hash = filebase64sha256("lambda.zip")

  role        = aws_iam_role.lambda_exec.arn
  timeout     = 900
  memory_size = 512

  environment {
    variables = local.common_env
  }
}

#############################################
# Student Belonging Leadership Agent
#############################################
resource "aws_lambda_function" "student_belonging_leadership_agent" {
  function_name = "student-belonging-leadership-agent"
  handler       = "lambda.student_belonging_leadership_handler"
  runtime       = "python3.12"

  filename         = "lambda.zip"
  source_code_hash = filebase64sha256("lambda.zip")

  role        = aws_iam_role.lambda_exec.arn
  timeout     = 900
  memory_size = 512

  environment {
    variables = local.common_env
  }
}

#############################################
# School Improvement Leadership Agent
#############################################
resource "aws_lambda_function" "school_improvement_leadership_agent" {
  function_name = "school-improvement-leadership-agent"
  handler       = "lambda.school_improvement_leadership_handler"
  runtime       = "python3.12"

  filename         = "lambda.zip"
  source_code_hash = filebase64sha256("lambda.zip")

  role        = aws_iam_role.lambda_exec.arn
  timeout     = 900
  memory_size = 512

  environment {
    variables = local.common_env
  }
}

##########################################################
# NEW 13 AGENTS – program / event pages only
##########################################################

#############################################
# Emerging Leaders Program Agent
#############################################
resource "aws_lambda_function" "emerging_leaders_program_agent" {
  function_name = "emerging-leaders-program-agent"
  handler       = "lambda.emerging_leaders_program_handler"
  runtime       = "python3.12"

  filename         = "lambda.zip"
  source_code_hash = filebase64sha256("lambda.zip")

  role        = aws_iam_role.lambda_exec.arn
  timeout     = 900
  memory_size = 512

  environment {
    variables = local.common_env
  }
}

#############################################
# Leadership Capstone Agent
#############################################
resource "aws_lambda_function" "leadership_capstone_agent" {
  function_name = "leadership-capstone-agent"
  handler       = "lambda.leadership_capstone_handler"
  runtime       = "python3.12"

  filename         = "lambda.zip"
  source_code_hash = filebase64sha256("lambda.zip")

  role        = aws_iam_role.lambda_exec.arn
  timeout     = 900
  memory_size = 512

  environment {
    variables = local.common_env
  }
}

#############################################
# Student Leadership Grant Agent
#############################################
resource "aws_lambda_function" "student_leadership_grant_agent" {
  function_name = "student-leadership-grant-agent"
  handler       = "lambda.student_leadership_grant_handler"
  runtime       = "python3.12"

  filename         = "lambda.zip"
  source_code_hash = filebase64sha256("lambda.zip")

  role        = aws_iam_role.lambda_exec.arn
  timeout     = 900
  memory_size = 512

  environment {
    variables = local.common_env
  }
}

#############################################
# Intercultural Leadership Agent
#############################################
resource "aws_lambda_function" "intercultural_leadership_agent" {
  function_name = "intercultural-leadership-agent"
  handler       = "lambda.intercultural_leadership_handler"
  runtime       = "python3.12"

  filename         = "lambda.zip"
  source_code_hash = filebase64sha256("lambda.zip")

  role        = aws_iam_role.lambda_exec.arn
  timeout     = 900
  memory_size = 512

  environment {
    variables = local.common_env
  }
}

#############################################
# Leadership Conference RFP Agent
#############################################
resource "aws_lambda_function" "leadership_conference_rfp_agent" {
  function_name = "leadership-conference-rfp-agent"
  handler       = "lambda.leadership_conference_rfp_handler"
  runtime       = "python3.12"

  filename         = "lambda.zip"
  source_code_hash = filebase64sha256("lambda.zip")

  role        = aws_iam_role.lambda_exec.arn
  timeout     = 900
  memory_size = 512

  environment {
    variables = local.common_env
  }
}

#############################################
# Leadership Week Agent
#############################################
resource "aws_lambda_function" "leadership_week_agent" {
  function_name = "leadership-week-agent"
  handler       = "lambda.leadership_week_handler"
  runtime       = "python3.12"

  filename         = "lambda.zip"
  source_code_hash = filebase64sha256("lambda.zip")

  role        = aws_iam_role.lambda_exec.arn
  timeout     = 900
  memory_size = 512

  environment {
    variables = local.common_env
  }
}

#############################################
# Student Success Workshop Series Agent
#############################################
resource "aws_lambda_function" "student_success_workshop_series_agent" {
  function_name = "student-success-workshop-series-agent"
  handler       = "lambda.student_success_workshop_series_handler"
  runtime       = "python3.12"

  filename         = "lambda.zip"
  source_code_hash = filebase64sha256("lambda.zip")

  role        = aws_iam_role.lambda_exec.arn
  timeout     = 900
  memory_size = 512

  environment {
    variables = local.common_env
  }
}

#############################################
# Leadership Institute Agent
#############################################
resource "aws_lambda_function" "leadership_institute_agent" {
  function_name = "leadership-institute-agent"
  handler       = "lambda.leadership_institute_handler"
  runtime       = "python3.12"

  filename         = "lambda.zip"
  source_code_hash = filebase64sha256("lambda.zip")

  role        = aws_iam_role.lambda_exec.arn
  timeout     = 900
  memory_size = 512

  environment {
    variables = local.common_env
  }
}

#############################################
# Professional Development Day Agent
#############################################
resource "aws_lambda_function" "professional_development_day_agent" {
  function_name = "professional-development-day-agent"
  handler       = "lambda.professional_development_day_handler"
  runtime       = "python3.12"

  filename         = "lambda.zip"
  source_code_hash = filebase64sha256("lambda.zip")

  role        = aws_iam_role.lambda_exec.arn
  timeout     = 900
  memory_size = 512

  environment {
    variables = local.common_env
  }
}

#############################################
# Student Engagement Conference Agent
#############################################
resource "aws_lambda_function" "student_engagement_conference_agent" {
  function_name = "student-engagement-conference-agent"
  handler       = "lambda.student_engagement_conference_handler"
  runtime       = "python3.12"

  filename         = "lambda.zip"
  source_code_hash = filebase64sha256("lambda.zip")

  role        = aws_iam_role.lambda_exec.arn
  timeout     = 900
  memory_size = 512

  environment {
    variables = local.common_env
  }
}

#############################################
# Leadership Webinar Agent
#############################################
resource "aws_lambda_function" "leadership_webinar_agent" {
  function_name = "leadership-webinar-agent"
  handler       = "lambda.leadership_webinar_handler"
  runtime       = "python3.12"

  filename         = "lambda.zip"
  source_code_hash = filebase64sha256("lambda.zip")

  role        = aws_iam_role.lambda_exec.arn
  timeout     = 900
  memory_size = 512

  environment {
    variables = local.common_env
  }
}

#############################################
# Commuter Student Leadership Agent
#############################################
resource "aws_lambda_function" "commuter_student_leadership_agent" {
  function_name = "commuter-student-leadership-agent"
  handler       = "lambda.commuter_student_leadership_handler"
  runtime       = "python3.12"

  filename         = "lambda.zip"
  source_code_hash = filebase64sha256("lambda.zip")

  role        = aws_iam_role.lambda_exec.arn
  timeout     = 900
  memory_size = 512

  environment {
    variables = local.common_env
  }
}

#############################################
# Campus Leadership Innovation Agent
#############################################
resource "aws_lambda_function" "campus_leadership_innovation_agent" {
  function_name = "campus-leadership-innovation-agent"
  handler       = "lambda.campus_leadership_innovation_handler"
  runtime       = "python3.12"

  filename         = "lambda.zip"
  source_code_hash = filebase64sha256("lambda.zip")

  role        = aws_iam_role.lambda_exec.arn
  timeout     = 900
  memory_size = 512

  environment {
    variables = local.common_env
  }
}

#############################################
# Daily Outreach Email Agent (NEW)
#############################################
resource "aws_lambda_function" "daily_outreach" {
  function_name = "speaking-leads-daily-outreach"
  handler       = "lambda.daily_outreach_handler"
  runtime       = "python3.12"

  filename         = "lambda.zip"
  source_code_hash = filebase64sha256("lambda.zip")

  role        = aws_iam_role.lambda_exec.arn
  timeout     = 900
  memory_size = 512

  environment {
    variables = local.common_env
  }
}
