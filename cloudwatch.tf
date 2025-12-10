######################################
# CLOUDWATCH EVENT RULES + TARGETS
######################################

# NOTE: cron format: cron(Minutes Hours Day-of-month Month Day-of-week Year)
# These are all UTC times.

# 1) Student Athlete – 13:00 UTC daily
resource "aws_cloudwatch_event_rule" "student_athlete_daily" {
  name                = "student-athlete-leadership-daily"
  schedule_expression = "cron(0 13 * * ? *)"
}

resource "aws_cloudwatch_event_target" "student_athlete_target" {
  rule      = aws_cloudwatch_event_rule.student_athlete_daily.name
  target_id = "student-athlete-lambda"
  arn       = aws_lambda_function.student_athlete_leadership_agent.arn
}

resource "aws_lambda_permission" "student_athlete_events" {
  statement_id  = "AllowExecutionFromCloudWatchStudentAthlete"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.student_athlete_leadership_agent.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.student_athlete_daily.arn
}

# 2) Men of Color – 13:05 UTC daily
resource "aws_cloudwatch_event_rule" "men_of_color_daily" {
  name                = "men-of-color-initiative-daily"
  schedule_expression = "cron(5 13 * * ? *)"
}

resource "aws_cloudwatch_event_target" "men_of_color_target" {
  rule      = aws_cloudwatch_event_rule.men_of_color_daily.name
  target_id = "men-of-color-lambda"
  arn       = aws_lambda_function.men_of_color_initiative_agent.arn
}

resource "aws_lambda_permission" "men_of_color_events" {
  statement_id  = "AllowExecutionFromCloudWatchMenOfColor"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.men_of_color_initiative_agent.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.men_of_color_daily.arn
}

# 3) First Gen – 13:10 UTC daily
resource "aws_cloudwatch_event_rule" "first_gen_daily" {
  name                = "first-gen-student-success-daily"
  schedule_expression = "cron(10 13 * * ? *)"
}

resource "aws_cloudwatch_event_target" "first_gen_target" {
  rule      = aws_cloudwatch_event_rule.first_gen_daily.name
  target_id = "first-gen-lambda"
  arn       = aws_lambda_function.first_gen_student_success_agent.arn
}

resource "aws_lambda_permission" "first_gen_events" {
  statement_id  = "AllowExecutionFromCloudWatchFirstGen"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.first_gen_student_success_agent.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.first_gen_daily.arn
}

# 4) Multicultural Center – 13:15 UTC daily
resource "aws_cloudwatch_event_rule" "multicultural_center_daily" {
  name                = "multicultural-center-leadership-daily"
  schedule_expression = "cron(15 13 * * ? *)"
}

resource "aws_cloudwatch_event_target" "multicultural_center_target" {
  rule      = aws_cloudwatch_event_rule.multicultural_center_daily.name
  target_id = "multicultural-center-lambda"
  arn       = aws_lambda_function.multicultural_center_leadership_agent.arn
}

resource "aws_lambda_permission" "multicultural_center_events" {
  statement_id  = "AllowExecutionFromCloudWatchMulticulturalCenter"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.multicultural_center_leadership_agent.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.multicultural_center_daily.arn
}

# 5) Service Learning / Civic – 13:20 UTC daily
resource "aws_cloudwatch_event_rule" "service_learning_daily" {
  name                = "service-learning-civic-engagement-daily"
  schedule_expression = "cron(20 13 * * ? *)"
}

resource "aws_cloudwatch_event_target" "service_learning_target" {
  rule      = aws_cloudwatch_event_rule.service_learning_daily.name
  target_id = "service-learning-lambda"
  arn       = aws_lambda_function.service_learning_civic_engagement_agent.arn
}

resource "aws_lambda_permission" "service_learning_events" {
  statement_id  = "AllowExecutionFromCloudWatchServiceLearning"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.service_learning_civic_engagement_agent.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.service_learning_daily.arn
}

# 6) HS Student Council – 13:25 UTC daily
resource "aws_cloudwatch_event_rule" "hs_student_council_daily" {
  name                = "hs-student-council-leadership-daily"
  schedule_expression = "cron(25 13 * * ? *)"
}

resource "aws_cloudwatch_event_target" "hs_student_council_target" {
  rule      = aws_cloudwatch_event_rule.hs_student_council_daily.name
  target_id = "hs-student-council-lambda"
  arn       = aws_lambda_function.hs_student_council_leadership_agent.arn
}

resource "aws_lambda_permission" "hs_student_council_events" {
  statement_id  = "AllowExecutionFromCloudWatchHSStudentCouncil"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.hs_student_council_leadership_agent.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.hs_student_council_daily.arn
}

# 7) Summer Bridge / Orientation – 13:30 UTC daily
resource "aws_cloudwatch_event_rule" "summer_bridge_daily" {
  name                = "summer-bridge-orientation-daily"
  schedule_expression = "cron(30 13 * * ? *)"
}

resource "aws_cloudwatch_event_target" "summer_bridge_target" {
  rule      = aws_cloudwatch_event_rule.summer_bridge_daily.name
  target_id = "summer-bridge-lambda"
  arn       = aws_lambda_function.summer_bridge_orientation_agent.arn
}

resource "aws_lambda_permission" "summer_bridge_events" {
  statement_id  = "AllowExecutionFromCloudWatchSummerBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.summer_bridge_orientation_agent.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.summer_bridge_daily.arn
}

######################################
# NEW AGENTS – leadership & CC focus
######################################

# 8) SGA Leadership – 13:35 UTC daily
resource "aws_cloudwatch_event_rule" "sga_leadership_daily" {
  name                = "sga-leadership-daily"
  schedule_expression = "cron(35 13 * * ? *)"
}

resource "aws_cloudwatch_event_target" "sga_leadership_target" {
  rule      = aws_cloudwatch_event_rule.sga_leadership_daily.name
  target_id = "sga-leadership-lambda"
  arn       = aws_lambda_function.sga_leadership_agent.arn
}

resource "aws_lambda_permission" "sga_leadership_events" {
  statement_id  = "AllowExecutionFromCloudWatchSGALeadership"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.sga_leadership_agent.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.sga_leadership_daily.arn
}

# 9) Student Leadership Retreat – 13:40 UTC daily
resource "aws_cloudwatch_event_rule" "student_leadership_retreat_daily" {
  name                = "student-leadership-retreat-daily"
  schedule_expression = "cron(40 13 * * ? *)"
}

resource "aws_cloudwatch_event_target" "student_leadership_retreat_target" {
  rule      = aws_cloudwatch_event_rule.student_leadership_retreat_daily.name
  target_id = "student-leadership-retreat-lambda"
  arn       = aws_lambda_function.student_leadership_retreat_agent.arn
}

resource "aws_lambda_permission" "student_leadership_retreat_events" {
  statement_id  = "AllowExecutionFromCloudWatchStudentLeadershipRetreat"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.student_leadership_retreat_agent.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.student_leadership_retreat_daily.arn
}

# 10) Student Leadership Conference – 13:45 UTC daily
resource "aws_cloudwatch_event_rule" "student_leadership_conference_daily" {
  name                = "student-leadership-conference-daily"
  schedule_expression = "cron(45 13 * * ? *)"
}

resource "aws_cloudwatch_event_target" "student_leadership_conference_target" {
  rule      = aws_cloudwatch_event_rule.student_leadership_conference_daily.name
  target_id = "student-leadership-conference-lambda"
  arn       = aws_lambda_function.student_leadership_conference_agent.arn
}

resource "aws_lambda_permission" "student_leadership_conference_events" {
  statement_id  = "AllowExecutionFromCloudWatchStudentLeadershipConference"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.student_leadership_conference_agent.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.student_leadership_conference_daily.arn
}

# 11) Leadership Summit – 13:50 UTC daily
resource "aws_cloudwatch_event_rule" "leadership_summit_daily" {
  name                = "leadership-summit-daily"
  schedule_expression = "cron(50 13 * * ? *)"
}

resource "aws_cloudwatch_event_target" "leadership_summit_target" {
  rule      = aws_cloudwatch_event_rule.leadership_summit_daily.name
  target_id = "leadership-summit-lambda"
  arn       = aws_lambda_function.leadership_summit_agent.arn
}

resource "aws_lambda_permission" "leadership_summit_events" {
  statement_id  = "AllowExecutionFromCloudWatchLeadershipSummit"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.leadership_summit_agent.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.leadership_summit_daily.arn
}

# 12) Officer Training – 13:55 UTC daily
resource "aws_cloudwatch_event_rule" "officer_training_daily" {
  name                = "officer-training-daily"
  schedule_expression = "cron(55 13 * * ? *)"
}

resource "aws_cloudwatch_event_target" "officer_training_target" {
  rule      = aws_cloudwatch_event_rule.officer_training_daily.name
  target_id = "officer-training-lambda"
  arn       = aws_lambda_function.officer_training_agent.arn
}

resource "aws_lambda_permission" "officer_training_events" {
  statement_id  = "AllowExecutionFromCloudWatchOfficerTraining"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.officer_training_agent.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.officer_training_daily.arn
}

# 13) Speaker / Lyceum Series – 14:00 UTC daily
resource "aws_cloudwatch_event_rule" "speaker_series_lyceum_daily" {
  name                = "speaker-series-lyceum-daily"
  schedule_expression = "cron(0 14 * * ? *)"
}

resource "aws_cloudwatch_event_target" "speaker_series_lyceum_target" {
  rule      = aws_cloudwatch_event_rule.speaker_series_lyceum_daily.name
  target_id = "speaker-series-lyceum-lambda"
  arn       = aws_lambda_function.speaker_series_lyceum_agent.arn
}

resource "aws_lambda_permission" "speaker_series_lyceum_events" {
  statement_id  = "AllowExecutionFromCloudWatchSpeakerSeriesLyceum"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.speaker_series_lyceum_agent.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.speaker_series_lyceum_daily.arn
}

# 14) Orientation Leaders – 14:05 UTC daily
resource "aws_cloudwatch_event_rule" "orientation_leader_daily" {
  name                = "orientation-leader-daily"
  schedule_expression = "cron(5 14 * * ? *)"
}

resource "aws_cloudwatch_event_target" "orientation_leader_target" {
  rule      = aws_cloudwatch_event_rule.orientation_leader_daily.name
  target_id = "orientation-leader-lambda"
  arn       = aws_lambda_function.orientation_leader_agent.arn
}

resource "aws_lambda_permission" "orientation_leader_events" {
  statement_id  = "AllowExecutionFromCloudWatchOrientationLeader"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.orientation_leader_agent.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.orientation_leader_daily.arn
}

# 15) Res Life / RA Leadership – 14:10 UTC daily
resource "aws_cloudwatch_event_rule" "res_life_ra_leadership_daily" {
  name                = "res-life-ra-leadership-daily"
  schedule_expression = "cron(10 14 * * ? *)"
}

resource "aws_cloudwatch_event_target" "res_life_ra_leadership_target" {
  rule      = aws_cloudwatch_event_rule.res_life_ra_leadership_daily.name
  target_id = "res-life-ra-leadership-lambda"
  arn       = aws_lambda_function.res_life_ra_leadership_agent.arn
}

resource "aws_lambda_permission" "res_life_ra_leadership_events" {
  statement_id  = "AllowExecutionFromCloudWatchResLifeRALeadership"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.res_life_ra_leadership_agent.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.res_life_ra_leadership_daily.arn
}

# 16) Honors Program Leadership – 14:15 UTC daily
resource "aws_cloudwatch_event_rule" "honors_program_leadership_daily" {
  name                = "honors-program-leadership-daily"
  schedule_expression = "cron(15 14 * * ? *)"
}

resource "aws_cloudwatch_event_target" "honors_program_leadership_target" {
  rule      = aws_cloudwatch_event_rule.honors_program_leadership_daily.name
  target_id = "honors-program-leadership-lambda"
  arn       = aws_lambda_function.honors_program_leadership_agent.arn
}

resource "aws_lambda_permission" "honors_program_leadership_events" {
  statement_id  = "AllowExecutionFromCloudWatchHonorsProgramLeadership"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.honors_program_leadership_agent.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.honors_program_leadership_daily.arn
}

# 17) Leadership Certificate Programs – 14:20 UTC daily
resource "aws_cloudwatch_event_rule" "leadership_certificate_program_daily" {
  name                = "leadership-certificate-program-daily"
  schedule_expression = "cron(20 14 * * ? *)"
}

resource "aws_cloudwatch_event_target" "leadership_certificate_program_target" {
  rule      = aws_cloudwatch_event_rule.leadership_certificate_program_daily.name
  target_id = "leadership-certificate-program-lambda"
  arn       = aws_lambda_function.leadership_certificate_program_agent.arn
}

resource "aws_lambda_permission" "leadership_certificate_program_events" {
  statement_id  = "AllowExecutionFromCloudWatchLeadershipCertificateProgram"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.leadership_certificate_program_agent.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.leadership_certificate_program_daily.arn
}

# 18) Leadership Academy – 14:25 UTC daily
resource "aws_cloudwatch_event_rule" "leadership_academy_daily" {
  name                = "leadership-academy-daily"
  schedule_expression = "cron(25 14 * * ? *)"
}

resource "aws_cloudwatch_event_target" "leadership_academy_target" {
  rule      = aws_cloudwatch_event_rule.leadership_academy_daily.name
  target_id = "leadership-academy-lambda"
  arn       = aws_lambda_function.leadership_academy_agent.arn
}

resource "aws_lambda_permission" "leadership_academy_events" {
  statement_id  = "AllowExecutionFromCloudWatchLeadershipAcademy"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.leadership_academy_agent.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.leadership_academy_daily.arn
}

# 19) Student Activities Leadership – 14:30 UTC daily
resource "aws_cloudwatch_event_rule" "student_activities_leadership_daily" {
  name                = "student-activities-leadership-daily"
  schedule_expression = "cron(30 14 * * ? *)"
}

resource "aws_cloudwatch_event_target" "student_activities_leadership_target" {
  rule      = aws_cloudwatch_event_rule.student_activities_leadership_daily.name
  target_id = "student-activities-leadership-lambda"
  arn       = aws_lambda_function.student_activities_leadership_agent.arn
}

resource "aws_lambda_permission" "student_activities_leadership_events" {
  statement_id  = "AllowExecutionFromCloudWatchStudentActivitiesLeadership"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.student_activities_leadership_agent.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.student_activities_leadership_daily.arn
}

# 20) College Success Leadership – 14:35 UTC daily
resource "aws_cloudwatch_event_rule" "college_success_leadership_daily" {
  name                = "college-success-leadership-daily"
  schedule_expression = "cron(35 14 * * ? *)"
}

resource "aws_cloudwatch_event_target" "college_success_leadership_target" {
  rule      = aws_cloudwatch_event_rule.college_success_leadership_daily.name
  target_id = "college-success-leadership-lambda"
  arn       = aws_lambda_function.college_success_leadership_agent.arn
}

resource "aws_lambda_permission" "college_success_leadership_events" {
  statement_id  = "AllowExecutionFromCloudWatchCollegeSuccessLeadership"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.college_success_leadership_agent.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.college_success_leadership_daily.arn
}

# 21) Career Success Leadership – 14:40 UTC daily
resource "aws_cloudwatch_event_rule" "career_success_leadership_daily" {
  name                = "career-success-leadership-daily"
  schedule_expression = "cron(40 14 * * ? *)"
}

resource "aws_cloudwatch_event_target" "career_success_leadership_target" {
  rule      = aws_cloudwatch_event_rule.career_success_leadership_daily.name
  target_id = "career-success-leadership-lambda"
  arn       = aws_lambda_function.career_success_leadership_agent.arn
}

resource "aws_lambda_permission" "career_success_leadership_events" {
  statement_id  = "AllowExecutionFromCloudWatchCareerSuccessLeadership"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.career_success_leadership_agent.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.career_success_leadership_daily.arn
}

# 22) Social Justice / DEI Leadership – 14:45 UTC daily
resource "aws_cloudwatch_event_rule" "social_justice_leadership_daily" {
  name                = "social-justice-leadership-daily"
  schedule_expression = "cron(45 14 * * ? *)"
}

resource "aws_cloudwatch_event_target" "social_justice_leadership_target" {
  rule      = aws_cloudwatch_event_rule.social_justice_leadership_daily.name
  target_id = "social-justice-leadership-lambda"
  arn       = aws_lambda_function.social_justice_leadership_agent.arn
}

resource "aws_lambda_permission" "social_justice_leadership_events" {
  statement_id  = "AllowExecutionFromCloudWatchSocialJusticeLeadership"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.social_justice_leadership_agent.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.social_justice_leadership_daily.arn
}

# 23) CC Student Leadership (catch-all) – 14:50 UTC daily
resource "aws_cloudwatch_event_rule" "cc_student_leadership_daily" {
  name                = "cc-student-leadership-daily"
  schedule_expression = "cron(50 14 * * ? *)"
}

resource "aws_cloudwatch_event_target" "cc_student_leadership_target" {
  rule      = aws_cloudwatch_event_rule.cc_student_leadership_daily.name
  target_id = "cc-student-leadership-lambda"
  arn       = aws_lambda_function.cc_student_leadership_agent.arn
}

resource "aws_lambda_permission" "cc_student_leadership_events" {
  statement_id  = "AllowExecutionFromCloudWatchCCStudentLeadership"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.cc_student_leadership_agent.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.cc_student_leadership_daily.arn
}

# 24) CC Success & Retention – 14:55 UTC daily
resource "aws_cloudwatch_event_rule" "cc_success_and_retention_daily" {
  name                = "cc-success-and-retention-daily"
  schedule_expression = "cron(55 14 * * ? *)"
}

resource "aws_cloudwatch_event_target" "cc_success_and_retention_target" {
  rule      = aws_cloudwatch_event_rule.cc_success_and_retention_daily.name
  target_id = "cc-success-and-retention-lambda"
  arn       = aws_lambda_function.cc_success_and_retention_agent.arn
}

resource "aws_lambda_permission" "cc_success_and_retention_events" {
  statement_id  = "AllowExecutionFromCloudWatchCCSuccessAndRetention"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.cc_success_and_retention_agent.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.cc_success_and_retention_daily.arn
}

####################################################
# NEW: RA / Ambassador / Sophomore daily schedules
####################################################

# 25) Resident Assistant Leadership – 15:00 UTC daily
resource "aws_cloudwatch_event_rule" "resident_assistant_leadership_daily" {
  name                = "resident-assistant-leadership-daily"
  schedule_expression = "cron(0 15 * * ? *)"
}

resource "aws_cloudwatch_event_target" "resident_assistant_leadership_target" {
  rule      = aws_cloudwatch_event_rule.resident_assistant_leadership_daily.name
  target_id = "resident-assistant-leadership-lambda"
  arn       = aws_lambda_function.resident_assistant_leadership_agent.arn
}

resource "aws_lambda_permission" "resident_assistant_leadership_events" {
  statement_id  = "AllowExecutionFromCloudWatchResidentAssistantLeadership"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.resident_assistant_leadership_agent.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.resident_assistant_leadership_daily.arn
}

# 26) Campus Ambassador Events – 15:05 UTC daily
resource "aws_cloudwatch_event_rule" "campus_ambassador_events_daily" {
  name                = "campus-ambassador-events-daily"
  schedule_expression = "cron(5 15 * * ? *)"
}

resource "aws_cloudwatch_event_target" "campus_ambassador_events_target" {
  rule      = aws_cloudwatch_event_rule.campus_ambassador_events_daily.name
  target_id = "campus-ambassador-events-lambda"
  arn       = aws_lambda_function.campus_ambassador_events_agent.arn
}

resource "aws_lambda_permission" "campus_ambassador_events_events" {
  statement_id  = "AllowExecutionFromCloudWatchCampusAmbassadorEvents"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.campus_ambassador_events_agent.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.campus_ambassador_events_daily.arn
}

# 27) Sophomore Leadership – 15:10 UTC daily
resource "aws_cloudwatch_event_rule" "sophomore_leadership_daily" {
  name                = "sophomore-leadership-daily"
  schedule_expression = "cron(10 15 * * ? *)"
}

resource "aws_cloudwatch_event_target" "sophomore_leadership_target" {
  rule      = aws_cloudwatch_event_rule.sophomore_leadership_daily.name
  target_id = "sophomore-leadership-lambda"
  arn       = aws_lambda_function.sophomore_leadership_agent.arn
}

resource "aws_lambda_permission" "sophomore_leadership_events" {
  statement_id  = "AllowExecutionFromCloudWatchSophomoreLeadership"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.sophomore_leadership_agent.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.sophomore_leadership_daily.arn
}

#######################################################
# NEW: High-school / specialty leadership daily runs
#######################################################

# 28) HS Student Leadership Conferences – 15:15 UTC daily
resource "aws_cloudwatch_event_rule" "hs_student_leadership_conferences_daily" {
  name                = "hs-student-leadership-conferences-daily"
  schedule_expression = "cron(15 15 * * ? *)"
}

resource "aws_cloudwatch_event_target" "hs_student_leadership_conferences_target" {
  rule      = aws_cloudwatch_event_rule.hs_student_leadership_conferences_daily.name
  target_id = "hs-student-leadership-conferences-lambda"
  arn       = aws_lambda_function.hs_student_leadership_conferences_agent.arn
}

resource "aws_lambda_permission" "hs_student_leadership_conferences_events" {
  statement_id  = "AllowExecutionFromCloudWatchHSStudentLeadershipConferences"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.hs_student_leadership_conferences_agent.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.hs_student_leadership_conferences_daily.arn
}

# 29) HS Faculty / Staff Training – 15:20 UTC daily
resource "aws_cloudwatch_event_rule" "hs_faculty_staff_training_daily" {
  name                = "hs-faculty-staff-training-daily"
  schedule_expression = "cron(20 15 * * ? *)"
}

resource "aws_cloudwatch_event_target" "hs_faculty_staff_training_target" {
  rule      = aws_cloudwatch_event_rule.hs_faculty_staff_training_daily.name
  target_id = "hs-faculty-staff-training-lambda"
  arn       = aws_lambda_function.hs_faculty_staff_training_agent.arn
}

resource "aws_lambda_permission" "hs_faculty_staff_training_events" {
  statement_id  = "AllowExecutionFromCloudWatchHSFacultyStaffTraining"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.hs_faculty_staff_training_agent.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.hs_faculty_staff_training_daily.arn
}

# 30) Transfer Student Leadership – 15:25 UTC daily
resource "aws_cloudwatch_event_rule" "transfer_student_leadership_daily" {
  name                = "transfer-student-leadership-daily"
  schedule_expression = "cron(25 15 * * ? *)"
}

resource "aws_cloudwatch_event_target" "transfer_student_leadership_target" {
  rule      = aws_cloudwatch_event_rule.transfer_student_leadership_daily.name
  target_id = "transfer-student-leadership-lambda"
  arn       = aws_lambda_function.transfer_student_leadership_agent.arn
}

resource "aws_lambda_permission" "transfer_student_leadership_events" {
  statement_id  = "AllowExecutionFromCloudWatchTransferStudentLeadership"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.transfer_student_leadership_agent.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.transfer_student_leadership_daily.arn
}

# 31) TRIO Leadership – 15:30 UTC daily
resource "aws_cloudwatch_event_rule" "trio_leadership_daily" {
  name                = "trio-leadership-daily"
  schedule_expression = "cron(30 15 * * ? *)"
}

resource "aws_cloudwatch_event_target" "trio_leadership_target" {
  rule      = aws_cloudwatch_event_rule.trio_leadership_daily.name
  target_id = "trio-leadership-lambda"
  arn       = aws_lambda_function.trio_leadership_agent.arn
}

resource "aws_lambda_permission" "trio_leadership_events" {
  statement_id  = "AllowExecutionFromCloudWatchTRIOLeadership"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.trio_leadership_agent.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.trio_leadership_daily.arn
}

# 32) Greek Life Leadership – 15:35 UTC daily
resource "aws_cloudwatch_event_rule" "greek_life_leadership_daily" {
  name                = "greek-life-leadership-daily"
  schedule_expression = "cron(35 15 * * ? *)"
}

resource "aws_cloudwatch_event_target" "greek_life_leadership_target" {
  rule      = aws_cloudwatch_event_rule.greek_life_leadership_daily.name
  target_id = "greek-life-leadership-lambda"
  arn       = aws_lambda_function.greek_life_leadership_agent.arn
}

resource "aws_lambda_permission" "greek_life_leadership_events" {
  statement_id  = "AllowExecutionFromCloudWatchGreekLifeLeadership"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.greek_life_leadership_agent.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.greek_life_leadership_daily.arn
}

# 33) Peer Mentor Leadership – 15:40 UTC daily
resource "aws_cloudwatch_event_rule" "peer_mentor_leadership_daily" {
  name                = "peer-mentor-leadership-daily"
  schedule_expression = "cron(40 15 * * ? *)"
}

resource "aws_cloudwatch_event_target" "peer_mentor_leadership_target" {
  rule      = aws_cloudwatch_event_rule.peer_mentor_leadership_daily.name
  target_id = "peer-mentor-leadership-lambda"
  arn       = aws_lambda_function.peer_mentor_leadership_agent.arn
}

resource "aws_lambda_permission" "peer_mentor_leadership_events" {
  statement_id  = "AllowExecutionFromCloudWatchPeerMentorLeadership"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.peer_mentor_leadership_agent.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.peer_mentor_leadership_daily.arn
}

# 34) Women in Leadership – 15:45 UTC daily
resource "aws_cloudwatch_event_rule" "women_in_leadership_daily" {
  name                = "women-in-leadership-daily"
  schedule_expression = "cron(45 15 * * ? *)"
}

resource "aws_cloudwatch_event_target" "women_in_leadership_target" {
  rule      = aws_cloudwatch_event_rule.women_in_leadership_daily.name
  target_id = "women-in-leadership-lambda"
  arn       = aws_lambda_function.women_in_leadership_agent.arn
}

resource "aws_lambda_permission" "women_in_leadership_events" {
  statement_id  = "AllowExecutionFromCloudWatchWomenInLeadership"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.women_in_leadership_agent.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.women_in_leadership_daily.arn
}

# 35) Leadership Honor Society – 15:50 UTC daily
resource "aws_cloudwatch_event_rule" "leadership_honor_society_daily" {
  name                = "leadership-honor-society-daily"
  schedule_expression = "cron(50 15 * * ? *)"
}

resource "aws_cloudwatch_event_target" "leadership_honor_society_target" {
  rule      = aws_cloudwatch_event_rule.leadership_honor_society_daily.name
  target_id = "leadership-honor-society-lambda"
  arn       = aws_lambda_function.leadership_honor_society_agent.arn
}

resource "aws_lambda_permission" "leadership_honor_society_events" {
  statement_id  = "AllowExecutionFromCloudWatchLeadershipHonorSociety"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.leadership_honor_society_agent.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.leadership_honor_society_daily.arn
}

# 36) Student Belonging Leadership – 15:55 UTC daily
resource "aws_cloudwatch_event_rule" "student_belonging_leadership_daily" {
  name                = "student-belonging-leadership-daily"
  schedule_expression = "cron(55 15 * * ? *)"
}

resource "aws_cloudwatch_event_target" "student_belonging_leadership_target" {
  rule      = aws_cloudwatch_event_rule.student_belonging_leadership_daily.name
  target_id = "student-belonging-leadership-lambda"
  arn       = aws_lambda_function.student_belonging_leadership_agent.arn
}

resource "aws_lambda_permission" "student_belonging_leadership_events" {
  statement_id  = "AllowExecutionFromCloudWatchStudentBelongingLeadership"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.student_belonging_leadership_agent.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.student_belonging_leadership_daily.arn
}

# 37) School Improvement Leadership – 16:00 UTC daily
resource "aws_cloudwatch_event_rule" "school_improvement_leadership_daily" {
  name                = "school-improvement-leadership-daily"
  schedule_expression = "cron(0 16 * * ? *)"
}

resource "aws_cloudwatch_event_target" "school_improvement_leadership_target" {
  rule      = aws_cloudwatch_event_rule.school_improvement_leadership_daily.name
  target_id = "school-improvement-leadership-lambda"
  arn       = aws_lambda_function.school_improvement_leadership_agent.arn
}

resource "aws_lambda_permission" "school_improvement_leadership_events" {
  statement_id  = "AllowExecutionFromCloudWatchSchoolImprovementLeadership"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.school_improvement_leadership_agent.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.school_improvement_leadership_daily.arn
}

##########################################################
# NEW 13 AGENTS – program / event pages only (38–50)
##########################################################

# 38) Emerging Leaders Program – 16:05 UTC daily
resource "aws_cloudwatch_event_rule" "emerging_leaders_program_daily" {
  name                = "emerging-leaders-program-daily"
  schedule_expression = "cron(5 16 * * ? *)"
}

resource "aws_cloudwatch_event_target" "emerging_leaders_program_target" {
  rule      = aws_cloudwatch_event_rule.emerging_leaders_program_daily.name
  target_id = "emerging-leaders-program-lambda"
  arn       = aws_lambda_function.emerging_leaders_program_agent.arn
}

resource "aws_lambda_permission" "emerging_leaders_program_events" {
  statement_id  = "AllowExecutionFromCloudWatchEmergingLeadersProgram"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.emerging_leaders_program_agent.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.emerging_leaders_program_daily.arn
}

# 39) Leadership Capstone – 16:10 UTC daily
resource "aws_cloudwatch_event_rule" "leadership_capstone_daily" {
  name                = "leadership-capstone-daily"
  schedule_expression = "cron(10 16 * * ? *)"
}

resource "aws_cloudwatch_event_target" "leadership_capstone_target" {
  rule      = aws_cloudwatch_event_rule.leadership_capstone_daily.name
  target_id = "leadership-capstone-lambda"
  arn       = aws_lambda_function.leadership_capstone_agent.arn
}

resource "aws_lambda_permission" "leadership_capstone_events" {
  statement_id  = "AllowExecutionFromCloudWatchLeadershipCapstone"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.leadership_capstone_agent.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.leadership_capstone_daily.arn
}

# 40) Student Leadership Grant – 16:15 UTC daily
resource "aws_cloudwatch_event_rule" "student_leadership_grant_daily" {
  name                = "student-leadership-grant-daily"
  schedule_expression = "cron(15 16 * * ? *)"
}

resource "aws_cloudwatch_event_target" "student_leadership_grant_target" {
  rule      = aws_cloudwatch_event_rule.student_leadership_grant_daily.name
  target_id = "student-leadership-grant-lambda"
  arn       = aws_lambda_function.student_leadership_grant_agent.arn
}

resource "aws_lambda_permission" "student_leadership_grant_events" {
  statement_id  = "AllowExecutionFromCloudWatchStudentLeadershipGrant"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.student_leadership_grant_agent.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.student_leadership_grant_daily.arn
}

# 41) Intercultural Leadership – 16:20 UTC daily
resource "aws_cloudwatch_event_rule" "intercultural_leadership_daily" {
  name                = "intercultural-leadership-daily"
  schedule_expression = "cron(20 16 * * ? *)"
}

resource "aws_cloudwatch_event_target" "intercultural_leadership_target" {
  rule      = aws_cloudwatch_event_rule.intercultural_leadership_daily.name
  target_id = "intercultural-leadership-lambda"
  arn       = aws_lambda_function.intercultural_leadership_agent.arn
}

resource "aws_lambda_permission" "intercultural_leadership_events" {
  statement_id  = "AllowExecutionFromCloudWatchInterculturalLeadership"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.intercultural_leadership_agent.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.intercultural_leadership_daily.arn
}

# 42) Leadership Conference RFP – 16:25 UTC daily
resource "aws_cloudwatch_event_rule" "leadership_conference_rfp_daily" {
  name                = "leadership-conference-rfp-daily"
  schedule_expression = "cron(25 16 * * ? *)"
}

resource "aws_cloudwatch_event_target" "leadership_conference_rfp_target" {
  rule      = aws_cloudwatch_event_rule.leadership_conference_rfp_daily.name
  target_id = "leadership-conference-rfp-lambda"
  arn       = aws_lambda_function.leadership_conference_rfp_agent.arn
}

resource "aws_lambda_permission" "leadership_conference_rfp_events" {
  statement_id  = "AllowExecutionFromCloudWatchLeadershipConferenceRFP"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.leadership_conference_rfp_agent.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.leadership_conference_rfp_daily.arn
}

# 43) Leadership Week – 16:30 UTC daily
resource "aws_cloudwatch_event_rule" "leadership_week_daily" {
  name                = "leadership-week-daily"
  schedule_expression = "cron(30 16 * * ? *)"
}

resource "aws_cloudwatch_event_target" "leadership_week_target" {
  rule      = aws_cloudwatch_event_rule.leadership_week_daily.name
  target_id = "leadership-week-lambda"
  arn       = aws_lambda_function.leadership_week_agent.arn
}

resource "aws_lambda_permission" "leadership_week_events" {
  statement_id  = "AllowExecutionFromCloudWatchLeadershipWeek"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.leadership_week_agent.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.leadership_week_daily.arn
}

# 44) Student Success Workshop Series – 16:35 UTC daily
resource "aws_cloudwatch_event_rule" "student_success_workshop_series_daily" {
  name                = "student-success-workshop-series-daily"
  schedule_expression = "cron(35 16 * * ? *)"
}

resource "aws_cloudwatch_event_target" "student_success_workshop_series_target" {
  rule      = aws_cloudwatch_event_rule.student_success_workshop_series_daily.name
  target_id = "student-success-workshop-series-lambda"
  arn       = aws_lambda_function.student_success_workshop_series_agent.arn
}

resource "aws_lambda_permission" "student_success_workshop_series_events" {
  statement_id  = "AllowExecutionFromCloudWatchStudentSuccessWorkshopSeries"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.student_success_workshop_series_agent.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.student_success_workshop_series_daily.arn
}

# 45) Leadership Institute – 16:40 UTC daily
resource "aws_cloudwatch_event_rule" "leadership_institute_daily" {
  name                = "leadership-institute-daily"
  schedule_expression = "cron(40 16 * * ? *)"
}

resource "aws_cloudwatch_event_target" "leadership_institute_target" {
  rule      = aws_cloudwatch_event_rule.leadership_institute_daily.name
  target_id = "leadership-institute-lambda"
  arn       = aws_lambda_function.leadership_institute_agent.arn
}

resource "aws_lambda_permission" "leadership_institute_events" {
  statement_id  = "AllowExecutionFromCloudWatchLeadershipInstitute"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.leadership_institute_agent.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.leadership_institute_daily.arn
}

# 46) Professional Development Day – 16:45 UTC daily
resource "aws_cloudwatch_event_rule" "professional_development_day_daily" {
  name                = "professional-development-day-daily"
  schedule_expression = "cron(45 16 * * ? *)"
}

resource "aws_cloudwatch_event_target" "professional_development_day_target" {
  rule      = aws_cloudwatch_event_rule.professional_development_day_daily.name
  target_id = "professional-development-day-lambda"
  arn       = aws_lambda_function.professional_development_day_agent.arn
}

resource "aws_lambda_permission" "professional_development_day_events" {
  statement_id  = "AllowExecutionFromCloudWatchProfessionalDevelopmentDay"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.professional_development_day_agent.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.professional_development_day_daily.arn
}

# 47) Student Engagement Conference – 16:50 UTC daily
resource "aws_cloudwatch_event_rule" "student_engagement_conference_daily" {
  name                = "student-engagement-conference-daily"
  schedule_expression = "cron(50 16 * * ? *)"
}

resource "aws_cloudwatch_event_target" "student_engagement_conference_target" {
  rule      = aws_cloudwatch_event_rule.student_engagement_conference_daily.name
  target_id = "student-engagement-conference-lambda"
  arn       = aws_lambda_function.student_engagement_conference_agent.arn
}

resource "aws_lambda_permission" "student_engagement_conference_events" {
  statement_id  = "AllowExecutionFromCloudWatchStudentEngagementConference"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.student_engagement_conference_agent.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.student_engagement_conference_daily.arn
}

# 48) Leadership Webinar – 16:55 UTC daily
resource "aws_cloudwatch_event_rule" "leadership_webinar_daily" {
  name                = "leadership-webinar-daily"
  schedule_expression = "cron(55 16 * * ? *)"
}

resource "aws_cloudwatch_event_target" "leadership_webinar_target" {
  rule      = aws_cloudwatch_event_rule.leadership_webinar_daily.name
  target_id = "leadership-webinar-lambda"
  arn       = aws_lambda_function.leadership_webinar_agent.arn
}

resource "aws_lambda_permission" "leadership_webinar_events" {
  statement_id  = "AllowExecutionFromCloudWatchLeadershipWebinar"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.leadership_webinar_agent.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.leadership_webinar_daily.arn
}

# 49) Commuter Student Leadership – 17:00 UTC daily
resource "aws_cloudwatch_event_rule" "commuter_student_leadership_daily" {
  name                = "commuter-student-leadership-daily"
  schedule_expression = "cron(0 17 * * ? *)"
}

resource "aws_cloudwatch_event_target" "commuter_student_leadership_target" {
  rule      = aws_cloudwatch_event_rule.commuter_student_leadership_daily.name
  target_id = "commuter-student-leadership-lambda"
  arn       = aws_lambda_function.commuter_student_leadership_agent.arn
}

resource "aws_lambda_permission" "commuter_student_leadership_events" {
  statement_id  = "AllowExecutionFromCloudWatchCommuterStudentLeadership"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.commuter_student_leadership_agent.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.commuter_student_leadership_daily.arn
}

# 50) Campus Leadership Innovation – 17:05 UTC daily
resource "aws_cloudwatch_event_rule" "campus_leadership_innovation_daily" {
  name                = "campus-leadership-innovation-daily"
  schedule_expression = "cron(5 17 * * ? *)"
}

resource "aws_cloudwatch_event_target" "campus_leadership_innovation_target" {
  rule      = aws_cloudwatch_event_rule.campus_leadership_innovation_daily.name
  target_id = "campus-leadership-innovation-lambda"
  arn       = aws_lambda_function.campus_leadership_innovation_agent.arn
}

resource "aws_lambda_permission" "campus_leadership_innovation_events" {
  statement_id  = "AllowExecutionFromCloudWatchCampusLeadershipInnovation"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.campus_leadership_innovation_agent.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.campus_leadership_innovation_daily.arn
}
