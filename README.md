Speaking Agent (Autonomous Conference Researcher)

A fully automated pipeline that discovers student leadership conferences, structures the information, and stores it in DynamoDB.

🚀 Overview

The Speaking Agent is an autonomous research system that automatically finds student leadership conferences, student leadership programs, and large-scale student leadership events hosted by colleges and universities.

Once configured, the system runs completely on its own — no human input required.

On each scheduled run, it:

Searches .edu websites for new conferences

Structures the information using OpenAI

Extracts contact emails (when available)

Saves the results into a DynamoDB table

Updates past items with seen_count, status, and new emails

This creates a continuous stream of fresh speaking opportunities for future speaking engagements.

⚙️ How It Works (High-Level)
1. EventBridge Schedule (2x per day)

Triggers the Lambda with a predefined topic like:

student leadership conferences and large-scale student leadership programs...


This ensures the agent runs automatically every day.

2. Google Custom Search Engine (CSE)

The Lambda performs a 3-page Google search:

Page 1 → first 10 results

Page 2 → next 10 results

Page 3 → next 10 results

Up to 30 results per run.

All results are filtered by:

site:.edu
student leadership conference

3. OpenAI Structuring

Google’s raw search output is messy.
OpenAI transforms it into clean JSON with fields like:

conference_name

school_name

organization

location

audience

date

url

contact_email

notes

Up to 8–10 conferences per run.

4. Email Extraction

For each conference (up to 5 per run):

The agent fetches the conference webpage

Scans the HTML for any valid email address

Adds it as contact_email

If no email is found, the record still saves — you can follow up manually.

5. DynamoDB Storage

Each conference is saved with:

id (stable, used to avoid duplicates)

conference_name

url

contact_email

status (default: NEW)

seen_count (increments every time the conference appears)

created_at

updated_at

This allows DynamoDB to serve as a lead database / mini CRM.

🗂 Example DynamoDB Item
{
  "id": "vacuho-student-leadership-conference",
  "conference_name": "VACUHO Student Leadership Conference",
  "school_name": "Virginia Community Colleges",
  "location": "Virginia, USA",
  "url": "https://vacuho.org/student-leadership-conference",
  "contact_email": "info@vacuho.org",
  "status": "NEW",
  "seen_count": 3,
  "created_at": "2025-12-01T14:00:00Z",
  "updated_at": "2025-12-02T14:00:00Z"
}

📦 Files Included
lambda_function.py

The main logic:

Google search (multi-page)

OpenAI conference structuring

Email extraction

DynamoDB upsert logic

Full orchestration pipeline

main.tf / variables.tf / outputs.tf (optional)

Terraform infrastructure (Lambda, IAM roles, EventBridge Schedule, DynamoDB, etc.)

🧠 Why This Agent Exists

For speakers who want to book more student leadership events, it is extremely difficult to manually:

Search dozens of universities daily

Identify which events are relevant

Extract contact information

Track leads over time

This autonomous agent does all of that automatically.

It becomes your:

24/7 tireless research assistant
finding real student leadership conferences for you
while you sleep.

🔮 Future Enhancements (Optional)

These features can be added later:

1. Outreach Email Generator Lambda

Reads NEW conferences → drafts personalized outreach emails for you.

2. Daily/Weekly Report Email

Automatically sends you a summary of:

New conferences found

Emails collected

Top opportunities

Repeated high-value leads

3. Multi-topic Rotation

Searches different slices such as:

Community colleges

HBCUs

West Coast programs

Leadership retreats

Orientation leader conferences

🏁 Summary

This agent is fully autonomous, hands-free, and built specifically for student leadership speaking opportunities. You set it once — and it continuously discovers relevant events, enriches them, and stores them for outreach.

Perfect for speakers who want:

A continuous pipeline

Without manual research

Without guessing

Without burnout

You now have a fully automated speaking-opportunity engine.
