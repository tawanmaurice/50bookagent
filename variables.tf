#################################
# Terraform Variables
#################################

# AWS region for deployment
variable "aws_region" {
  description = "AWS region to deploy resources in"
  type        = string
  default     = "us-east-1"
}

# Google API key for Programmable Search
variable "google_api_key" {
  description = "Google Programmable Search API key"
  type        = string
  sensitive   = true
}

# Google Custom Search Engine CX ID
variable "google_cx" {
  description = "Google Programmable Search Engine CX ID"
  type        = string
  sensitive   = true
}

# OpenAI API key
variable "openai_api_key" {
  description = "OpenAI API key"
  type        = string
  sensitive   = true
}

# OpenAI model name (optional override)
variable "openai_model" {
  description = "OpenAI model to use"
  type        = string
  default     = "gpt-4.1-mini"
}
