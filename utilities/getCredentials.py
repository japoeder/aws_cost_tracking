"""
This file is used to grab the credentials for the cost tracking script.
It is meant to be used as a utility for the cost tracking script.
"""
import os

def get_cred():
    """
    This function is used to grab the credentials for the cost tracking script.
    It is meant to be used as a utility for the cost tracking script.
    """
    env_vals = {}
    env_vals["mysql_usr"] = os.getenv("COLLECTABILITY_COST_TRACKING_MYSQL_USER")
    env_vals["mysql_pwd"] = os.getenv("COLLECTABILITY_COST_TRACKING_MYSQL_PWD")
    env_vals["mysql_url"] = os.getenv("COLLECTABILITY_COST_TRACKING_MYSQL_URL")
    env_vals["mysql_db"] = os.getenv("COLLECTABILITY_COST_TRACKING_MYSQL_DB")
    env_vals["slack_token"] = os.getenv("COLLECTABILITY_COST_TRACKING_SLACK_TOKEN")
    env_vals["s3_url"] = os.getenv("COLLECTABILITY_COST_TRACKING_S3_URL")
    env_vals["s3_bucket"] = os.getenv("COLLECTABILITY_COST_TRACKING_S3_BUCKET")
    return env_vals
