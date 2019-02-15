import os

BASE_DATA_FOLDER = os.getcwd() + "/data/"
CLEANED_FILE_NAME_TEMPLATE = BASE_DATA_FOLDER + "cleaned_"
AGGREGATED_INFO = BASE_DATA_FOLDER + "aggregated_info.json"
USER_FILE = "user.json"
REVIEW_FILE = "review.json"
SAMPLE_USERS_FILE = BASE_DATA_FOLDER + "sample_users.csv"
SAMPLE_USERS_REVIEW_FILE = BASE_DATA_FOLDER + "sample_users_review.csv"
SAMPLE_USERS_NO_REVIEW_FOLDER = BASE_DATA_FOLDER + "sample_users_no_review_within_time_interval/"
DEFAULT_SAMPLE_USERS = 10000
DEFAULT_LINES_CHUNK_SIZE = 500000
SAMPLE_USERS_ID_FIELD = ['user_id']
