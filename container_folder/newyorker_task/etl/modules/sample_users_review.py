import csv, json, time, pandas as pd
import multiprocessing as mp

"""Task Modules"""
from etl.modules import SAMPLE_USERS_FILE, \
    SAMPLE_USERS_ID_FIELD, SAMPLE_USERS_REVIEW_FILE, \
    REVIEW_FILE, CLEANED_FILE_NAME_TEMPLATE, DEFAULT_LINES_CHUNK_SIZE
from etl.utils.commons import module_format, read_file, check_fobj_exists, remove_file


def process_file(chunk_info):
    """ For every review chuck (dataframe) inner
    join with sample users (dataframe)"""
    SAMPLE_USERS, chunk, id = chunk_info
    print("\nProcessing lines chunk : ", id)
    header = False
    if not check_fobj_exists(SAMPLE_USERS_REVIEW_FILE):
        header = True
    (pd.merge(chunk, SAMPLE_USERS, on=['user_id'], how='inner')).to_csv(SAMPLE_USERS_REVIEW_FILE,
                   index=False, header=header,mode='a+')
    print("Done")


class IO():

    def __init__(self, definition):
        # Module Definition
        self.definition = definition
        # Set number of lines to process at once
        self.chunk_size = DEFAULT_LINES_CHUNK_SIZE
        if 'chunk_lines' in self.definition:
            self.chunk_size = self.definition['chunk_lines']
        # Pretty Print Module Name
        module_format(self.definition['name'])
        # Check existence
        self._delete_file_if_exists()

    def _delete_file_if_exists(self):
        """ Remove files when we run it the next time, so that results are refreshed """
        if check_fobj_exists(SAMPLE_USERS_REVIEW_FILE):
            remove_file(SAMPLE_USERS_REVIEW_FILE)

    def read_file(self, filename):
        assert check_fobj_exists(filename), "Cleaned Review File not found in " + filename
        return pd.read_json(filename, chunksize=self.chunk_size, lines=True)

    def _sample_users_review(self, SAMPLE_USERS):
        """ Write all reviews of the sampled users to 'sample_users_review.csv' (with headers) """
        pool = mp.Pool(mp.cpu_count())
        # Get sample user's review
        jobs = []
        for count, chunk in enumerate(self.read_file(CLEANED_FILE_NAME_TEMPLATE + REVIEW_FILE)):
            # process each data frame
            f = pool.apply_async(process_file, ((SAMPLE_USERS, chunk, count), ))
            jobs.append(f)
        for job in jobs:
            job.get(timeout=120)

    def run(self):
        start_time = time.time()
        # Load sample users
        assert check_fobj_exists(SAMPLE_USERS_FILE), "Sample users File not found in " + SAMPLE_USERS_FILE
        SAMPLE_USERS = read_file(SAMPLE_USERS_FILE, header=SAMPLE_USERS_ID_FIELD)
        # Run query
        self._sample_users_review(SAMPLE_USERS)
        print("--- %s seconds ---" % (time.time() - start_time))
        module_format(self.definition['name'], type=1)