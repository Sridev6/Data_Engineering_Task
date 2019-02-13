import csv, json, time, psutil, pandas as pd
import multiprocessing as mp

"""Task Modules"""
from etl.modules import SAMPLE_USERS_FILE, \
    SAMPLE_USERS_ID_FIELD, SAMPLE_USERS_REVIEW_FILE, \
    REVIEW_FILE, CLEANED_FILE_NAME_TEMPLATE
from etl.utils.commons import module_format, read_file, check_fobj_exists, remove_file

# Load sample users
SAMPLE_USERS = read_file(SAMPLE_USERS_FILE, header=SAMPLE_USERS_ID_FIELD)

def process_file(chunk_info):
    """ For every review chuck (dataframe) inner
    join with sample users (dataframe)"""
    chunk, id = chunk_info
    print("\nProcessing chunk : ", id)
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
        # Pretty Print Module Name
        module_format(self.definition['name'])
        # Check existence
        self._delete_file_if_exists()

    def _delete_file_if_exists(self):
        """ Remove files when we run it the next time, so that results are refreshed """
        if check_fobj_exists(SAMPLE_USERS_REVIEW_FILE):
            remove_file(SAMPLE_USERS_REVIEW_FILE)

    def _sample_users_review(self):
        """ Write all reviews of the sampled users to 'sample_users_review.csv' (with headers) """
        pool = mp.Pool(mp.cpu_count())
        # Get sample user's review
        jobs = []
        for count, chunk in enumerate(
                pd.read_json(CLEANED_FILE_NAME_TEMPLATE + REVIEW_FILE, chunksize=500000, lines=True)):
            # process each data frame
            f = pool.apply_async(process_file, ((chunk, count), ))
            jobs.append(f)
        for job in jobs:
            job.get(timeout=120)
        # pool = Pool(processes=mp.cpu_count())
        # for count, chunk in enumerate(pd.read_json(CLEANED_FILE_NAME_TEMPLATE + REVIEW_FILE, chunksize=500000, lines=True)):
        #     pool.map(process_file, ((chunk, count), ))
        # pool.close()
        # pool.join()

    def run(self):
        try:
            start_time = time.time()
            print("This kernel has ", mp.cpu_count(),
                  "cores and you can find the information regarding the memory usage:",
                  psutil.virtual_memory())
            self._sample_users_review()
            print("This kernel has ", mp.cpu_count(), "cores and you can find the information regarding the memory usage:",
                  psutil.virtual_memory())
            print("--- %s seconds ---" % (time.time() - start_time))
        finally:
            module_format(self.definition['name'], type=1)