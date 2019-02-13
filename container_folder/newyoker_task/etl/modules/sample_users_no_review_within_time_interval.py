import csv, json, time, psutil, shutil, pandas as pd, numpy as np
import multiprocessing as mp

"""Task Modules"""
from etl.modules import SAMPLE_USERS_FILE, \
    SAMPLE_USERS_ID_FIELD, SAMPLE_USERS_NO_REVIEW_FOLDER, \
    REVIEW_FILE, CLEANED_FILE_NAME_TEMPLATE
from etl.utils.commons import module_format, read_file, check_fobj_exists, \
    delete_folder, create_directory

# Load sample users
SAMPLE_USERS = read_file(SAMPLE_USERS_FILE, header=SAMPLE_USERS_ID_FIELD)

def process_file(chunk_info):
    """ For every review chuck (dataframe) inner join with
    sample users (dataframe) and pick the latest reviewed
    date of the sample user"""
    chunk, id, time_filter, filename = chunk_info
    print("\nProcessing chunk : ", id)
    header = False
    if not check_fobj_exists(filename):
        header = True
    chunk = (((pd.merge(SAMPLE_USERS, chunk, on=['user_id'], how='inner'))
           .groupby(['user_id']))
          .agg({'date' : np.max}))
    (chunk.loc[chunk['date'] < time_filter]).to_csv(filename,
                   index=True, header=header,mode='a+')

class IO():

    def __init__(self, definition):
        # Module Definition
        self.definition = definition
        # Check for time range filters
        assert 'time_filter' in self.definition, "'time_filter' is not specified!"
        # Pretty Print Module Name
        module_format(self.definition['name'])
        # If present, delete folder. If not, create empty folder
        self._execute_fs_initializations()

    def _execute_fs_initializations(self):
        """ Remove files/Create folder when we run it the next time,
        so that results are refreshed """
        if check_fobj_exists(SAMPLE_USERS_NO_REVIEW_FOLDER, type=1):
            delete_folder(SAMPLE_USERS_NO_REVIEW_FOLDER)
        else:
            create_directory(SAMPLE_USERS_NO_REVIEW_FOLDER)

    def _set_read_file(self, recursive_count):
        """ Open csv files that was created in previous run and open new file to push
         the current processes results """
        if recursive_count == 0:
            return CLEANED_FILE_NAME_TEMPLATE + REVIEW_FILE, \
                   SAMPLE_USERS_NO_REVIEW_FOLDER + str(recursive_count) + '.csv'
        else:
            return SAMPLE_USERS_NO_REVIEW_FOLDER + str(recursive_count-1) + '.csv', \
                   SAMPLE_USERS_NO_REVIEW_FOLDER + str(recursive_count) + '.csv'

    def _set_reader(self, recursive_count, read_filename):
        """ Switch file reader based on the order of recursive strategy """
        if recursive_count > 0:
            return pd.read_csv(read_filename, chunksize=500000)
        else:
            return pd.read_json(read_filename, chunksize=500000, lines=True)

    def _sample_users_no_review(self, recursive_count=0):
        """ Write all sampled users who didn't write review within teh specified time range
        to 'sample_users_no_review_within_time_interval.csv' (with headers) """
        print("Recursive round : ", recursive_count)
        pool = mp.Pool(mp.cpu_count())
        # Get sample user's review
        jobs = []
        read_filename, write_filename = self._set_read_file(recursive_count)
        chunk_count = None
        for count, chunk in enumerate(self._set_reader(recursive_count, read_filename)):
            chunk_count = count
            # process each data frame
            f = pool.apply_async(process_file, ((chunk, count, self.definition['time_filter'], write_filename), ))
            jobs.append(f)
        for job in jobs:
            job.get(timeout=120)
        if chunk_count > 0:
            self._sample_users_no_review(recursive_count+1)

    def run(self):
        try:
            start_time = time.time()
            print("This kernel has ", mp.cpu_count(),
                  "cores and you can find the information regarding the memory usage:",
                  psutil.virtual_memory())
            self._sample_users_no_review()
            print("This kernel has ", mp.cpu_count(), "cores and you can find the information regarding the memory usage:",
                  psutil.virtual_memory())
            print("--- %s seconds ---" % (time.time() - start_time))
        finally:
            module_format(self.definition['name'], type=1)