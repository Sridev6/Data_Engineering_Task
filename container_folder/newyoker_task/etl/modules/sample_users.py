import json, csv, time

"""Task Modules"""
from etl.utils.commons import module_format, read_file_line_by_line, check_fobj_exists
from etl.modules import AGGREGATED_INFO, USER_FILE, DEFAULT_SAMPLE_USERS, \
    CLEANED_FILE_NAME_TEMPLATE, SAMPLE_USERS_FILE


class IO():

    def __init__(self, definition):
        # Module Definition
        self.definition = definition
        # Pretty Print Module Name
        module_format(self.definition['name'])
        # From Aggregated info file, get the total users recorded
        self._calculate_percentage(self.definition['percentage'])
        # Initiate write file stream buffer
        self.write_fileObj = open(SAMPLE_USERS_FILE, 'w')

    def _calculate_percentage(self, sample_percentage):
        """ Calculate Number of total users * (give_percentage_to_sample) """
        assert check_fobj_exists(AGGREGATED_INFO), "Aggregate info file not found in " \
                                                   + AGGREGATED_INFO
        info = open(AGGREGATED_INFO, 'r')
        self.to_sample_users = DEFAULT_SAMPLE_USERS
        for data in info:
            if USER_FILE in json.loads(data):
                self.to_sample_users = int(json.loads(data)[USER_FILE] * (sample_percentage/100))

    def read_fle(self, filename):
        assert check_fobj_exists(filename), "Cleaned User File not found in " + filename
        return read_file_line_by_line(filename)

    def _sample_users(self):
        """ Write x percentage of users to 'sample_users.csv file (with headers) '"""
        writer = None
        for count, line in enumerate(self.read_fle(CLEANED_FILE_NAME_TEMPLATE + USER_FILE)):
            if count < self.to_sample_users:
                data = json.loads(line)
                if count == 0:
                    writer = csv.DictWriter(self.write_fileObj, data.keys())
                    writer.writeheader()
                writer.writerow(data)
        self.write_fileObj.close()

    def run(self):
        start_time = time.time()
        self._sample_users()
        print("--- %s seconds ---" % (time.time() - start_time))
        module_format(self.definition['name'], type=1)
