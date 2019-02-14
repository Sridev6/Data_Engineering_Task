import os, time, json, tarfile, gc, pandas as pd, sys
from itertools import islice

"""Task Modules"""
from etl.utils.commons import module_format, check_fobj_exists, remove_file
from etl.modules import CLEANED_FILE_NAME_TEMPLATE, AGGREGATED_INFO, USER_FILE


class IO():

    def __init__(self, definition):
        # Module Definition
        self.definition = definition
        # Pretty Print Module Name
        module_format(self.definition['name'])

    def format_input(self, files_to_process):
        """ Add more info to file process procedure """
        return [(file, self.definition) for file in files_to_process]

    def _delete_file_if_exists(self):
        """ Remove files when we run it the next time, so that results are refreshed """
        if check_fobj_exists(AGGREGATED_INFO):
            remove_file(AGGREGATED_INFO)

    def _check_file_existence(self, file_to_check):
        """ Remove files when we run it the next time, so that results are refreshed """
        if file_to_check == USER_FILE and check_fobj_exists(AGGREGATED_INFO):
            remove_file(AGGREGATED_INFO)


    def process_file(self, inputs_to_process):
        file_to_process, definition = inputs_to_process
        print("Processing for file : ", file_to_process)
        # File to record total records in processed files
        line_count = 0
        file_to_write = CLEANED_FILE_NAME_TEMPLATE + file_to_process
        try:
            # If file already exists, do not decompress and clean
            if not os.path.isfile(file_to_write):
                # When decompressing user file, re-write aggregate file
                self._check_file_existence(file_to_process)
                # open files to read and write
                aggregated_info = open(AGGREGATED_INFO, 'a+')
                tar = tarfile.open(definition['filename'], "r")
                write_file_obj = open(file_to_write, 'w', buffering=100 * (1024 ** 2))
                # Extract only required files from tar
                for member in tar:
                    if member.name == file_to_process:
                        # Process file object line by line
                        fobj = tar.extractfile(member)
                        for line in fobj:
                            line_count += 1
                            # Decode bytes to JSON
                            jsonstr = line.decode('utf8').replace("','", '","')
                            try:
                                # Extract specific fields from JSON
                                json_data = json.loads(jsonstr)
                                updated_data = dict(
                                    (k, json_data[k]) for k in definition['to_extract_files'][file_to_process])
                                # Write NEWLINE delimited JSON along with new lines at the end
                                write_file_obj.write(json.dumps(updated_data))
                                write_file_obj.write('\n')
                            except ValueError:
                                print(jsonstr)
                                print('Decoding JSON has failed')
                        break
                    # Recoup tar members to gain memory and collect garbage
                    tar.members = []
                    gc.collect()
                # Close all opened files
                write_file_obj.close()
                tar.close()
                # Push total rows count to the info file
                aggregated_info.write(json.dumps({file_to_process: line_count}))
                aggregated_info.write("\n")
                aggregated_info.close()
            else:
                print(file_to_write + " Exists! Hence skipping !!")
        except Exception as e:
            print(e)

    def run(self):
        try:
            start_time = time.time()
            files_to_process = self.definition['to_extract_files'].keys()
            for files in self.format_input(files_to_process):
                self.process_file(files)
            print("--- %s seconds ---" % (time.time() - start_time))
        finally:
            module_format(self.definition['name'], type=1)