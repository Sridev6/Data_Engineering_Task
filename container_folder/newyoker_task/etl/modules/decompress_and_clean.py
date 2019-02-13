import psutil, time
import multiprocessing as mp

"""Task Modules"""
from etl.utils.commons import module_format
from etl.modules import CLEANED_FILE_NAME_TEMPLATE, AGGREGATED_INFO, process_file


class IO():

    def __init__(self, definition):
        # Module Definition
        self.definition = definition
        # Pretty Print Module Name
        module_format(self.definition['name'])

    def format_input(self, files_to_process):
        """ Add more info to file process procedure """
        return [(file, self.definition) for file in files_to_process]

    def run(self):
        try:
            start_time = time.time()
            # Depending on number of CPU cores, processes efficiency differs
            num_cores = mp.cpu_count()
            #pool = mp.Pool(processes=num_cores)
            files_to_process = self.definition['to_extract_files'].keys()
            process_file(self.format_input(files_to_process))
            #pool.map(process, self.format_input(files_to_process))
            #pool.close()
            print("This kernel has ", num_cores, "cores and you can find the information regarding the memory usage:",
                   psutil.virtual_memory())
            print("--- %s seconds ---" % (time.time() - start_time))
        finally:
            module_format(self.definition['name'], type=1)