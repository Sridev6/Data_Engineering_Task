import subprocess, time

"""Task Modules"""
from etl.modules import REVIEW_FILE, USER_FILE
from etl.utils.commons import module_format, check_fobj_exists, create_directory

class IO():

    def __init__(self, definition):
        # Module Definition
        self.definition = definition
        # Pretty Print Module Name
        module_format(self.definition['name'])
        self.file_existence = self.check_file_existence()

    def check_file_existence(self):
        if check_fobj_exists(self.definition['to_save_path'] + REVIEW_FILE) and \
                check_fobj_exists(self.definition['to_save_path'] + USER_FILE):
            return True
        else:
            if not check_fobj_exists(self.definition['to_save_path'], type=1):
                create_directory(self.definition['to_save_path'])
        return False

    def run(self):
        try:
            start_time = time.time()
            print(self.definition['to_save_path'])
            if not self.file_existence:
                print("Extracting files . . .")
                subprocess.run(["tar", "-zxvf",
                                self.definition['filename'],
                                "-C",
                                self.definition['to_save_path']])
            print("--- %s seconds ---" % (time.time() - start_time))
        finally:
            module_format(self.definition['name'], type=1)