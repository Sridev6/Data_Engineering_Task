import os
import json, csv
import pandas as pd

from pathlib import Path

from etl.utils.error_handlers import handle_errors

@handle_errors
def read_file(filepath, reader=None,
              header=None):
    """
    Read Extension specific files
    :param filepath: string
    :return: list
    """
    if reader:
        return open(filepath, reader)
    file_ext = get_file_format(filepath)
    if file_ext in ('.csv'):
        return pd.read_csv(filepath, usecols=header)
    else:
        with open(filepath) as data_file:
            if file_ext in ('.js', '.json'):
                return json.load(data_file)
            else:
                return data_file.read()

def read_csv_line_as_dict(filepath, key=None):
    """
    Reads every row as a key, value pair with specified
    row index as key
    :param filepath: filename - string
    :param key: optional row index value
    :return: dict
    """
    with open(filepath, mode='r') as fobj:
        reader = csv.DictReader(fobj)
        dataDict = {}
        for row in reader:
            k = row[key]
            del row[key]
            dataDict[k] = row
    return dataDict

def create_directory(path):
    if not os.path.exists(path):
        os.makedirs(path)

def check_fobj_exists(path, type=0):
    if type == 0 and os.path.isfile(path):
        return True
    if type == 1 and os.path.isdir(path):
        return True
    return False

def remove_file(filepath):
    os.remove(filepath)

def delete_folder(folderpath):
    for the_file in os.listdir(folderpath):
        file_path = os.path.join(folderpath, the_file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
                # elif os.path.isdir(file_path): shutil.rmtree(file_path)
        except Exception as e:
            print(e)


def get_file_format(filepath):
    """
    Get the File extension i.e 'json', 'csv'
    :param filepath: string
    :return: string
    """
    return Path(filepath).suffix

def module_format(str, type=0):
    module_type = "start"
    if type == 1:
        module_type = "end"
    print("\n\n")
    print(" ######## ", module_type.upper(), " ", str.upper(), " MODULE  ######## ")
    print("\n\n")

def get_base(path, hops=3):
    """
    Get the parent directory for the file path
    :param path: file path - string
    :param hops: number of parents to go back
    :return: string
    """
    if hops > 0:
        path = os.path.dirname(path)
        return get_base(path, hops-1)
    else:
        return path

def read_file_line_by_line(filename):
    """
    Read a file and yield every line
    in the file
    :param filename: string
    :return: line - string
    """
    with open(filename, 'r') as fobj:
        for line in fobj:
            yield line


def diff_lists(full_list, sub_list):
    """
        Returns a list containing the items in the first list
        but not in the second one.
    """
    return [x for x in full_list if x not in sub_list]