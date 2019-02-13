import os, json, tarfile, gc, pandas as pd, sys
from itertools import islice

BASE_DATA_FOLDER = os.getcwd() + "/data/"
CLEANED_FILE_NAME_TEMPLATE = BASE_DATA_FOLDER + "extracted_"
AGGREGATED_INFO = BASE_DATA_FOLDER + "aggregated_info.json"
USER_FILE = "user.json"
REVIEW_FILE = "review.json"
SAMPLE_USERS_FILE = BASE_DATA_FOLDER + "sample_users.csv"
SAMPLE_USERS_REVIEW_FILE = BASE_DATA_FOLDER + "sample_users_review.csv"
SAMPLE_USERS_NO_REVIEW_FOLDER = BASE_DATA_FOLDER + "sample_users_no_review_within_time_interval/"
DEFAULT_SAMPLE_USERS = 10000
SAMPLE_USERS_ID_FIELD = ['user_id']

def process_file(inputs_to_process):
    file_to_process, definition = inputs_to_process[0]
    print("Processing for file : ", file_to_process )
    # File to record total records in processed files
    aggregated_info = open(AGGREGATED_INFO, 'a+')
    line_count = 0
    file_to_write = CLEANED_FILE_NAME_TEMPLATE + file_to_process
    try:
        if not os.path.isfile(file_to_write):
            tar = tarfile.open(definition['filename'], "r")
            write_file_obj = open(file_to_write, 'w', buffering=100 * (1024 ** 2))
            for member in tar:
                if member.name == file_to_process:
                    fobj = tar.extractfile(member)
                    for line in fobj:
                        line_count += 1
                        jsonstr = line.decode('utf8').replace("','", '","')
                        try:
                            json_data = json.loads(jsonstr)
                            updated_data = dict(
                                (k, json_data[k]) for k in definition['to_extract_files'][file_to_process])
                            write_file_obj.write(json.dumps(updated_data))
                            write_file_obj.write('\n')
                        except ValueError:
                            print(jsonstr)
                            print('Decoding JSON has failed')
                    break
                tar.members = []
                gc.collect()
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


def get_chunks(fobj, n=500000):
    while True:
        lines = list(islice(fobj, n))
        if not lines:
            break
        yield lines

def process(inputs_to_process):
    file_to_process, definition = inputs_to_process[0]
    print("Processing for file : ", file_to_process)
    file_to_write = CLEANED_FILE_NAME_TEMPLATE + 'review.csv'
    if not os.path.isfile(file_to_write):
        tar = tarfile.open(definition['filename'], "r")
        for member in tar:
            print(member.name)
            if member.name == file_to_process:
                fobj = tar.extractfile(member)
                for count, next_lines in enumerate(get_chunks(fobj)):
                    if not next_lines:
                        break
                    print(count)
                    header = False
                    if count == 0:
                        header = True
                    pd.DataFrame.from_records((pd.Series(next_lines).str.decode('utf8'))
                                              .replace("','", '","')
                                              .apply(json.loads).values.tolist())\
                        .to_csv(file_to_write, index=False, header=header,mode='a+',
                                columns=definition['to_extract_files'][file_to_process])
                break
    else:
        print(file_to_write + " Exists! Hence skipping !!")
