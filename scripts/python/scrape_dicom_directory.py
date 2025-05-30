"""
Script for scraping a directory of DICOM files (D:\All Scans).
And generating a database with available imaging, should help keep track of upload progress...
"""
import os
import glob
import polars as pl
import sqlite3
from tqdm import tqdm
import SimpleITK as sitk
import traceback

## PATHS
OS = 'UNIX' # or UNIX --- this is just to handle different paths
root_dir = '/mnt/j/All Scans/' 



#root_dir = 'G:\\ArmK\\DICOM\\'
arms_to_check = ['ACE']#'ACE' 'AG''AH',
# skippable = ['SR Dose Report', '] 
header_keys = {
    'patient_id': '0010|0010', 
    'series_date':'0008|0021',
    'study_date': '0008|0020',
    'series_uid': '0020|000e',
    'study_uid': '0020|000d',
    'modality': '0008|0060',
    'acquisition_date': '0008|0022'
}

#++++++++++++++  Database
schema = """CREATE TABLE IF NOT EXISTS dicomdb (
    id integer PRIMARY KEY,
    patient_id text NOT NULL,
    trial_arm text NOT NULL,
    series_uid text NOT NULL,
    study_uid text NOT NULL,
    filepath text NOT NULL,
    modality text,
    series_date text,
    study_date text,
    acquisition_date text
    );"""

error_schema = """CREATE TABLE IF NOT EXISTS errors (
    id integer PRIMARY KEY,
    filepath text NOT NULL,
    error text NOT NULL);"""
#### +++++++++++++++++

### HELPERS ###
def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(f'SQLITE version:', sqlite3.version)
    except sqlite3.Error as e:
        print(e)
    return conn

def create_table(conn, schema):
    try:
        c = conn.cursor()
        c.execute(schema)
    except sqlite3.Error as e:
        print(e)

def read_header(path):
    reader = sitk.ImageFileReader()
    reader.LoadPrivateTagsOn()
    reader.SetFileName(path)
    reader.ReadImageInformation()
    metadata = {}
    for k in reader.GetMetaDataKeys():
        v = reader.GetMetaData(k)
        if v == '': #Replace empty string
            v=None
        metadata[k] = v
    data = {}
    for key, val in header_keys.items():
        try:
            data[key] = metadata[val]
        except KeyError:
            data[key] = None # For sql
    return data

def record_error(path, e):
    print('ERROR:', e)
    err = {'filepath': path, 'error': str(e)}
    columns = ', '.join(err.keys())
    placeholders = ':'+', :'.join(err.keys())
    sql = """INSERT INTO errors (%s) VALUES (%s)""" % (columns, placeholders)
    cursor.execute(sql, err)
    conn.commit()

def filter_filepaths(source):
    paths = []
    for root, dirs, files in os.walk(source):
        if files:
            #for file in files:
            #filepath = os.path.join(root, file)
            if OS == 'WINDOWS':
                unix_stem = root.lstrip(root_dir).replace('\\', '/')
                if unix_stem in paths_to_skip:
                    continue
            else:
                windows_stem = root.lstrip(root_dir).replace('/', '\\')
                if windows_stem in paths_to_skip:
                    continue
                elif root.lstrip(root_dir) in paths_to_skip:
                    continue
                # if root in paths_to_skip:
                #     continue
            


            if '[CT - KEY IMAGES]' in root:
                continue
            if '[PT - KEY IMAGES]' in root:
                continue
            if '[NM - SAVE SCREENS]' in root:
                continue


            print("APPENDING: ", root)
            paths.append(root)
    print(f"Found {len(paths)} paths to scan.")
    return paths

def collect_subject_info(paths, arm):
    for path in paths:
        for file in tqdm(os.listdir(path), position=1, leave=False):
            filepath = os.path.join(path, file)
            #print(filepath)
            try:
                header = read_header(filepath)
            except Exception as e:
                record_error(filepath, e)
                continue
            if header is None:
                record_error(filepath, "Can't open file!")
                continue

            header['filepath'] = filepath
            header['trial_arm'] = arm

            #print(filepath)

            if header['patient_id'] is None:
                header['patient_id'] = filepath

            if header['series_uid'] is None:
                header['series_uid'] = filepath

            if header['study_uid'] is None:
                header['study_uid'] = filepath

            # Insert into db
            columns = ', '.join(header.keys())
            placeholders = ':'+', :'.join(header.keys())
            sql = """INSERT INTO dicomdb (%s) VALUES (%s)""" % (columns, placeholders)
            cursor.execute(sql, header)
            conn.commit()

############
def init_db(db_filename):
    global conn, cursor
    conn = create_connection(db_filename)
    cursor = conn.cursor()
    create_table(conn, schema)
    create_table(conn, error_schema)
    # Check the above worked
    res = cursor.execute("SELECT name from sqlite_master").fetchone()
    assert res is not None, "Database doesn't exist"
    
    ## Get filepaths already analysed
    global paths_to_skip
    cursor.row_factory = lambda cursor, row: row[0]
    paths_to_skip = cursor.execute(f"SELECT DISTINCT filepath from dicomdb").fetchall()
    if OS == 'WINDOWS':
        paths_to_skip = {os.path.dirname(x).lstrip(unix_root_dir) for x in paths_to_skip}
    else:
        paths_to_skip = {os.path.dirname(x).lstrip(root_dir) if root_dir in x else os.path.dirname(x) for x in paths_to_skip }
    
    
    ## Skip the errors
    errors_to_skip = cursor.execute(f'SELECT DISTINCT filepath from errors').fetchall()
    if OS == 'WINDOWS':
        errors_to_skip = {os.path.dirname(x).lstrip(unix_root_dir) for x in paths_to_skip}
    else:
        errors_to_skip = {os.path.dirname(x).lstrip(root_dir) if root_dir in x else os.path.dirname(x) for x in errors_to_skip }
    
    print(f"----- Paths to skip: {len(paths_to_skip)} -----")
    cursor.row_factory = sqlite3.Row
    paths_to_skip = paths_to_skip | errors_to_skip # union of two sets

def main():
    skip = False # Use this as a flag to quickly skip all directories up to end_skip 
    end_skip = '/mnt/j/All\\ Scans/' #! THIS NEEDS TO BE UPDATED

    for arm in arms_to_check:
        init_db(f'./outputs/audit/allScansData_{arm}.db')
        #TODO replace with commented out line if not scanning AK.
        #source_dir = root_dir
        source_dir = os.path.join(root_dir, arm)
        
        ## Go through source dir and get paths to process
        paths_to_scan = glob.glob(os.path.join(source_dir, '*'))
        print(f"------ Processing {len(paths_to_scan)} paths in {source_dir} -------")
        
        ## Walk through source directory & collect all series info
        for path in tqdm(paths_to_scan, position=0):
            if path == end_skip:
                skip = False
            
            if skip:
                continue

            print(f'Scanning {path}')
            filepaths = filter_filepaths(path)
            collect_subject_info(filepaths, arm)

if __name__ == '__main__':
    main()