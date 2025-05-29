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
from multiprocessing import Pool, cpu_count
import gc
## PATHS

# Path to raw data
root_dir = '/mnt/md0/stampede/AJ-test' 

# What trial arm does the data belong to?
trial_arm = 'AJ'

# Enter a path here to skip all directories prior to it. Useful if the script crashed and need to go back to the error.
## Leave as empty string to ignore
end_skip = ''

## Tags to read from every dicom header
header_keys = {
    'patient_id': '0010|0010', 
    'series_date':'0008|0021',
    'study_date': '0008|0020',
    'series_uid': '0020|000e',
    'study_uid': '0020|000d',
    'modality': '0008|0060',
    'acquisition_date': '0008|0022'
}

#++++++++++++++  DATABASE SCHEMAS ++++++++++++++++++++
schema = """CREATE TABLE IF NOT EXISTS dicomdb (
    id integer PRIMARY KEY,
    patient_id text NOT NULL,
    trial_arm text NOT NULL,
    series_uid text NOT NULL,
    study_uid text NOT NULL,
    filepath text NOT NULL,
    dirname text NOT NULL,
    modality text,
    series_date text,
    study_date text,
    acquisition_date text
    );"""

error_schema = """CREATE TABLE IF NOT EXISTS errors (
    id integer PRIMARY KEY,
    filepath text NOT NULL,
    dirname text NOT NULL,
    error text NOT NULL);"""

#### +++++++++++++++++++++++++++++++++++++++++++++++++

##### ================= MAIN PROCESS =======================
def main():
    # Setup logic to skip paths if end_skip is defined
    if end_skip:
        skip=True
    else: 
        skip=False

    init_db(f'./outputs/audit/allScansData_{trial_arm}_TEST.db')
    
    ## Go through source dir and get paths to process
    paths_to_scan = glob.glob(os.path.join(root_dir, '*'))
    print(f"------ Processing {len(paths_to_scan)} paths in {root_dir} -------")
    
    ## Split paths amongst workers
    cpus = cpu_count()//2
    pool = Pool(processes=cpus)
    print(f'Allocating {cpus} cpus to the job!')
    #pool.map_async
    
    #for path in paths_to_scan:
        # if path == end_skip:
        #     skip = False

        # if skip:
        #     continue
            #print(f'Scanning {path}')


        #print(f'Adding {path} to Pool')
    pool.map(thread_process, paths_to_scan)
    pool.close()
    #pool.join()

    conn.close()

def thread_process(path):
    filepaths = filter_filepaths(path)
    if not filepaths: #If empty:
        return
    collect_subject_info(filepaths, trial_arm)

####=============================================================


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
    #print('ERROR:', e)
    err = {'filepath': path, 'error': str(e), 'dirname': os.path.dirname(path)}
    columns = ', '.join(err.keys())
    placeholders = ':'+', :'.join(err.keys())
    sql = """INSERT INTO errors (%s) VALUES (%s)""" % (columns, placeholders)
    cursor.execute(sql, err)
    conn.commit()

def filter_filepaths(source):
    paths = []
    for root, dirs, files in os.walk(source):
        if files:
            ## If this directory has been processed and all the files have been accounted for..
            if root in paths_to_skip and len(files) == paths_to_skip[root]:
                continue
            
            ## Skip these -- SimpleITK can't read them and they're not useful
            if '[CT - KEY IMAGES]' in root:
                continue
            if '[PT - KEY IMAGES]' in root:
                continue
            if '[NM - SAVE SCREENS]' in root:
                continue

            #print("APPENDING: ", root)
            paths.append(root)
    print(f"Found {len(paths)} paths to scan in {source}")
    return paths

def collect_subject_info(paths, arm):
    for path in paths:
        for file in tqdm(os.listdir(path), position=1, leave=False):
            filepath = os.path.join(path, file)

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

            ##  These entries can't be null in DB schema--if empty replace with filename
            ## Should be very rare that these are empty but allows user to find the files and manually get info if needed.

            if header['patient_id'] is None:
                header['patient_id'] = filepath

            if header['series_uid'] is None:
                header['series_uid'] = filepath

            if header['study_uid'] is None:
                header['study_uid'] = filepath

            # Get dirname 
            header['dirname'] = os.path.dirname(filepath)

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

    ## Figure out directories to skip and number of files.
    dicoms_to_skip = cursor.execute(f"SELECT dirname, COUNT(*) FROM dicomdb GROUP BY dirname").fetchall()
    ## Convert to a dict
    paths_to_skip = {k: v for k, v in dicoms_to_skip}

    ## Repeat for files in the errors
    errors_to_skip = cursor.execute(f"SELECT dirname, COUNT(*) FROM errors GROUP BY dirname").fetchall()

    ## Aggregate total number of files scanned in the directory
    for (path, num_files) in errors_to_skip:
        if path in paths_to_skip:
            paths_to_skip[path] += num_files
        else:
            paths_to_skip[path] = num_files    
    print(f"----- Paths to skip: {len(paths_to_skip.keys())} -----")
    
    ## Delete from memory
    del dicoms_to_skip, errors_to_skip
    gc.collect()


if __name__ == '__main__':
    main()
