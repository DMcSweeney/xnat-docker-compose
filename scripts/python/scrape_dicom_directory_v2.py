"""
Script for scraping a directory of DICOM files
And generating a database with information from headers
"""

import os
import sqlite3
import SimpleITK as sitk
import glob
from tqdm import tqdm
import time
from multiprocessing import Process, Queue, Pool, Manager, cpu_count
from queue import Empty

# What trial arm does the data belong to?
trial_arm = 'AJ'
# Path to raw data
root_dir = '/mnt/md0/stampede/AJ-test' 
## Database name 
db_filename = f'./outputs/audit/allScansData_{trial_arm}_TEST.db'

## CPU cores to use when multiprocessing
cpus_to_use=cpu_count()//2

debug = False


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
    acquisition_date text,
    UNIQUE(patient_id, series_uid, study_uid, filepath, dirname, modality, series_date, study_date, acquisition_date)
    );"""

error_schema = """CREATE TABLE IF NOT EXISTS errors (
    id integer PRIMARY KEY,
    filepath text NOT NULL,
    dirname text NOT NULL,
    error text NOT NULL,
    UNIQUE(filepath, dirname, error)
    );"""

#### +++++++++++++++++++++++++++++++++++++++++++++++++
def main():
    global paths_to_skip
    ## Initialise the database and figure out what paths have been analysed
    paths_to_skip = init_db(db_filename)

    ## Go through source dir and get top-level directories to process (usually by patientID)
    paths_to_scan = glob.glob(os.path.join(root_dir, '*'))
    print(f"------ Processing {len(paths_to_scan)} paths in {root_dir} -------")
    
    ## Add tasks to queue and allocate to separate workers
    task_queue = Queue()
    print('Finding directories to scan...')
    with Pool(cpus_to_use) as pool:
        res = pool.map(filter_directories, [path for path in paths_to_scan])
    
    ## Flatten results from all workers
    tasks = [x for r in res for x in r]
    for t in tasks:
        task_queue.put(t)

    print(f'Using {cpus_to_use} CPUs to process approx. {task_queue.qsize()} directories')
    workers=[]
    for _ in range(cpus_to_use):
        p = Process(target=process_directory, args=(task_queue,))
        p.start()
        workers.append(p)

    # Wait for workers to finish
    for p in workers:
        p.join()


def process_directory(task_queue):
    while True:
        try:
            job = task_queue.get(timeout=0.001)
        except Empty:
            print('Queue is empty')
            break
        if job['path'] is None:
            print('No path')
            break
        scan_directory(job)

### HELPERS ###
def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        #print(f'SQLITE version:', sqlite3.version)
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

def fetch_missing_filepaths(directory):
    ## If directory already in database but not all files are accounted for in dicomdb or errorsdb
    ## Get files that need to be analysed
    ## Get files in dicomdb
    with create_connection(db_filename) as conn:
        conn.row_factory = lambda cursor, row: row[0]
        cursor = conn.cursor()

        paths_to_skip = cursor.execute(f"SELECT filepath FROM dicomdb WHERE dirname LIKE '{directory}';").fetchall()
        errors_to_skip = cursor.execute(f"SELECT filepath FROM errors WHERE dirname LIKE '{directory}';").fetchall()
        filepaths = {x for x in paths_to_skip} | {y for y in errors_to_skip} ## Union of both sets 

    return list(filepaths)

def filter_directories(source):
    ## Creates list of directories to scan
    ## Drops directories that are already in the database

    paths = []
    for root, dirs, files in os.walk(source):
        if files:
            
            ## If this directory has been processed and all the files have been accounted for..
            if root in paths_to_skip and len(files) == paths_to_skip[root]:
                #print('Skipping')
                continue
            
            ## If it has been processed but the length doesn't match, need to figure out what files to skip
            if root in paths_to_skip and len(files) != paths_to_skip[root]:
                filepaths = fetch_missing_filepaths(root)
                paths.append({'path': filepaths, 'type': 'file'})
                continue

            ## Skip these -- SimpleITK can't read them and they're not useful
            ## This assumes directories are organised as: PatientID/Study Description/Series Description
            if '[CT - KEY IMAGES]' in root:
                continue
            if '[PT - KEY IMAGES]' in root:
                continue
            if '[NM - SAVE SCREENS]' in root:
                continue

            if debug:
                print('Should this path be skipped: ', root in paths_to_skip)
                if root in paths_to_skip:
                    print(f'Found {len(files)} files but {paths_to_skip[root]} in database')
                print(root)
            
            paths.append({'path': root, 'type': 'directory'})

    print(f"Found {len(paths)} paths to scan in {source}")
    return paths

def record_error(path, e):
    #print('ERROR:', e)
    err = {'filepath': path, 'error': str(e), 'dirname': os.path.dirname(path)}
    columns = ', '.join(err.keys())
    placeholders = ':'+', :'.join(err.keys())
    sql = """INSERT OR IGNORE INTO errors (%s) VALUES (%s)""" % (columns, placeholders)
    
    with create_connection(db_filename) as conn:
        cursor = conn.cursor()
        cursor.execute(sql, err)
        conn.commit()
    
    #queue.put((sql, err))

def scan_directory(job):
    path, type_ = job['path'], job['type']
    if type_ == 'file':
        ## Path is list of files to skip
        files_to_skip = path
        path = os.path.dirname(path[0])
    else:
        files_to_skip = []

    for file in tqdm(os.listdir(path), position=1, leave=False):
        filepath = os.path.join(path, file)
        if type_ == 'file' and filepath in files_to_skip:
            continue

        try:
            header = read_header(filepath)
        except Exception as e:
            record_error(filepath, e)
            continue
        if header is None:
            record_error(filepath, "Can't open file!")
            continue

        header['filepath'] = filepath
        header['trial_arm'] = trial_arm

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
        sql = """INSERT OR IGNORE INTO dicomdb (%s) VALUES (%s)""" % (columns, placeholders)      

        with create_connection(db_filename) as conn:
            cursor = conn.cursor()
            cursor.execute(sql, header)
            conn.commit()
            
            #queue.put((sql, header))
        #print(header['patient_id'])


def init_db(db_filename):
    conn = create_connection(db_filename)
    cursor = conn.cursor()
    create_table(conn, schema)
    create_table(conn, error_schema)
    # Check the above worked
    res = cursor.execute("SELECT name from sqlite_master").fetchone()
    assert res is not None, "Database doesn't exist"
    
    ## Get filepaths already analysed
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
    
    conn.close()
    return paths_to_skip



if __name__ == '__main__':
    main()