"""
Script for reading database and organising into directories for every study

Updated for better error handling.

After running this, run upload-from-inbox.py
"""
import os
import sqlite3
from datetime import datetime
from tqdm import tqdm
import SimpleITK as sitk
import polars as pl


PROJECT='STAMPEDE-AG' # Project ID from XNAT 
db_filename = './outputs/audit/allScansData_AG.db'
#csv_filename = "/mnt/d/xnat/XNAT-stampede/python/outputs/allScansData_AJ_small.csv"
trial_arm='AG'
data_mount_directory = '/mnt/j/'


error_filename= f'./logs/ERRORS-{trial_arm}-organise-for-inbox-{datetime.now().strftime("%Y-%m-%d--%H:%M")}.db'
target_dir = '/mnt/d/xnat/1.8/inbox/'#'/mnt/h/ACE_batches' ## /mnt/h/AG_batches

#path_to_csv = '/mnt/d/xnat/XNAT-STAMPEDE/csv/AJ_altID_to_trialID_trimmed.csv' #AltID to trial ID conversion
path_to_csv = '/mnt/d/patientID_to_trialNo.csv'

# SCHEMAS
error_schema = """CREATE TABLE IF NOT EXISTS errors (
    id integer PRIMARY KEY,
    subject_id text NOT NULL,
    study_uid text NOT NULL,
    error text NOT NULL);"""
upload_schema = """CREATE TABLE IF NOT EXISTS upload_status (
    id integer PRIMARY KEY,
    project_id text NOT NULL,
    subject_id text NOT NULL,
    experiment_id text NOT NULL,P
    status_code text NOT NULL);"""

def scan_for_empty_directories(path, batched=False):
    """
    Scans output directory for any empty directories (usually caused by errors in previous runs)
    Set batched = True if the output has already been split into batches for upload (via split_inbox_into_batch.py)
    """


    empty_dirs = []
    non_empty_dirs = []
    print('Scanning for empty directories')

    if batched:

        for batch in tqdm(os.listdir(path)):
            for dir_name in tqdm(os.listdir(os.path.join(path, batch)), position=1, leave=False ):
                exp_dir = os.path.join(path, batch, dir_name)
                #print(exp_dir)
                if len(os.listdir(exp_dir)) == 0:
                    #print('dirname ', dir_name)
                    empty_dirs.append(dir_name)
                else:
                    non_empty_dirs.append(dir_name)

    else:
        for dir_name in tqdm(os.listdir(path), position=1, leave=False ):
            exp_dir = os.path.join(path, dir_name)
            #print(exp_dir)
            if len(os.listdir(exp_dir)) == 0:
                #print('dirname ', dir_name)
                empty_dirs.append(dir_name)
            else:
                non_empty_dirs.append(dir_name)
            
    return empty_dirs, non_empty_dirs


def create_connection(db_file):
    print(f'Starting connection to {db_file}')
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except sqlite3.Error as e:
        print(e)
    return conn

def create_table(conn, schema):
    try:
        c = conn.cursor()
        c.execute(schema)
    except sqlite3.Error as e:
        print(e)

def process_study(subset, session_path, subject_id, **kwargs):
    filepaths = subset.select("filepath").to_series().to_list()
    study_uid = subset.select("study_uid").unique().item()
    for filepath in tqdm(filepaths, position=1, leave=False):
        filepath = filepath.replace('/mnt/d/', data_mount_directory)
        filename = os.path.basename(filepath)
        slice_ = load_slice(filepath, subject_id, study_uid)
        if slice_ is None:
            print("Can't load slice")
            continue # Catch if error loading slice
        # Write slice with updated metadata 
        writer = sitk.ImageFileWriter()
        writer.KeepOriginalImageUIDOn()
        writer.SetFileName(os.path.join(session_path, filename))
        try:
            writer.Execute(slice_)
        except Exception as e:
            error = {'subject_id': subject_id, 'study_uid': study_uid, 'error': str(e)}
            columns = ', '.join(error.keys())
            placeholders = ':'+', :'.join(error.keys())
            sql = """INSERT INTO errors (%s) VALUES (%s)""" % (columns, placeholders)
            err_cursor.execute(sql, error)
            err.commit()
            continue


def load_slice(path, trial_id, study_uid):
    # Check missing info and replace where needed
    # Minimum is change PID to trialNo
    # Update study date

    reader = sitk.ImageFileReader()
    reader.SetFileName(path)
    reader.LoadPrivateTagsOn()
    reader.ReadImageInformation()
    
    try:
        slice_ = reader.Execute()
    except Exception as e:
        error = {'subject_id': trial_id, 'study_uid': study_uid, 'error': str(e)}
        columns = ', '.join(error.keys())
        placeholders = ':'+', :'.join(error.keys())
        sql = """INSERT INTO errors (%s) VALUES (%s)""" % (columns, placeholders)
        err_cursor.execute(sql, error)
        err.commit()
        return

    slice_.SetMetaData('0010|0010', trial_id) # Patient Name
    slice_.SetMetaData('0010|0020', trial_id) # Patient ID
    return slice_

def main():
    global err, err_cursor
    # Make db for catching errors + POST response status
    err = create_connection(error_filename)
    err_cursor = err.cursor()
    create_table(err, error_schema)
    create_table(err, upload_schema)

    ## Load trial ID <-> altID csv
    if trial_arm == 'AJ':
        id_df = pl.read_csv(path_to_csv, dtypes={'patient_id': str, 'trialno':str})
    else:
        id_df = None

    os.makedirs(os.path.join(target_dir, PROJECT), exist_ok=True)    
    
    empty_dirs, non_empty_dirs = scan_for_empty_directories(os.path.join(target_dir, PROJECT))
    print(f'Found {len(empty_dirs)} empty directories and {len(non_empty_dirs)} non-empty directories.')
    print(empty_dirs)
    #exit()
    # Connect to imaging database
    global df
    conn = create_connection(db_filename)
    df = pl.read_database("SELECT * from dicomdb", conn)
    #df = pl.read_csv('./outputs/audit/debugging_AltID892.csv')#csv_filename)

    num_patients = df.select("patient_id").n_unique()
    # Group by study UID
    # Don't do by series UID otherwise XNAT groups everything by series.
    groups = df.unique("study_uid", maintain_order=True)
    print(f"{num_patients} patient(s) with {len(groups)} studies to process")

    for row in tqdm(groups.rows(named=True), position=0):
        subset = df.filter(pl.col("study_uid") == row['study_uid'])
        try:
            patID = subset.select("patient_id").unique().item()
        except ValueError:
            print(f'Too many patient IDs??: {subset.select("patient_id").unique()}')
            error = {'subject_id': f"{subset.select('patient_id').unique()}", 'study_uid': row['study_uid'], 'error': f'Too many patient IDs: {subset.select("patient_id").unique()} -- study_date: {subset.select("study_date").unique()}'}
            columns = ', '.join(error.keys())
            placeholders = ':'+', :'.join(error.keys())
            sql = """INSERT INTO errors (%s) VALUES (%s)""" % (columns, placeholders)
            err_cursor.execute(sql, error)
            err.commit()
            continue
        
        # Remove whitespace
        patID = patID.strip()

        if trial_arm == 'AJ':
            assert 'AltID' in patID
            #altID = int(patID.lstrip('AltID'))
            print(f'Reading {patID}')
            try:
                trial_id = str(id_df.filter(pl.col("patient_id")== str(patID)).select("trialno").item())
            except ValueError as e:
                print(f"Couldn't find matching trial ID for {patID}. Row: {row}")

                error = {'subject_id': patID, 'study_uid': row['study_uid'], 'error': str(e)}
                columns = ', '.join(error.keys())
                placeholders = ':'+', :'.join(error.keys())
                sql = """INSERT INTO errors (%s) VALUES (%s)""" % (columns, placeholders)
                err_cursor.execute(sql, error)
                err.commit()
                continue

        else:
            trial_id = str(patID)

        id_ = subset.select("id")[0].item()
        modalities = subset.select("modality").unique().to_series().to_list()
        modalities = [x.strip() for x in modalities if x is not None]
        



        if len(modalities) != 1 and 'OT' in modalities:
            modalities.remove('OT')
        if len(modalities) != 1 and 'SC' in modalities:
            modalities.remove('SC')
        if len(modalities) != 1 and 'SR' in modalities:
            modalities.remove('SR')
        if len(modalities) != 1 and 'SD' in modalities:
            modalities.remove('SD')
        if len(modalities) != 1 and 'CR' in modalities:
            modalities.remove('CR')
        if len(modalities) != 1 and 'RTIMAGE' in modalities:
            modalities.remove('RTIMAGE')
        if len(modalities) != 1 and 'SEG' in modalities:
            modalities.remove('SEG')

        if len(modalities) == 1:
            modality = modalities[0]
        elif len(modalities) == 2 and sorted(modalities) == ['CT', 'PT']:
            modality = 'PT'
        elif len(modalities) == 2 and sorted(modalities) == ['CT', 'NM']:
            modality = 'NM'

        else:
            print(f'Too many modalities detected: {modalities}. {subset}')
            print(subset['filepath'][:1])
            error = {'subject_id': patID, 'study_uid': row['study_uid'], 'error': f'Too many modalities detected: {modalities}.'}
            columns = ', '.join(error.keys())
            placeholders = ':'+', :'.join(error.keys())
            sql = """INSERT INTO errors (%s) VALUES (%s)""" % (columns, placeholders)
            err_cursor.execute(sql, error)
            err.commit()
            continue
        
        #continue


        # Make output directory
        experiment_id = f'{trial_id}_{modality}_{id_}'#

        session_path = os.path.join(target_dir, PROJECT, experiment_id)
        if experiment_id in non_empty_dirs:
            print(f"{experiment_id} is non-empty directory, skipping")
            continue

        if experiment_id in empty_dirs:
            print(f'Attempting to process and empty directory: {experiment_id}')

        if os.path.isdir(session_path):
            print(f'{session_path} already processed, skipping')
            continue
        
        params= {'subset': subset, 'session_path': session_path, 'subject_id': trial_id, 'experiment_id': experiment_id}
        print(f'Processing: {params}')
        os.makedirs(params['session_path'], exist_ok=True)
        process_study(**params)

        #break

if __name__ == '__main__':
    main()