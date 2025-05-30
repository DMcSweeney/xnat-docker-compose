"""
Script for making REST calls to XNAT based on an organised project directory mounted at /data/xnat/inbox

TODO:
- Add monitoring calls. Maybe in a different script? Shell script or python?
"""
import os
import requests
from tqdm import tqdm
import glob
import time
import polars as pl
from datetime import datetime

PROJECT='STAMPEDE-AG'
#path_to_cert = '/mnt/d/xnat/XNAT-stampede/configs/xnat-release/ssl/xnat-vagrant-CA.pem' 
#'D:\\xnat\\XNAT-stampede\\configs\\xnat-release\\ssl\\xnat-vagrant-CA.pem'
#url = 'https://release.xnat.org'
#url = 'http://192.168.56.101:80'
#url = 'http://172.21.80.1:8080'
url = 'http://localhost:80'
#batch = 'batch_6' ### Last tried 4 - skipping to end
#source_dir =f'/mnt/d/xnat/1.8/inbox/STAMPEDE-AJ/{batch}/' 


# 

def check_xnat(subject_id, experiment_id, **kwargs):
    ## Check if session to upload has already been uploaded
    with requests.Session() as sess:
        sess.auth = ('admin', 'admin')
        #sess.verify=path_to_cert

        res = sess.get(f"{url}/data/projects/{PROJECT}/subjects/{subject_id}/experiments/{experiment_id}/")
        if res.status_code == 200: #If exists, return True
            return True
        else:
            return False

def get_experiments_to_skip():
    ## Check if session to upload has already been uploaded
    with requests.Session() as sess:
        sess.auth = ('admin', 'admin')
        #sess.verify=path_to_cert

        res = sess.get(f"{url}/data/projects/{PROJECT}/experiments?format=json")
        if res.status_code == 200: #If exists, return True
            return res.json()
        else:
            raise ValueError

def main_loop(source_dir, batch):
    uploads = glob.glob(os.path.join(source_dir, '*'))
    print(f"Submitting {len(uploads)} uploads")
    
    ## Experiments to skip
    exp_in_project = get_experiments_to_skip()
    experiments_to_skip = [x['label'] for x in exp_in_project['ResultSet']['Result']] 
    print(f"Found {len(experiments_to_skip)} experiments to skip")
    experiments_to_upload = [x for x in uploads if x.replace(source_dir, '') not in experiments_to_skip]
    print(f"Attempting to upload {len(experiments_to_upload)} experiments")
    #exit()

    fails = 0
    failed_paths = []
    for upload in tqdm(experiments_to_upload):
        expt_id = upload.replace(source_dir, '')
        subject_id = expt_id.split('_')[0]
        if batch is None:
            path = upload.replace(source_dir, f'/data/xnat/inbox/{PROJECT}/')
        else:
            path = upload.replace(source_dir, f'/data/xnat/inbox/{PROJECT}/{batch}/')
        if expt_id in experiments_to_skip:
            print(f'SKipping {expt_id}')
            continue

        with requests.Session() as sess:
            sess.auth = ('admin', 'admin')
            #sess.verify=path_to_cert
            print(f'Posting {path} to {subject_id} - {expt_id}')
            res = sess.post(f"{url}/data/services/import?import-handler=inbox&cleanupAfterImport=false&PROJECT_ID={PROJECT}&SUBJECT_ID={subject_id}&EXPT_LABEL={expt_id}&path={path}")
            if res.status_code != 200:
                fails += 1
                failed_paths.append({'status': res.status_code, 'path': upload})
                print(f"Upload failed with status code: {res.status_code} -- FAIL # {fails}")
            # Wait one minute to upload
            #print('Waiting...')
            #time.sleep(60)

            #break


    return failed_paths
        #break

def main():
    #batches = ['batch_1', 'batch_2', 'batch_3', 'batch_4', 'batch_5', 'batch_6', 'batch_7', 'batch_8', 'batch_9', 'batch_10']
    batches=None
    #failed_outputs = {}
    
    if batches is not None:
        for batch in batches:
            #source_dir = f'D:\\xnat\\1.8\\inbox\\STAMPEDE-AJ\\{batch}\\'
            source_dir = f'/mnt/d/xnat/1.8/inbox/{PROJECT}/'
            # source_dir=f'/mnt/h/ACE_batches_to_upload/{batch}/'
            _ = main_loop(source_dir, batch)

    else:
        source_dir = f'/mnt/d/xnat/1.8/inbox/{PROJECT}/'
        _ = main_loop(source_dir, batches) 

if __name__ == '__main__':
    main()
