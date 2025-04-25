import os
import glob
import shutil


source_dir ='/mnt/h/ACE_batches'
target_dir = '/mnt/h/ACE_batches_to_upload'


def main():
    inbox = glob.glob(os.path.join(source_dir, '*'))
    i=0
    batch_num=1
    for series in inbox:
        print(i, series)
        expt_id = series.replace(source_dir, '')
        output_dir = os.path.join(target_dir, f'batch_{batch_num}')
        os.makedirs(output_dir, exist_ok=True)
        shutil.move(series, output_dir)
        if i == 1000:
            # Reset counter and update batch num
            i=0
            batch_num += 1
            
        else:
            i+=1
        #break


if __name__ == '__main__':
    main()