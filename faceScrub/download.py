import os
from os.path import join, exists
import requests  # to get image from the web
import shutil  # to save it locally
import time
from dask import delayed, compute
import uuid

def download(line):
    """ 
        download the url image and save it in the folder 
        with the name of the label
    """
    folder = f"./images/{line['name']}"

    if not exists(folder):
        os.mkdir(folder)

    filename = f"{folder}/{uuid.uuid4()}.{line['ext']}"
    try:
        r = requests.get(line['url'], stream=True)

        if r.status_code == 200:
            r.raw.decode_content = True

            with open(filename, 'wb') as f:
                shutil.copyfileobj(r.raw, f)
            print('Image sucessfully Downloaded: ', f'./{filename}')
        else:
            print('Image Couldn\'t be retreived')
    except:
        print('Image Couldn\'t be retreived')

    

def load_files(files=['facescrub_actors.txt', 'facescrub_actresses.txt']):
    """
    Read files and return a list of actors

    Args:
        files (list, optional): List files faceScrup. Defaults to ['facescrub_actors.txt', 'facescrub_actresses.txt'].
    Returns:
        [list]: Return list actors and actresses
    """
    data = []
    for f in files:
        with open(f, 'r') as fd:
            fd.readline()
            for line in fd.readlines():
                components = line.split('\t')
                assert(len(components) == 6)
                name = components[0].replace(' ', '_')
                url = components[3]
                bbox = [int(_) for _ in components[4].split(',')]
                data.append({
                    'name': name,
                    'ext': url.split('/')[-1].split('.')[-1],
                    'url': url,
                    'bbox': bbox
                })

    return data

if __name__ == '__main__':

    # Create directory
    if not exists(f'./images'):
        os.mkdir(f'./images')

    # Load data from files
    data = load_files()
    print(f'{len(data)} files loaded.')

    start_time = time.time()

    task_dask = []

    # Download file
    for line in data:
        task = delayed(download)(line)
        task_dask.append(task)

    
    result_dask = compute(*task_dask)

    print("--- %s seconds ---" % (time.time() - start_time))



