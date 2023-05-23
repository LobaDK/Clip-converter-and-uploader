# Import required modules
from distutils.command.upload import upload
import logging
import httplib2
import sys
import random
import time
import os

from tqdm import tqdm
from pathlib import Path
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client.tools import run_flow


logging.basicConfig(
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%d-%b-%y %H:%M:%S',
    filename='convert & uploader.log',
    filemode='a',
    level=logging.DEBUG
)

output_folder = 'AV1'

httplib2.RETRIES = 1

MAX_RETRIES = 10

RETRIABLE_EXCEPTIONS = (httplib2.HttpLib2Error, IOError)

RETRIABLE_STATUS_CODES = [500, 502, 503, 504]

CLIENT_SECRETS_FILE = 'client_oauth.json'

YOUTUBE_SCOPES = ['https://www.googleapis.com/auth/youtube.readonly',
                    'https://www.googleapis.com/auth/youtube.upload']
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"

def get_authenticated_service():
    flow = flow_from_clientsecrets(CLIENT_SECRETS_FILE,
        scope=YOUTUBE_SCOPES)

    storage = Storage("%s-oauth2.json" % sys.argv[0])
    credentials = storage.get()

    if credentials is None or credentials.invalid:
        credentials = run_flow(flow, storage)

    return build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, http=credentials.authorize(httplib2.Http()))

# Function for uploading the video.
# This should be multithreaded with the converter
def upload_video(file: str, youtube):
    body=dict(
        snippet=dict(
            title=Path(file).stem,
            description='Icon & outro by @stardust-buckethead8594',
            categoryId='20'
        ),
        status=dict(
            privacyStatus='private'
        )
    )

    insert_request = youtube.videos().insert(
        part=','.join(body.keys()),
        body=body,
        media_body=MediaFileUpload(file, chunksize=1024 * 1024, resumable=True)
    )

    resumable_upload(insert_request)

def resumable_upload(insert_request):
    response = None
    error = None
    retry = 0
    file_size = os.path.getsize(filename)
    progress_bar = tqdm(total=file_size, unit='bytes', unit_scale=True, desc='Uploading')
    while response is None:
        try:
            status, response = insert_request.next_chunk()
            if status:
                progress = int(status.progress() * file_size)
                progress_bar.update(progress - progress_bar.n)

            if response is not None:
                progress_bar.close()
                if 'id' in response:
                    print("Video id '%s' was successfully uploaded." % response['id'])
                else:
                    exit("The upload failed with an unexpected response: %s" % response)
        except HttpError as e:
            progress_bar.close()
            if e.resp.status in RETRIABLE_STATUS_CODES:
                error = "A retriable HTTP error %d occurred:\n%s" % (e.resp.status,
                                                                    e.content)
            else:
                raise
        except RETRIABLE_EXCEPTIONS as e:
            progress_bar.close()
            error = f"A retriable error occurred: {e}"

        if error is not None:
            print(error)
            retry += 1
            if retry > MAX_RETRIES:
                exit("No longer attempting to retry.")

            max_sleep = 2 ** retry
            sleep_seconds = random.random() * max_sleep
            print(f"Sleeping {sleep_seconds} seconds and then retrying...")
            time.sleep(sleep_seconds)

filename = r"C:\Users\nichel\Downloads\Recordings\GTA5\lossless\GTA5 Raphy_952 the modder 2023-02-06 00-54-28.mp4"

upload_video(filename)

# Function for searching own channel for the video
def video_exists_on_channel(filename: str, youtube) -> bool:
    response = youtube.search().list(
        part='snippet',
        forMine=True,
        maxResults=1,
        q=Path(filename).stem,
        type='video'
    ).execute()

    total_results = int(response['pageInfo']['totalResults'])

    return total_results != 0


# Function for converting clip to AV1
# This should be multithreaded with the uploader
def convert_to_av1(youtube):
    for root, dirs, files in os.walk(r'E:\Recordings'):
        for dirname in dirs:
            # If the folder is the "lossless" folder where I keep my edited clips
            if dirname == 'lossless':
                
                # Log "lossless" folder location
                log_info(f'Found {os.path.join(root, dirname)}!')

                # Get list of files and iterate over them.
                # If folder is empty, an empty list will be returned, and thus not run
                for filename in os.listdir(os.path.join(root, dirname)):
                    
                    # Use the root folder variable to create a variable
                    # containing the path for the converted folder
                    # and a variable containing the path as well as filename
                    # for the new converted file
                    dirname_converted = os.path.join(root, output_folder)
                    filename_converted = os.path.join(root, output_folder, f"{os.path.splitext(filename)[0]}.mp4")

                    # I exlusively work with the mp4 and mkv containers.
                    # If the file does not have either, assume it should be ignored
                    if os.path.splitext(filename)[1].casefold() == ('.mkv' or '.mp4'):
                        log_info(f'Skipping {filename_converted}. Reason: Not an mp4 or mkv container')
                        continue

                    # Check if the folder for converted clips does not exist
                    # and create it, as well as log it, if it does not
                    # If it fails, it will instead log the exception and continue with the next file/folder
                    if not os.path.exists(dirname_converted):
                        try:
                            os.mkdir(dirname_converted)
                            log_info(f'Created {dirname_converted}.')
                        except OSError as e:
                            log_exception(e)
                            continue

                    # Check if a file with the same name already exists
                    # in the converted folder.
                    # If it is 1,048,576 bytes (1 megabyte) or less
                    # delete, log and convert it.
                    # Otherwise, assume it has aleady been converted and log it
                    if os.path.exists(filename_converted):
                        if os.stat(filename_converted).st_size <= 1_048_576:
                            os.remove(filename_converted)
                            log_info(f'Removed {filename_converted} due to being 1 megabyte or less in size')
                        
                        else:
                            log_info(f'Skipping {filename_converted}. Reason: Already exists')
                            continue

                    log_info('Checking if video has been uploaded...')
                    
                    if video_exists_on_channel(filename, youtube):
                        log_info('Video has aleady been uploaded')
                    
                    else:
                        log_info('No matching title found on channel')
                        



# Logging functions
def log_info(message: str):
    logging.info(message)

def log_exception(e: Exception):
    logging.exception('Exception occurred')


if __name__ == '__main__':
    log_info('#################'
             '\n\t\t\t#Starting script#'
             '\n\t\t\t#################')

    youtube = get_authenticated_service()
    
    convert_to_av1(youtube)