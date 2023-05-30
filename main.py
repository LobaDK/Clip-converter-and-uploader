# Import required modules
import logging
import os
import random
import sys
import time
import multiprocessing
from json import loads
from pathlib import Path
from subprocess import CalledProcessError, run, CREATE_NEW_CONSOLE

import httplib2
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from oauth2client.client import flow_from_clientsecrets
from oauth2client.clientsecrets import InvalidClientSecretsError
from oauth2client.file import Storage
from oauth2client.tools import run_flow
from tqdm import tqdm

logging.basicConfig(
    format='%(asctime)s %(name)s %(levelname)s %(message)s',
    datefmt='%d-%b-%y %H:%M:%S',
    filename='clip converter and uploader.log',
    filemode='w',
    level=logging.DEBUG
)

logger = logging.getLogger('main.py')

subfolder_upload_whitelist = ['Grand Theft Auto V']
whitelisted_extensions = ['.mkv', '.mp4']

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
    try:
        flow = flow_from_clientsecrets(CLIENT_SECRETS_FILE,
            scope=YOUTUBE_SCOPES)

        storage = Storage("%s-oauth2.json" % sys.argv[0])
        credentials = storage.get()

        if credentials is None or credentials.invalid:
            log_info('No valid credentials found. Attempting to authenticate with the YouTube API')
            credentials = run_flow(flow, storage)

        return build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION, http=credentials.authorize(httplib2.Http()))
    
    except InvalidClientSecretsError as e:
        print('"client_oath.json" could not be found or had errors')
        log_exception(e)
        exit()
    
    except Exception as e:
        print('Unknown error. Check logs for details')
        log_exception(e)
        exit()

# Function for uploading the video.
# This should be multithreaded with the converter
def upload_video(file: str):
    log_info('Creating body for uploading')
    body=dict(
        snippet=dict(
            title=Path(file).stem,
            description='Icon & outro by @Stardust_Buckethead',
            categoryId='20'
        ),
        status=dict(
            privacyStatus='private'
        )
    )

    log_info('Creating insert request')
    insert_request = youtube.videos().insert(
        part=','.join(body.keys()),
        body=body,
        media_body=MediaFileUpload(file, chunksize=1024 * 1024, resumable=True)
    )

    resumable_upload(file, insert_request)

def resumable_upload(filename, insert_request):
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
            if e.resp.status in RETRIABLE_STATUS_CODES:
                error = "A retriable HTTP error %d occurred:\n%s" % (e.resp.status,
                                                                    e.content)
            else:
                progress_bar.close()
                raise
        except RETRIABLE_EXCEPTIONS as e:
            error = f"A retriable error occurred: {e}"

        if error is not None:
            print(error)
            retry += 1
            if retry > MAX_RETRIES:
                progress_bar.close()
                exit("No longer attempting to retry.")

            max_sleep = 2 ** retry
            sleep_seconds = random.random() * max_sleep
            print(f"Sleeping {sleep_seconds} seconds and then retrying...")
            time.sleep(sleep_seconds)


# Function for searching own channel for the video
def video_exists_on_channel(filename: str) -> bool:
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
def convert_to_av1():
    for root, dirs, files in os.walk(r'E:\Recordings'):
        for dirname in dirs:
            # If the folder is the "lossless" folder where I keep my edited clips
            if dirname == 'lossless':
                
                # Log "lossless" folder location
                log_info(f'Found {Path(root, dirname)}!')

                # Get list of files and iterate over them.
                # If folder is empty, an empty list will be returned, and thus not run
                for filename in os.listdir(Path(root, dirname)):
                    
                    # I exlusively work with the mp4 and mkv containers.
                    # If the file does not have either, assume it should be ignored
                    if Path(filename).suffix.casefold() not in whitelisted_extensions:
                        log_info(f'Skipping {filename} with reason: Not in an mp4 or mkv container')
                        continue
                    
                    # Use the root folder variable to create a variable
                    # containing the path for the converted folder
                    # and a variable containing the path as well as filename
                    # for the new converted file
                    dirname_converted = Path(root, output_folder)
                    full_file_path_converted = Path(root, output_folder, f"{os.path.splitext(filename)[0]}.mp4")
                    full_file_path = Path(root, dirname, filename)

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
                    
                    p = multiprocessing.Process(target=upload_video, args=(full_file_path))

                    if any(_ in root for _ in subfolder_upload_whitelist):
                        log_info('Video is in whitelisted subfolder. Checking if video has been uploaded...')
                        
                        if video_exists_on_channel(filename):
                            log_info(f'{filename} has aleady been uploaded')
                        
                        else:
                            log_info('No matching title found on channel. Uploading...')
                            # MULTITHREADING SEEMS TO BREAK THE Google API's LIBRARY'S ABILITY TO AUTHORIZE
                            # THIS IS GONNA BE A HEADACHE...
                            # SHOULD BE FIXED NOW? i JUST NEED TO TEST IT MORE
                            #upload_video(full_file_path, upload_event)
                            p.start()
                            


                    # Check if a file with the same name already exists
                    # in the converted folder.
                    # If their frame counts do not match
                    # delete, log and convert it.
                    # Otherwise, assume it has aleady been converted and log it
                    if os.path.exists(full_file_path_converted):
                        if get_video_length(full_file_path) != get_video_length(full_file_path_converted):
                            os.remove(full_file_path_converted)
                            log_info(f'Removed converted {filename} with reason: Framecount mismatch')
                        
                        else:
                            log_info(f'Skipping {filename} with reason: Already exists')
                            continue
                    

                    # Let the user know which file is about to be converted, and log it
                    print(f'\nConverting {filename}\n')
                    log_info(f'Converting {filename}.')
                    
                    # Create a list with ffmpeg and it's paramters, for a high-quality medium-slow AV1 encoding
                    # a CRF of 45 may seem too high, but it's the perfect mix between
                    # low filesize and good-enough quality for online sharing.
                    cmd = ['ffmpeg', '-n', '-i', str(full_file_path), '-c:v',
                        'libsvtav1', '-preset', '4', '-crf', '45', '-b:v', '0', '-c:a', 'aac', '-b:a',
                        '192k', '-movflags', '+faststart',
                        str(full_file_path_converted)]

                    # Attempt to run command, and assume any non-zero codes are bad
                    # This isn't the best, but in our case it should be more than fine
                    # If exit-code is non-zero, log the exception and continue with the next file
                    try:
                        run(cmd, check=True, creationflags=CREATE_NEW_CONSOLE)
                    except CalledProcessError as e:
                        log_exception('Process error occured')
                        exit()
                    except FileNotFoundError as e:
                        log_exception('Failed to find ffmpeg executable')
                        print('No ffmpeg exectuable was found.')
                        exit()
                    except KeyboardInterrupt:
                        print('Keyboard interrupt received. Quitting...')
                        os.remove(full_file_path_converted)
                        log_info(f'Removed converted {filename} with reason: Keyboard interrupt')
                        exit()

                    p.join()

            # Log folders ignored by the script
            else:
                log_info(f'Ignoring folder {Path(root, dirname)}.')

# Function for getting the length of the original and converted video, in frames
def get_video_length(filename: str) -> int:
    cmd = ['ffprobe', '-v', 'error', '-select_streams', 'v:0',
           '-show_entries', 'stream=nb_frames', '-of', 'json', str(filename)]

    try:
        p = run(cmd, check=True, capture_output=True)
    except CalledProcessError as e:
        log_exception(e)
        print('Error getting video durations. Check logs for details')
        if 'Invalid data found when processing input' in e.stderr.decode():
            os.remove(filename)
            log_info(f'Removed converted {filename} with reason: Corruption or unfinished encoding')
        exit()
    except FileNotFoundError as e:
        log_exception('Failed to find ffprobe executable')
        print('No ffprobe exectuable was found.')
        exit()

    try:
        frames = int(loads(p.stdout)['streams'][0]['nb_frames'])
        return frames
    except KeyError as e:
        log_exception(e)
        print('Could not find any frames metadata in the video')
        exit()


# Logging functions
def log_info(message: str):
    logger.info(message)

def log_exception(e: Exception):
    logger.exception('Exception occurred')

youtube = get_authenticated_service()

if __name__ == '__main__':
    log_info('#Starting script#')

    
    convert_to_av1()
