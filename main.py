# Import required modules
import logging
import os
import random
import sys
import time
import multiprocessing
from json import loads
from pathlib import Path
from subprocess import CalledProcessError, run, Popen, PIPE, STDOUT
from logging.handlers import QueueHandler
from queue import Empty

import httplib2
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
from oauth2client.client import flow_from_clientsecrets
from oauth2client.clientsecrets import InvalidClientSecretsError
from oauth2client.file import Storage
from oauth2client.tools import run_flow
from tqdm import tqdm

class Values:
    # List of extensions/containers from which the script will convert to AV1 MP4
    whitelisted_extensions = ['.mkv', '.mp4']

    # Folder the converted files will be stored in, relative to the folder they came from
    output_folder = 'AV1'

    # Tell httplib not to handle retrying after errors, as we handle it ourselves
    httplib2.RETRIES = 1

    # Upload retry attempts before quitting
    MAX_RETRIES = 10

    # Exceptions that still allow us to retry
    RETRIABLE_EXCEPTIONS = (httplib2.HttpLib2Error, IOError)

    # Status codes that still allow us to retry
    RETRIABLE_STATUS_CODES = [500, 502, 503, 504]

    # Name of the oauth file containing the oauth data for the project
    CLIENT_SECRETS_FILE = 'client_oauth.json'

    # Scopes we'll be using in the API
    YOUTUBE_SCOPES = ['https://www.googleapis.com/auth/youtube.readonly',
                        'https://www.googleapis.com/auth/youtube.upload']

    # Name of the service we're using
    YOUTUBE_API_SERVICE_NAME = "youtube"

    # Version of the service we're using
    YOUTUBE_API_VERSION = "v3"

    # Add empty youtube variable for storing the youtube API object
    youtube = None

    # Add empty queue variable for storing the Queue object
    queue = None


# Logger function and thread
def logger_process(queue: multiprocessing.Queue):
    # Create logger
    logger = logging.getLogger()
    
    # Create format formatted as
    # <date> <logger name> <log level> <log message>
    # and the date format as
    # <numerical day>-<numerical month>-<last 2 digits of year> hh:mm:ss
    log_format = logging.Formatter(fmt='%(asctime)s %(name)s %(levelname)s %(message)s',
                                   datefmt='%d-%b-%y %H:%M:%S')
    
    # Create logging handler that uses a file on disk as the log location
    # and overwrites on new instances
    logger_handle = logging.FileHandler(
            filename='clip converter and uploader.log',
            mode='w')

    # Apply the format in the handler
    logger_handle.setFormatter(log_format)
    # Add the handler to the logger
    logger.addHandler(logger_handle)
    # Set minimum required log level severity
    logger.setLevel(logging.DEBUG)

    # Listen indefinitely for new logs
    while True:
        try:
            # Wait and block until new log is added to the queue
            log = queue.get()
            
            # We use None to signal end of execution
            if log is None:
                break
            
            # Call and use the previously created logger handler
            logger.handle(log)
        
        # If we use CTLR+C to quit the script early
        # we wanna make sure the queue is emptied
        # and the thread can close to help prevent deadlocks
        except KeyboardInterrupt:
            try:
                while True:
                    _ = queue.get(block=False)
            except Empty:
                queue.close()
            
            break

# Returns an object that can be used to interact with the API
def get_authenticated_service(values: Values):
    # Create the logger, add the queue handler
    # and set the minimum log severity
    logger = logging.getLogger('authenticator')
    logger.addHandler(QueueHandler(values.queue))
    logger.setLevel(logging.DEBUG)

    try:
        # Create a flow object from the oauth file and scopes
        flow = flow_from_clientsecrets(values.CLIENT_SECRETS_FILE,
            scope=values.YOUTUBE_SCOPES)

        # Create a storage object from a previously saved oauth token
        # and get the credentials. If it doesn't exist, credentials will be None
        storage = Storage("%s-oauth2.json" % sys.argv[0])
        credentials = storage.get()

        # If the credentials didn't already exist or are incorrect
        # get new credentials and save the token to disk
        if credentials is None or credentials.invalid:
            logger.info('No valid credentials found. Running local webserver to authenticate with user')
            credentials = run_flow(flow, storage)

        # Build and return the object used to interact with the YouTube API
        return build(values.YOUTUBE_API_SERVICE_NAME, values.YOUTUBE_API_VERSION, http=credentials.authorize(httplib2.Http()))
    
    # If the oauth file does not exist or is incorrectly formatted/corrupted
    # and log it
    except InvalidClientSecretsError as e:
        print('"client_oath.json" could not be found or had errors')
        logger.exception(e)
        exit()
    
    # Catch any other error and log it as well
    except Exception as e:
        print('Unknown error. Check logs for details')
        logger.exception(e)
        exit()

# Function for uploading the video.
# This should be multithreaded with the converter
def upload_video(file: str, values: Values):
    # Create the logger, add the queue handler
    # and set the minimum log severity
    logger = logging.getLogger('uploader')
    logger.addHandler(QueueHandler(values.queue))
    logger.setLevel(logging.DEBUG)

    logger.info('Authenticating for upload')
    
    # We're authenticating again here because the
    # youtube._http.connections object is an SSLSocket
    # and cannot be serialized/copied to the new thread.
    # If this is not done, or the youtube object is not global
    # the youtube object will lose the entire SSLSocket connection object
    # and fail with something like:
    # 'HttpError 401 when requesting None returned
    # "Request is missing required authentication credentials...'
    youtube = get_authenticated_service(values)

    logger.info('Creating body for uploading')

    # Remore the upload flag from the filename
    # that'll be used as the video title
    filename = str(file).replace(' ytupload', '')

    # Create a body dictionary containing the
    # video title, description and category
    # as well as the privacy status
    body=dict(
        snippet=dict(
            title=Path(filename).stem,
            description='Icon & outro by @Stardust_Buckethead',
            categoryId='20'
        ),
        status=dict(
            privacyStatus='private'
        )
    )

    # Create an insert_request object used
    # to upload the video, with the body dictonary as the body
    # and a chunksize of 1 Mebibyte that is resumeable
    logger.info('Creating insert request')
    insert_request = youtube.videos().insert(
        part=','.join(body.keys()),
        body=body,
        media_body=MediaFileUpload(file, chunksize=1024 * 1024, resumable=True)
    )

    resumable_upload(file, insert_request, values)

def resumable_upload(filename, insert_request, values: Values):
    # Create the logger, add the queue handler
    # and set the minimum log severity
    logger = logging.getLogger('resumeable_uploader')
    logger.setLevel(logging.DEBUG)
    logger.addHandler(QueueHandler(values.queue))
    
    response = None
    error = None
    retry = 0

    # Get the size of the file in bytes, and use it as the "goal" in tqdm
    file_size = os.path.getsize(filename)
    progress_bar = tqdm(total=file_size, unit='bytes', unit_scale=True, desc='Uploading', position=0)
    logger.info(f'Uploading {Path(filename).stem}')
    
    # response will be None until upload is complete
    while response is None:
        try:
            status, response = insert_request.next_chunk()
            if status:

                # status.resumable_progress returns the total uploaded bytes so far.
                # By subtracting it from the current progress bar's progress, we add only
                # the newly uploaded chunk.
                progress_bar.update(status.resumable_progress - progress_bar.n)

            if response is not None:

                # When upload is complete, no status is returned, so the last
                # bit of progress gets handled here, where we instead use
                # the filesize of the file, to add the remaining progress
                progress_bar.update(file_size - progress_bar.n)
                
                progress_bar.close()
                
                if 'id' in response:
                    print(f"Successfully uploaded {Path(filename).stem}\nWith ID {response['id']}\nAt https://studio.youtube.com/video/{response['id']}/edit")
                else:
                    exit("The upload failed with an unexpected response: %s" % response)
        
        except HttpError as e:
            if e.resp.status in values.RETRIABLE_STATUS_CODES:
                error = "A retriable HTTP error %d occurred:\n%s" % (e.resp.status,
                                                                    e.content)
            else:
                progress_bar.close()
                raise
        except values.RETRIABLE_EXCEPTIONS as e:
            error = f"A retriable error occurred: {e}"

        if error is not None:
            print(error)
            retry += 1
            if retry > values.MAX_RETRIES:
                progress_bar.close()
                exit("No longer attempting to retry.")

            max_sleep = 2 ** retry
            sleep_seconds = random.random() * max_sleep
            print(f"Sleeping {sleep_seconds} seconds and then retrying...")
            time.sleep(sleep_seconds)


# Function for searching own channel for the video
def video_exists_on_channel(filename: str) -> bool:

    filename = str(filename).replace(' ytupload', '')

    response = values.youtube.search().list(
        part='snippet',
        forMine=True,
        maxResults=1,
        q=Path(filename).stem,
        type='video'
    ).execute()

    total_results = int(response['pageInfo']['totalResults'])

    return total_results != 0


# Function for converting clip to AV1
def convert_to_av1(values: Values):
    # Create the logger, add the queue handler
    # and set the minimum log severity
    logger = logging.getLogger('converter')
    logger.setLevel(logging.DEBUG)
    logger.addHandler(QueueHandler(values.queue))

    for root, dirs, files in os.walk(r'C:\Users\nichel\Downloads\Recordings'):
        for dirname in dirs:
            # If the folder is the "lossless" folder where I keep my edited clips
            if dirname == 'lossless':
                
                # Log "lossless" folder location
                logger.info(f'Found {Path(root, dirname)}!')

                # Get list of files and iterate over them.
                # If folder is empty, an empty list will be returned, and thus not run
                for filename in os.listdir(Path(root, dirname)):
                    
                    # I exlusively work with the mp4 and mkv containers.
                    # If the file does not have either, assume it should be ignored
                    if Path(filename).suffix.casefold() not in values.whitelisted_extensions:
                        logger.info(f'Skipping {filename} with reason: Not in an mp4 or mkv container')
                        continue
                    
                    # Use the root folder variable to create a variable
                    # containing the path for the converted folder
                    # and a variable containing the path as well as filename
                    # for the new converted file
                    dirname_converted = Path(root, values.output_folder)
                    full_file_path_converted = Path(root, values.output_folder, f"{os.path.splitext(filename)[0]}.mp4")
                    full_file_path = Path(root, dirname, filename)

                    # Check if the folder for converted clips does not exist
                    # and create it, as well as log it, if it does not
                    # If it fails, it will instead log the exception and continue with the next file/folder
                    if not os.path.exists(dirname_converted):
                        try:
                            os.mkdir(dirname_converted)
                            logger.info(f'Created {dirname_converted}.')
                        except OSError as e:
                            logger.exception(e)
                            continue

                    mp = multiprocessing.Process(target=upload_video, args=(full_file_path, values))

                    # the phrase "ytupload" in the filename will be used
                    # to tell the script it should upload the video.
                    # If it is not in the filename, then it should not be uploaded
                    if 'ytupload' in filename.casefold():
                        logger.info('Video is marked for upload. Checking if video has been uploaded...')
                        
                        if video_exists_on_channel(filename):
                            logger.info(f'{filename} has aleady been uploaded')
                        
                        else:
                            logger.info('No matching title found on channel. Uploading...')
                            mp.start()
                            


                    # Check if a file with the same name already exists
                    # in the converted folder.
                    # If their frame counts do not match
                    # delete, log and convert it.
                    # Otherwise, assume it has aleady been converted and log it
                    if os.path.exists(full_file_path_converted):
                        if get_video_length(full_file_path, values) != get_video_length(full_file_path_converted, values):
                            os.remove(full_file_path_converted)
                            logger.info(f'Removed converted {filename} with reason: Framecount mismatch')
                        
                        else:
                            logger.info(f'Skipping {filename} with reason: Already exists')
                            continue
                    

                    # Log the file we're about to convert
                    logger.info(f'Converting {filename}.')
                    
                    # Create a list with ffmpeg and it's paramters, for a high-quality medium-slow AV1 encoding
                    # a CRF of 45 may seem too high, but it's the perfect mix between
                    # low filesize and good-enough quality for online sharing.
                    cmd = ['ffmpeg', '-v', 'fatal', '-n', '-i', str(full_file_path),
                        '-progress', '-', '-c:v', 'libsvtav1', '-preset', '4',
                        '-crf', '45', '-b:v', '0', '-c:a', 'aac', '-b:a', '192k',
                        '-movflags', '+faststart', str(full_file_path_converted)]

                    frames = get_video_length(full_file_path, values)

                    ffmpeg_progress_bar = tqdm(total=frames, unit='frames', desc='Converting', position=2)

                    # Run process and args from above in a non-blocking way
                    # and pipe the stdout and stderr outputs.
                    # Reading the output is blocking, and therefore stderr is piped to stdout
                    # to make sure we're always reading from a pipe that has data
                    # wether it be the progress of the conversion or an error
                    try:
                        p = Popen(cmd, stdout=PIPE, stderr=STDOUT)

                        # Run an infinite loop
                        while True:
                            # Decode the output to pure text
                            stdout = p.stdout.readline().decode()
                            
                            # Get rid of newlines. It's not actually required
                            # but it bothers me knowing each 2nd line is basically empty
                            # without it
                            stdout = stdout.replace('\n', '')
                            
                            # if the current string in our output is the frames progress
                            if 'frame=' in stdout:

                                    # Add only the new frames by subtracting the total converted with the total progress
                                    ffmpeg_progress_bar.update(int(stdout.split('=')[1]) - ffmpeg_progress_bar.n)
                            
                            # If the current string in our output instead is the returned progress type.
                            # ffmpeg uses this to display if it's done or not, by being either "continue"
                            # or "end"
                            if 'progress=' in stdout:

                                    # if the progress is end, it means it's done converting, and we can break out of the loop
                                    if stdout.split('=')[1] == 'end': break

                            # If the process exited due to the file already existing
                            if 'already exists' in stdout:
                                    break

                        ffmpeg_progress_bar.close()
                    
                    except FileNotFoundError as e:
                        ffmpeg_progress_bar.close()
                        logger.exception('Failed to find ffmpeg executable')
                        print('No ffmpeg exectuable was found.')
                        exit()
                    
                    except KeyboardInterrupt:
                        ffmpeg_progress_bar.close()
                        print('Keyboard interrupt received. Quitting...')
                        os.remove(full_file_path_converted)
                        logger.info(f'Removed converted {filename} with reason: Keyboard interrupt')
                        exit()

                    mp.join()

            # Log folders ignored by the script
            else:
                logger.info(f'Ignoring folder {Path(root, dirname)}.')

# Function for getting the length of the original and converted video, in frames
def get_video_length(filename: str, values: Values) -> int:
    # Create the logger, add the queue handler
    # and set the minimum log severity
    logger = logging.getLogger('video_length')
    logger.addHandler(QueueHandler(values.queue))
    logger.setLevel(logging.DEBUG)

    cmd = ['ffprobe', '-v', 'error', '-select_streams', 'v:0',
           '-show_entries', 'stream=nb_frames', '-of', 'json', str(filename)]

    try:
        p = run(cmd, check=True, capture_output=True)
    
    except CalledProcessError as e:
        logger.exception(e)
        print('Error getting video durations. Check logs for details')
        if 'Invalid data found when processing input' in e.stderr.decode():
            os.remove(filename)
            logger.info(f'Removed converted {filename} with reason: Corruption or unfinished encoding')
        exit()
    
    except FileNotFoundError as e:
        logger.exception('Failed to find ffprobe executable')
        print('No ffprobe exectuable was found.')
        exit()

    try:
        frames = int(loads(p.stdout)['streams'][0]['nb_frames'])
        return frames
    
    except KeyError as e:
        logger.exception(e)
        print('Could not find any frames metadata in the video')
        exit()


if __name__ == '__main__':
    # Instance an object of our values class for easier passing and workflow
    values = Values()

    # Create and add a multiprocessing queue to our values object
    values.queue = multiprocessing.Queue()

    # Set the name of the program the logs will appear under
    # This will make it easier to see which script the log appeared from
    logger = logging.getLogger('main')

    # Add queue handler using our queue inside the object
    # and set the minimum log severity
    logger.addHandler(QueueHandler(values.queue))
    logger.setLevel(logging.DEBUG)

    # create and start logger thread
    logger_p = multiprocessing.Process(target=logger_process, args=(values.queue,))
    logger_p.start()

    logger.info('#Starting script#')
    logger.info(f'Started logging on {logger_p.name}')

    # Create and add youtube API interaction object
    # to our values object
    values.youtube = get_authenticated_service(values)
    
    # Later versions do not seem to play nice with the Google API modules
    # resulting in uploads failing with an error resembling
    # "Redirected but the response is missing a Location: header"
    # if a chunksize is specified in MediaFileUpload.
    # External sources say 0.15.0 and down work, but as I haven't tested this
    # we will assume only 0.15.0 works, but still allow the script to run
    if httplib2.__version__ != '0.15.0':
        logger.warning(f'httplib2 version 0.15.0 is specifically required, but {httplib2.__version__} is installed')

    convert_to_av1(values)
    # Add None to our queue to let the logger thread know
    # we're done executing, and should quit
    values.queue.put(None)
