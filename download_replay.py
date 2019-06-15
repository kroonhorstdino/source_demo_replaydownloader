import urllib.request
import requests
import time
import json
import bz2

config = json.loads(open('./config.json').read()) #parameters stored in json file

PATH_TO_REPLAY_FOLDER = config["PATH_TO_REPLAY_FOLDER"]

NUMBER_OF_MATCHES = config["NUMBER_OF_MATCHES"] #Number of matches to download beginning with the latest replay
REQUEST_TIMEOUT_LIMIT = config["REQUEST_TIMEOUT_LIMIT"]

ARE_MATCHIDS_DESCENDING = False

def http_request_matches(less_than_match_id=None):  #Get data about matches from OpenDota API
    
    PROMATCHES_API_ENDPOINT = 'https://api.opendota.com/api/proMatches'

    if less_than_match_id == None:
        # Get 100 matches from match id onwards
        http_response = requests.get(url=PROMATCHES_API_ENDPOINT)
    else:
        http_response = requests.get(url=PROMATCHES_API_ENDPOINT, params={
                           "less_than_match_id": less_than_match_id})  # Get 100 matches from match id onwards

    return http_response

#Get match IDs of last professional Dota2 matches
def fetch_match_ids(numberOfMatches):
    iterations = max(1, numberOfMatches // 100)  # Get integer instead of float

    print('Getting ' + str(iterations) + ' responses with data about 100 pro matches each')

    last_match_id = None  #First request for latest matches
    match_id_list = []  #List of all match ids
    

    for i in range(0, iterations):  #Collect 100 matches iteration times
        print("Fetch data for response " + str(i+1))
        match_res = http_request_matches(last_match_id)  #Collect matches beginning at the match after the last match that was collected
        
        if match_res.status_code == 200:
            res = match_res.json()
        elif match_res.status_code == 400:
            print('Replay request: Bad response')
            return 0       

        last_match_id = res[len(res)-1]['match_id']

        for m in range(len(res)): #Go through all match entries in http response
            match_id_list.append(res[m]['match_id'])  #Append it to list of ids

    print('Collected ' + str(NUMBER_OF_MATCHES) + ' match ids')

    return match_id_list[0:NUMBER_OF_MATCHES]
            


# Collect data about replays from OpenDota to construct URL
def construct_replay_urls(match_id_list):

    REPLAY_API_ENDPOINT = 'https://api.opendota.com/api/replays'

    replay_url_list = []  #All replay urls
    chunk_size = 5
    chunks = max(1, len(match_id_list) // chunk_size)
    
    for chunk_counter in range(chunks):  #Get replay information in chunks of 5 from API
        chunk_first_index = chunk_counter * chunk_size # 0, 5, 10 ....
        chunk_last_index = min(len(match_id_list), chunk_first_index + chunk_size) #End of chunk and also end of list in last chunk 

        match_id_list_chunk = match_id_list[chunk_first_index:chunk_last_index]
        
        MATCH_IDS = {
            'match_id': match_id_list_chunk
        }
        
        attempts = 0
        while attempts < 5:  #Try multiple times
            attempts += 1

            replay_res = requests.get(url=REPLAY_API_ENDPOINT, params=MATCH_IDS) #Get replay data from API

            if replay_res.status_code == 200:
                res = replay_res.json()
                break
            elif replay_res.status_code == 400:
                print('Bad API response for chunk' + str(chunk_counter))
                print('Try again in a few seconds... \n')
                time.sleep(30)
                continue

        for r in range(len(res)):
            template_URL = 'http://replay{cluster}.valve.net/570/{match_id}_{replay_salt}.dem.bz2'
            replay_URL = template_URL.format(
                cluster=str(res[r]["cluster"]),
                match_id=str(res[r]["match_id"]),
                replay_salt=str(res[r]["replay_salt"])
            )
            
            replay_url_list.append(replay_URL)
        
        if (chunk_counter < chunks - 1):
            time.sleep(1)

    return replay_url_list

# Download from Valve's replay servers

def download_replays(replay_url_list):
    
    print('Downloading replays')
    for replay_index in range(len(replay_url_list)):

        replay_url = replay_url_list[replay_index]
        replay_file_name = '.'.join((replay_url.split('/')[-1]).split('.')[0:2]) #From http://.../570/15453434_001212.dem.bz2 to 15453434_001212.dem

        print("Downloading replay [{replay_index}] | {replay}".format(replay_index=str(replay_index), replay=replay_file_name))
        #urllib.request.urlretrieve(replay_url, PATH_TO_REPLAY_FOLDER + replay_file_name)

        replay_data = urllib.request.urlopen(replay_url)        

        CHUNK = 16 * 1024

        decompressor = bz2.BZ2Decompressor()
        with open(PATH_TO_REPLAY_FOLDER + replay_file_name, 'wb') as fp:
            while True:
                chunk = replay_data.read(CHUNK)
                if not chunk:
                    break
                fp.write(decompressor.decompress(chunk))
        replay_data.close()
    
    print("Finished downloading replays")
    return

### CODE EXECUTION

match_id_list = fetch_match_ids(NUMBER_OF_MATCHES)

if match_id_list == 0:
    print("Match ID requisition failed! Abort...")
    exit(1)

replay_url_list = construct_replay_urls(match_id_list)

if len(replay_url_list) == 0:
    print("Replay URLs requisition failed! Abort...")
    exit(1)

download_replays(replay_url_list)
