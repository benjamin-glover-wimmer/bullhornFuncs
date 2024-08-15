import logging
import azure.functions as func
import os
import requests
import pymongo
from cryptography.fernet import Fernet
import base64
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
import datetime
import time

app = func.FunctionApp()

@app.schedule(schedule="*/8 * * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def bullhornOAuth(myTimer: func.TimerRequest) -> None:
    try:
        encryption_key = os.getenv('ENCRYPTION_KEY')
        cipher_suite = Fernet(encryption_key.encode())
        
        CLIENT_ID = os.getenv('CLIENT_ID')
        CLIENT_SECRET = os.getenv('CLIENT_SECRET')

        mongo = pymongo.MongoClient(os.getenv('MONGO_CONNECTION_STRING'))
        db = mongo['Bullhorn']
        collection = db['BullhornToken']

        refresh_token_doc = collection.find_one({'_id': 1})
        if not refresh_token_doc:
            logging.error("No document found with _id 1 in BullhornToken collection")
            return
        
        logging.info(refresh_token_doc)

        refresh_token_base64 = refresh_token_doc['refresh_token'].decode()
        logging.info(refresh_token_base64)
       
        # Decrypt the refresh_token
        refresh_token = cipher_suite.decrypt(refresh_token_base64).decode()

        url = f"https://auth.bullhornstaffing.com/oauth/token?grant_type=refresh_token&refresh_token={refresh_token}&client_id={CLIENT_ID}&client_secret={CLIENT_SECRET}"
        response = requests.post(url)
        
        if response.status_code == 200:
            new_access_token = response.json().get('access_token')
            new_refresh_token = response.json().get('refresh_token')

            LOGIN_URL = 'https://rest.bullhornstaffing.com/rest-services/login'
            login_params = {
                'version': '*',
                'access_token': new_access_token
            }
            login_response = requests.post(LOGIN_URL, data=login_params)
            
            if login_response.status_code == 200:
                login_data = login_response.json()
                new_bhrest_token = login_data['BhRestToken']
                
                encrypted_access_token = cipher_suite.encrypt(new_access_token.encode())
                encrypted_refresh_token = cipher_suite.encrypt(new_refresh_token.encode())
                encrypted_bhrest_token = cipher_suite.encrypt(new_bhrest_token.encode())

                os.environ['ACCESS_TOKEN'] = encrypted_access_token.decode()
                os.environ['BH_REFRESH_TOKEN'] = encrypted_refresh_token.decode()
                os.environ['BHREST_TOKEN'] = encrypted_bhrest_token.decode()

                doc = collection.find({'_id': 1})
            

                if doc:
                    collection.update_one(
                        {'_id': 1},
                        {'$set': {
                            'access_token': encrypted_access_token,
                            'refresh_token': encrypted_refresh_token,
                            'bhrest_token': encrypted_bhrest_token
                        }}, upsert=True
                    )
                else:
                    collection.insert_one(
                        {
                            '_id': 1,
                            'access_token': encrypted_access_token,
                            'refresh_token': encrypted_refresh_token,
                            'bhrest_token': encrypted_bhrest_token
                        }
                    )


    except Exception as e:
        logging.error(f'Error: {e}')
        raise            

@app.route(route="ip_test", auth_level=func.AuthLevel.ANONYMOUS)
def ip_test(req: func.HttpRequest) -> func.HttpResponse:
    response = requests.get('https://api.ipify.org?format=json')
    if response.status_code == 200:
        ip_address = response.json().get('ip')
        return func.HttpResponse(f"The outbound IP address is: {ip_address}")
    else:
        return func.HttpResponse("Failed to retrieve the outbound IP address", status_code=500)


@app.timer_trigger(schedule="*/30 * * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def pull_reqs(myTimer: func.TimerRequest) -> None:
    time.sleep(30)
    
    encryption_key = os.getenv('ENCRYPTION_KEY')
    cipher_suite = Fernet(encryption_key.encode())
    
    mongo = pymongo.MongoClient(os.getenv('MONGO_CONNECTION_STRING'))
    db = mongo['Bullhorn']
    collection = db['BullhornToken']

    token_doc = collection.find_one({'_id': 1})
    if not token_doc:
        logging.error("No document found with _id 1 in BullhornToken collection")
        return

    bh_token_base64 = token_doc['bhrest_token'].decode()
    
    bhrest_token = cipher_suite.decrypt(bh_token_base64).decode()
    
    db = mongo['ResumeDB']
    collection = db['openReqs']
    rest_url = "https://rest32.bullhornstaffing.com/rest-services/2ggxhg/"
    headers = {'BhRestToken': bhrest_token}

    start = 0
    count = 100
    total_items = 0
    addedIds = []

    while True:
        url = f"{rest_url}search/JobOrder?query=isDeleted:0 AND isOpen:1&fields=id,title,employmentType,isOpen,status,owner,assignedUsers,clientCorporation,clientBillRate,submissions,dateAdded,type,correlatedCustomText9,description,address&sort=id&count={count}&start={start}"
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            logging.error(response.text)
        fail_count = 0

        items = response.json()['data']
        total_items += len(items)
        

        for i in items:
            entry = i.copy()
            entry['_id'] = entry.pop('id')
            entry['descriptionText'] = BeautifulSoup(entry['description'], 'html.parser').get_text()
            entry['lastUpdated'] = datetime.datetime.now().isoformat()
            entry['timeAdded'] = datetime.datetime.now().isoformat()
            entry['dateAdded'] = datetime.datetime.fromtimestamp(i['dateAdded'] / 1000).isoformat()
            collection.update_one({'_id': i['id']}, {'$set': entry}, upsert=True)
            addedIds.append(entry['_id'])

        invalid = collection.find({'_id': {'$nin': addedIds}})
        for i in invalid:
            collection.update_one({'_id': i['_id']}, {'$set': {'isOpen': False}})

        if len(items) < count:
            break

        start += count

        print(f"Total items processed: {total_items}")


@app.timer_trigger(schedule="*/30 * * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False) 
def pull_candidates(myTimer: func.TimerRequest) -> None:
    time.sleep(10)
    fields = os.getenv('fields')
    rest_url = "https://rest32.bullhornstaffing.com/rest-services/2ggxhg/"

    def search_candidates(rest_url, start):
        encryption_key = os.getenv('ENCRYPTION_KEY')
        cipher_suite = Fernet(encryption_key.encode())
        
        mongo = pymongo.MongoClient(os.getenv('MONGO_CONNECTION_STRING'))
        db = mongo['Bullhorn']
        token_collection = db['BullhornToken']
        
        candidate_collection = mongo['ResumeDB']['candidates']

        token_doc = token_collection.find_one({'_id': 1})
        if not token_doc:
            logging.error("No document found with _id 1 in BullhornToken collection")
            return

        bh_token_base64 = token_doc['bhrest_token'].decode()
        
        bhrest_token = cipher_suite.decrypt(bh_token_base64).decode()

        from pymongo import DESCENDING

        # Find the most recent document with 'lastUpdated' field
        last_ran_doc = candidate_collection.find_one({'lastUpdated': {'$exists': True}}, sort=[('lastUpdated', DESCENDING)])

        # Extract the 'lastUpdated' value
        if last_ran_doc:
            last_ran = last_ran_doc['lastUpdated']
        else:
            last_ran = None  # Handle the case where no document is found

        print(last_ran)

        # Parse the original datetime string into a datetime object
        original_datetime = datetime.datetime.fromisoformat(last_ran)

        # Subtract 7 hours from the datetime object
        adjusted_datetime = original_datetime - datetime.timedelta(hours=7)

        # Convert the modified datetime object to the desired format
        formatted_datetime_str = adjusted_datetime.strftime('%Y%m%d%H%M%S')

        print(formatted_datetime_str)

        url = f"{rest_url}search/Candidate?query=isDeleted:0 AND dateLastModified:[{formatted_datetime_str} TO 2099]&fields={fields}&sort=id&count=100&start={start}"
        headers = {'BhRestToken': bhrest_token}
        
        response = requests.get(url, headers=headers)
        fail_count = 0

        while response.status_code != 200:
            if response.status_code == 401:
                token_doc = token_collection.find_one({'_id': 1})
                if not token_doc:
                    logging.error("No document found with _id 1 in BullhornToken collection")
                    return

                bh_token_base64 = token_doc['bhrest_token'].decode()
                bhrest_token = cipher_suite.decrypt(bh_token_base64).decode()
            else:
                time.sleep(10)
                response = requests.get(url, headers=headers)
                fail_count += 1

        items = response.json()['data']

        for i in items:
            
            entry = {
                '_id': i['id'],
                'firstName': i['firstName'],
                'lastName': i['lastName'],
                'email': i['email'],
                'phone': i['phone'],
                'status': i['status'],
                'description': i['description'],
                'owner': i['owner'],
                'dateLastModified': datetime.fromtimestamp(i['dateLastModified'] / 1000).isoformat(),
                'companyName': i['companyName'],
                'hourlyRateLow': i['hourlyRateLow'],
                'minimumPayRate': i['customFloat1'],
                'usEmploymentStatus': i['customText4'],
                'referredBy': i['referredBy'],
                'independentContractorOrC2C': i['customText5'],
                'mobile': i['mobile'],
                'title': i['occupation'],
                'lastUpdated': datetime.now().isoformat(),
                'primarySkills': i['primarySkills'],
                'secondarySkills': i['secondarySkills'],
                'address': i['address'],
                "notes": i['notes']
            }

            print(i['notes'])

            if (entry['email'] is None or entry['email'] == "") and (entry['phone'] is None or entry['phone'] == ""):
                pass
            else:
                try:
                    url = f"{rest_url}/entityFiles/Candidate/{i['id']}"
                    response = requests.get(url, headers=headers)
                    print(response.status_code)
                    files = response.json()['EntityFiles']
                    resumes = []
                    for file in files:
                        if file['type'] == 'Resume' or file['type'] == 'Formatted Resume':
                            resumes.append(file)

                    resumes = sorted(resumes, key=lambda x: x['dateAdded'], reverse=True)
                    latest_resume = resumes[0]
                    latest_resume['lastUpdated'] = datetime.fromtimestamp(latest_resume['dateAdded'] / 1000).isoformat()
                    entry['resume'] = latest_resume
                    entry['resumeText'] = latest_resume['description']
                except Exception as e:
                    print(f"Error processing candidate {i['id']}: {e}")
                    latest_resume = {}
                    entry['resume'] = latest_resume
                    entry['resumeText'] = ""
                
                try:
                    entry['descriptionText'] = BeautifulSoup(i['description'], 'html.parser').get_text()
                except Exception as e:
                    entry['descriptionText'] = ""

                candidate_collection.update_one({'_id': entry['_id']}, {'$set': entry}, upsert=True)
    encryption_key = os.getenv('ENCRYPTION_KEY')
    cipher_suite = Fernet(encryption_key.encode())
    
    mongo = pymongo.MongoClient(os.getenv('MONGO_CONNECTION_STRING'))
    db = mongo['Bullhorn']
    token_collection = db['BullhornToken']
    
    token_doc = token_collection.find_one({'_id': 1})
    if not token_doc:
        logging.error("No document found with _id 1 in BullhornToken collection")
        return

    bh_token_base64 = token_doc['bhrest_token'].decode()
    
    bhrest_token = cipher_suite.decrypt(bh_token_base64).decode()


    url = f"{rest_url}search/Candidate?query=isDeleted:0&fields=id&sort=id"

    headers = {'BhRestToken': bhrest_token}
    
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print(f"Error retrieving candidate count: {response.status_code}")
        return

    total_count = response.json().get('total')
    print(f"Total candidates: {total_count}")

    start = 0
    count = 100  # Number of candidates to fetch per request

    while start < total_count:
        search_candidates(rest_url, start)
        time.sleep(10)
        start += count