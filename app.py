from flask import Flask, jsonify, request, Blueprint, current_app
from datetime import datetime
from urllib.parse import unquote
import json, os, pymongo, urllib

# Local Imports
from filedecryption import Decrypt_File
from JSONEncoder import JSONEncoder
from pubsub import PubSub

# Get all environment variables
PROJECT_ID = os.environ.get('PROJECT_ID')
ENCRYPTION_KEY = os.environ.get('ENCRYPTION_KEY')

MONGO_USER = os.environ.get('MONGO_USER')
MONGO_PASS = os.environ.get('MONGO_PASS')
MONGO_STORE_USER = os.environ.get('MONGO_STORE_USER')
MONGO_STORE_PASS = os.environ.get('MONGO_STORE_PASS')


# Decrypt Google Services authentication file and store it in the environment variables
Decrypt_File('service_authentication_file.bin', 'service_authentication_file.json', ENCRYPTION_KEY)
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'service_authentication_file.json'

# Create the Flask app
app = Flask(__name__)

# Set up blueprints for every route
blueprints = [
    Blueprint('mongodb_storage_upload', __name__, url_prefix='/mongodb/storage/upload/<db>/<collection>'),
    Blueprint('mongodb_storage_query', __name__, url_prefix='/mongodb//storage/query/<db>/<collection>/<query>'),
    Blueprint('mongodb_task_details', __name__, url_prefix='/mongodb/task/details/'),
    Blueprint('mongodb_task_get', __name__, url_prefix='/mongodb/task/get/<db>/<collection>/<task_id>'),
    Blueprint('mongodb_task_block', __name__, url_prefix='/mongodb/task/block/<db>/<collection>/<task_id>'),
    Blueprint('mongodb_task_unblock', __name__, url_prefix='/mongodb/task/unblock/<db>/<collection>/<task_id>'),
    Blueprint('mongodb_task_delete', __name__, url_prefix='/mongodb/task/delete/<db>/<collection>/<task_id>'),
    Blueprint('mongodb_task_schedule', __name__, url_prefix='/mongodb/task/schedule/<db>/<collection>/<task_id>'),
    Blueprint('mongodb_task_create', __name__, url_prefix='/mongodb/task/create/<db>/<collection>/<key>'),
    Blueprint('mongodb_task_exists', __name__, url_prefix='/mongodb/task/exists/<db>/<collection>/<task_id>'),
    Blueprint('mongodb_task_log', __name__, url_prefix='/mongodb/task/log/<db>/<collection>/<task_id>'),
    Blueprint('pubsub_publish', __name__, url_prefix='/pubsub/publish/<customer_name>/<task_type>/<task_id>')
]
# Register all blueprints
for blueprint in blueprints:
    app.register_blueprint(blueprint)

# Connect to the MongoDB server
mongoclient_tasks = pymongo.MongoClient(f"mongodb+srv://{MONGO_USER}:{MONGO_PASS}@datastack-mongodb-tasks.yt2p9sd.mongodb.net/?retryWrites=true&w=majority")
#mongoclient_store = pymongo.MongoClient(f'mongodb+srv://{MONGO_STORE_USER}:{MONGO_STORE_PASS}@datastack-mongodb-528cfa2f.mongo.ondigitalocean.com/datastack-mongodb-storage?replicaSet=datastack-mongodb&tls=true&authSource=admin')

# Connect to Google Services
pubsub = PubSub(PROJECT_ID)



# MongoDB Storage API
## DB = customer_name
## Collection = data_type
## Document ID = Random
@app.route('/mongodb/storage/upload/<db>/<collection>', methods=['POST'])
def Upload_MongoDB(db, collection):
    # Select the database
    db = mongoclient_tasks[db]
    # Select the collection
    collection = db[f'{collection}_storage']
    # Get the data from the request
    documents = request.get_json()
    # Insert the data into the collection
    collection.insert_many(documents)
    # Return a success message
    return f'Successfully uploaded JSON documents to {collection} collection'


@app.route('/mongodb/storage/query/<db>/<collection>/<query>')
def Query_MongoDB(db, collection, query):
    # Decode the query string
    query = unquote(query)
    # Convert the query string to a dictionary
    query = json.loads(query)
    # Select the database 
    db = mongoclient_tasks[db]
    # Perform the query and store the results in a cursor
    cursor = db[f'{collection}_storage'].find(query)
    # Convert the cursor to a list of documents
    results = list(cursor)
    # Convert with custom JSONEncoder to JSON
    json_data = json.dumps(results, cls=JSONEncoder)
    # Return the results as a JSON response
    return json_data



# MongoDB Tasks API
## DB = customer_name
## Collection = task_type
## Document ID = task_id
@app.route('/mongodb/task/details/', methods=['GET'])
def Get_Task_Details():
    # JSON body of the request
    task = request.get_json()['task']

    if len(task.split('/')) != 3:
        return 'Invalid task format'

    task_split = task.split('/')
    customer_name = task_split[0]
    task_type = task_split[1]
    task_id = task_split[2]

    return {
        'customer_name': customer_name,
        'task_type': task_type,
        'task_id': task_id
    }


@app.route('/mongodb/task/get/<db>/<collection>/<task_id>', methods=['GET'])
def Get_Task(db, collection, task_id):
    # Select the database
    db = mongoclient_tasks[db]
    # Select the collection
    collection = db[collection]
    # Get the task document
    task = collection.find({'_id': task_id})
    # Return the task document
    return task


@app.route('/mongodb/task/block/<db>/<collection>/<task_id>', methods=['POST'])
def Block_Task(db, collection, task_id):
    # Select the database
    db = mongoclient_tasks[db]
    # Select the collection
    collection = db[collection]
    # Get the task document
    task = collection.find_one({'_id': task_id})
    # Block the task
    task['running'] = True
    # Update the task document
    collection.update_one({'_id': task_id}, {'$set': task})
    # Return a success message
    return f'Successfully blocked task {task_id}'


@app.route('/mongodb/task/unblock/<db>/<collection>/<task_id>', methods=['POST'])
def Unblock_Task(db, collection, task_id):
    # Select the database
    db = mongoclient_tasks[db]
    # Select the collection
    collection = db[collection]
    # Get the task document
    task = collection.find_one({'_id': task_id})
    # Unblock the task
    task['running'] = False
    # Update the task document
    collection.update_one({'_id': task_id}, {'$set': task})
    # Return a success message
    return f'Successfully unblocked task {task_id}'


@app.route('/mongodb/task/delete/<db>/<collection>/<task_id>', methods=['POST'])
def Delete_Task(db, collection, task_id):
    # Select the database
    db = mongoclient_tasks[db]
    # Select the collection
    collection = db[collection]
    # Delete the task document
    collection.delete_one({'_id': task_id})
    # Return a success message
    return f'Successfully deleted task {task_id}'


@app.route('/mongodb/task/schedule/<db>/<collection>/<task_id>', methods=['POST'])
def Schedule_Task(db, collection, task_id):
    # Select the database
    db = mongoclient_tasks[db]
    # Select the collection
    collection = db[collection]
    # Get the task document
    task = collection.find_one({'_id': task_id})
    # Schedule the task
    # Get interval from and set next_run to now + interval
    task['next_run'] = datetime.now() + task['interval']
    # Update the task document
    collection.update_one({'_id': task_id}, {'$set': task})
    # Return a success message
    return f'Successfully scheduled task {task_id}'


@app.route('/mongodb/task/create/<db>/<collection>/<key>', methods=['POST'])
def Create_Task(db, collection, key):
    # Select the database
    db = mongoclient_tasks[db]
    # Select the collection
    collection = db[collection]
    # Get the task document
    task = request.get_json()
    # Query the database for an existing task document with the given key and value
    query = {key: task[key]}
    existing_task = collection.find_one(query)
    # If a task document does not exist, create the task document
    if not existing_task:
        result = collection.insert_one(task)
        return f'Successfully created task'
    else:
        return f'Task already exists'




@app.route('/mongodb/task/exists/<db>/<collection>/<task_id>', methods=['GET'])
def Task_Exists(db, collection, task_id):
    # Select the database
    db = mongoclient_tasks[db]
    # Select the collection
    collection = db[collection]
    # Get the task document
    task = collection.find({'_id': task_id})
    # Check if task exists
    if task:
        return 'True'
    else:
        return 'False'
    

@app.route('/mongodb/task/log/<db>/<collection>/<task_id>', methods=['POST'])
def Log_Task(db, collection, task_id):
    # Select the database
    db = mongoclient_tasks[db]
    # Select the collection
    collection = db[collection]
    # Get the task document
    task = collection.find({'_id': task_id})
    # Get the log document
    log = request.get_json()
    # Add the log document to the task document
    task['logs'].append(log)
    # Update the task document
    collection.update_one({'_id': task_id}, {'$push': { 'history': { '$each': [log], '$position': 0 }}})
    # Return a success message
    return f'Successfully logged task {task_id}'
    


# PubSub API
@app.route('/pubsub/publish/<customer_name>/<task_type>/<task_id>')
def Publish_PubSub(customer_name, task_type, task_id):
    # Get the topic path
    topic_path = pubsub.Get_Topic_Path(f'ds_{task_type}_topic')
    # Publish the message with the message name set to the task ID 
    # which is a db/collection/document path to the task document
    pubsub.Publish(topic_path, task_id, f'{customer_name}/{task_type}/{task_id}')
    # Return a success message
    return f'Successfully published message {customer_name}/{task_type}/{task_id} to ds_{task_type}_topic topic'



# Run the app
if __name__ == '__main__':
    app.run(debug=True)
