from flask import Flask, jsonify, request, Blueprint, current_app
from datetime import datetime
from urllib.parse import unquote
import json, os, pymongo

# Local Imports
from filedecryption import Decrypt_File
from JSONEncoder import JSONEncoder
from pubsub import PubSub


# Get all environment variables
PROJECT_ID = os.environ.get('PROJECT_ID')
ENCRYPTION_KEY = os.environ.get('ENCRYPTION_KEY')

MONGO_USER = os.environ.get('MONGO_USER')
MONGO_PASS = os.environ.get('MONGO_PASS')

# Decrypt Google Services authentication file and store it in the environment variables
Decrypt_File('service_authentication_file.bin', 'service_authentication_file.json', ENCRYPTION_KEY)
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'service_authentication_file.json'

# Create the Flask app
app = Flask(__name__)



# Register blueprints
mongodb_bp = Blueprint('mongodb', __name__, url_prefix='/mongodb')
pubsub_bp = Blueprint('pubsub', __name__, url_prefix='/pubsub')
app.register_blueprint(mongodb_bp)
app.register_blueprint(pubsub_bp)



# Connect to the MongoDB server
mongoclient_tasks = pymongo.MongoClient(f"mongodb+srv://{MONGO_USER}:{MONGO_PASS}@datastack-mongodb-tasks.yt2p9sd.mongodb.net/?retryWrites=true&w=majority")
mongoclient_store = None # Use later for Storage of data


# Connect to Google Services
pubsub = PubSub(PROJECT_ID)



# MongoDB Storage API
## DB = customer_name
## Collection = data_type
## Document ID = Random
@mongodb_bp.route('/storage/upload/<db>/<collection>', methods=['POST'])
def Upload_MongoDB(db, collection):
    # Select the database
    db = mongoclient_store[db]
    # Select the collection
    collection = db[collection]
    # Get the data from the request
    documents = request.get_json()
    # Insert the data into the collection
    collection.insert_many(documents)
    # Return a success message
    return f'Successfully uploaded JSON documents to {collection} collection'


@mongodb_bp.route('/storage/query/<db>/<collection>/<query>')
def Query_MongoDB(db, collection, query):
    # Decode the query string
    query = unquote(query)
    # Convert the query string to a dictionary
    query = json.loads(query)
    # Select the database 
    db = mongoclient_store[db]
    # Perform the query and store the results in a cursor
    cursor = db[collection].find(query)
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
@mongodb_bp.route('/task/details/', methods=['GET'])
def Get_Task_Details():
    # JSON body of the request
    task = request.get_json()

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


@mongodb_bp.route('/task/get/<db>/<collection>/<task_id>', methods=['GET'])
def Get_Task(db, collection, task_id):
    # Select the database
    db = mongoclient_tasks[db]
    # Select the collection
    collection = db[collection]
    # Get the task document
    task = collection.find({'_id': task_id})
    # Return the task document
    return task


@mongodb_bp.route('/task/block/<db>/<collection>/<task_id>', methods=['POST'])
def Block_Task(db, collection, task_id):
    # Select the database
    db = mongoclient_tasks[db]
    # Select the collection
    collection = db[collection]
    # Get the task document
    task = collection
    # Block the task
    task['running'] = True
    # Update the task document
    collection.update_one({'_id': task_id}, {'$set': task})
    # Return a success message
    return f'Successfully blocked task {task_id}'


@mongodb_bp.route('/task/unblock/<db>/<collection>/<task_id>', methods=['POST'])
def Unblock_Task(db, collection, task_id):
    # Select the database
    db = mongoclient_tasks[db]
    # Select the collection
    collection = db[collection]
    # Get the task document
    task = collection
    # Unblock the task
    task['running'] = False
    # Update the task document
    collection.update_one({'_id': task_id}, {'$set': task})
    # Return a success message
    return f'Successfully unblocked task {task_id}'


@mongodb_bp.route('/task/delete/<db>/<collection>/<task_id>', methods=['POST'])
def Delete_Task(db, collection, task_id):
    # Select the database
    db = mongoclient_tasks[db]
    # Select the collection
    collection = db[collection]
    # Delete the task document
    collection.delete_one({'_id': task_id})
    # Return a success message
    return f'Successfully deleted task {task_id}'


@mongodb_bp.route('/task/schedule/<db>/<collection>/<task_id>', methods=['POST'])
def Schedule_Task(db, collection, task_id):
    # Select the database
    db = mongoclient_tasks[db]
    # Select the collection
    collection = db[collection]
    # Get the task document
    task = collection
    # Schedule the task
    # Get interval from and set next_run to now + interval
    task['next_run'] = datetime.now() + task['interval']
    # Update the task document
    collection.update_one({'_id': task_id}, {'$set': task})
    # Return a success message
    return f'Successfully scheduled task {task_id}'


@mongodb_bp.route('/task/create/<db>/<collection>/<task_id>', methods=['POST'])
def Create_Task(db, collection, task_id):
    # Select the database
    db = mongoclient_tasks[db]
    # Select the collection
    collection = db[collection]
    # Get the task document
    task = request.get_json()
    # Create the task document
    collection.insert_one(task)
    # Return a success message
    return f'Successfully created task {task_id}'


@mongodb_bp.route('/task/exists/<db>/<collection>/<task_id>', methods=['GET'])
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
    

@mongodb_bp.route('/task/log/<db>/<collection>/<task_id>', methods=['POST'])
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
@pubsub_bp.route('/publish/<customer_name>/<task_type>/<task_id>')
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
