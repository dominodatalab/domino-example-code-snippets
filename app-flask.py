import subprocess

# This is a sample Python/Flask app showing Domino's App publishing functionality.
# You can publish an app by clicking on "Publish" and selecting "App" in your
# quick-start project.

import json
import flask
from flask import request, redirect, url_for
import numpy as np

class ReverseProxied(object):
  def __init__(self, app):
      self.app = app
  def __call__(self, environ, start_response):
      script_name = environ.get('HTTP_X_SCRIPT_NAME', '')
      if script_name:
          environ['SCRIPT_NAME'] = script_name
          path_info = environ['PATH_INFO']
          if path_info.startswith(script_name):
              environ['PATH_INFO'] = path_info[len(script_name):]
      # Setting wsgi.url_scheme from Headers set by proxy before app
      scheme = environ.get('HTTP_X_SCHEME', 'https')
      if scheme:
        environ['wsgi.url_scheme'] = scheme
      # Setting HTTP_HOST from Headers set by proxy before app
      remote_host = environ.get('HTTP_X_FORWARDED_HOST', '')
      remote_port = environ.get('HTTP_X_FORWARDED_PORT', '')
      if remote_host and remote_port:
          environ['HTTP_HOST'] = f'{remote_host}:{remote_port}'
      return self.app(environ, start_response)

app = flask.Flask(__name__)
app.wsgi_app = ReverseProxied(app.wsgi_app)

# Homepage which uses a template file
@app.route('/')
def index_page():
  return flask.render_template("index.html")

# Sample redirect using url_for
@app.route('/redirect_test')
def redirect_test():
  return redirect( url_for('another_page') )

# Sample return string instead of using template file
@app.route('/another_page')
def another_page():
  msg = "You made it with redirect( url_for('another_page') )." + \
        "A call to flask's url_for('index_page') returns " + url_for('index_page') + "."
  return msg

@app.route("/random")
@app.route("/random/<int:n>")
def random(n = 100):
  random_numbers = list(np.random.random(n))
  return json.dumps(random_numbers)


import boto3
import os
import random
from datetime import datetime

def get_headers():
    token = requests.get('http://localhost:8899/access-token')
    domino_token = token.text
    
    headers = {
      'Authorization': f'Bearer {domino_token}',
      'Content-Type': 'application/json'
    }
    return headers

@app.route("/add_to_s3")
def add_to_s3():
    now = datetime.now() # current date and time
    filename = now.strftime("%Y%m%d")
    object_key = f'{filename}.txt'
    r = str(random.random())
    test_bucket='domino-acme-test-bucket'
    update_bucket_profile_name='acme-update-bucket-role'
    session = boto3.session.Session(profile_name=update_bucket_profile_name)
    s3_client = session.client('s3')
    object_data = f"This is a random string with number {r}"
   
    s3_client.put_object(Body=object_data, Bucket=test_bucket, Key=object_key)
    s3_client.close()
    return "Success"
    
@app.route("/list_s3")
def list_s3():
    test_bucket='domino-acme-test-bucket'
    list_bucket_profile_name='acme-list-bucket-role'
    session = boto3.session.Session(profile_name=list_bucket_profile_name)
    s3_client = session.client('s3')
    l = []
    for key in s3_client.list_objects(Bucket=test_bucket)['Contents']:
        l.append(key)
    s3_client.close()
    return {'Result':l}
        
from domino import Domino
import requests
@app.route("/start_job")
def start_job():
    print('Starting Job')
    now = datetime.now() # current date and time
    filename = now.strftime("%Y%m%d")
    object_key = f'{filename}.txt'

    token = requests.get('http://localhost:8899/access-token')
    domino_token = token.text
    domino = Domino(
        "sameer_wadkar/irsa-enablement",
        auth_token=domino_token,
        host=os.environ["DOMINO_API_HOST"],
    )
    run_response = domino.runs_start(['/mnt/job.py',object_key])        
    return {'Result' : run_response}

@app.route("/list_dataset")
def list_dataset():
    path = "/domino/datasets/local/irsa-enablement"
    
    dir_list = os.listdir(path)
 
    print("Files and directories in '", path, "' :")
 
    # prints all files
    print(dir_list)
    return {'Result':dir_list}
