''' precompute_dashboard.py
    NeuronBridge precompute dashboard
'''

from datetime import timedelta
import inspect
from operator import attrgetter
import os
import re
import sys
from time import time
import traceback
import boto3
import botocore
from boto3.dynamodb.conditions import Key
from flask import (Flask, make_response, render_template, request, jsonify)
from flask_cors import CORS
import pymongo
import jrc_common.jrc_common as JRC


# pylint: disable=no-member, R1710, W1401, E0602
# pylint: disable=C0302,C0103,W0703

__version__ = '1.0.0'
app = Flask(__name__, template_folder='templates')
app.config.from_pyfile("config.cfg")
CORS(app)
app.config['STARTTIME'] = time()
# Database and AWS
DB = {}
DYNAMO = {}
AWS = {}
S3_SECONDS = 60 * 60 * 12
# Navigation
NAV = {"Precompute": {"EM datasets": None,
                      "LM releases": None},
       "DynamoDB": {"Published": None,
                    "Denormalization": None,
                    "DOIs": None,
                    "Skeletons": None,
                    "Stacks": None
                   },
       "Search": None,
      }


# *****************************************************************************
# * Flask                                                                     *
# *****************************************************************************


@app.before_request
def before_request():
    ''' Set transaction start time and increment counters.
        If needed, initilize global variables.
    '''
    # pylint: disable=W0603
    if "neuronbridge" not in DB:
        try:
            dbconfig = JRC.get_config("databases")
        except Exception as err: # pragma: no cover
            temp = "{2}: An exception of type {0} occurred. Arguments:\n{1!r}"
            mess = temp.format(type(err).__name__, err.args, inspect.stack()[0][3])
            return render_template('error.html', urlroot=request.url_root,
                                   title='Invalid or missing response from Configuration server',
                                   message=mess)
        for dbname in ('jacs', 'neuronbridge'):
            dbo = attrgetter(f"{dbname}.prod.read")(dbconfig)
            try:
                DB[dbname] = JRC.connect_database(dbo)
            except Exception as err:
                print(err)
                temp = "{2}: An exception of type {0} occurred. Arguments:\n{1!r}"
                mess = temp.format(type(err).__name__, err.args, inspect.stack()[0][3])
                return render_template('error.html', urlroot=request.url_root,
                                       title=f"Could not connect to MongoDB {dbname}",
                                       message=mess)
        try:
            DYNAMO['client'] = boto3.client('dynamodb', region_name='us-east-1')
            DYNAMO['resource'] = boto3.resource('dynamodb', region_name='us-east-1')
        except Exception as err:
            temp = "{2}: An exception of type {0} occurred. Arguments:\n{1!r}"
            mess = temp.format(type(err).__name__, err.args, inspect.stack()[0][3])
            return render_template('error.html', urlroot=request.url_root,
                                   title='Could not connect to DynamoDB',
                                   message=mess)
    try:
        dbconfig = JRC.get_config("databases")
        aws = JRC.get_config("aws")
    except Exception as err:
        temp = "{2}: An exception of type {0} occurred. Arguments:\n{1!r}"
        mess = temp.format(type(err).__name__, err.args, inspect.stack()[0][3])
        return render_template('error.html', urlroot=request.url_root,
                               title='Invalid or missing response from Configuration server',
                               message=mess)
    try:
        sts_client = boto3.client('sts')
        aro = sts_client.assume_role(RoleArn=attrgetter("role_arn")(aws),
                                     RoleSessionName="AssumeRoleSession1",
                                     DurationSeconds=S3_SECONDS)
        credentials = aro['Credentials']
        AWS['client'] = boto3.client('s3',
                                     aws_access_key_id=credentials['AccessKeyId'],
                                     aws_secret_access_key=credentials['SecretAccessKey'],
                                     aws_session_token=credentials['SessionToken'])
    except Exception as err:
        temp = "{2}: An exception of type {0} occurred. Arguments:\n{1!r}"
        mess = temp.format(type(err).__name__, err.args, inspect.stack()[0][3])
        return render_template('error.html', urlroot=request.url_root,
                               title='Could not connect to AWS',
                               message=mess)
    app.config['STARTTIME'] = time()


@app.after_request
def after_request_func(response):
    ''' Close database connections
    '''
    return response


# *****************************************************************************
# * Classes                                                                   *
# *****************************************************************************
class InvalidUsage(Exception):
    ''' Return an error response
    '''
    status_code = 400

    def __init__(self, message, status_code=None, payload=None):
        Exception.__init__(self)
        self.message = message
        if status_code is not None:
            self.status_code = status_code
        self.payload = payload

    def to_dict(self):
        ''' Build error response
        '''
        retval = dict(self.payload or ())
        retval['rest'] = {'error': self.message}
        return retval


# ******************************************************************************
# * Utility functions                                                          *
# ******************************************************************************

def generate_navbar(active):
    ''' Generate the web navigation bar
        Keyword arguments:
          active: name of active nav
        Returns:
          Navigation bar
    '''
    nav = '''
    <nav class="navbar navbar-expand-lg navbar-dark bg-dark">
      <div class="collapse navbar-collapse" id="navbarSupportedContent">
        <ul class="navbar-nav mr-auto">
    '''
    for heading, subhead in NAV.items():
        basic = '<li class="nav-item active">' if heading == active else '<li class="nav-item">'
        drop = '<li class="nav-item dropdown active">' if heading == active \
               else '<li class="nav-item dropdown">'
        menuhead = '<a class="nav-link dropdown-toggle" href="#" id="navbarDropdown" ' \
                   + 'role="button" data-toggle="dropdown" aria-haspopup="true" ' \
                   + f"aria-expanded=\"false\">{heading}</a><div class=\"dropdown-menu\" "\
                   + 'aria-labelledby="navbarDropdown">'
        if subhead:
            nav += drop + menuhead
            for itm, val in subhead.items():
                if not val:
                    link = ('/' + itm.replace(" ", "_")).lower()
                    nav += f"<a class='dropdown-item' href='{link}'>{itm}</a>"
            nav += '</div></li>'
        else:
            nav += basic
            link = ('/' + heading.replace(" ", "_")).lower()
            nav += f"<a class='nav-link' href='{link}'>{heading}</a></li>"
    nav += '</ul></div></nav>'
    return nav


def initialize_result():
    ''' Initialize the result dictionary
        An auth header with a JWT token is required for all POST and DELETE requests
        Returns:
          decoded partially populated result dictionary
    '''
    result = {"rest": {'requester': request.remote_addr,
                       'url': request.url,
                       'endpoint': request.endpoint,
                       'error': False,
                       'elapsed_time': '',
                       'row_count': 0,
                       'pid': os.getpid()}}
    return result


def generate_response(result):
    ''' Generate a response to a request
        Keyword arguments:
          result: result dictionary
        Returns:
          JSON response
    '''
    result['rest']['elapsed_time'] = str(timedelta(seconds=time() - app.config['STARTTIME']))
    return jsonify(**result)


@app.errorhandler(InvalidUsage)
def handle_invalid_usage(error):
    ''' Error handler
        Keyword arguments:
          error: error object
    '''
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response


def generate_version_pulldown(coll, version):
    ''' Return a version pulldown
        Keyword arguments:
          coll: MongoDB collection
          version: default version
        Returns:
          Pulldown HTML
    '''
    pulldown = "<br>Select a DynamoDB published data version: "
    pulldown += "<select id='version' onchange='select_version();'>"
    versions = coll.distinct("dynamodb_version")
    if not versions:
        return "", ""
    if len(versions) == 1:
        return "", versions[0]
    if not version:
        for ver in versions:
            version = ver
    for ver in versions:
        if ver == version:
            pulldown += f"<option selected>{ver}</option>"
        else:
            pulldown += f"<option>{ver}</option>"
    pulldown += "</select><br><br>"
    return pulldown, version


def humansize(num, suffix='B'):
    ''' Return a human-readable storage size
        Keyword arguments:
          num: size
          suffix: default suffix
        Returns:
          string
    '''
    for unit in ['', 'K', 'M', 'G', 'T']:
        if abs(num) < 1024.0:
            return f"{num:.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}{suffix}"


def ddb_table(table):
    ''' Return DynamoDB table stats
        Keyword arguments:
          table: table name
        Returns:
          string containing stats
    '''
    ddbt = DYNAMO['resource'].Table(table)
    dynamo = "Status: <span style='color: " \
             + f"{'lime' if ddbt.table_status == 'ACTIVE' else 'gold'}'>" \
             + f"{ddbt.table_status}</span><br>"
    dynamo += f"ARN: {ddbt.table_arn}<br>"
    dynamo += f"Key schema: {ddbt.key_schema}<br>"
    if ddbt.billing_mode_summary:
        dynamo += f"Billing mode: {ddbt.billing_mode_summary['BillingMode']}<br>"
    dynamo += f"{ddbt.item_count:,} entries<br>"
    dynamo += f"Size: {humansize(ddbt.table_size_bytes)}<br>"
    response = DYNAMO['client'].list_tags_of_resource(ResourceArn=ddbt.table_arn)
    if response and 'Tags' in response:
        dynamo += "Tags: <ul>"
        for tag in response['Tags']:
            dynamo += f"<li>{tag['Key']}: {tag['Value']}</li>"
        dynamo += "</ul>"
    return dynamo


# *****************************************************************************
# * Utility functions for search                                              *
# *****************************************************************************

def check_s3(uploaded, s3files, outs3, errtype):
    ''' Look for files on AWS S3
        Keyword arguments:
          uploaded: dictionary of files (key: file type, value: full path)
          s3files: files already checked on S3
          outs3: output list
          colsize: dictionary of column sizes
          errtype: dictionary of error types
        Returns:
          None
    '''
    for ftype, full in uploaded.items():
        floc = full.replace('https://s3.amazonaws.com/', '')
        bucket, key = floc.split('/', 1)
        if ftype in s3files and key in s3files[ftype]:
            continue
        if ftype not in s3files:
            s3files[ftype] = {}
        s3files[ftype][key] = True
        if ftype not in s3files:
            s3files[ftype] = {}
        s3files[ftype][key] = True
        try:
            AWS['client'].head_object(Bucket=bucket, Key=key.replace('+', ' '))
            outs3.append([ftype, url_link(full)])
        except botocore.exceptions.ClientError as err:
            if err.response['Error']['Code'] == "404":
                errtype['notfound'] = True
                outs3.append([ftype, f"<span style='color: red'>{key}</span>"])
            else:
                errtype['other'] = True
                outs3.append([ftype, f"<span style='color: red'>{key} {err}</span>"])


def show_jacs(ival, itype='Slide code', table='sample'):
    ''' Show data from sample
        Keyword arguments:
          ival: value to search fo
          itype: type of value
          table: table to search
        Returns:
          HTML string
    '''
    payload = {}
    if itype == 'Publishing name':
        payload = {"publishingName": {"$regex": f"^{ival}$", "$options": "i"}}
    elif itype == 'Sample':
        payload = {"_id": int(ival)} if table == 'sample' else {"sampleRef": f"Sample#{ival}"}
    elif itype == 'Slide code':
        payload = {"slideCode": ival}
    rows = None
    try:
        cnt =  DB['jacs'][table].count_documents(payload)
        if cnt:
            rows = DB['jacs'][table].find(payload)
    except Exception as err:
        raise err
    if not cnt:
        return f"<span style='textcolor:goldenrod'>{itype} {ival} was not found in " \
               + f"{table}</span><br>", ""
    if table == 'sample':
        headers = ['_id', 'slideCode', 'line', 'publishingName', 'gender', 'dataSet',
                   'releaseLabel', 'status']
    else:
        headers = ['sampleRef', 'slideCode', 'line', 'anatomicalArea', 'tile', 'objective',
                   'gender', 'dataSet', 'name']
    html = f"<br><div class='hr-with-text'><span>{table} ({cnt})</span></div>"
    html += "<table id='jacs' class='tablesorter standard'><thead><tr><th>" \
        + '</th><th>'.join(headers) + "</th></tr></thead><tbody>"
    pname = ""
    for row in rows:
        for header in headers:
            if header not in row:
                row[header] = ''
        if table == 'sample':
            row['_id'] = str(row['_id'])
            if 'publishingName' in row:
                pname = row['publishingName']
        else:
            row['sampleRef'] = row['sampleRef'].replace('Sample#','')
        html += "<tr><td>" + '</td><td>'.join([row[header] for header in headers]) + "</td></tr>"
    html += "</tbody></table>"
    return html, pname


def show_emb(ival, itype='Body ID'):
    ''' Show data from emBody
        Keyword arguments:
          ival: value to search for
          itype: type of value
        Returns:
          HTML string
    '''
    table = 'emBody'
    if itype in ('Neuron type', 'Neuron instance'):
        payload = {"neuronType": ival}
    else:
        payload = {"name": ival}
    rows = None
    try:
        cnt =  DB['jacs'][table].count_documents(payload)
        if cnt:
            rows = DB['jacs'][table].find(payload)
    except Exception as err:
        raise err
    if not cnt:
        return f"<span style='textcolor:goldenrod'>Body ID {ival} was not found in " \
               + f"{table}</span><br>"
    headers = ['_id', 'name', 'neuronType', 'neuronInstance', 'status', 'statusLabel',
               'dataSetIdentifier']
    html = f"<br><div class='hr-with-text'><span>{table} ({cnt})</span></div>"
    html += "<table id='emb' class='tablesorter standard'><thead><tr><th>" \
        + '</th><th>'.join(headers) + "</th></tr></thead><tbody>"
    for row in rows:
        row['_id'] = str(row['_id'])
        for field in ['neuronType', 'neuronInstance', 'status', 'statusLabel', 'dataSetIdentifier']:
            if field not in row:
                row[field] = ''
        html += "<tr><td>" + '</td><td>'.join([row[header] for header in headers]) + "</td></tr>"
    html += "</tbody></table>"
    return html


def show_nmd_purl(ival, itype='Slide code', table='neuronMetadata'):
    ''' Show data from neuronMetadata or publishedURL and AWS S3
        Keyword arguments:
          ival: value to search for
          itype: type of value
          table: table to search
        Returns:
          HTML string, release name, list of publishing names
    '''
    payload = {}
    if itype == 'Publishing name':
        payload = {"publishedName": {"$regex": f"^{ival}$", "$options": "i"}}
    elif itype == 'Sample':
        payload = {"sourceRefId": "Sample#" + ival} if table == 'neuronMetadata' \
                   else {"sampleRef": f"Sample#{ival}"}
    elif itype == 'Slide code':
        payload = {"slideCode": ival}
    elif itype == 'Neuron type':
        payload = {"neuronType": ival}
    elif itype == 'Neuron instance':
        payload = {"neuronInstance": ival}
    elif itype == 'Body ID':
        if table == 'publishedURL':
            payload = {"publishedName": {"$regex": f":{ival}$"}}
        else:
            payload = {"publishedName": ival}
    rows = None
    pname = []
    try:
        cnt =  DB['neuronbridge'][table].count_documents(payload)
        if cnt:
            rows = DB['neuronbridge'][table].find(payload)
    except Exception as err:
        raise err
    if not cnt:
        return f"<span style='textcolor:goldenrod'>{itype} {ival} was not found in " \
               + f"{table}</span><br>", None, None
    html = f"<br><div class='hr-with-text'><span>{table} ({cnt})</span></div>"
    if itype in ('Body ID', 'Neuron type', 'Neuron instance'):
        headers = ['sourceRefId', 'mipId', 'alignmentSpace', 'publishedName', 'neuronType',
                   'neuronInstance', 'datasetLabels']
    else:
        headers = ['sourceRefId', 'mipId', 'alignmentSpace', 'slideCode', 'publishedName',
                  'anatomicalArea', 'objective', 'gender', 'datasetLabels']
    if table == 'publishedURL':
        headers[0] = 'sampleRef'
        if itype in ('Sample', 'Slide code'):
            headers[-1] = 'alpsRelease'
        elif itype == 'Body ID':
            headers = headers[:-3]
    html += "<table id='nmd_purl' class='tablesorter standard'><thead><tr><th>" \
            + '</th><th>'.join(headers) + "</th></tr></thead><tbody>"
    release = None
    row = None
    for row in rows:
        for col in ['sourceRefId', 'neuronType', 'neuronInstance']:
            if col not in row:
                row[col] = ''
        if 'datasetLabels' in row:
            row['datasetLabels'] = ', '.join(row['datasetLabels'])
        else:
            row['datasetLabels'] = ''
        if 'alpsRelease' in row and release is None:
            release = row['alpsRelease']
        if row['publishedName'] not in pname:
            pname.append(row['publishedName'])
        html += "<tr><td>" + '</td><td>'.join([row[field] for field in headers]) + "</td></tr>"
    html += "</tbody></table>"
    if table != 'publishedURL' or not ('uploaded' in row and row['uploaded']):
        return html, release, pname
    s3files = {}
    outs3 = []
    errtype = {'notfound': False, 'other': False}
    if 'uploaded' in row and row['uploaded']:
        check_s3(row['uploaded'], s3files, outs3, errtype)
    html += "<br><div class='hr-with-text'><span>publishedURL files uploaded to AWS S3 " \
            + f"({len(outs3)})</span></div>"
    html += "<table id='s3files' class='tablesorter standard'><thead><tr><th>File type</th>" \
            + "<th>Key</th></tr></thead><tbody>"
    for key in outs3:
        html += f"<tr><td>{key[0]}</td><td>{key[1]}</td></tr>"
    html += "</tbody></table>"
    if errtype['notfound']:
        html += "<span style='color: red'>Some files were not found on S3</span>"
    if errtype['other']:
        html += "<span style='color: red'>Some files caused errors</span>"
    return html, release, pname


def get_custom(key):
    ''' Show entries in janelia-neuronbridge-custom-annotations
        Keyword arguments:
          key: search key
        Returns:
          Release name
    '''
    tbl = 'janelia-neuronbridge-custom-annotations'
    DB[tbl] = DYNAMO['resource'].Table(tbl)
    try:
        response = DB[tbl].query(
        KeyConditionExpression=Key('entryType').eq('searchString') \
                                   & Key('searchKey').eq(key.lower()))
    except Exception as err:
        raise err
    if not response or ('Items' not in response) or not response['Items']:
        return ""
    data = response['Items'][0]
    html = f"<br><div class='hr-with-text'><span>DynamoDB {tbl}</span></div>"
    html += "<table id='skeletons' class='tablesorter standard'><thead><tr><th>Annotation</th>" \
            + "<th>Annotator</th><th>Region</th><th>Dataset</th><th>Line</th></tr></thead><tbody>"
    for item in data['matches']:
        html += f"<tr><td>{item['annotation']}</td><td>{item['annotator']}</td>" \
                + f"<td>{item['region']}</td><td>{item['dataset']}</td><td>{item['line']}</td></tr>"
    html += "</tbody></table>"
    return html


def get_skeletons(key):
    ''' Show entries in janelia-neuronbridge-published-skeletons
        Keyword arguments:
          key: partition key
        Returns:
          Release name
    '''
    tbl = 'janelia-neuronbridge-published-skeletons'
    DB[tbl] = DYNAMO['resource'].Table(tbl)
    try:
        response = DB[tbl].query(KeyConditionExpression= \
                                 Key('publishedName').eq(key))
    except Exception as err:
        raise err
    if not response or ('Items' not in response) or not response['Items']:
        return ""
    html = f"<br><div class='hr-with-text'><span>DynamoDB {tbl}</span></div>"
    html += "<table id='skeletons' class='tablesorter standard'><thead><tr><th>Key</th>" \
            + "<th>Value</th></tr></thead><tbody>"
    for key2, val in response['Items'][0].items():
        if key2.startswith('skeleton'):
            val = url_link(val)
        html += f"<tr><td>{key2}</td><td>{val}</td></tr>"
    html += "</tbody></table>"
    return html


def get_stacks(key):
    ''' Show entries in janelia-neuronbridge-published-stacks
        Keyword arguments:
          key: partition key (lowercase)
        Returns:
          Release name
    '''
    tbl = 'janelia-neuronbridge-published-stacks'
    DB[tbl] = DYNAMO['resource'].Table(tbl)
    try:
        response = DB[tbl].query(KeyConditionExpression= \
                                 Key('itemType').eq(key))
    except Exception as err:
        raise err
    if not response or ('Items' not in response) or not response['Items']:
        return None
    return response['Items'][0]['releaseName']


def show_pli(ival, itype='Slide code', release=None):
    ''' Show data from publishedLMImage and AWS S3
        Keyword arguments:
          ival: value to search for
          itype: type of value
          release: release name
        Returns:
          HTML string
    '''
    payload = {}
    if itype == 'Publishing name':
        payload = {"name": ival}
    elif itype == 'Sample':
        payload = {"sampleRef": "Sample#" + ival}
    elif itype == 'Slide code':
        payload = {"slideCode": ival}
    rows = None
    try:
        cnt =  DB['neuronbridge']['publishedLMImage'].count_documents(payload)
        if cnt:
            rows = DB['neuronbridge']['publishedLMImage'].find(payload)
    except Exception as err:
        raise err
    if not cnt:
        return f"<span style='textcolor:goldenrod'>{itype} {ival} was not found in " \
               + "publishedLMImage</span><br>"
    html = f"<br><div class='hr-with-text'><span>publishedLMImage ({cnt})</span></div>"
    headers = ['sampleRef', 'slideCode', 'name', 'area', 'tile', 'objective', 'releaseName',
               'alignment']
    html += "<table id='pli' class='tablesorter standard'><thead><tr><th>" \
            + '</th><th>'.join(headers) + "</th></tr></thead><tbody>"
    s3files = {}
    outs3 = []
    errtype = {'notfound': False, 'other': False}
    ddb = {}
    for row in rows:
        row['alignment'] = "<span style='color: yellow'>No</span>"
        if 'files' in row and 'VisuallyLosslessStack' in row['files']:
            row['alignment'] = "<span style='color: lime'>Yes</span>"
        if itype != 'Publishing name' and release and row['releaseName'] != release:
            row['releaseName'] = f"<span style='color: red'>{row['releaseName']}</span>"
        for field in headers:
            if field not in row or row[field] is None:
                row[field] = ''
        html += "<tr><td>" + '</td><td>'.join([row[header] for header in headers]) + "</td></tr>"
        if 'files' in row and row['files']:
            check_s3(row['files'], s3files, outs3, errtype)
        ddb_key = '-'.join([row['slideCode'], row['objective'], row['alignmentSpace']]).lower()
        if ddb_key not in ddb:
            ret = get_stacks(ddb_key)
            if ret:
                ddb[ddb_key] = ret
    html += "</tbody></table>"
    html += "<br><div class='hr-with-text'><span>publishedLMImage files uploaded to AWS S3 " \
            + f"({len(outs3)})</span></div>"
    html += "<table id='s3files' class='tablesorter standard'><thead><tr><th>File type</th>" \
            + "<th>Key</th></tr></thead><tbody>"
    for key in outs3:
        html += f"<tr><td>{key[0]}</td><td>{key[1]}</td></tr>"
    html += "</tbody></table>"
    if errtype['notfound']:
        html += "<span style='color: red'>Some files were not found on S3</span>"
    if errtype['other']:
        html += "<span style='color: red'>Some files caused errors</span>"
    if not ddb:
        return html
    html += "<br><div class='hr-with-text'><span>DynamoDB janelia-neuronbridge-published-stacks " \
            + f"({len(ddb)})</span></div>"
    html += "<table id='pli' class='tablesorter standard'><thead><tr><th>itemType</th>" \
            + "<th>Release</th></tr></thead><tbody>"
    for key, val in ddb.items():
        html += f"<tr><td>{key}</td><td>{val}</td></tr>"
    html += "</tbody></table>"
    return html


def url_link(url):
    ''' Create a link to a URL
        Keyword arguments:
          url: URL
        Returns:
          HTML string
    '''
    abbrev = re.sub(r'^https:\/\/s3.amazonaws.com\/[^\/]+\/', '', url)
    return f"<a href='{url}' target='_blank'>{abbrev}</a>"


def get_dois(pname):
    ''' Show data from doi
        Keyword arguments:
          pname: list of publishing names
        Returns:
          HTML string
    '''
    table = 'janelia-neuronbridge-publishing-doi'
    DB[table] = DYNAMO['resource'].Table(table)
    out = []
    html = ""
    for pn in pname:
        try:
            response = get_dynamodb(table, 'name', pn)
        except Exception as err:
            raise err
        if not response:
            continue
        out.append({'name': response['name'], 'link': response['doi'][0]['link'],
             'citation': response['doi'][0]['citation']})
    if not out:
        return html
    html = f"<br><div class='hr-with-text'><span> DynamoDB {table} ({len(out)})</span></div>"
    html += "<table id='dois' class='tablesorter standard'><thead><tr><th>Publishing name</th>" \
            + "<th>Citation</th><th>Link</th></tr></thead><tbody>"
    for doi in out:
        if doi['link']:
            doi['link'] = url_link(doi['link'])
        html += f"<tr><td>{doi['name']}</td><td>{doi['citation']}</td><td>{doi['link']}</td></tr>"
    html += "</tbody></table>"
    return html


def get_dynamodb(table, key, search, sort_key=None, sort_value=None):
    ''' Get a DynamoDB entry
        Keyword arguments:
          table: DynamoDB table
          key: search key
          search: search value
          sort_key: sort key
          sort_value: sort value
          return_json: return JSON string
        Returns:
          dictionary
    '''
    ddbt = DYNAMO['resource'].Table(table)
    try:
        if not sort_key:
            response = ddbt.get_item(Key={key: search})
            if 'Item' not in response or not response['Item']:
                return None
            return response['Item']
        response = ddbt.query(KeyConditionExpression=Key(key).eq(search) \
                   & Key(sort_key).eq(sort_value))
        if 'Items' not in response or not response['Items']:
            return None
        return response['Items']
    except Exception as err:
        raise err


def get_published_versioned(pname):
    ''' Get the published versioned data for a body ID
        Keyword arguments:
          pname: sort key value
          itype: type of value
        Returns:
          HTML and item name
    '''
    name = ""
    _, version = generate_version_pulldown(DB['neuronbridge']['ddb_published_versioned'], None)
    tbl = f"janelia-neuronbridge-published-{version}"
    try:
        rec = get_dynamodb(tbl, 'itemType', 'searchString', 'searchKey', pname.split(':')[-1])
    except Exception:
        return "", ""
    html = f"<br><div class='hr-with-text'><span>DynamoDB {tbl}</span></div>"
    html += "<h5>Body IDs</h5>"
    for row in rec:
        if not name:
            name = row['name']
        html += '<br>'.join(row['bodyIDs'])
    return html, name


# *****************************************************************************
# * Web content                                                               *
# *****************************************************************************

@app.route("/stats", methods=['GET'])
def stats():
    '''
    Show stats
    Show uptime/requests statistics
    ---
    tags:
      - Diagnostics
    responses:
      200:
          description: Stats
      400:
          description: Stats could not be calculated
    '''
    result = initialize_result()
    result['stats'] = {"version": __version__,
                       "python": sys.version,
                       "pid": os.getpid()}
    return generate_response(result)


@app.route('/library/<library>', methods=['GET'])
def library_query(library):
    ''' Show information for a library
    '''
    payload = ([{"$match": {"libraryName": library}},
                {"$unwind": "$tags"},
                {"$group": {"_id": "$tags", "count":{"$sum": 1}}},
                {"$sort": {"_id": 1}}
               ])
    try:
        rows = DB['neuronbridge'].neuronMetadata.aggregate(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not query emBodies"),
                               message=err)
    html = "<table id='library' class='tablesorter standard'>" \
           + "<thead><tr><th>Tag</th><th>Count</th></tr></thead><tbody>"
    for row in rows:
        html += f"<tr><td>{row['_id']}</td><td>{row['count']:,}</td></tr>"
    html += "</tbody></table>"
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         navbar=generate_navbar('Precompute'),
                                         title=library, html=html))


@app.route('/em_datasets', methods=['GET'])
def em_datasets_query():
    ''' Show information for em datasets
    '''
    dset = {}
    try:
        payload = [{"$group": {"_id": "$dataSetIdentifier", "count":{"$sum": 1}}}]
        payload = [{"$group": {"_id": {"dataset": "$dataSetIdentifier",
                                       "is_neuron": {"$cond": {"if": {"$eq":
                                                                      ["$neuronType", None]},
                                                                      "then": 0, "else": 1}}},
                                 "count":{"$sum": 1}}}
                    ]
        rows = DB['jacs'].emBody.aggregate(payload)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not query emBodies"),
                               message=err)
    for row in rows:
        if row['_id']['dataset'] not in dset:
            dset[row['_id']['dataset']] = {"body": 0, "neuron": 0}
        dset[row['_id']['dataset']]["neuron" if row['_id']['is_neuron'] == 1 \
             else "body"] = row['count']
    try:
        rows = DB['jacs'].emDataSet.find({}).sort([("name", pymongo.ASCENDING),
                                                   ("version", pymongo.DESCENDING)])
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=render_warning("Could not query emDataSets"),
                               message=err)
    html = 'Reported by emBody/emDataSet collections'
    html += "<table id='em_datasets' class='tablesorter standard'>" \
            + "<thead><tr><th>Dataset</th><th>Version</th><th>Bodies</th><th>Neurons</th>" \
            + "</thead><tbody>"
    for row in rows:
        name = row['name']
        if row['version']:
            name += f":v{row['version']}"
        html += f"<tr><td>{row['name']}</td><td>{row['version']}</td>" \
                + f"<td>{dset[name]['body']:,}</td><td>{dset[name]['neuron']:,}</td></tr>"
    html += "</tbody></table>"
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         navbar=generate_navbar('Precompute'),
                                         title="EM datasets", html=html))


@app.route('/published', defaults={'version': ''}, methods=['GET'])
@app.route('/published/<version>', methods=['GET'])
def published_query(version):
    ''' Home page
    '''
    coll = DB["neuronbridge"]["ddb_published_versioned"]
    pulldown, ver = generate_version_pulldown(coll, version)
    if ver and not version:
        version = ver
    libraries = ""
    dynamo = ""
    title = "Select version"
    if version:
        payload = {"dynamodb_version": version}
        results = coll.find_one(payload)
        if not results:
            return render_template('error.html', urlroot=request.url_root,
                                   title='Version not found',
                                   message=f"Published version {version} was not found")
        libraries += "<table id='libraries' class='tablesorter standard'>" \
                     + "<thead><tr><th>Library</th><th>Version</th>" \
                     + "<th>Count</th></tr></thead><tbody>"
        for lib, comp in sorted(results['components'].items()):
            link = f"<a href='/library/{lib}'>{lib}</a>"
            libraries += f"<tr><td>{link}</td><td>{comp['version']}</td>" \
                         + f"<td>{comp['count']:,}</td></tr>"
        libraries += "<tbody></table>"
        table = "janelia-neuronbridge-published-" + version
        title = f"DynamoDB {table}"
        dynamo = ddb_table(table)
    response = make_response(render_template('home.html', urlroot=request.url_root,
                                             navbar=generate_navbar('DynamoDB'),
                                             pulldown=pulldown, libraries=libraries,
                                             title=title, dynamo=dynamo))
    return response


@app.route('/denormalization', methods=['GET'])
def denormalization_query():
    ''' Denormalization page
    '''
    table = 'janelia-neuronbridge-denormalization-prod'
    docs = DYNAMO['resource'].Table(table).scan()
    title = f"DynamoDB {table}"
    html = "<table id='denormalization' class='tablesorter standard'>" \
           + "<thead><tr><th>Library</th><th>AWS S3 prefixes</th>" \
           + "<th>Count</th><th>Subprefixes</th></tr></thead><tbody>"
    for lib in docs['Items']:
        subpre = []
        if "subprefixes" in lib:
            for spre, count in sorted(lib["subprefixes"].items()):
                subpre.append(f"{spre} ({count['count']:,} items)")
        html += f"<tr><td>{lib['keyname']}</td><td>" \
                + f"{re.sub(r'.*amazonaws.com/', '', lib['prefix'])}</td>" \
                + f"<td>{lib['count']:,}</td><td>{'<br>'.join(subpre)}</td></tr>"
    html += "<tbody></table>"
    html += ddb_table(table)
    response = make_response(render_template('general.html', urlroot=request.url_root,
                                             navbar=generate_navbar('DynamoDB'),
                                             title=title, html=html))
    return response


@app.route('/dois', methods=['GET'])
def doi_query():
    ''' DOI page
    '''
    table = "janelia-neuronbridge-publishing-doi"
    response = make_response(render_template('general.html', urlroot=request.url_root,
                                             navbar=generate_navbar('DynamoDB'),
                                             title=f"DynamoDB {table}", html=ddb_table(table)))
    return response


@app.route('/skeletons', methods=['GET'])
def skeletons_query():
    ''' Skeletons page
    '''
    table = "janelia-neuronbridge-published-skeletons"
    response = make_response(render_template('general.html', urlroot=request.url_root,
                                             navbar=generate_navbar('DynamoDB'),
                                             title=f"DynamoDB {table}", html=ddb_table(table)))
    return response


@app.route('/stacks', methods=['GET'])
def stacks_query():
    ''' Stacks page
    '''
    table = "janelia-neuronbridge-published-stacks"
    response = make_response(render_template('general.html', urlroot=request.url_root,
                                             navbar=generate_navbar('DynamoDB'),
                                             title=f"DynamoDB {table}", html=ddb_table(table)))
    return response


@app.route('/lm_releases', methods=['GET'])
def lm_releases_query():
    ''' LM releases page
    '''
    coll = DB["neuronbridge"]["publishedURL"]
    nb_releases = coll.distinct("alpsRelease")
    coll = DB["neuronbridge"]["lmRelease"]
    payload = {"public": True}
    releases = coll.find(payload).sort("release", 1)
    rt = "style='text-align: right'"
    html = 'Reported by lmRelease/publishedURL collections'
    html += "<table id='lm_releases' class='tablesorter standard'>" \
            + "<thead><tr><th>Release</th><th>Lines</th><th>Samples</th>" \
            + "<th>Images</th><th>Secondary images</th></tr></thead><tbody>"
    for rel in releases:
        tc = "style='color: lime !important'" if rel['release'] in nb_releases \
             else "style='color: gold !important'"
        html += f"<tr><td {tc}>{rel['release']}</td><td {rt}>{rel['lines']:,}</td>" \
                + f"<td {rt}>{rel['samples']:,}</td><td {rt}>{rel['images']:,}</td>" \
                + f"<td {rt}>{rel['secondaryImages']:,}</td></tr>"
    html += "</tbody></table>"
    html += "Releases in <span style='color: gold'>gold</span> are public but not " \
            + "yet available on NeuronBridge"
    response = make_response(render_template('general.html', urlroot=request.url_root,
                                             navbar=generate_navbar('LM releases'),
                                             title="LM releases", html=html))
    return response


@app.route('/', methods=['GET'])
@app.route('/search', methods=['GET'])
def show_search():
    ''' Show the search page
        Keyword arguments:
          None
        Returns:
          HTML page
    '''
    return make_response(render_template('search.html', urlroot=request.url_root,
                                         navbar=generate_navbar('Search')))


@app.route("/run_search/<string:key>/<string:stype>", methods=['GET'])
def run_search(key, stype='Publishing name'):
    ''' Sample status page
    '''
    html = neuron = pvhtml = release = ""
    fly_light = stype in ('Publishing name','Sample', 'Slide code')
    if stype in ('Body ID', 'Sample') and not key.isdigit():
        return render_template('error.html', urlroot=request.url_root,
                               title=f"Invalid {stype}",
                               message=f"{key} is not a valid {stype}")
    if stype in ('Publishing name', 'Slide code', 'Neuron type', 'Neuron instance') and key.isdigit():
        return render_template('error.html', urlroot=request.url_root,
                               title=f"Invalid {stype}",
                               message=f"{key} is not a valid {stype}")
    if stype in ('Neuron type', 'Neuron instance'):
        pvhtml, neuron = get_published_versioned(key.lower())
        if neuron:
            key = neuron
    if stype == "Slide code":
        key = key.upper()
    try:
        if fly_light:
            html, pname = show_jacs(key, itype=stype)
            if stype == 'Publishing name' and pname:
                key = pname
            html2, _ = show_jacs(key, itype=stype, table='image')
            html += html2
        else:
            html += show_emb(key, itype=stype)
        html2, _, _ = show_nmd_purl(key, itype=stype)
        html += html2
        if stype in ('Neuron type', 'Neuron instance'):
            pname = ""
            html += pvhtml
        else:
            html2, release, pname = show_nmd_purl(key, itype=stype, table='publishedURL')
            html += html2
        if fly_light:
            html += show_pli(key, itype=stype, release=release)
        elif stype == 'Body ID':
            html += get_skeletons(pname[0])
            html += get_custom(key)
        if pname:
            html += get_dois(pname)
    except Exception as err:
        return render_template('error.html', urlroot=request.url_root,
                               title=f"Error querying {stype} {key}",
                               message=f"{str(err)}<br><pre>{traceback.format_exc()}</pre>")
    return make_response(render_template('general.html', urlroot=request.url_root,
                                         navbar=generate_navbar('Search'),
                                         title=f"{stype} {key}", html=html))

# *****************************************************************************


if __name__ == '__main__':
    app.run(debug=True)
