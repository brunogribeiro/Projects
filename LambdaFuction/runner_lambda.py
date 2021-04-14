import boto3
import constants
import json
import logger
import requests
from decomp_utils import db
from datetime import datetime, date
from sys import argv


def get_date_from_file(file_name):
    """
    args:
        file_name(str): Name of the file to be parsed
    return:
        Date of the file as (dd.mm.aaaa)
    """
    current_date = file_name[-10:]
    # print(type(current_date))
    # print(f'date {current_date}')
    return datetime.strptime(current_date, "%d.%m.%Y")


def is_current_older(file_rds, file_s3):
    """
    args:
        file_rds(str):  file path at s3 for the most recent  file in database
        file_s3(str): File name for the most recent  file at s3
    return(bool):
        True if database is NOT up to date (current is older)
        False if database is up to date (current is not older)
    """
    # print('is_current_older')
    try:
        datetime_file_rds = get_date_from_file(file_rds)
        # print(datetime_file_rds)
        datetime_file_s3 = get_date_from_file(file_s3)
        # print(datetime_file_s3)
    except Exception as error:
        raise Exception(
            f"Converting date in  file name to datetime. Error: {error}")
    if datetime_file_rds < datetime_file_s3:
        return True
    else:
        return False


def get_file_to_update():
    """
    Checks if the database (RDS) is up to date with the newest file from S3 (ons bucket)
    return:
        (str) if RDS need to be updated, return newest  file path at s3
        (None) if RDS is up to date
    """
    print('get_file_to_update')

    # Hard coded variables
    # aws variables
    aws_access_key_id = ""
    aws_secret_access_key = ""
    aws_region = ""
    aws_bucket = ""
    # db variables
    db_name = "postgres"
    db_user = "postgres"
    db_password = "Amarelo9"
    db_host = ""
    db_port = ""
    db_table = ""
    #  variables
    last_file_s3 = ""
    last__filename_s3 = ""
    _file_lambda = f'/tmp/{last__filename_s3}'


    s3_client = boto3.client('s3',
                            aws_access_key_id=aws_access_key_id,
                            aws_secret_access_key=aws_secret_access_key,
                            region_name=aws_region
                            )

    # Query the DB for the max value in the TimesTamp field
    db_connection = db.get_connection(db_name, db_user, db_password, db_host, db_port)
    sql = f"select to_char(timestamp, 'MM.DD.YYYY') as teste from (select max(timestamp) as timestamp from {db_table}) a"
    rds_last_timestamp = db.consult(db_connection, sql)
    last__file_rds = (f'_{rds_last_timestamp[0][0]}')
    # Check if download is required
    try:
        s3_response = s3_client.list_objects_v2(
            Bucket=aws_bucket, Prefix="/")
    except Exception as error:
        raise Exception(f"Getting  files from S3. Error: {error}")

    _files_s3 = s3_response["Contents"]
    if len(_files_s3) < 2:
        raise Exception('There is no file in AWS-S3 for download')
    else:
        for file in files_s3:
            file_s3 = file['Key']
            if '_' in file_s3:
                # s3  file format: _dd.mm.aaaa.xls
                file_name_s3 = file_s3.split('/')[-1][:-4]
                # print(f'last__file_rds {last__file_rds}')
                # print(f'_file_name_s3 {_file_name_s3}')
                if is_current_older(last__file_rds, _file_name_s3):
                    return file_s3
        print("RDS already up to date")
        return None


def run_lambda(file_s3):
    print('run_lambda')
    # invoke lambda api
    # endpoint for lambda api
    url = ""
    print(file_s3)
    data = json.dumps({'last_file_s3': file_s3})

    response = requests.post(url=url, data=data)
    print(
        f'status code from lambda: {response.status_code}')
    print(f'text from lambda: {response.text}')


# ------- M A I N -------

if argv[1] not in ('Parametro'):
    raise Exception('ERROR: Invalid parameters!')

if argv[1] == 'Parametro':
    file_to_update = get_file_to_update()
    if file_to_update:
        run_lambda(file_to_update)

startTime = datetime.now()
if logger.hasError(startTime):
    raise Exception(f'{argv[1]} error!')

