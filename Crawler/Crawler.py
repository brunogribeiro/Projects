from sys import path as sys_path
sys_path.insert(0, '/home/user1/')
sys_path.insert(0, '/home/user2/')
from datetime import date
from decomp_utils import s3
from selenium import webdriver
from shutil import rmtree, move, make_archive
from zipfile import ZipFile
import boto3
import constants
import time
import os

"""
defining parameters for running the cralwer
"""
s3Client = boto3.client('s3')

bucket = constants.aws_bucket
actualDate = date.today().strftime('%Y-%m-%d')
path_ = '/home/user1/crawler'
pathDownloads = '/home/user1/crawler/downloads'

# Clean download's folder
if os.path.exists(path_):
    rmtree(path_)

os.mkdir(path_)
os.mkdir(pathDownloads)

chromeOptions = webdriver.ChromeOptions()
prefs = {"download.default_directory": pathDownloads}
chromeOptions.add_experimental_option("prefs", prefs)


# arguments to run in container with chrome headless
chromeOptions.add_argument("--disable-dev-shm-usage")
chromeOptions.add_argument("--no-sandbox")
chromeOptions.add_argument("--headless")
chromeOptions.add_argument('--disable-gpu')
chromeOptions.add_argument('--window-size=1280x1696')
chromeOptions.add_argument('--user-data-dir=/tmp/user-data')
chromeOptions.add_argument('--hide-scrollbars')
chromeOptions.add_argument('--enable-logging')
chromeOptions.add_argument('--log-level=0')
chromeOptions.add_argument('--v=99')
chromeOptions.add_argument('--single-process')
chromeOptions.add_argument('--data-path=/tmp/data-path')
chromeOptions.add_argument('--ignore-certificate-errors')
chromeOptions.add_argument('--homedir=/tmp')
chromeOptions.add_argument('--disk-cache-dir=/tmp/cache-dir')
chromeOptions.add_argument(
    'user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36')

driver = webdriver.Chrome(
    # service_log_path='/tmp/chromedriver.log',
    options=chromeOptions
)


def site_login():
    """
    Login to account in
    """
    driver.get('https://SITE.app/home/')
    driver.find_element_by_id('nomeUsuario').send_keys('USER')
    driver.find_element_by_id('senha').send_keys('senha!')
    driver.find_element_by_css_selector('.btn-primary').click()
    print('Login in the site successful!!')


def downloadFiles():
    """
    Download files from
    """
    driver.find_element_by_id('ckbSelecionarTodos').click()
    time.sleep(5)
    id_ = 'btnBaixarSelecionadosPrevs'
    driver.find_element_by_id(id_).click()
    print(' downloads complete!')


def manage_files():
    """
    Search for zip files in a directory, unzip them in a given folder,
    iterate through its sub folders and move files to a given path
    """
    # list all zip files in cwd folder
    zipFiles = [f for f in os.listdir(pathDownloads) if '.zip' in f]
    print(zipFiles)
    # unzip files
    for file in zipFiles:
        zipFilePath = pathDownloads + '/' + file
        unzipedFolderPath = os.path.splitext(zipFilePath)[0] + '/'
        # Unzip file to unzipedFolderPath
        with ZipFile(zipFilePath, 'r') as f:
            f.extractall(unzipedFolderPath)
        print("File unziped")
        # delete zip file
        os.remove(zipFilePath)
    # iterate through all folder in unziped folder and move the files to one single folder
    SourceFolders = [f for f in os.listdir(
        unzipedFolderPath) if not f.startswith('.')]
    print("Moving files")
    for SourceFolder in SourceFolders:
        SourceFolderPath = unzipedFolderPath + SourceFolder
        files = [f for f in os.listdir(
            SourceFolderPath) if not f.startswith('.')]
        for file in files:
            filePath = SourceFolderPath + '/' + file
            move(filePath, unzipedFolderPath)
        rmtree(SourceFolderPath)
    print("Manage files complete")


def uploadS3():
    """"
    upload the collected files to S3.
    """
    SourceFolder = [f for f in os.listdir(pathDownloads) if not f.startswith('.')][0]
    SourceFolderPath = f'{pathDownloads}/{SourceFolder}'
    files = [f for f in os.listdir(SourceFolderPath) if not f.startswith('.')]
    for file in files:
        filePath = f'{SourceFolderPath}/{file}'
        s3Path = f'/prevs/{actualDate}/{file}'
        print(f'uploading file {filePath} to {s3Path}')
        s3Client.upload_file(filePath, bucket, s3Path)
    print("Success! Files Uploaded")


def verifyS3():
    """"
    checks that all collected files have been sent correctly to S3.
    """
    print('waiting s3 to update...')
    localFiles = len(os.listdir(pathDownloads))
    s3Files = len(s3.get_files_list(bucket, s3Path))
    start = datetime.now()
    attempt = 0
    while localFiles != s3Files:
        attempt += 1
        print(f"attempt: {attempt}")
        print(f'files uploaded {s3Files}/{localFiles}')
        sleep(10)
        s3Files = len(s3.get_files_list(bucket, s3Folder))
        if attempt == 10:
            print('attempts limit reached')
            break
    end = datetime.now()
    uploadTime = end - start
    print(f'time to upload {uploadTime}')
    print('all files uploaded')


# -------------- M A I N ------------------


try:
    site_login()
    time.sleep(15)
    downloadFiles()
    time.sleep(20)
    manage_files()
    s3.delete_folder(bucket, f'/process/{actualDate}')
    uploadS3()
    verifyS3()
except Exception as e:
    print(e)
finally:
    driver.close()
    print("driver closed")
