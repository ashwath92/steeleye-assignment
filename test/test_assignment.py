#-------------------------------------------------------------------------------
# Test cases
#-------------------------------------------------------------------------------
import os
import sys
import pytest
from configparser import ConfigParser
from lxml import etree
import pandas as pd
import boto3

sys.path.append(os.path.abspath('.'))
from src import assignment

config = ConfigParser(interpolation=None)
#config.read(os.path.join('..', 'src', 'config.cfg'))
curwd = os.getcwd()
files_path = os.path.join(curwd, 'files')
config_file = os.path.abspath(os.path.join('src', 'config.cfg'))
if curwd.endswith('test'):
    files_path = os.path.abspath(os.path.join('..', 'files'))
    config_file = os.path.abspath(os.path.join('..', 'src', 'config.cfg'))

config.read(config_file)

base_xml_file = config.get('meta', 'xmllink')
output_csv_file = config.get('meta', 'csvfile')
region = config.get('aws', 'aws_region')
key = config.get('aws', 'aws_access_key_id')
secret = config.get('aws', 'aws_secret_access_key')
token = config.get('aws', 'aws_session_token')
bucket = config.get('aws', 's3_bucket')

def test_request_url_success():
    """ Tests if the URL request was made correctly"""
    assert assignment.request_url(base_xml_file).status_code == 200

def test_request_url_failure():
    """ Tests if the URL request returned an http client error code for
    an incorrect url"""
    incorrect_url = f'{base_xml_file}q'
    with pytest.raises(SystemExit):
        assignment.request_url(incorrect_url)

def test_get_zip_file_name_success():
    """ Tests if the correct zip file is extracted from the base xml file"""
    expected = 'http://firds.esma.europa.eu/firds/DLTINS_20210117_01of01.zip'
    assert assignment.get_zip_file_name(base_xml_file) == expected

def test_get_zip_file_name_failure():
    """ Tests if the correct zip file is extracted from the base xml file"""
    incorrect_url = f'{base_xml_file}q'
    expected = 'http://firds.esma.europa.eu/firds/DLTINS_20210117_01of01.zip'
    with pytest.raises(SystemExit):
        assignment.get_zip_file_name(incorrect_url)

def test_download_unzip_zip_file_default():
    """ tests the download_unzip_zip_file with default args, i.e.,
        with save_zip set to false and zipfile_name=''. ASSERTIONS:
        1. Test if the zip file is extracted to give one xml file
        2. Test if the returned xml filename is as expected. We return the
        filename with the directory, so the test uses os.path.basename
        2. if the zip file is saved when the option save_zip is set to True
        3.
    """
    zip_url = 'http://firds.esma.europa.eu/firds/DLTINS_20210117_01of01.zip'
    xml_file = assignment.download_unzip_zip_file(zip_url, files_path)
    expected_xml_file = 'DLTINS_20210117_01of01.xml'
    # Assert that the xml file is present in the files directory
    list_of_files = os.listdir(files_path)
    assert expected_xml_file in list_of_files
    # Assert that the returned xml file name is as expected
    assert os.path.basename(xml_file) == expected_xml_file

def test_download_unzip_zip_file_save_zip():
    """ tests the download_unzip_zip_file with save_zip set to True
    and zipfile_name='xml.zip'. ASSERTIONS:
        1. Test if the zip file is extracted to give one xml file
        2. Test if the returned xml filename is as expected. We return the
        filename with the directory, so the test uses os.path.basename
        3. Test if the zip file is saved when the option save_zip is set to True
    """
    zip_url = 'http://firds.esma.europa.eu/firds/DLTINS_20210117_01of01.zip'
    xml_file = assignment.download_unzip_zip_file(
                    zip_url, files_path, save_zip=True, zipfile_name='xml.zip')
    expected_xml_file = 'DLTINS_20210117_01of01.xml'
    expected_zip_file = 'xml.zip'
    # Assert that the xml file is present in the files directory
    list_of_files = os.listdir(files_path)
    assert expected_xml_file in list_of_files
    assert expected_zip_file in list_of_files
    # Assert that the returned xml file name is as expected
    assert os.path.basename(xml_file) == expected_xml_file

def test_parse_xml():
    """ Tests if the xml from the unzipped xml file can be parsed properly,
    and if the function returns an lxml.etree._Element object with the
    expected tag. This object should have 141382 children. """
    xml_file_with_path = os.path.join(files_path, 'DLTINS_20210117_01of01.xml')
    fininstr = assignment.parse_xml(xml_file_with_path)
    assert isinstance(fininstr, etree._Element)
    prefix = '{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}'
    expected_val = f'{prefix}FinInstrmRptgRefDataDltaRpt'
    assert expected_val == fininstr.tag
    assert len(fininstr.getchildren()) == 141382

def test_xml_to_csv():
    """ Tests if the parsed xml is written correctly to a csv file. If
    successfuly, 141381 records must be written to the csv file (all except
    the first child of fininstr). """
    csv_file_path = os.path.join(files_path, 'fininstr.csv')
    expected_csv = 'fininstr.csv'
    xml_file_with_path = os.path.join(files_path, 'DLTINS_20210117_01of01.xml')
    fininstr = assignment.parse_xml(xml_file_with_path)
    assignment.xml_to_csv(fininstr, csv_file_path)
    list_of_files = os.listdir(files_path)
    assert expected_csv in list_of_files
    df_fininstr = pd.read_csv(csv_file_path)
    csv_columns = df_fininstr.columns.values.tolist()
    expected_header_cols = ['FinInstrmGnlAttrbts.Id',
        'FinInstrmGnlAttrbts.FullNm',
        'FinInstrmGnlAttrbts.ClssfctnTp',
        'FinInstrmGnlAttrbts.CmmdtyDerivInd',
        'FinInstrmGnlAttrbts.NtnlCcy',
        'Issr']
    assert expected_header_cols == csv_columns
    # Check if all the children have been written to the csv file
    assert len(df_fininstr) == 141381

def test_write_csv_to_s3():
    """ Tests if (i) the new s3 bucket has been created and (ii) if the
    csv file has been written into the bucket. """
    s3 = boto3.client(
        's3',
        aws_access_key_id=key,
        aws_secret_access_key=secret,
        aws_session_token=token,
        region_name=region)

    s3r = boto3.resource(
        's3',
        aws_access_key_id=key,
        aws_secret_access_key=secret,
        aws_session_token=token,
        region_name=region)

    csv_file = os.path.join(files_path, 'fininstr.csv')
    assignment.write_csv_to_s3(csv_file, region, key, secret, token, bucket)
    bucket_response = s3.list_buckets()
    #print(bucket_response["Buckets"], type(bucket_response["Buckets"]))
    #assert bucket in bucket_response["Buckets"]
    my_bucket = s3r.Bucket(bucket)
    #my_bucket.name
    s3_keys = []
    for file in my_bucket.objects.all():
        s3_keys.append(file)
    s3_filename = os.path.basename(csv_file)
    assert s3_filename == s3_keys[0].key