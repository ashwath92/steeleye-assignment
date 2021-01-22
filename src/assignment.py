""""
Assignment for Steeleye Python engineer
AUTHOR: Ashwath Sampath
Requirement:

Download the xml from this link
From the xml, please parse through to the first download link whose file_type is DLTINS and download the zip
Extract the xml from the zip.
Convert the contents of the xml into a CSV with the following header:
FinInstrmGnlAttrbts.Id
FinInstrmGnlAttrbts.FullNm
FinInstrmGnlAttrbts.ClssfctnTp
FinInstrmGnlAttrbts.CmmdtyDerivInd
FinInstrmGnlAttrbts.NtnlCcy
Issr
Store the csv from step 4) in an AWS S3 bucket
The above function should be run as an AWS Lambda (Optional)

"""
import os
import sys
import csv
import boto3
from botocore.exceptions import ClientError
import requests
import logging
import uuid
from lxml import etree
import zipfile
from configparser import ConfigParser
import io
from bs4 import BeautifulSoup

LOG_FORMAT = '%(levelname)s %(asctime)s %(message)s'
logfile = os.path.join('logs', 'project_log.log')
logging.basicConfig(filename=logfile,
                    filemode='w',
                    level=logging.INFO,
                    format=LOG_FORMAT)
logger = logging.getLogger()


def request_url(url):
    """
    Sends an http request to the url, returns the response. If there is an
    error, it exits from the program

    ARGUMENTS:
    url (string): xml/html url

    RETURNS:
    response (HttpResponse): the http response if requests.get(url).ok
    """
    try:
        response = requests.get(url)
        # Raise http errors
        response.raise_for_status()
    except requests.exceptions.HTTPError as httpe:
        logger.error(f'Http error: {httpe}\n')
        sys.exit(1)
    except requests.exceptions.ConnectionError as conne:
        logger.error(f'Connection error: {conne}\n')
        sys.exit(2)
    except requests.exceptions.Timeout as timee:
        logger.error(f'Timeout error: {timee}\n')
        sys.exit(3)
    except requests.exceptions.RequestException as othere:
        logger.error(f'Other error: {othere}\n')
        sys.exit(4)
    else:
        # No exceptions, return response
        return response


def get_zip_file_name(base_xml_file):
    """
    Parses the xml in the given xml file, gets the zip file name from the
    first download link whose file_type is DLTINS, and returns it

    ARGUMENTS:
    base_xml_file(string): the ESMA Register xml file url. The xml contains a
                           no. of files along with download links and metadata
    RETURNS:
    zip_filename (string): the name of the zip file which has to be downloaded
                           If no zip file url is present, '' is returned

    """
    main_xml_page = request_url(base_xml_file)
    if main_xml_page.ok:
        try:
            soup = BeautifulSoup(main_xml_page.content, 'lxml')
        except Exception as e:
            logging.error(f"Unexpected exception while parsing xml: {e}")
            sys.exit(6)
        download_links = [
            link.text for link in soup.find_all(
                'str', {
                    'name': 'download_link'})]
        for download_link in download_links:
            if download_link.rsplit('/')[-1].startswith('DLTINS'):
                logger.info(
                    f"The {download_link} ZIP file will be downloaded. \n")
                return download_link
        return ''

    raise Exception('Unexpected error during http request of xml file')
    logger.error('Unexpected error during http request of xml file')
    sys.exit(4)


def download_unzip_zip_file(url, subdir, save_zip=False, zipfile_name=''):
    """
    Downloads the zip file mentioned in url in chunks, and unzips it.
    By default, this function does not save the zip file on disk, it
    only saves the final xml file extracted from the zip file. The xml
    file is saved in the files/subdirectory.
    If you want the zip file to be saved, set save_zip=True

    ARGUMENTS:
    url (string): a string url which points to a zip file
    subdir (string): subdirectory in which to save the xml
                     and (optionally) the zip file.
    save_zip (boolean): False by default.
                        If False, it extracts the zip file in memory
                        If True, it stores the extracted zip file on disk,
                        in 'subdirectory'
    zipfile_name (string): '' by default
                           Name which the zip file will be saved as (only
                           if save_zip is True)

    RETURNS:
    xml_file_path (string): the name of the extracted xml file, with the full
                            path

    """
    zip_page = request_url(url)
    if zip_page.ok:
        try:
            if not save_zip:
                # Create a file-like BytesIO object instead of
                # saving the zip file
                zipfile_obj = io.BytesIO(zip_page.content)
                zip_file = zipfile.ZipFile(zipfile_obj)
                zip_file.extractall(subdir)
                # Return unzipped file name
                xml_filename = os.path.join('files', zip_file.namelist()[0])
                logger.info(f'Extracted XML file: {xml_filename}\n')
                return xml_filename
            else:
                # Save zip file to disk
                zip_filepath = os.path.join(subdir, zipfile_name)
                with open(zip_filepath, 'wb') as zipfile_obj:
                    zipfile_obj.write(zip_page.content)
                logger.info(f'Zip file stored at: {zip_filepath}\n')
                with zipfile.ZipFile(zip_filepath, 'r') as zip_file:
                    #logging.info(f'Zip file contents: {zip_file.printdir}')
                    unzipped_files = zip_file.namelist()
                    zip_file.extractall(subdir)
                    xml_filename = os.path.join(
                        'files', zip_file.namelist()[0])
                    logger.info(f'Extracted XML file: {xml_filename}\n')
                    return xml_filename

        except zipfile.BadZipFile as bade:
            logging.error(f'Corrupt zip file: {bade}')
            sys.exit(5)

    raise Exception('Unexpected error during http request of zip file')
    logger.error('Unexpected error during http request on zip file')
    sys.exit(4)


def parse_xml(xml_file_with_path):
    """
    Takes an xml file as argument, gets its root as an lxml etree element,
    and returns the root[1][0][0] etree element (subtree) which corresponds
    to the FinInst elements from whose children data has to be fetched.

    ARGUMENTS:
    xml_file_with_path (string): the path of the xml file which has to be
                                 parsed

    RETURNS:
    fininstr (lxml.etree._Element): an lxml.etree element whose children
                                   hold the required data
    """
    try:
        parser = etree.XMLParser(ns_clean=True)
        tree = etree.parse(xml_file_with_path, parser)
        root = tree.getroot()
        fininstr = root[1][0][0]
        return fininstr
    except etree.ParseError as epe:
        logging.error(f'The extracted xml file could not be parsed, {epe}')
        sys.exit(6)


def xml_to_csv(fininstr, csv_file_path):
    """
    Takes the root of an xml tree (fininstr), parses it, and writes the
    resulting data into a csv file.

    ARGUMENTS:
    fininstr (lxml.etree._Element): an lxml.etree element whose children
                                   hold the required data
    csv_file_path (string): file name (with path) of the output csv file
    """
    logger.info(f'No. of records in xml file:{len(fininstr.getchildren())-1}')
    fieldnames = ['FinInstrmGnlAttrbts.Id',
                  'FinInstrmGnlAttrbts.FullNm',
                  'FinInstrmGnlAttrbts.ClssfctnTp',
                  'FinInstrmGnlAttrbts.CmmdtyDerivInd',
                  'FinInstrmGnlAttrbts.NtnlCcy',
                  'Issr']
    try:
        with open(csv_file_path, mode='w') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            prefix = '{urn:iso:std:iso:20022:tech:xsd:auth.036.001.02}'
            for finelement in fininstr[1:]:
                # finelement[0][0] is the FinInstrmGnlAttrbts element:
                #  4 of the required tags are its children
                id = finelement[0][0].find(f'./{prefix}Id').text
                full_nm = finelement[0][0].find(f'./{prefix}FullNm').text
                clssfctn_tp = finelement[0][0].find(
                    f'./{prefix}ClssfctnTp').text
                cmmdty_deriv_ind = finelement[0][0].find(
                    f'./{prefix}CmmdtyDerivInd').text
                ntnl_ccy = finelement[0][0].find(f'./{prefix}NtnlCcy').text
                issr = finelement[0].find(f'./{prefix}Issr').text

                writer.writerow(
                    {'FinInstrmGnlAttrbts.Id': id,
                     'FinInstrmGnlAttrbts.FullNm': full_nm,
                     'FinInstrmGnlAttrbts.ClssfctnTp': clssfctn_tp,
                     'FinInstrmGnlAttrbts.CmmdtyDerivInd': cmmdty_deriv_ind,
                     'FinInstrmGnlAttrbts.NtnlCcy': ntnl_ccy,
                     'Issr': issr})

        logger.info("Wrote csv file successfully \n")

    except etree.ParseError as epe:
        logging.error(f'The extracted xml file could not be parsed, {epe}')
        sys.exit(6)
    except csv.Error as cse:
        logging.error(f'CSV writing error, {cse}')
        sys.exit(7)

def write_csv_to_s3(csv_file, region, key, secret, token, bucket):
    """
    Writes the csv_file in the arguments to a new s3 bucket.

    ARGUMENTS:
    csv_file (string): the csv file which has to be written to s3
    region (string): the AWS region
    key (string): the AWS access key
    secret (string): the AWS secret access key
    token (string): the AWS access token
    bucket (string): the name of the new S3 bucket
    """
    s3 = boto3.client(
        's3',
        aws_access_key_id=key,
        aws_secret_access_key=secret,
        aws_session_token=token,
        region_name=region)
    # Create bucket
    try:
        #location = {'LocationConstraint': region}
        s3.create_bucket(Bucket=bucket)

        # Write file into new bucket
        s3_filename = os.path.basename(csv_file)
        #with open(csv_file, 'rb') as file:
        s3.upload_file(csv_file, bucket, s3_filename)
        logger.info('CSV file written into s3 bucket')

    except ClientError as ce:
        logger.error(ce)
        sys.exit(8)


def main():
    """
    Main function which reads the configuration parameters from the config
    file and calls the various functions.
    """
    # Disable interpolation so that we can read in urls with % properly
    config = ConfigParser(interpolation=None)
    config.read('config.cfg')
    base_xml_file = config.get('meta', 'xmllink')
    output_csv_file = config.get('meta', 'csvfile')
    region = config.get('aws', 'aws_region')
    key = config.get('aws', 'aws_access_key_id')
    secret = config.get('aws', 'aws_secret_access_key')
    token = config.get('aws', 'aws_session_token')
    bucket = config.get('aws', 's3_bucket')
    # Get zip file url from base xml file
    zip_file_url = get_zip_file_name(base_xml_file)
    if zip_file_url == '':
        logging.exception('No Zip file url found in xml file. Exited program')
        raise 'No Zip file url found in xml file. Exited program'
        sys.exit(0)

    # Get xml file from zip file
    unzipped_xml = download_unzip_zip_file(zip_file_url, 'files')
    logger.info(f"Unzipped XML File Name (in main): {unzipped_xml}\n")

    # Parse xml file, get the root, and return the [1][0][0] child
    # of the root (this is an lxml.etree._Element element).
    fininstr = parse_xml(unzipped_xml)

    # Write contents of lxml.etree element to csv
    csv_file_path = os.path.join('files', output_csv_file)
    xml_to_csv(fininstr, csv_file_path)

    # Write the newly created csv file into a fresh s3 bucket
    write_csv_to_s3(csv_file_path, region, key, secret, token, bucket)

    log_l = os.path.abspath('../logs/')
    print(f"Go to S3 to see the CSV file, and {log_l} to see the log file. ")


if __name__ == '__main__':
    main()
