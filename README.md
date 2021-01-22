## Directory structure

steeleye-assignment/<br/>
├── files/<br/>
├── logs/<br/>
├── src/<br/>
└── test/

### Main module
The main module is located at steeleye-assignment/src/assignment.py.
This performs the required tasks which are mentioned below. A much more detailed explanation is available in the pydoc.

Download the xml from this link
From the xml, please parse through to the first download link whose file_type is DLTINS and download the zip
Extract the xml from the zip (by default, I don't store the zip file, I extract it from memory)
Convert the contents of the xml into a CSV with the following header:
FinInstrmGnlAttrbts.Id
FinInstrmGnlAttrbts.FullNm
FinInstrmGnlAttrbts.ClssfctnTp
FinInstrmGnlAttrbts.CmmdtyDerivInd
FinInstrmGnlAttrbts.NtnlCcy
Issr
Store the csv from step 4) in an AWS S3 bucket

### Config file
The config file contains a number of config parameters -- filenames and AWS-related fields. __The AWS-fields
HAVE TO BE CHANGED WHEN ANOTHER PERSON RUNS THIS CODE__.

### Test cases
The test cases are present in the steeleye_assignment/test/ directory. This is done using pytest 
__Run the test cases from the base directory using py.test.__ 

### Log file
A log file is generated and subsequently overwritten each time the program runs. This is present in python/assignment/logs.

### Generated file
Finally, the generated files will be stored in steeleye_assignment/files/. These include 
(i) The zip file: not saved by default, but can be saved by setting a parameter in assignment.py
(ii) The xml file extracted from the zip file
(iii) the csv file obtained after parsing the extracted xml file. 

### Pydoc
To generate the pydoc for the main module, run __python -m pydoc src.assignment__ from the main directory

To view as html, run: python -m pydoc -p 0,
and select the port where the server starts up.
E.g.: http://localhost:55966/src.assignment.html