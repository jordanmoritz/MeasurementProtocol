#csv_parse.py
import csv
from datetime import datetime

#Detects and verifies delimiter
def check_csv_delimiter(delimiter, csv_file):
    dialect = csv.Sniffer().sniff(csv_file.readline())
    assert dialect.delimiter is delimiter, f'Delimiters is incorrect, expected: {delimiter}'
    csv_file.seek(0)

#Verifies file headers exist
def check_csv_has_headers(csv_file):
    has_headers = csv.Sniffer().has_header(csv_file.readline())
    assert has_headers is True, f'File doesn\'t appear to have headers'
    csv_file.seek(0)

#Checks file headers against intended
def validate_headers(dict_reader, headers, csv_file):
    fields = dict_reader.fieldnames
    assert fields == headers, f'Headers don\'t match, expected: {headers}'
    csv_file.seek(0)

#Verifies datetime formatting of timestamp
def check_time(time_value, time_format):
    try:
        datetime.strptime(time_value, time_format)
    except ValueError:
        raise ValueError(f'Date formatting is incorrect, expected {time_format}')

#Checks values for correct flags
#Range portion is not great, but a little more extensible than what I had
def check_flags(row, event_flag, headers):
    for header in range(2, 4):
        assert row[headers[header]] == event_flag or row[headers[header]] == '0', f'Unexpected values for {headers[header]} field'
        
#Validates row values match expected inputs
#Should consider running through all, then exporting all exceptions
def validate_values(dict_reader, headers, csv_file, time_format, event_flag):
    for row in dict_reader:
        
        #Moves past header row
        if row[headers[0]] == headers[0]:
            continue

        check_time(row[headers[0]], time_format)

        #Checks ID formatting
        #Should build this out more once I have real IDs
        assert type(row[headers[1]]) == str, f'Unexpected data type for {headers[1]} field'
        
        check_flags(row, event_flag, headers)
        
    csv_file.seek(0)

def csv_parse_validate(file, delimiter, headers, time_format, event_flag):
    with open(file, 'r') as csv_file:
        
        check_csv_delimiter(delimiter, csv_file)
        check_csv_has_headers(csv_file)
    
        #Creates DictReader object
        dict_reader = csv.DictReader(csv_file)

        validate_headers(dict_reader, headers, csv_file)
        validate_values(dict_reader, headers, csv_file, time_format, event_flag)
        
        csv_file.close()

def list_from_csv(file):
    with open(file, 'r') as csv_file:
        #Creates reader iterable object
        reader = csv.reader(csv_file)

        #Gets data in reader object to list
        file_list = []
        for row in reader:
            file_list.append(row)

        #Removes field names from list
        file_list.pop(0)

        csv_file.close()
        
        return file_list