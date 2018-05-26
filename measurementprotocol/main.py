#main.py
#ENHANCEMENTS
#Get the file from the sFTP 
#Parse zip code and set IP/Geo Override for geo reports
#Add user ID in addition to client id
#Add Google most recent gclid
#Notifications (failure and completion)

#MODULES
import os
from measurementprotocol import config
from measurementprotocol import csv_parse
from measurementprotocol import build_hit

#CONFIGURATION VARIABLES
config_dict = config.config_dict
events = config_dict['dimension_settings']['events']
file_headers = config_dict['file_parameters'].get('file_headers')
environments = config_dict['measurement_protocol']['environments']
os.environ['SCRIPT_ENV'] = environments['debug_domain']
env_var = os.environ['SCRIPT_ENV']
file_location = config_dict['file_parameters'].get('file_location')
file_delimiter = config_dict['file_parameters'].get('file_delimiter')
time_format = config_dict['file_parameters'].get('time_format')
event_flag = config_dict['file_parameters'].get('event_flag')
properties = config_dict['measurement_protocol'].get('properties')
event_param = config_dict['measurement_protocol'].get('event_param')

payload_dimensions = dict(protocol_version = config_dict['measurement_protocol'].get('protocol_version'),
                          hit_type = config_dict['dimension_settings'].get('hit_type'),
                          data_source = config_dict['dimension_settings'].get('data_source'),
                          event_category = config_dict['dimension_settings'].get('event_category'),
                          flag_dimension = config_dict['dimension_settings'].get('flag_dimension'),
                          flag_value = config_dict['dimension_settings'].get('flag_value'))

#SCRIPT
csv_parse.csv_parse_validate(file_location, file_delimiter, file_headers, time_format, event_flag)

file_list = csv_parse.list_from_csv(file_location)

build_hit.build_send_valid_hit(properties, file_list, file_headers, event_flag, event_param, events, env_var, environments, payload_dimensions, time_format)