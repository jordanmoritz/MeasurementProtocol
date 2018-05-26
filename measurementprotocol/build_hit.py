#build_hit.py
from datetime import datetime
import requests
import random
import json

#This should be rebuilt
def build_payload(properties, payload_dimensions):
    payload = {'v': payload_dimensions.get('protocol_version'),
               't': payload_dimensions.get('hit_type'),
               'ds': payload_dimensions.get('data_source'),
               'ec': payload_dimensions.get('event_category'),
               'tid': properties,
               payload_dimensions.get('flag_dimension'): payload_dimensions.get('flag_value')}
    
    return payload

def time_diff_append(timestamp, time_format, payload):
    hit_time = datetime.strptime(timestamp, time_format)
    time_diff = datetime.now() - hit_time
    queue_time = round(time_diff.total_seconds() * 1000)
    payload.update({'qt': queue_time})

def event_append(row, field, flag, param, events_list, payload):
    header_index = row.index(flag)
    event_header = field[header_index]
    for event in range(len(events_list)):
        if event_header == events_list[event].get('header'):
            payload.update({param: events_list[event].get('event_action'),
                            events_list[event].get('dimension_index'): events_list[event].get('event_value')})
            return payload

def check_hit_validity(hit_result):
    hit_is_valid = hit_result.get('valid')
    hit_parser_message = hit_result.get('parserMessage')

    #Stores number of errors/warnings from response (not great)
    error_count = 0
    warn_count = 0

    #If invalid, parses out error messages and raises error
    #I can consolidate and refactor this with the below
    if hit_is_valid == False:
        for error in range(len(hit_result.get('parserMessage'))):
            hit_error_type = hit_result.get('parserMessage')[error].get('messageType')
            hit_error_desc = hit_result.get('parserMessage')[error].get('description')
            hit_error_code = hit_result.get('parserMessage')[error].get('messageCode')
            hit_error_parameter = hit_result.get('parserMessage')[error].get('parameter')
            print(f'{hit_error_type} - {hit_error_code} - {hit_error_parameter}: {hit_error_desc} \n')
            error_count += 1
        assert hit_is_valid == True, f'{error_count} error(s) in request formatting. See log for specifics.'

    #Same idea, but for WARN/INFO messages
    elif hit_parser_message:
        for message in range(len(hit_result.get('parserMessage'))):
            hit_warn_type = hit_result.get('parserMessage')[message].get('messageType')
            hit_warn_desc = hit_result.get('parserMessage')[message].get('description')
            hit_warn_code = hit_result.get('parserMessage')[message].get('messageCode')
            hit_warn_parameter = hit_result.get('parserMessage')[message].get('parameter')
            print(f'{hit_warn_type} - {hit_warn_code} - {hit_warn_parameter}: {hit_warn_desc} \n')
            warn_count += 1
        assert not hit_parser_message, f'{warn_count} warning(s) in request. See log for specifics.'

def parse_hit_result(request):
    
    #Decodes server response stores in dict
    encoding = request.apparent_encoding
    response_dict = json.loads(request.content.decode(encoding))
    
    #Stores whether hit was valid and any error message
    hit_result = response_dict.get('hitParsingResult')[0]
    
    return hit_result

def test_hit(payload, env_var, prod_environment):

        #HTTP post to debug server using payload from first row, 
        request = requests.post(env_var, data=payload)
        
        hit_result = parse_hit_result(request)
        
        #Lil peek at the processed hit on debug server
        print(hit_result.get('hit'), '\n')
        
        check_hit_validity(hit_result)

        #Hit is valid, update environments variable
        env_var = prod_environment
        
        return env_var

def build_send_valid_hit(properties, file_list, file_headers, event_flag, event_param, events, env_var, environments, payload_dimensions, time_format):
    for p in range(len(properties)):
        
        #Defines initial payload dict with static values from config
        #This should be rebuilt
        payload = build_payload(properties[p], payload_dimensions)
        
        #Runs through each 'row'
        for r in range(len(file_list)):    

            #Pulls the row into a variable for easier parsing
            row = file_list[r]
            
            time_diff_append(row[0], time_format, payload)
            
            #Grabs client id value, updates payload
            client_id = row[1]
            payload.update({'cid': client_id})
            
            event_append(row, file_headers, event_flag, event_param, events, payload)

            #Cache buster for use as a parameter
            cache_buster = str(random.random())[2:12]
            payload.update({'z': cache_buster})

            #Just a lil peek, can remove for production
            print('Prop:', p, 'Row:', r, 'Payload:', payload, '\n')
            
            while env_var == environments['debug_domain']:
                env_var = test_hit(payload, env_var, environments['prod_domain'])

            #HTTP request to production collection
            request = requests.post(env_var, data=payload)

            #Grabs HTTP status code, raises error if not 200
            status = request.status_code
            reason = request.reason
            assert status == 200, f'Error with hit collection. HTTP Request Status Code: {status}, {reason}'