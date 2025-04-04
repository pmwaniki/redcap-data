import asyncio
from typing import List, Optional, Callable

import aiohttp

import json
import pandas as pd
import numpy as np
from datetime import datetime
import pyarrow as pa

from functools import reduce
import re



# def release_mem():
#     import ctypes
#     libc = ctypes.CDLL("libc.so.6")
#     libc.malloc_trim(0)

# gets data from redcap
def create_request_data(token,ids_=None,variables=None,forms=None,events=None):
    data = {
        'token': token,
        'content': 'record',
        'format': 'json',
        'type': 'flat',
        'rawOrLabel': 'raw',
        'rawOrLabelHeaders': 'raw',
        'exportCheckboxLabel': 'false',
        'exportSurveyFields': 'false',
        'exportDataAccessGroups': 'false',
        'returnFormat': 'json'
    }

    if ids_ is not None:
        for i, j in enumerate(ids_):
            data["records[{}]".format(i)] = '{}'.format(j)

    
    if variables is not None:
        for i,v in enumerate(variables):
            data[f'fields[{i}]'] = v

    if forms is not None:
        for i,f in enumerate(forms):
            data[f'forms[{i}]'] = f

    if events is not None:
        for i,e in enumerate(events):
            data[f'events[{i}]'] = e
    

    return data
    
async def async_post_one(url: str,data: dict,ssl_verify:bool = True,
                         session: Optional[aiohttp.ClientSession]=None,post_process:Optional[Callable]=None):
    if session is None:
        async with aiohttp.ClientSession() as session:
            async with session.post(url=url,data=data,verify_ssl=ssl_verify) as response:
                resp= await response.json()
                if post_process:
                    resp=post_process(resp)
                # resp= pa.Table.from_pylist(resp)
        # release_mem()
        return resp
    else:
        async with session.post(url=url, data=data, verify_ssl=ssl_verify) as response:
            resp = await response.json()
            if post_process:
                resp = post_process(resp)
        # release_mem()
        return resp


async def async_post_many(url:str,data:List[dict],ssl_verfy:bool=True,parallel_calls:int = 10,post_process:Optional[Callable]=None):
    connector = aiohttp.TCPConnector(limit=parallel_calls)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks=[]
        for dat in data:
            tasks.append(async_post_one(url,data=dat,ssl_verify=ssl_verfy,session=session,post_process=post_process))
        data_lists = await asyncio.gather(*tasks,return_exceptions=True)
        return data_lists

def conv_to_pd(x):
    return pa.Table.from_pylist(x).to_pandas(deduplicate_objects=False)
def get_data(url,token,id_var=None, ids=None, filter_fun=None, filter_vars=(),variables=None,forms=(),events=(), max_chunk_size=500,
             parallel_calls=10,ssl_verify=True,convert_to_pandas=False):
    """

    :param url: Redcap api url
    :param token: Redcap token
    :param id_var: variable for record id
    :param ids: list of ids to fetch. None for all records
    :param filter_fun: function for filtering records to fetch. Ignored if id_var=None
    :param filter_vars: variables to pull in initial request. Required by filter_fun and ignored if id_var=None
    :param variables: Variable to fetch. None for all
    :param max_chunk_size: Size of each chunk. Data is fetched in chunks. Ignored if id_var=None
    :param parallel_calls: Number of chucks to fetch in parallel. Ignored if id_var=None
    :param ssl_verify: Verify https certificate
    :return: Data as list of dictionaries
    """

    if id_var is None:
        request_data=create_request_data(token,variables=variables,forms=forms,events=events)
        # request = requests.post(url, data=request_data, verify=ssl_verify)
        # if request.status_code !=200:
        #     raise Exception(f"Error: {request.text}")
        # data = json.loads(request.text)
        data= asyncio.run(async_post_one(url,request_data,ssl_verify=ssl_verify,
                             post_process=conv_to_pd if convert_to_pandas else None))
        return data

    else:
        request_data_initial=create_request_data(token,ids_=ids,variables=list(set((id_var,)+filter_vars)),forms=None,events=None)
        # print("Fetching record ids and filter variables")
        # request = requests.post(url, data=request_data_initial, verify=ssl_verify)
        # if request.status_code !=200:
        #     raise Exception(f"Error: {request.text}")
        # data = json.loads(request.text)

        data =asyncio.run(async_post_one(url,data=request_data_initial,ssl_verify=ssl_verify))
        # data=json.loads(data)

        # data2=data
        if filter_fun is not None:
            data=filter(filter_fun,data)

        if len(data) == 0:
            return []

        # data=pd.DataFrame(data)
        data_ids=list(map(lambda x:x[id_var],data))
        unique_data_ids=list(set(data_ids))

        # if data[id_var].duplicated().any():
        #     raise Exception("There are duplicates in 'id_var'. Set id_var=None to fetch all records")


        # print(data2)


        ids_len=len(unique_data_ids)
        ids=[]
        for i in range(0, ids_len, max_chunk_size):
            if (i+max_chunk_size)<ids_len:
                ids.append(unique_data_ids[i:i+max_chunk_size])
            else:
                ids.append(unique_data_ids[i:i+max_chunk_size])

        requests_data=[create_request_data(ids_=ids_, token=token, variables=variables,forms=None,events=None)
        for ids_ in ids]



        data_lists=asyncio.run(async_post_many(url,data=requests_data,ssl_verfy=ssl_verify,parallel_calls=parallel_calls,
                                               post_process=conv_to_pd if convert_to_pandas else None))
        # data_combined=[]
        # for chunk in data_lists:
        #     data_combined= data_combined + chunk

        # data_combined=pa.concat_tables(data_lists)
        if convert_to_pandas:
            data_combined=pd.concat(data_lists,axis=0)
        else:
            data_combined=reduce(lambda x,y:x+y,data_lists)
        return data_combined

        # return data_combined
    

def post_data(url,token,rows,overwrite=True,max_chunk_size=500, parallel_calls=10,ssl_verify=True):
    """
    Post data into RedCap using API
    :param url: api url of RedCap Server
    :param token: token of RedCap project
    :param rows: list of dictionaries. ie json
    :overwrite: blank/empty values are valid and will overwrite data
    :param max_chunk_size: Maximum size of request
    :param parallel_calls: Number of parallel requests
    :param ssl_verify: Enforce ssl verification
    :return: Number of imported records
    """
    def create_post_data(token,chunk):
        return {
            'token': token,
            'content': 'record',
            'format': 'json',
            'type': 'flat',
            'overwriteBehavior': 'overwrite' if overwrite else 'normal',
            'forceAutoNumber': 'false',
            'data': json.dumps(chunk),
            'returnContent': 'count',
            'returnFormat': 'json'
        }
    ids_len = len(rows)
    list_rows = []
    for i in range(0, ids_len, max_chunk_size):
        if (i + max_chunk_size) < ids_len:
            list_rows.append(rows[i:i + max_chunk_size])
        else:
            list_rows.append(rows[i:ids_len])

    all_requests = []
    for chunk in list_rows:
        chunk_request = create_post_data( token=token, chunk=chunk)
        all_requests.append(chunk_request)

    all_responses = asyncio.run(async_post_many(url,all_requests,ssl_verfy=ssl_verify,parallel_calls=parallel_calls))
    number_imported=0
    for response in all_responses:
        number_imported+=int(response['count'])
    return number_imported



def get_metadata(url,token,ssl_verify=True):
    """

    :param url: Redcap URL
    :param token: project token
    :return: Metadata as list of dictionaries
    """
    data1 = {
        'token': token,
        'content': 'metadata',
        'format': 'json',
        'returnFormat': 'json'
    }

    data1 = asyncio.run(async_post_one(url, data=data1, ssl_verify=ssl_verify))
    return data1



class Metadata:

    def __init__(self, metadata):
        self.metadata = metadata
        self.vars_expanded = []
        self.vars_non_expanded = []
        self.metadata_expanded = {}
        self.metadata_non_expanded = {}
        for v in metadata:
            self.vars_non_expanded.append(v['field_name'])
            self.metadata_non_expanded[v['field_name']] = v
            if v['field_type'] == 'checkbox':
                t = v['select_choices_or_calculations']
                t2 = t.split("|")
                t3 = list(map(lambda x: x.split(",")[0], t2))
                t3b=[str.strip(i) for i in t3]
                t4 = [v['field_name'] + "___" + i for i in t3b]
                t5 = [i.replace("-", "_") for i in t4]
                self.vars_expanded = self.vars_expanded+t5
                for v2 in t5:
                    self.metadata_expanded[v2] = v

            else:
                self.vars_expanded.append(v['field_name'])
                self.metadata_expanded[v['field_name']] = v

            # self.variables={v['field_name']: v for v in self.metadata}
            # self.vars_non_expanded=list(self.variables.keys())


    def exists(self, variable):
        """
        :param variable: variable
        :return: True or False depending on whether the variable exists in the metadata
        """
        result = variable in (self.vars_expanded + self.vars_non_expanded)
        return result

    def get_variables(self, expand_checkbox=True):
        """
        :param expand_checkbox: if true the function returns expanded variables and vice versa
        :return:
        """
        if expand_checkbox:
            return self.vars_expanded
        else:
            return self.vars_non_expanded

    def get_variables_without_description(self):
        """
        :return: variables which
        """
        variables = self.get_variables(expand_checkbox=True)
        for variable in variables:
            if self.metadata_expanded[variable]['field_type'] == 'descriptive':
                variables.remove(variable)
        return variables

    def get_label(self, variable):
        """
               :param variable: variable
               :return: the label of the variable
        """
        if not self.exists(variable):
            raise Exception("Variable {} does not exist".format(variable))
        label=self.metadata_expanded[variable]['field_label']
        return label

    def get_type(self, variable):
        """
               :param variable: variable
               :return: the type of the data in the variable
        """
        if not self.exists(variable):
            raise Exception("Variable {} does not exist".format(variable))
        field_type=self.metadata_expanded[variable]['field_type']
        
        if field_type =="checkbox":
            return "checkbox"
        if field_type =="text":
            type_ = self.metadata_expanded[variable]['text_validation_type_or_show_slider_number']
            v_type = 'str'
            if type_ == '':
                v_type = 'str'
            elif 'date' in type_:
                v_type = 'date'
            elif type_ == "number":
                v_type = 'float'
            elif type_ == 'integer':
                v_type = 'int'

            return v_type
        if field_type == "descriptive":
            return "str"
        if field_type in ['radio','dropdown', 'yesno']:
            return 'categorical'
        if field_type == "calc":
            return "calc"
        # if field_type == 'yesno':
        #     return ''
        raise NotImplementedError(f'get_type Not implemented for field type {field_type}')
        
        


    def get_valid_range(self, variable):

        """
               :param variable: variable
               :return: the range of the given variable
        """
        if not self.exists(variable):
            raise Exception("Variable {} does not exist".format(variable))
        min = self.metadata_expanded[variable]['text_validation_min']
        if min == '':
            min=None
        else:
            type_=self.get_type(variable)
            if type_ == 'float':
                min=float(min)
            elif type_ == 'date':
                min=datetime.strptime(min,'%Y-%m-%d')
            elif type_ == 'int':
                min = int(min)

        max = self.metadata_expanded[variable]['text_validation_max']
        if max == '':
            max=None
        else:
            type_ = self.get_type(variable)
            if type_ == 'float':
                max = float(max)
            elif type_ == 'date':
                max = datetime.strptime(max, '%Y-%m-%d')
            elif type_ == 'int':
                max = int(max)

        range=None
        if (min is not None) | (max is not None): range = (min, max)
        return range

    def get_is_required(self,variable):
        """
               :param variable: variable
               :return: true or false depending on whether a variable is required or not
        """
        if not self.exists(variable):
            raise Exception("Variable {} does not exist".format(variable))
        required = self.metadata_expanded[variable]['required_field']
        if required == '': required = False
        else: required = True
        return required

    def get_choices(self, variable):
        if not self.exists(variable):
            raise Exception("Variable {} does not exist".format(variable))
        if self.metadata_expanded[variable]['field_type'] in ["yesno",]:
            return {'0':"No",'1':"Yes"}
        if self.metadata_expanded[variable]['field_type'] in ["checkbox",]:
            return {'0':"Unchecked",'1':"Checked"}
        choice = self.metadata_expanded[variable]['select_choices_or_calculations']
        if choice=="":
            raise Exception("variable %s does not have choices" % variable)
        choices = choice.split("|")
        pattern_keys=re.compile(r'(-?\d+)\s?,')
        keys=[pattern_keys.search(item).group(1) for item in choices]
        pattern_values=re.compile(r'-?\d+\s?,(.*)')
        values=[pattern_values.search(item).group(1) for item in choices]
        choices={k:v.strip() for k,v in zip(keys,values)}

        return choices

    def get_branching_logic(self, variable):
        """
        :param variable: variable
        :return: the branching logic of the variable
        """
        if not self.exists(variable):
            raise Exception("Variable {} does not exist".format(variable))
        logic = self.metadata_expanded[variable]['branching_logic']
        if logic == '':
            logic2 = None
        else:
            logic2 = logic
        return logic2

    def get_hidden(self, variable):
        """
               :param variable: variable
               :returns: true or false whether the variable is hidden or not
        """
        if not self.exists(variable):
            raise Exception("Variable {} does not exist".format(variable))
        hidden = self.metadata_expanded[variable]['field_annotation']
        if hidden == '':
            return False
        elif '@HIDDEN' in hidden:
            return True
        else:
            return False

    def format_data(self, row=None, labels=False):
        # for key, value in row.items():
        #     if not self.exists(key):
        #         raise Exception("Variable {} does not exist".format(key))
        """
               :param variable: row
               :return: a row whose values have been converted to their respective types
        """
        new_row = {}
        for variable, value in row.items():
            if value == '':
                new_row[variable] = None
                continue
            
            type_ = self.get_type(variable=variable)
            if type_ in ["categorical","checkbox"]:
                choices=self.get_choices(variable)
                new_row[variable]=choices.get(value,value)
            
            elif type_ == 'str':
                new_row[variable] = value
            elif type_ == 'float':
                new_row[variable] = float(value)
            elif type_ == 'int':
                new_row[variable] = int(re.compile(r'(\d+)').search(value).group(1))
            elif type_ == 'date':
                try:
                    new_row[variable] = datetime.strptime(value, '%Y-%m-%d')
                except:
                    new_row[variable] = datetime.strptime(value, '%Y/%m/%d')
                    
        return new_row
    def format_column(self,var_name,column):
        type_ = self.get_type(variable=var_name)
        if type_ in ["categorical","checkbox"]:
            choices=self.get_choices(var_name)
            column=column.map(choices)
        
        elif (type_ == 'str') | (type_ == 'calc'):
            column = column
        elif type_ == 'float':
            column = column.replace('',np.nan).astype(float)
        elif type_ == 'int':
            column=column.map(lambda x:re.compile(r'(\d+)').search(x).group(1) if x != '' else '')
            column=pd.to_numeric(column,downcast='integer')
#            column = column.replace('',np.nan).astype(float)
           
        elif type_ == 'date':
            # format=None
            try:
                column = pd.to_datetime(column, format='%Y-%m-%d',errors = 'coerce')
            except:
                column = pd.to_datetime(column, format='%Y/%m/%d',errors = 'coerce')
        else:
            raise NotImplementedError(f"format_column not implemented for type {type_}")
        return column
        
