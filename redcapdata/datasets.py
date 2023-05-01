import grequests
import requests
import json
import pandas as pd
import numpy as np
from datetime import datetime
# from multiprocessing import Pool
import os
from functools import reduce,partial
import re

class Project:
    def __init__(self, url, id_var, date_var, token, project="cin"):
        self.url = url
        self.date_var = date_var
        self.id_var = id_var
        self.token = token
        self.project = project

project=Project(url="",id_var="id",date_var="date_today",
            token="")



# gets data from redcap
def create_chunk_request_data(ids_,project,variables=None):
        x = {}
        for i, j in enumerate(ids_):
            x["records[{}]".format(i)] = '{}'.format(j)

        data = {
            'token': project.token,
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
    
        for k, v in x.items():
            data[k] = v
    
        if variables is not None:
            for i,v in enumerate(variables):
                data[f'fields[{i}]'] = v
    

        return data
    
    
def get_data(project, start=None, stop=None,variables=None, max_chunk_size=500, parallel_calls=50):
    """
    :param max_chunk_size: Maximum number of records in a chunk
    :param parallel_calls: Number of request to make at a time
    :param project: A project object
    :param start: start date eg '2009-01-01'. leave None for beginning of study
    :param stop: stop date eg '2009-01-02'. leave None for latest input
    :param variables: List of variables to fetch. None for all
    :return:
    """


    data = {
        'token': project.token,
        'content': 'record',
        'format': 'json',
        'type': 'flat',
        'fields[0]': project.id_var,
        'fields[1]': project.date_var,
        #'record[]': outputTwo(),
        'rawOrLabel': 'raw',
        'rawOrLabelHeaders': 'raw',
        'exportCheckboxLabel': 'false',
        'exportSurveyFields': 'false',
        'exportDataAccessGroups': 'false',
        'returnFormat': 'json'
    }
    print("Fetching record ids and dates")
    request = requests.post(project.url, data=data, verify=False)
    data = json.loads(request.text)
    data2 = pd.DataFrame(data)
    data2[project.date_var] = pd.to_datetime(data2[project.date_var])

    if start is not None:
        data2 = data2.loc[data2[project.date_var] >= pd.to_datetime(start), :]

    if stop is not None:
        data2 = data2.loc[data2[project.date_var] <= pd.to_datetime(stop), :]

    # print(data2)
    if data2.shape[0] == 0:
        return []

    ids_len=data2.shape[0]
    ids=[]
    for i in range(0, ids_len, max_chunk_size):
        if (i+max_chunk_size)<ids_len:
            ids.append(data2[project.id_var][i:i+max_chunk_size].values)
        else:
            ids.append(data2[project.id_var][i:i+max_chunk_size].values)
        
                
                
    all_requests=[]
    for id_chunk in ids:
        chunk_request=create_chunk_request_data(ids_=id_chunk,project=project,variables=variables)
        all_requests.append(grequests.post(project.url, data=chunk_request, verify=False))

    all_responses=grequests.map(all_requests,size=parallel_calls)
    data_lists=[]
    for response in all_responses:
        if response.status_code != 200:
            raise Exception(f"Error fetching data from redcap, message: {response.text} ")
        data_lists.append(json.loads(response.text))

    # download_fun=partial(get_chunk,project=project,variables=variables)
    # print("Fetching data in %d chunks in %d parallel processes" % (len(ids),parallel))
    # with Pool(processes=parallel) as pool:
    #     data_lists=pool.map(download_fun,ids)
    
    data_combined=reduce(lambda x,y:x+y,data_lists)
    
    return data_combined
    





def get_metadata(project):
    """
    :param project: project object
    :returns: metadata
    """
    data1 = {
        'token': project.token,
        'content': 'metadata',
        'format': 'json',
        'returnFormat': 'json'
    }

    request1 = requests.post(project.url, data=data1, verify=False)
    data1 = json.loads(request1.text)
    return data1

print(get_metadata(project))

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
        if field_type != "text":
            return "categorical"
        
        
        type_=self.metadata_expanded[variable]['text_validation_type_or_show_slider_number']
        v_type='str'
        if type_ == '':
            v_type = 'str'
        elif 'date' in type_:
            v_type = 'date'
        elif type_ == "number":
            v_type = 'float'
        elif type_ == 'integer':
            v_type = 'int'

        return v_type

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
        pattern_keys=re.compile(r'(\d+)\s?,')
        keys=[pattern_keys.search(item).group(1) for item in choices]
        pattern_values=re.compile(r'\d+\s?,(.*)')
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
        
        elif type_ == 'str':
            column = column
        elif type_ == 'float':
            column = column.replace('',np.nan).astype(float)
        elif type_ == 'int':
            column=column.map(lambda x:re.compile(r'(\d+)').search(x).group(1) if x != '' else '')
            column=pd.to_numeric(column,downcast='integer')
#            column = column.replace('',np.nan).astype(float)
           
        elif type_ == 'date':
            try:
                column = pd.to_datetime(column, format='%Y-%m-%d',errors = 'coerce')
            except:
                column = pd.to_datetime(column, format='%Y/%m/%d',errors = 'coerce')
        return column
        
