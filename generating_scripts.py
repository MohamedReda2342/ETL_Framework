import streamlit as st
import pandas as pd
import openpyxl
from dotenv import load_dotenv
import os
#import reading_text
import read_env
import numpy as np
from read_env import CurrentEnv 
import ast 
import sys
from colorama import init, Fore, Back, Style
init(autoreset=True)  # Initialize colorama
from itertools import chain  
import regex as re
import psutil
from util import Queries




def create_template(filename, key_type):
    functions_DF = pd.read_excel(filename,'FUNCTIONS')
    fs= list(functions_DF['Syntax'])
    start='.'
    end='('
    fs= [s.strip('EXEC') for s in fs]
    functions = [s[s.find(start)+len(start):s.rfind(end)] for s in fs]
    return functions 

def load_smx_file(filename):
    work_book = openpyxl.load_workbook(data_file)
    worksheets= work_book.worksheets
    sheetnames=work_book.sheetnames
    smx_model={}

    for work_sheet in sheetnames:
        df = pd.read_excel(filename,work_sheet)
        cols = [x.lower() for x in df.columns]
        df.columns=cols
        smx_model[work_sheet.lower()] = df

    smx_df = pd.DataFrame(smx_model.items())
    return smx_model, smx_df

def load_syntax_model(syntax_file):
    work_book = openpyxl.load_workbook(syntax_file)
    worksheets= work_book.worksheets
    sheetnames=work_book.sheetnames
    script_model={}

    for work_sheet in sheetnames:
        df = pd.read_excel(syntax_file,work_sheet)
        cols = [x.lower() for x in df.columns]
        df.columns=cols
        script_model[work_sheet.lower()] = df

    script_df = pd.DataFrame(script_model.items())
    return script_model

def load_env():
    env=read_env.CurrentEnv()
    return env

def filter_key_type(script_dict, key_type):
    df=script_dict['functions']
    filtered_functions = df[df['key_type'] ==key_type]
    
    df2=script_dict['parameters']
    df_filtered = pd.merge(filtered_functions, df2, on='function_code')
    df3=script_dict['unique_parameters']
    df_filtered2=  pd.merge(df_filtered, df3, left_on='parameters', right_on='parameter_name')
    return df_filtered2

def get_env_dict(env, bi_flag):
    envvar = CurrentEnv(env, bi_flag=1 )
    env_attributes = {
        k: v
        for k, v in vars(envvar).items()
        if not callable(v) and not k.startswith("__")
    }
    return env_attributes

def flatten(a):  
    res = []  
    for x in a:  
        if isinstance(x, list):  
            res.extend(flatten(x))  
        else:  
            res.append(x)  
    return res  

def join_bkey_stg_stream(smx_tabs, df,smx_dict):
    df1=smx_dict['bkey']
    df2=smx_dict['stg tables']
    on_cols=['key set name','key domain name']
    merged= pd.merge(df1, df2, on=on_cols, how='left')
    df3=smx_dict['stream']
    search_string='_BKEY'
    search_string=''
    filtered_df = df3[df3['stream name'].str.contains(search_string)]
    smx_df = pd.merge(merged, filtered_df, left_on='source system alias', right_on='system name', how='left')
    return smx_df

def join_columns(smx_tabs, df,smx_dict, key_type):
    the_joined_smx_tabs= pd.DataFrame()
    if key_type=='EXEC_SRCI':
        search_string='_SRCI'
        df2=smx_dict['stg tables']
        
        df3=smx_dict['stream']
        filtered_df=df3[df3['stream name'].str.contains(search_string)]
        the_joined_smx_tabs = pd.merge(df2, filtered_df, left_on='source system alias', right_on='system name', how='left')
    else:
       the_joined_smx_tabs= join_bkey_stg_stream(smx_tabs, df,smx_dict)
    return the_joined_smx_tabs
def get_params_values_better(smx_tab, df, smx_dict, key_type):
    smx_lst=list(df['smx_column'])
    smx_tabs_lst=list(df['source'])
    smx_source_list= [x.split(',') for x in smx_tabs_lst]
    ll2 = [item for item in smx_source_list if item != 'env']
    flattened_list = [item for sublist in ll2 for item in sublist]
    multiple_source_list = [x for x in smx_source_list if len(x) > 1] 
    import itertools
    if multiple_source_list:
        multiple_source_list = list(itertools.chain(*multiple_source_list))
        smx_tabs=multiple_source_list
    else:   
        smx_tabs=flattened_list

    smx_list = [x.lower() for x in smx_lst if pd.notnull(x)]
    smx_tabs_df=[]
    smx_df=pd.DataFrame()
    smx_df=join_columns(smx_tabs, df, smx_dict, key_type)

    if not(multiple_source_list):
        smx_df=smx_dict[smx_tab]

    smx_col_list= [x.split(',') for x in smx_list]

    flattened_list = [element for sublist in smx_col_list for element in sublist]
    smx_list=flattened_list
    smx_list_set = list(set(smx_list))
    missing_cols = [col for col in smx_list_set if col not in smx_df.columns]
    if missing_cols:

        smx_list_set = [col for col in smx_list_set if col in smx_df.columns]

    smx_df = smx_df[smx_list_set]

    final_df=pd.DataFrame()
    n=smx_df.shape[0]
    process_type_row = df.loc[df['parameter_name'] == 'Process_Type']
    process_type_row=df.loc[df['parameter_name'].str.contains('Process_Type')]
    process_type_str='null'
    if not process_type_row.empty:
        process_type=process_type_row['parameter']
        values_list = process_type.tolist()
        process_type_str =values_list[0]
    
    for index, row in df.iterrows():
        if row['source']=='env':

            my_string=row['parameter']
            s=my_string.replace('"', '')
            if s.isdigit():
                final_df[row['parameter_name']]=[s]*n
            else:
                final_df[row['parameter_name']]=[my_string]*n
            col_values=[row['parameter']]*n

        else:
            smx_cols= row['parameter'].split(',')
            smx_cols=[x.lower() for x in smx_cols]
            df_tmp=smx_df[smx_cols]

            if len(smx_cols) > 1:
                df_tmp['concatenation']= df_tmp[df_tmp.columns].astype(str).agg('_'.join, axis=1)
                
                df_tmp[row['parameter_name']]=df_tmp['concatenation'].values
                #if '21' in process_type_str:
                if key_type in ['BKEY_CALL', 'REG_BKEY_PROCESS']:
                    la_liste=df_tmp['concatenation'].values
                    la_liste=['BK_'+x for x in la_liste]

   
            else:
                df_tmp[row['parameter_name']] = df_tmp[smx_cols[0]].values


            if pd.notna(row['prefix']): 
                la_liste=[f'{row['prefix']}_{x}' for x in df_tmp[row['parameter_name']]]
                df_tmp[row['parameter_name']]=la_liste
            if row['presentation_col']=='quoted':
                  df_tmp[row['parameter_name']]=df_tmp[row['parameter_name']].apply(lambda x: f"'{x}'")
                  
            final_df[row['parameter_name']]=df_tmp[row['parameter_name']].values

    script = final_df.values.tolist()

    return final_df


def get_bkey_reg_script(smx_model, filtered_script_df, env_attributes, key_type):
    scripts=[]
    script=" place holder "
    
    df_by_function=filtered_script_df.groupby(['operation', 'schema','functions' ], sort=False)
       
    for group_name, df_group in df_by_function:
        e_schema = df_group['schema'].unique()
        env_schema = env_attributes[e_schema[0]]
        df_group['schema']=env_schema
        
        smx_tab= df_group['source'].unique()
        df_group['parameter'] = df_group.apply(lambda row: f'"{str(env_attributes[row.env_variable])}"' if  row.source=='env' else row.smx_column, axis=1)

        df=df_group

        parameters_string =tuple(list(df_group['parameter']))         
        
        df=df.drop_duplicates()

        df['schema_functions'] = df[['schema', 'functions']].agg('.'.join, axis=1)
        df['operation_schema_functions'] = df[['operation','schema_functions']].agg(' '.join, axis=1)
        df= df.drop(['operation', 'schema', 'functions', 'schema_functions'], axis=1)
        smx_tab_lst= list(smx_tab)

        if 'env' in list(smx_tab):
            smx_tab_lst.remove('env')
        
        smx_filtered_df = df[df['source'] != 'env']
        
        source_params_df= df[[ 'parameter_name', 'source', 'parameter', 'smx_column','env_variable','parameters','presentation_col','prefix', 'join_key']]
        
        params_df = get_params_values_better(smx_tab_lst[0], source_params_df, smx_model, key_type)

        cols=params_df.columns

        merged_column = params_df.apply(lambda row: ', '.join(map(str, row)), axis=1)
        params_df['merged']= merged_column
        
        df = df.reset_index()
        params_df=params_df.reset_index(drop=True)
        duplicate_cols = params_df.columns[params_df.columns.duplicated()]
        params_df.drop(columns=duplicate_cols, inplace=True)

        df = df.reset_index(drop=True)
        df = pd.concat([df, params_df], ignore_index=True)
        cols=params_df.columns
        
        function = df['operation_schema_functions'].unique()
        params_df['fn'] =function[0]
        params_df['combined']=params_df['merged'].apply(lambda row: f"({row})")
        params_df=params_df[['fn', 'combined']]
        
        params_df['script']=params_df['fn']+params_df['combined']

        fn_scripts=set(params_df['script'])
        scripts.append(list(fn_scripts))

    return scripts
def smx_preprocess(smx_model, smx_tab, condition):
    df = smx_model[smx_tab]
    condition = ['PK', '==', 'Y']
    c= " ".join(condition)
    c= f'df[df[{condition[0]} {condition[1]} {condition[2]}]'
    filtered_df = df[df[condition[0].lower()] == condition[2]]
    smx_model[smx_tab]=filtered_df
    return smx_model

def get_core_script_dict(script, smx_model):
        df = smx_model['core tables']
        
        list_tables=df['table name'].unique()
        script_dict={}
        script_dict = dict.fromkeys(list_tables, [])
        flat_list = [item for sublist in script for item in sublist]

        for s in flat_list:
            sub_s=  s[s.index("("):s.index(")")+1]
            sub_s.replace('"', '')

            my_tuple = eval(sub_s)
            print(f"Tuple value: {my_tuple}")
            ss=my_tuple[1]
            for k in list_tables:
                if k == ss:
                    if script_dict[k] :
                        items=[script_dict[k]]
                        items.append(s)
                        script_dict.update({k:items})
                    else:
                        script_dict.update({k: s})
        return script_dict

@st.cache_resource
def load_cached_model():
    script_file = "schema_functions_MAPPED_script.xlsx"
    return load_syntax_model(script_file)


def main(smx_model, key_type, env , bigint_flag):
    syntax_model = load_cached_model()
    
    filtered_script_df=filter_key_type(syntax_model, key_type)
    
    env_attributes=get_env_dict(str(env), bigint_flag)

    match key_type:
        case "BKEY_CALL" : 
            script= get_bkey_reg_script(smx_model, filtered_script_df, env_attributes, key_type)
            
        case "REG_BKEY_PROCESS":
            script= get_bkey_reg_script(smx_model, filtered_script_df, env_attributes, key_type)
                    
        case "REG_BKEY_DOMAIN" :
           script= get_bkey_reg_script(smx_model, filtered_script_df, env_attributes, key_type)

        case "REG_BKEY" :
           script= get_bkey_reg_script(smx_model, filtered_script_df, env_attributes, key_type)            

        case "STREAM" :
            script= get_bkey_reg_script(smx_model,filtered_script_df, env_attributes, key_type )

        case "REG_BMAP" :
            script= get_bkey_reg_script(smx_model,filtered_script_df, env_attributes, key_type )

        case "REG_BMAP_DOMAIN" :
            script= get_bkey_reg_script(smx_model,filtered_script_df, env_attributes, key_type )
            
        case "EXEC_SRCI" :
            script= get_bkey_reg_script(smx_model,filtered_script_df, env_attributes, key_type )          

        case "CORE_KEY_COL_REG" :
            smx_model = smx_preprocess(smx_model, 'core tables', f'PK==Y')
            script = get_bkey_reg_script(smx_model, filtered_script_df, env_attributes, key_type)

            script_dict = get_core_script_dict(script, smx_model)
            script2 = []
            for table_name, value in script_dict.items():
                base_sql = f"""SELECT * FROM G{env}1V_GCFR.GCFR_TRANSFORM_KEYCOL WHERE OUT_OBJECT_NAME = '{table_name}';
            DELETE FROM G{env}1V_GCFR.GCFR_TRANSFORM_KEYCOL WHERE OUT_OBJECT_NAME = '{table_name}';"""

                flattened = []

                for item in value:
                    if isinstance(item, list):
                        for subitem in item:
                            # print(subitem)
                            flattened.append(subitem)
                    else:
                        flattened.append(item)
                    
                    value_str = '\n'.join(flattened)
                    print(value_str)
                    sql = f"{base_sql}\n{value_str}"
                else:
                    sql = f"{base_sql}\n{value}"
                script2.append(sql)
            script = script2
        case "bkey_views":
            script = Queries.generate_bkey_views(smx_model,env)
        case "Insert BMAP values":
            script = Queries.insert_bmap_values(smx_model,env)
        case"Create LKP views":
            script = Queries.create_LKP_views (smx_model, env)
        case"create_stg_table_and_view":
            script = Queries.create_stg_table_and_view (smx_model, env)
        case"create_SCRI_table":
            script = Queries.create_SCRI_table (smx_model, env)
        case"create_SCRI_view":
            script = Queries.create_SCRI_view (smx_model, env)
        case"create_SCRI_input_view":
           script = Queries.create_SCRI_input_view (smx_model, env)
        case"create_core_table":
            script = Queries.create_core_table (smx_model, env)
        case"create_core_table_view":
            script = Queries.create_core_table_view (smx_model, env)
        case"create_core_input_view":
            script = Queries.create_core_input_view (smx_model, env)
            

#            # Get the current process
#     process = psutil.Process(os.getpid())

# # Get memory information for the process
# # rss (Resident Set Size) is the non-swapped physical memory a process uses.
#     mem_info = process.memory_info()
#     print(f"Current process memory usage (RSS): {round(mem_info.rss / (1024**2), 2)} MB")
    return script

    