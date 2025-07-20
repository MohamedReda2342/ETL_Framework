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
from util import Queries


# Load the .env file
#load_dotenv()
#host = os.getenv("TD_DB_HOST")

## constants: to be moved to env
#'Key_Set_Name'	bkey	key set name
#Key_Set_Id	bkey	key set id

#Key_Table_Name= os.getenv('KEY_TABLE_NAME')	
#'Key_Domain_Name'		key domain name
'''
BIGINT_Flag=	0	
Key_View_DB_Name=os.getenv('KEY_VIEW_DB_NAME')		
Key_Table_DB_Name=os.getenv('KEY_TABLE_DB_NAME')		
#Domain_Id		key domain id
'''
'''
def generate_script(area, function, df_selection):
    print(area)
    print(function)
    print(df_selection)
'''


def load_script_model():
    obj = pd.read_pickle(r'pickled_df/bkey_functions.pkl')
    return obj

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

def get_params_values(smx_tab, df, smx_dict):
    smx_lst=list(df['smx_column'])
    smx_tabs_lst=list(df['source'])
    smx_join_key=list(df.query('join_key == join_key')['join_key'])
    
    smx_source_list= [x.split(',') for x in smx_tabs_lst]
    ll2 = [item for item in smx_source_list if item != 'env']
    flattened_list = [item for sublist in ll2 for item in sublist]

    multiple_source_list = [x for x in smx_source_list if len(x) > 1] #[ len(l) for l in smx_source_list]
    print(Back.CYAN+ "Multiple sources list")
    print(multiple_source_list)
    #check the multiple sources and the maximum in order to join the dataframes

    print(type(multiple_source_list))
    
    import itertools
    if multiple_source_list:
        multiple_source_list = list(itertools.chain(*multiple_source_list))
        smx_tabs=multiple_source_list
    else:
        smx_tabs=flattened_list

    smx_list = [x.lower() for x in smx_lst if pd.notnull(x)]
    smx_tabs_df=[]

    smx_df=pd.DataFrame()
    print("**********************************************************************************************")
    
    print(Fore.CYAN+f"{smx_tabs}")
    print(len(smx_tabs))   
    print("**********************************************************************************************")
    
    '''
    if len(smx_tabs) >1:
        print(Back.LIGHTRED_EX+ "-------------- MULTIPLE SOURCE -----------------------------------------")    
        print(len(smx_tabs))
        if len(smx_tabs)==2:
            print(smx_join_key)
            print(type(smx_join_key))
            on_cols=smx_join_key[0].split(",")
            print(on_cols)
            df1=smx_dict[smx_tabs[0]]
            #print(df1)
            df2=smx_dict[smx_tabs[1]]
            #print(df2)
            #smx_df= pd.merge(df1, df2, on=['key set name','key domain name'], how='left')
            smx_df= pd.merge(df1, df2, on=on_cols, how='left')
            print(smx_df)
        elif len(smx_tabs)>2:
            print(Fore.GREEN + "3 parameters")
            smx_df=join_bkey_stg_stream(smx_tabs, df, smx_dict)
            print(smx_df)
        
    else:
        smx_df=smx_dict[smx_tab]
    '''

    

    smx_df=join_bkey_stg_stream(smx_tabs, df, smx_dict)
   
    if not(multiple_source_list):
        smx_df=smx_dict[smx_tab]

    smx_col_list= [x.split(',') for x in smx_list]
    flattened_list = [element for sublist in smx_col_list for element in sublist]
    
    smx_list=flattened_list

    params_list=[x.lower() for x in df['parameter']]
    params_list=[x.split(',') for x in params_list]
    params_list = flatten(params_list)  
    df_with_empty_cols = smx_df.reindex(columns=params_list)

    for col in df_with_empty_cols.columns:
        df_with_empty_cols[col] = df_with_empty_cols[col].fillna(col)

    filtered_df = df[(df['presentation_col'] == "quoted") & (df['smx_column'].notna())]

    quoted_cols= list(filtered_df['smx_column'])
    quoted_cols=[x.lower() for x in quoted_cols ]
    
    filtered_df = df[(df['presentation_col'] == "concatenate") & (df['smx_column'].notna())]
    concatenated_cols= list(filtered_df['smx_column'])

    for c in quoted_cols:
        df_with_empty_cols[c] = df_with_empty_cols[c].apply(lambda x: f'"{x}"')
    
    if len(concatenated_cols) > 0:
        col_name="Process_Name"
        concatenates_cols_list = concatenated_cols[0].split(",")
        cols_list= list(df_with_empty_cols.columns)
        df_with_empty_cols[col_name] = df_with_empty_cols[concatenates_cols_list].astype(str).agg('_'.join, axis=1)
        df_with_empty_cols[col_name]= "BK_" + df_with_empty_cols[col_name]

        cols_list.insert(0, col_name)
        df_with_empty_cols=df_with_empty_cols[cols_list]
        df_with_empty_cols.drop(columns=concatenates_cols_list, inplace=True) 

    return df_with_empty_cols

def get_bkey_domain_script(smx_model, filtered_script_df, env_attributes):
    scripts=[]
    script=" place holder "
    df_by_function=filtered_script_df.groupby(['operation', 'schema','functions' ])
    for group_name, df_group in df_by_function:
        env_schema = df_group['schema'][0]
        env_schema = env_attributes[df_group['schema'][0]]
        df_group['schema']=env_schema

        df_group['parameter'] = df_group.apply(lambda row: str(env_attributes[row.parameter_name]) if  row.source=='env' else np.nan, axis=1)
        smx_tab= df_group['source'].unique()
        df_group['parameter'] = df_group.apply(lambda row: str(env_attributes[row.parameter_name]) if  row.source=='env' else row.smx_column, axis=1)

        df=df_group[['operation','schema' ,'functions']]
        
        parameters_string =tuple(list(df_group['parameter']))         
        p_s= list(df_group['parameter'])
        join_str = " , ".join(p_s)
        
        df = df.assign(function_parameters=join_str)

        df=df.drop_duplicates()

        df['schema_functions'] = df[['schema', 'functions']].agg('.'.join, axis=1)
        df['operation_schema_functions'] = df[['operation','schema_functions']].agg(' '.join, axis=1)
        
        df= df.drop(['operation', 'schema', 'functions', 'schema_functions'], axis=1)

        params_df = get_smx_values(smx_tab[0],list(df['function_parameters'])[0], smx_model)
        df = pd.concat([df, params_df], ignore_index=True)
        cols=params_df.columns
        params_df['combined'] = params_df[cols].apply(lambda row: tuple(row.values.astype(str)), axis=1)

        function = df['operation_schema_functions'].unique()
        params_df['fn'] =function[0]
        params_df=params_df[['fn', 'combined']]
        params_df['combined']=params_df['combined'].apply(lambda row: str(tuple(row)))
        params_df['script']=params_df['fn']+params_df['combined']
        scripts.append(list(params_df['script']))

    return scripts

def get_bkey_reg_script(smx_model, filtered_script_df, env_attributes):
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
        
        source_params_df= df[['source', 'parameter', 'smx_column','env_variable','parameters','presentation_col','join_key']]
        
        params_df = get_params_values(smx_tab_lst[0], source_params_df, smx_model)

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
        del params_df, merged_column
    del df_by_function

    return scripts

def main(smx_model, key_type, env , bigint_flag):
    @st.cache_resource
    def load_cached_model():
        script_file = "schema_functions_MAPPED_script.xlsx"
        return load_syntax_model(script_file)
    syntax_model = load_cached_model()
    
    filtered_script_df=filter_key_type(syntax_model, key_type)
    
    env_attributes=get_env_dict(str(env), bigint_flag)

    match key_type:
        case "BKEY_CALL" : 
            cols_list=['operation','schema','functions', 'parameters', 'parameter_name', 'source', 'smx_column'] 
            script= get_bkey_reg_script(smx_model, filtered_script_df, env_attributes)

            with open('BKEY_CALL_script_output.txt', mode='wt', encoding='utf-8') as myfile:
                for l in script:
                    myfile.write('\n'.join(l))
                    myfile.write('\n\n')
            
        case "REG_BKEY_PROCESS":
            script= get_bkey_reg_script(smx_model, filtered_script_df, env_attributes)

            with open('REG_BKEY_PROCESS_output.txt', mode='wt', encoding='utf-8') as myfile:
                for l in script:
                    myfile.write('\n'.join(l))
                    myfile.write('\n\n')
                    
        case "REG_BKEY_DOMAIN" :
           script= get_bkey_reg_script(smx_model, filtered_script_df, env_attributes)
        
           with open('REG_BKEY_DOMAIN_script_output.txt', mode='wt', encoding='utf-8') as myfile:
                for l in script:
                    myfile.write('\n'.join(l))
                    myfile.write('\n\n')

        case "REG_BKEY" :
            script= get_bkey_reg_script(smx_model,filtered_script_df, env_attributes )
            
            with open('REG_KEY_script_output.txt', mode='wt', encoding='utf-8') as myfile:
                for l in script:
                    myfile.write('\n'.join(l))
                    myfile.write('\n\n')

        case "STREAM" :
            script= get_bkey_reg_script(smx_model,filtered_script_df, env_attributes )
            
            with open('REG_STREAM_script_output.txt', mode='wt', encoding='utf-8') as myfile:
                for l in script:
                    myfile.write('\n'.join(l))
                    myfile.write('\n\n')

        case "REG_BMAP" :
            script= get_bkey_reg_script(smx_model,filtered_script_df, env_attributes )
            
            with open('REG_BMAP_script_output.txt', mode='wt', encoding='utf-8') as myfile:
                for l in script:
                    myfile.write('\n'.join(l))
                    myfile.write('\n\n')

        case "REG_BMAP_DOMAIN" :
            script= get_bkey_reg_script(smx_model,filtered_script_df, env_attributes )
            
            with open('REG_BMAP_DOMAIN_script_output.txt', mode='wt', encoding='utf-8') as myfile:
                for l in script:
                    myfile.write('\n'.join(l))
                    myfile.write('\n\n')

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
    return script
