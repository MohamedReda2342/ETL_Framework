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

def join_columns(smx_tabs, df,smx_dict, key_type):
    print(Fore.YELLOW+"-------------------- In the join function -----------------------")
    the_joined_smx_tabs= pd.DataFrame()
    print(df)
    print(smx_tabs)
    print(key_type)
    if key_type=='EXEC_SRCI':
        search_string='_SRCI'
        df2=smx_dict['stg tables']
        
        df3=smx_dict['stream']
        filtered_df=df3[df3['stream name'].str.contains(search_string)]
        the_joined_smx_tabs = pd.merge(df2, filtered_df, left_on='source system alias', right_on='system name', how='left')

    else:
       
       the_joined_smx_tabs= join_bkey_stg_stream(smx_tabs, df,smx_dict)


    print(the_joined_smx_tabs)
    return the_joined_smx_tabs
def get_params_values_better(smx_tab, df, smx_dict, key_type):
    print(Back.LIGHTRED_EX+ "==================    get_params_values_better   ========================================================")
    print(key_type)
    print(smx_tab)
    print(df.columns)
    print(Back.CYAN+ "incoming df parameters - content of parameters")
    print(df.columns)
    print(df[['parameter_name','source', 'parameter']])

    smx_lst=list(df['smx_column'])
    smx_tabs_lst=list(df['source'])
    #smx_join_key=list(df.query('join_key == join_key')['join_key'])
    
    print(Fore.LIGHTGREEN_EX+ f'{smx_lst}')
    print(Fore.RED+"this is the smx_tabs_lst")
    print(smx_tabs_lst)
    #print(smx_join_key)
    
    smx_source_list= [x.split(',') for x in smx_tabs_lst]

    print(smx_source_list)
    ll2 = [item for item in smx_source_list if item != 'env']
    print(Back.CYAN+ "ll2")

    print(ll2)
    flattened_list = [item for sublist in ll2 for item in sublist]
    print(flattened_list)


    multiple_source_list = [x for x in smx_source_list if len(x) > 1] #[ len(l) for l in smx_source_list]
    print(Back.CYAN+ "Multiple sources list")
    print(multiple_source_list)
    #check the multiple sources and the maximum in order to join the dataframes

    print(type(multiple_source_list))
    
    import itertools
    if multiple_source_list:
        print(Fore.LIGHTGREEN_EX+'here in multiple source list \n checking on the tabs')
        multiple_source_list = list(itertools.chain(*multiple_source_list))
        print(Back.CYAN+ "FLATTENED Multiple sources list")
        print(multiple_source_list)
        print(Back.CYAN+ "SET FLATTENED Multiple sources list")
        print(set(multiple_source_list))
        print(list(set(multiple_source_list)))
        smx_tabs=multiple_source_list
    else:   
        print(Fore.LIGHTGREEN_EX+'here in multiple source list: else ie not multiple source')
        smx_tabs=flattened_list

    

    #smx_cols_list = [x.split(',') for x in smx_lst]
    #print(smx_cols_list)
    smx_list = [x.lower() for x in smx_lst if pd.notnull(x)]
    print(Back.CYAN+ "SMX list - content of smx_column")

    print(smx_list)
    print(smx_tab)
    smx_tabs_df=[]
    #smx_tabs=smx_tab.split(',')
    smx_df=pd.DataFrame()
    print("**********************************************************************************************")
    
    print(Fore.CYAN+f"{smx_tabs}")
    print(len(smx_tabs))   
    print("**********************************************************************************************")
    
    
   
    #smx_df=join_bkey_stg_stream(smx_tabs, df, smx_dict)
    smx_df=join_columns(smx_tabs, df, smx_dict, key_type)

    print(smx_df)
   
    print(multiple_source_list)
    if not(multiple_source_list):
        print(Fore.YELLOW+ " it is a single source SO WE JUST FILTER THE TAB ON THE COLUMNS")
        print(smx_dict.keys())
        smx_df=smx_dict[smx_tab]
        print(smx_df)

    print(Back.LIGHTRED_EX+ "----------------------------------------------------------------------------")
    

    smx_col_list= [x.split(',') for x in smx_list]
    print(Back.CYAN+ "smx_col_list")
    print(smx_col_list)
    flattened_list = [element for sublist in smx_col_list for element in sublist]
    print(flattened_list)
    print(Back.LIGHTRED_EX+ "----------------------------------------------------------------------------")
    
    #smx_list=flattened_list
    print(smx_df)
    print(smx_df.columns)
    smx_list=flattened_list
    print(Back.LIGHTCYAN_EX+ "----------------------------------------------------------------------------")

    print(smx_list)
    print(smx_df[smx_list])

    print(Back.LIGHTRED_EX+ "----------------------------------------------------------------------------")
    '''
    New section is here
    we create the unique set of smx_list and use the dataframe
    then we loop over the parameters and assign the values to the new columns

    '''
    print(Back.YELLOW+ "*********************************************************************************")

    print(Fore.YELLOW+" &&&&&&&&&&&&&&&&&   we start here ")

    smx_list_set = list(set(smx_list))
    print(smx_list_set)
    print(len(smx_list_set))
    
    print(Back.YELLOW+" smx_df ")
    missing_cols = [col for col in smx_list_set if col not in smx_df.columns]
    if missing_cols:
        print(f"Missing columns in smx_df: {missing_cols}")
        # Optionally, raise an error or handle gracefully
        # raise KeyError(f"Columns {missing_cols} not in DataFrame")
        # Or skip missing columns:
        smx_list_set = [col for col in smx_list_set if col in smx_df.columns]

    smx_df = smx_df[smx_list_set]
    print(smx_df.shape)
    print(smx_df)

    print(Back.YELLOW+" iterate over the df to see what to do ")
    print(df[['parameter_name', 'source', 'parameter', 'smx_column', 'env_variable']])
    print(df.columns)
    final_df=pd.DataFrame()
    n=smx_df.shape[0]
    print(Fore.CYAN+f'printing n = {n}' )
    #process_type=df['Process_type']
    #check if there is a process type in the list of parameters
    print(Fore.YELLOW+' hhhhhhhhhhhhhhhhhhhhhhh checking the parameter name for process')
    process_type_row = df.loc[df['parameter_name'] == 'Process_Type']
    print(process_type_row)
    process_type_row=df.loc[df['parameter_name'].str.contains('Process_Type')]

    print(Fore.YELLOW+f'{process_type_row}')
    print(type(process_type_row))
    
    print(Fore.YELLOW+ f'checking the process type exists: {process_type_row}')

    process_type_str='null'
    if not process_type_row.empty:
        process_type=process_type_row['parameter']
        print(process_type_row)
        print(Fore.LIGHTRED_EX+ f'process_type : {process_type}')
        print(Fore.LIGHTYELLOW_EX+ f'{process_type}')
        print(type(process_type))
        values_list = process_type.tolist()
        process_type_str =values_list[0]
        print(Fore.CYAN +f'{process_type_str}')
    
    for index, row in df.iterrows():
        print(Fore.YELLOW+"------------------------ checking the rows attributes -------------------")
        print(row)
        print(row['prefix'])
        print(row['parameter'])
        '''
        checking if the source is env or the smx document
        if from env, we need to remove the quotes check if the value is a number or not so we put it as is or with quotes
        '''
        if row['source']=='env':
            print(Fore.YELLOW+f'here in the env parameter')
            print(row['parameter'])
            my_string=row['parameter']
            s=my_string.replace('"', '')
            print(s.isdigit())
            if s.isdigit():
                #new_string = my_string.replace('"', '')
                #print(new_string)
                final_df[row['parameter_name']]=[s]*n
            else:
                final_df[row['parameter_name']]=[my_string]*n
            print(row['parameter_name'])
            print(final_df[row['parameter_name']])
            col_values=[row['parameter']]*n
            
            #print(final_df)

        else:
            smx_cols= row['parameter'].split(',')
            print(Fore.CYAN+f'smx_cols: {smx_cols}')
            smx_cols=[x.lower() for x in smx_cols]
            print(smx_cols)
            df_tmp=smx_df[smx_cols]
            #print(df_tmp)
            print(Fore.CYAN+ 'after printing df_tmp')
            print(len(smx_cols))
            #print(row['prefix'])
            if len(smx_cols) > 1:
                df_tmp['concatenation']= df_tmp[df_tmp.columns].astype(str).agg('_'.join, axis=1)
                print(Fore.YELLOW+'concatenated values')
                print(df_tmp['concatenation'])
                print(Fore.CYAN+f'{process_type_str} and {len(process_type_str)}')
                
                df_tmp[row['parameter_name']]=df_tmp['concatenation'].values
                #if '21' in process_type_str:
                if key_type in ['BKEY_CALL', 'REG_BKEY_PROCESS']:
                    print(Fore.YELLOW+'value of process type is 21')
                    print(row['parameter_name'])
                    print(df_tmp['concatenation'].values)
                    la_liste=df_tmp['concatenation'].values
                    la_liste=['BK_'+x for x in la_liste]
                    print(la_liste)
   
            else:
                print(df_tmp)
                print(df_tmp.columns)
                print(df_tmp[smx_cols[0]])
                df_tmp[row['parameter_name']] = df_tmp[smx_cols[0]].values

            print(Fore.LIGHTMAGENTA_EX+ f'we need to apply the prefix if needed : {row['prefix']}')

            if pd.notna(row['prefix']): 
                print(Fore.LIGHTMAGENTA_EX+ f'we need to apply the prefix {row['prefix']}')
                la_liste=[f'{row['prefix']}_{x}' for x in df_tmp[row['parameter_name']]]
                df_tmp[row['parameter_name']]=la_liste
            if row['presentation_col']=='quoted':
                  print(Fore.MAGENTA+ f'we need to apply the quoted  {row['presentation_col']}')
                  df_tmp[row['parameter_name']]=df_tmp[row['parameter_name']].apply(lambda x: f"'{x}'")
                  



            print(row['parameter_name'])
            final_df[row['parameter_name']]=df_tmp[row['parameter_name']].values
            


    
    print(final_df)
    print(Fore.YELLOW+ f'printing final df type : {type(final_df)}')
    script = final_df.values.tolist()
    '''
    with open('REG_BKEY_PROCESS_output.SQL', mode='wt', encoding='utf-8') as myfile:
                for l in script:
                    myfile.write('\n'.join(l))
                    myfile.write('\n\n')
    '''
    print(Fore.YELLOW+" &&&&&&&&&&&&&&&&&   we end here ")
    print(Back.YELLOW+ "*********************************************************************************")
    '''
    end of new section
    '''

    
    #return df_with_empty_cols
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

        case "CORE_KEY_COL_REG" :# STG tables".lower():
            print("key type = ", key_type)
            print(env_attributes)
            smx_model= smx_preprocess(smx_model, 'core tables', f'PK==Y')
            script= get_bkey_reg_script(smx_model,filtered_script_df, env_attributes, key_type )

            script_dict = get_core_script_dict(script, smx_model)
            script2 = []
            for table_name, value in script_dict.items():
                sql = f"""SELECT * FROM GDEV1V_GCFR.GCFR_TRANSFORM_KEYCOL WHERE OUT_OBJECT_NAME = '{table_name}';
            DELETE FROM GDEV1V_GCFR.GCFR_TRANSFORM_KEYCOL WHERE OUT_OBJECT_NAME = '{table_name}';
            {value}"""
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

#            # Get the current process
#     process = psutil.Process(os.getpid())

# # Get memory information for the process
# # rss (Resident Set Size) is the non-swapped physical memory a process uses.
#     mem_info = process.memory_info()
#     print(f"Current process memory usage (RSS): {round(mem_info.rss / (1024**2), 2)} MB")
    return script