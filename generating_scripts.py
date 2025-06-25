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
    #functions_DF['function_name'] = 
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
    #print(sheetnames)
    
    smx_model={}

    for work_sheet in sheetnames:
        df = pd.read_excel(filename,work_sheet)
        cols = [x.lower() for x in df.columns]
        df.columns=cols
        smx_model[work_sheet.lower()] = df #list(df.columns)

    smx_df = pd.DataFrame(smx_model.items())
    return smx_model, smx_df


def load_syntax_model(syntax_file):
    work_book = openpyxl.load_workbook(syntax_file)
    worksheets= work_book.worksheets
    sheetnames=work_book.sheetnames
    print('syntax model sheet names')
    print(sheetnames)
    
    script_model={}

    for work_sheet in sheetnames:
        df = pd.read_excel(syntax_file,work_sheet)
        print(df)
        #print(df)
        cols = [x.lower() for x in df.columns]
        df.columns=cols
        script_model[work_sheet.lower()] = df #list(df.columns)

    script_df = pd.DataFrame(script_model.items())
    print('=====================================================================')
    #print('script model')
    print('=====================================================================')
    print('=====================================================================')
    #print('script df')
    print('=====================================================================')
    #print(script_df)
    return script_model #, script_df


def load_env():
    env=read_env.CurrentEnv()
    print(env)
    return env

def filter_key_type(script_dict, key_type):
    print('================     filter_key_type      =====================================================')
    print('========= returns a filtered script model to the key_type selected  ============')

    #print(script_dict)
    for k in script_dict.keys():
        print (k)
        
    df=script_dict['functions']
    print('=============================================== df',df)
    filtered_functions = df[df['key_type'] ==key_type]
    
    print('========= now iterate over the filtered functions so we can get the parameters  ============')

    df2=script_dict['parameters']
    df_filtered = pd.merge(filtered_functions, df2, on='function_code')
    print(df_filtered)
    df3=script_dict['unique_parameters']
    df_filtered2=  pd.merge(df_filtered, df3, left_on='parameters', right_on='parameter_name')
    print(df_filtered2)


    return df_filtered2



'''
def get_key_type():
    
    BKEY_CALL
    REG_BKEY_PROCESS
    REG_BKEY_DOMAIN
    REG_BKEY
    
    for testing return  BKEY_CALL
    key_type= 'BKEY_CALL' #'BKEY_CALL'
    return key_type
'''
'''


def get_process_name(Filtered_tables_DF):
    print("=================================    In get_process_name ======================================")
    print(Filtered_tables_DF)
    print(Filtered_tables_DF.columns)
    cols = [x.lower() for x in Filtered_tables_DF.columns]

    Filtered_tables_DF.columns= cols
    process_names = []
    for _, row in Filtered_tables_DF.iterrows():
        process_name = f"BK_{row['key set name']}_{row['key domain name']}_{row['table name source']}_{row['column name source']}"
        process_names.append(process_name)
    
    print(process_names)

    print("=================================    In get_process_name ======================================")

    return process_names


'''

def get_env_dict(env, bi_flag):
    envvar = CurrentEnv(env, bi_flag=1 )
    env_attributes = {
        k: v
        for k, v in vars(envvar).items()
        if not callable(v) and not k.startswith("__")
    }
    print(env_attributes)
    return env_attributes



def flatten(a):  
    res = []  
    for x in a:  
        if isinstance(x, list):  
            res.extend(flatten(x))  # Recursively flatten nested lists  
        else:  
            res.append(x)  # Append individual elements  
    return res  

def join_bkey_stg_stream(smx_tabs, df,smx_dict):
    print(Back.LIGHTGREEN_EX+ "==================    join_bkey_stg_stream   ========================================================")
    print(df)
    print(df.columns)
    print(smx_tabs)
    print("SMX Dictionary Contents:")
    for key, value in smx_dict.items():
        print(f"\nTable: {key}")
        print("-" * 80)
        print(value)
        print("-" * 80)
    df1=smx_dict['bkey']
    df2=smx_dict['stg tables']
    on_cols=['key set name','key domain name']
    merged= pd.merge(df1, df2, on=on_cols, how='left')
    #print(merged)
    df3=smx_dict['stream']
    print(df3)
    #hard coded process type = 21 ==> _bkey
    search_string='_BKEY'
    filtered_df = df3[df3['stream name'].str.contains(search_string)]
    print(filtered_df)
    #on_cols=[source system alias, system name]
    smx_df = pd.merge(merged, filtered_df, left_on='source system alias', right_on='system name', how='left')
    print(smx_df)
    return smx_df


def get_params_values(smx_tab, df, smx_dict):
    print(Back.LIGHTRED_EX+ "==================    get_params_values   ========================================================")
    print(smx_tab)
    #a_df=pd.DataFrame()
    print(df.columns)
    print(Back.CYAN+ "incoming df parameters - content of parameters")

    print(df['parameter'])

    smx_lst=list(df['smx_column'])
    smx_tabs_lst=list(df['source'])
    smx_join_key=list(df.query('join_key == join_key')['join_key'])
    
    print(smx_lst)
    print(Fore.RED+"this is the smx_tabs_lst")
    print(smx_tabs_lst)
    print(smx_join_key)
    
   

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
        multiple_source_list = list(itertools.chain(*multiple_source_list))
        print(Back.CYAN+ "FLATTENED Multiple sources list")
        print(multiple_source_list)
        print(Back.CYAN+ "SET FLATTENED Multiple sources list")
        print(set(multiple_source_list))
        print(list(set(multiple_source_list)))
        smx_tabs=multiple_source_list
    else:
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
    print(Back.LIGHTRED_EX+ "----------------------------------------------------------------------------")
    

    smx_col_list= [x.split(',') for x in smx_list]
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

    params_list=[x.lower() for x in df['parameter']]

    print(params_list)
    params_list=[x.split(',') for x in params_list]
    print(params_list)
    params_list = flatten(params_list)  
    df_with_empty_cols = smx_df.reindex(columns=params_list)
    print(df_with_empty_cols)
    print(Back.LIGHTGREEN_EX+ "----------------------------------------------------------------------------")

    #create_empty_columns(df, columns_to_ensure)
    print(df_with_empty_cols.isnull())
    for col in df_with_empty_cols.columns:
        df_with_empty_cols[col] = df_with_empty_cols[col].fillna(col)

    print(df[['smx_column', 'presentation_col']])
    filtered_df = df[(df['presentation_col'] == "quoted") & (df['smx_column'].notna())]
    #filtered_df = df[df['presentation_col'] == "quoted"]
    print(filtered_df)

    quoted_cols= list(filtered_df['smx_column'])
    quoted_cols=[x.lower() for x in quoted_cols ]
    print(Fore.RED + "Quote ************ ")
    print(quoted_cols)

    
    filtered_df = df[(df['presentation_col'] == "concatenate") & (df['smx_column'].notna())]
    concatenated_cols= list(filtered_df['smx_column'])
    print(Fore.RED + "Concatenate ************ ")

    print(concatenated_cols)

    for c in quoted_cols:
        #df_with_empty_cols[c].apply
        df_with_empty_cols[c] = df_with_empty_cols[c].apply(lambda x: f'"{x}"')
    
    
    print(df_with_empty_cols.columns)
    print( df_with_empty_cols)
    print(Fore.RED + "then we concatenate the columns")
    if len(concatenated_cols) > 0:
        col_name="Process_Name"
        concatenates_cols_list = concatenated_cols[0].split(",")
        cols_list= list(df_with_empty_cols.columns)
        df_with_empty_cols[col_name] = df_with_empty_cols[concatenates_cols_list].astype(str).agg('_'.join, axis=1)
        df_with_empty_cols[col_name]= "BK_" + df_with_empty_cols[col_name]
        print(Fore.RED + "cols list")

        cols_list.insert(0, col_name)
        print(cols_list)
        df_with_empty_cols=df_with_empty_cols[cols_list]
        df_with_empty_cols.drop(columns=concatenates_cols_list, inplace=True) 



    print(df_with_empty_cols.columns)
    print( df_with_empty_cols)
    return df_with_empty_cols

def get_bkey_domain_script(smx_model, filtered_script_df, env_attributes):
    scripts=[]
    print(filtered_script_df)
    print(" ***************************************** columns of the filtered script workbook :")
    print(filtered_script_df.columns)
    script=" place holder "
    df_by_function=filtered_script_df.groupby(['operation', 'schema','functions' ])
    for group_name, df_group in df_by_function:
        print(df_group)
        print(group_name)

        print('- 1 -  FIND THE ENVIRONMENT ', env)
        env_schema = df_group['schema'][0]
        print( env_schema)
        env_schema = env_attributes[df_group['schema'][0]]
        print(env_schema)
        print(df_group.columns)
        print(df_group.shape)
        df_group['schema']=env_schema
        print(df_group['schema'])
        
        print('======================== apply schema and other environment variables ==========', env)

    

        df_group['parameter'] = df_group.apply(lambda row: str(env_attributes[row.parameter_name]) if  row.source=='env' else np.nan, axis=1)
        print(df_group.columns)
        #print(df_group[['parameter','source', 'smx_column']])
        #parameter_name	source	smx_column
        
        #df_group['parameter'] = df_group.apply(lambda row: ({row['source']} , {row['smx_column']}) if  row['source']!='env' else row['parameter'],  axis=1)
        smx_tab= df_group['source'].unique()
        print(df_group[['parameter','source', 'smx_column']])
        print(df_group.columns)
        df_group['parameter'] = df_group.apply(lambda row: str(env_attributes[row.parameter_name]) if  row.source=='env' else row.smx_column, axis=1)
        print(df_group)

        
        print(" oooooooooooooooooooooooooooooooooooooo Now we select the important columns, namely operation, schema and functions")
        df=df_group[['operation','schema' ,'functions']]
        print(df.columns)
        print(df.shape)
        print(df)
        
        print(" oooooooooooooooooooooooooooooooooooooo join the parameters in one string tuple")

        parameters_string =tuple(list(df_group['parameter']))         
        print("printing parameters_string")
        print(parameters_string)

        print(type(parameters_string))
        print("assigning the same value to all rows")
        
        p_s= list(df_group['parameter'])
        join_str = " , ".join(p_s)

        
        print(join_str)
        print(" oooooooooooooooooooooooooooooooooooooo this tuple is assigned to the column function_parameters")
        df = df.assign(function_parameters=join_str)
        print(df)
        #np.tile(parameters_string, len(df)).reshape(-1, len(parameters_string)).tolist()

        print(" oooooooooooooooooooooooooooooooooooooo Now drop duplicates ")

        df=df.drop_duplicates()
        print(df)
        print(df.shape)


        print(" oooooooooooooooooooooooooooooooooooooo aggregations of schema and functions ")
        print(" oooooooooooooooooooooooooooooooooooooo aggregations operation with schema.function ")

        df['schema_functions'] = df[['schema', 'functions']].agg('.'.join, axis=1)
        print(df)
        df['operation_schema_functions'] = df[['operation','schema_functions']].agg(' '.join, axis=1)
        
        df= df.drop(['operation', 'schema', 'functions', 'schema_functions'], axis=1)
        print(df)


        
        print(smx_tab)

        params_df = get_smx_values(smx_tab[0],list(df['function_parameters'])[0], smx_model)
        print(params_df)
        print(df)
        df = pd.concat([df, params_df], ignore_index=True)
        print(df)
        cols=params_df.columns
        #params_df['combined'] = params_df[cols].apply(lambda row: ','.join(row.values.astype(str)), axis=1)
        params_df['combined'] = params_df[cols].apply(lambda row: tuple(row.values.astype(str)), axis=1)

        function = df['operation_schema_functions'].unique()
        print(function)
        #params_df =params_df.assign(['function'], function[0])
        #params_df = params_df.assign(function=function)
        params_df['fn'] =function[0]
        params_df=params_df[['fn', 'combined']]
        params_df['combined']=params_df['combined'].apply(lambda row: str(tuple(row)))
        params_df['script']=params_df['fn']+params_df['combined']
        print(Back.LIGHTRED_EX+"=--------------------------------------------------")
        print(params_df)
        scripts.append(list(params_df['script']))


    print(scripts)
    return scripts


def get_bkey_reg_script(smx_model, filtered_script_df, env_attributes):
    scripts=[]
    print(Fore.LIGHTRED_EX+" 0- Lets check the filtered operation *****************************************  :")
    print(filtered_script_df)

    print(filtered_script_df.columns)
    script=" place holder "
    print(env_attributes)
    print(type(env_attributes))
    
    print("==========================================================================")
   
    print(filtered_script_df)
    df_by_function=filtered_script_df.groupby(['operation', 'schema','functions' ], sort=False)
       
    for group_name, df_group in df_by_function:
        print(Back.RED + f"Group Name: {group_name}")
        #print(Back.GREEN + df_group)
        print(Back.GREEN + str(group_name))
        e_schema = df_group['schema'].unique()
        print(e_schema)
        #print('==================== find the environment variables  ==========', env)
        env_schema = env_attributes[e_schema[0]]
        print(env_schema)
        print(df_group.columns)
        print(df_group.shape)
        df_group['schema']=env_schema
        print(df_group['schema'])
        print(df_group.columns)
        print('======================== apply schema and other environment variables ==========')
        print(df_group)
    
        print(df_group[['parameter_name','source', 'smx_column']])

        #df_group['parameter'] = df_group.apply(lambda row: str(env_attributes[row.parameter_name]).strip("\'") if  row.source=='env' else np.nan, axis=1)
        print(df_group.columns)
        #print(df_group[['parameter','source', 'smx_column']])
        #parameter_name	source	smx_column
        
        smx_tab= df_group['source'].unique()
        print(df_group.columns)
        df_group['parameter'] = df_group.apply(lambda row: f'"{str(env_attributes[row.env_variable])}"' if  row.source=='env' else row.smx_column, axis=1)
        print(df_group)
        print(Back.CYAN + str(df_group['parameter']))

        
        print(" oooooooooooooooooooooooooooooooooooooo Now we select the important columns, namely operation, schema and functions")
        #df=df_group[['operation','schema' ,'functions', 'source', 'smx_column', 'parameters', 'parameter','presentation_col']]
        df=df_group
        print(df.columns)
        print(df.shape)
        print(df)
        
        print(" oooooooooooooooooooooooooooooooooooooo join the parameters in one string tuple")
        print(" oooooooooooooooooooooooooooooooooooooo to be done after retrieving the values from the smx file ")

        parameters_string =tuple(list(df_group['parameter']))         
        print("printing parameters_string")
        print(parameters_string)

        print(type(parameters_string))
        print("assigning the same value to all rows")
        
        print(df)
        #np.tile(parameters_string, len(df)).reshape(-1, len(parameters_string)).tolist()

        print(" oooooooooooooooooooooooooooooooooooooo Now drop duplicates ")
        df=df.drop_duplicates()
        print(df)
        print(df.shape)


        print(" oooooooooooooooooooooooooooooooooooooo aggregations operation with schema.function ")

        df['schema_functions'] = df[['schema', 'functions']].agg('.'.join, axis=1)
        print(df)
        df['operation_schema_functions'] = df[['operation','schema_functions']].agg(' '.join, axis=1)
        df= df.drop(['operation', 'schema', 'functions', 'schema_functions'], axis=1)
        print(df)


       
        print(" xxxxxxxxxxxxxxxxxxxxxxxxx Printing smx_tab and smx_tab[0]")
        print(smx_tab)
        print(smx_tab[0])
        smx_tab_lst= list(smx_tab)
        if 'env' in list(smx_tab):
            smx_tab_lst.remove('env')
        
        print(Back.LIGHTMAGENTA_EX + str(smx_tab_lst))

        smx_filtered_df = df[df['source'] != 'env']
        print(df)
        print(list(smx_filtered_df['source'].unique()))
        print(smx_filtered_df['smx_column'])
        print(list(smx_filtered_df['smx_column']))

        print(" xxxxxxxxxxxxxxxxxxxxxxxxx df['function_parameters'] and df['function_parameters'][0]")
        #print(df['function_parameters'])
        #print(list(df['function_parameters'])[0])

        #params_df = get_smx_values(smx_tab[0],list(df['function_parameters'])[0], smx_model)
        source_params_df= df[['source', 'parameter', 'smx_column','env_variable','parameters','presentation_col','join_key']]
        print(source_params_df)
        
        params_df = get_params_values(smx_tab_lst[0], source_params_df, smx_model)

        #params_df = get_smx_values(smx_tab[0],list(smx_filtered_df['smx_column']), smx_model)
        
        print(Back.LIGHTRED_EX + "====================== after get  values ============================")
        print(params_df)
       

        print(Back.CYAN + "params_df with combined parameters")
        cols=params_df.columns
        print(cols)

        merged_column = params_df.apply(lambda row: ', '.join(map(str, row)), axis=1)
        params_df['merged']= merged_column
        print(Back.CYAN +" ================== params_df[merged] --------------- ")
        print(Back.CYAN +  params_df['merged'])
        print( "params_df columns " + params_df.columns)
        print(Back.CYAN +" ================== df --------------- ")

        print(df)
        print(df.columns)
        print(df.shape)
        #df3= pd.merge(df, params_df, left_index=True, right_index=True, sort=True)
        #print(df3)
        #df3 = pd.concat([df, params_df], ignore_index=True)
        #print(df3)
        print(Back.CYAN +" ================== params_df --------------- ")

        #print(params_df)
        print(params_df.shape)
        print(params_df.columns)
        print(params_df['merged'])
        #df = pd.concat([df, params_df], ignore_index=True)
        df = df.reset_index()
        params_df=params_df.reset_index(drop=True)
        duplicate_cols = params_df.columns[params_df.columns.duplicated()]
        params_df.drop(columns=duplicate_cols, inplace=True)
        #df = pd.concat([df, params_df], ignore_index=True)
        print(Back.CYAN +" ================== check the number of cols  --------------- ")

        print(len(df.columns) == len(set(df.columns)))

        print(len(params_df.columns) == len(set(params_df.columns)))
        df = df.reset_index(drop=True)
        df = pd.concat([df, params_df], ignore_index=True)
        #print(df)
        cols=params_df.columns
        #params_df['combined'] = params_df[cols].apply(lambda row: ','.join(row.values.astype(str)), axis=1)
        #params_df['combined'] = params_df[cols].apply(lambda row: tuple(row.values.astype(str)), axis=1)
        print(Back.CYAN+ "df['operation_schema_functions'].unique()")
        function = df['operation_schema_functions'].unique()
        print(function)
        #params_df =params_df.assign(['function'], function[0])
        #params_df = params_df.assign(function=function)
        params_df['fn'] =function[0]
        params_df['combined']=params_df['merged'].apply(lambda row: f"({row})")
        params_df=params_df[['fn', 'combined']]
        print(Back.CYAN + "params_df['combined']")

        print(params_df['combined'])
        params_df['script']=params_df['fn']+params_df['combined']
        print(Back.CYAN + "params_df['script']")

        print(params_df['script'])
        fn_scripts=set(params_df['script'])
        print(fn_scripts)
        print(len(fn_scripts))
        scripts.append(list(fn_scripts))
        for s in list(params_df['script']):
            print(Back.LIGHTRED_EX+ s)
        #break

    print(Back.CYAN+ str(scripts))
    print(type(scripts))
    print(len(scripts))
   
    return scripts


# if __name__ == '__main__':
def main(smx_model, key_type, env , bigint_flag):

    '''
    the first step is to read the source mapping file and the script model
    1- calling the function load_smx_file() function to return :
        smx_model of type i=dict, with tabs as keys
        smx_df  containing all the dict items
    '''
    print('=====================================================================')

    #data_file = 'C:/Users/hanaa.hamad/Documents/projects/eBANK/Preparation/ETL_script_generator/WAVZ_SMX_Version_0.5.xlsx'
    # data_file = 'WAVZ_SMX_Version_0.5.xlsx'
    # smx_model, smx_df=load_smx_file(data_file)
    
    print('=====================================================================')


    #script_file = "C:/Users/hanaa.hamad/Documents/GitHub/HH-ETL-scripts/documentation/scripts/0528/schema_functions_2.xlsx"
    #script_file = "C:/Users/hanaa.hamad/Documents/GitHub/HH-ETL-scripts/documentation/scripts/0528/schema_functions_MAPPED_script.xlsx"
    script_file = "schema_functions_MAPPED_script.xlsx"
    syntax_model= load_syntax_model(script_file)
    #print(syntax_model)
    print("THE TYPE OF Syntax model", type(syntax_model))
    print('=====================================================================')

    print("==============================  READING command line arguments ================================ ")
    #key_type = sys.argv[1]  # flag is N to create a new file, othewise it will append to the existing file
    #env = sys.argv[2] ## you can send the env in command line DEV - TST - PROD 
    # env = input("Enter the environment: DEV TST PROD ")
    # key_type = input("enter key_type: BKEY_CALL     REG_BKEY_PROCESS    REG_BKEY_DOMAIN REG_BKEY ")
    #key_type='REG_BKEY' #'REG_BKEY_DOMAIN'
    #env='TST'
    print(f"Generating {key_type} script for environment {env}")
    # NEXT STEP:
    # filter the functions to those related to the selected key type
    #key_type = get_key_type()
    filtered_script_df=filter_key_type(syntax_model, key_type)
    
    print('=====================================================================')
    print(filtered_script_df)
    print(filtered_script_df.columns)
    print('=====================================================================')

    env_attributes=get_env_dict(str(env), bigint_flag)

    match key_type:
        case "BKEY_CALL" : 
            print("key type = ", key_type)
            cols_list=['operation','schema','functions', 'parameters', 'parameter_name', 'source', 'smx_column'] 
            print(filtered_script_df.columns)
            
            script= get_bkey_reg_script(smx_model, filtered_script_df, env_attributes)

            print("============ Generated Script ===================")
            print(script)
            with open('BKEY_CALL_script_output.txt', mode='wt', encoding='utf-8') as myfile:
                for l in script:
                    myfile.write('\n'.join(l))
                    myfile.write('\n\n')
            
        case "REG_BKEY_PROCESS": #System".lower():
            print("key type = ", key_type)
            print(filtered_script_df.columns)
            script= get_bkey_reg_script(smx_model, filtered_script_df, env_attributes)

            print("============ Generated Script ===================")
            print(script)
            with open('REG_BKEY_PROCESS_output.txt', mode='wt', encoding='utf-8') as myfile:
                for l in script:
                    myfile.write('\n'.join(l))
                    myfile.write('\n\n')
                    

        case "REG_BKEY_DOMAIN" : #Stream".lower():
           print("key type = ", key_type)
           #script= get_bkey_domain_script(smx_model,filtered_script_df, env_attributes )
           script= get_bkey_reg_script(smx_model, filtered_script_df, env_attributes)

           print("======================= returned script : ")
           print(script)
           print(len(script))
           print(type(script))
        
           with open('REG_BKEY_DOMAIN_script_output.txt', mode='wt', encoding='utf-8') as myfile:
                for l in script:
                    print(l)
                    myfile.write('\n'.join(l))
                    myfile.write('\n\n')

        case "REG_BKEY" :# STG tables".lower():
            print("key type = ", key_type)
            print(env_attributes)
            script= get_bkey_reg_script(smx_model,filtered_script_df, env_attributes )
            #script= get_bkey_call_script(smx_model,filtered_script_df, env_attributes )
            print("======================= returned script : ")
            print(script)
            for s in script:
                print(Back.LIGHTGREEN_EX+ str(s))
            
            with open('REG_KEY_script_output.txt', mode='wt', encoding='utf-8') as myfile:
                for l in script:
                    myfile.write('\n'.join(l))
                    myfile.write('\n\n')
        case "bkey_views":
            script = generate_bkey_views(smx_model,env)
    return script
    # if name == 'main':
def generate_bkey_views(smx_model , env):
    """
    Generate BKEY views by extracting required parameters from SMX model
    and calling the Queries.bkey_views method
    """
    # Get STG tables dataframe from smx_model
    stg_tables_df = smx_model['stg tables']
    
    # Filter for rows with non-null key set name and key domain name
    bkey_df = stg_tables_df.dropna(subset=['key set name', 'key domain name'])
    
    # Also filter out empty strings if needed
    bkey_df = bkey_df[
        (bkey_df['key set name'] != '') & 
        (bkey_df['key domain name'] != '')
    ]
    
    # Extract required parameters
    view_params = []
    for _, row in bkey_df.iterrows():
        params = {
            'key_set_name': row['key set name'],
            'key_domain_name': row['key domain name'],
            'table_name': row['table name source'],
            'column_name': row['column name source']
        }
        view_params.append(params)
    
    # Generate views using Queries.bkey_views
    scripts = []
    print("ennnnnnnnnnvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv"+env)
    for params in view_params:
        view_script = Queries.bkey_views(
            params['key_set_name'],
            params['key_domain_name'], 
            params['table_name'],
            params['column_name'],
            "G"+env+"1V_INP"
        )
        scripts.append(view_script)
        
    return scripts