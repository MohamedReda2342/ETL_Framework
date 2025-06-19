import pandas as pd
import concat_util as cu
from datetime import datetime

#the_file_to_read = "file_example_XLSX_5000.xlsx"
the_file_to_read = "WAVEZ_SMX_Version_0.1.xlsx"

def get_sheets(xl):
    sheet_names = xl.sheet_names  # see all sheet names
    return sheet_names

#df_names from sheet names
def df_names(sheet_names) :
    signals = [s.replace(" ",'') for s in sheet_names]
    df_names = list(map(lambda x: 'df_' + x , signals))
    return df_names


def read_tabs_contents(xl, tabs):
    content_list = [xl.parse(t) for t in tabs]
    return content_list
     

def process_system_tab(sheet_content):

    pref = "EXEC GCFR_Register_System( "
    sufx = ");"
    new_sheet_content =  pd.DataFrame(sheet_content[:2])

    sys_cols = new_sheet_content.columns

    generated_script = []
    for indices, row in new_sheet_content.iterrows():
        print(type(row))
        a = cu.concat_4(row['Source System ID'], '/', row['Source System Alias'], row['Source System Name'])
        generated_script.append(pref+ a + sufx )

    #print(new_sheet_content)
    print(generated_script)
    return generated_script 


def process_stream_tab(sheet_content):
    pref = "CALL GCFR_UT_Register_Stream( "
    sufx = ");"
    new_sheet_content =  pd.DataFrame(sheet_content[:4])
    generated_script = []
    for indices, row in new_sheet_content.iterrows():
        today = datetime.today().strftime('%Y-%m-%d')
        a= str(int(row['System Id'])) + ',' + str(int(row['Stream Key'])) + ','
        a =  a + cu.concat_2(row['Stream Name'], today )
        generated_script.append(pref+ a + sufx )
    return generated_script 


xl = pd.ExcelFile(the_file_to_read)
workbook_sheets_names = get_sheets(xl)
new_names_list = df_names(workbook_sheets_names)
df= pd.DataFrame(list(zip(workbook_sheets_names, df_names(workbook_sheets_names))))
df.columns= ['tab', 'df_name']
df['content'] = read_tabs_contents(xl, df['tab'])


df['script'] = [list() for x in range(len(df.index))]
gen_script = process_system_tab(df['content'][1])

df['script'][1]=gen_script
gen_script = process_stream_tab(df['content'][2])
df['script'][2].append(gen_script)
