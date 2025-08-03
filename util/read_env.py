from datetime import date

class CurrentEnv:
  def __init__(self, env, bi_flag):
    env=str(env)
    self.env = env #'DEV' # 'TST' 'PROD'
    
    self.Debug_Level=6
    self.oReturn_Code= '01'
    self.oReturn_Message= '02' 
    self.OMessage = ':OMessage'
    

    self.BIGINT_Flag = str(bi_flag) 

  #GDEV1T_GCFR	GCFR Standard Tables
  #GDEV1V_GCFR	GCFR Standard Views

    
    self.Key_Table_DB_Name= "G"+env+"1T_UTLFW"
    self.Key_View_DB_Name = "G"+env+"1V_UTLFW"
    
    self.Map_Table_DB_Name = "G"+env+"1T_UTLFW"
    self.Map_View_DB_Name = "G"+env+"1V_UTLFW"

    #self.GCFR_Standard_Macros = 'G'+env+'1M_GCFR'
    self.GCFR_Standard_Utility_Stored_Procedure ='G'+env+'1P_UT'
    self.GCFR_Standard_Processing_Pattern_Stored_Procedure =  "G"+env+"1_PP"
    
    self.Staging_Tables = "G"+str(env)+"T_STG"
    self.Staging_Tables_Views = "G"+env+"1V_STG"
    self.Source_Tables_With_Bkey_Bmap = "G"+env+"1T_SRCI"
    self.SRCI_Table_Views = "G"+env+"1V_SRCI"
    self.Target_Tables = "G"+env+"1T_CORE"
    self.Target_Tables_Views = "G"+env+"1V_CORE"
    self.GCFR_Standard_Macros = "G"+env+"1M_GCFR"
    self.GCFR_API_Procedures = "G"+env+"1P_API"
    self.GCFR_Building_Blocks_Procedures = "G"+env+"1P_BB"
    self.GCFR_Control_Patterns_Procedures = "G"+env+"1P_CP"
    self.GCFR_Functional_Flow_Procedures = "G"+env+"1_FF"
    self.GCFR_Processing_Patterns_Procedures = "G"+env+"1_PP"
    self.GCFR_Utilities_Procedures = "G"+env+"1_UT"
    self.GCFR_Tables = "G"+env+"1T_GCFR"
    self.GCFR_Views = "G"+env+"1V_GCFR"
    self.GCFR_standard_database_to_hold_macros = "G"+env+"1M_OPR"
    self.GCFR_standard_database_to_hold_any_tables = "G"+env+"1T_OPR"
    self.GCFR_standard_database_to_hold_operational_views = "G"+env+"1V_OPR"
    self.Access_Layer_Tables = "G"+env+"1T_SEM"
    self.Access_Layer_Views = "G"+env+"1V_SEM"
    self.Temporary_Tables = "G"+env+"1T_TMP"
    self.Work_Tables = "G"+env+"1T_WRK"
    self.Transform_Input_Views = "G"+env+"1V_INP"
    self.Transform_Output_Views = "G"+env+"1V_OUT"
    # Ker view db name 
    self.UTL_Tables = "G"+env+"1T_UTLFW"
    self.Key_View_DB_Name = "G"+env+"1V_UTLFW"
    self.Collect_Stats = 0
    self.Truncate_Target = 0
    self.Verification_Flag =0
    self.File_Qualifier_Reset_Flag = 0

    self.Description=""
    

    self.SRCI_Process_Type =25
    self.SRCI_Out_DB_Name = "G"+env+ "1V_SRCI" 
    self.SRCI_Target_TableDatabaseName = "G"+env+"1T_SRCI" # = SRCI Tables DB

    self.SRCI_Collect_Stats =1
    
    self.SRCI_Truncate_Target=1
    self.SRCI_Verification_Flag=0
    
    
    self.SRCI_File_Qualifier_Reset_Flag=0
    self.SRCI_Key_Set_ID =""
    self.SRCI_Domain_Id =""
    self.SRCI_Code_Set_Id =""

# Get the current date
    self.Business_Date = date.today()
    self.Process_Type = 21
