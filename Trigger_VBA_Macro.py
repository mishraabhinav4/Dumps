
import os, os.path
import win32com.client


#Python for macro trigger

if os.path.exists("file_path_&_name.xlsm"):
    print('VBA file exists')
    xl=win32com.client.Dispatch("Excel.Application")
    print("VBA file opening")
    xl.Workbooks.Open(os.path.abspath("file_path_&_name.xlsm"), ReadOnly=1)
    print("VBA running")
    xl.Application.Run("file_name.xlsm!Module_Name.macro_name")
##    xl.Application.Save() # if you want to save then uncomment this line and change delete the ", ReadOnly=1" part from the open function.
    xl.Application.Quit() # Comment this out if your excel script closes
    del xl


