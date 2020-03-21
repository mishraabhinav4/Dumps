
import os
#Go to parent directory from 'Scripts' folder
if 'Scripts' in str(os.getcwd()) :os.chdir(os.path.abspath(os.path.join(os.getcwd(), os.pardir)))
#
#Body of code
#Body of code
#Body of code
#Body of code
#Body of code
#Body of code
#
#update the python scripyt file. Always place this script at the end
try:
    os.chdir('Scripts')
    get_ipython().system('jupyter nbconvert --to script <ScriptName>.ipynb')
except: pass

