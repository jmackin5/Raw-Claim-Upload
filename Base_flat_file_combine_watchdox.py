###Importing the right packaages / aliasing / room folder look ups

import pandas as pd
from workspaces.workspaces import *
import datetime as dt
import os
import csv
import io
import zipfile

# Variables
##fp = 'C:/Users/PatrickGagnon/Documents/hi-data-upload-utility-1.6/bin/Files/OPA_Univera'
date = dt.date.today()
file_release = dt.datetime.strftime(date,'%Y-%m-%d')
w = Workspaces()

afile = [] 
bifile = []
bpfile = [] 

def claim_combine(file):
    if ('~ST*835' in file) and ('ISA*' in file):
        afile.append(file)
    elif ('~ST*837' in file) and ('ISA*' in file) and ('005010X222A1' not in file):
        bifile.append(file)
    elif ('~ST*837' in file) and ('ISA*' in file) and ('005010X222A1' in file):
        bpfile.append(file)
    else:
        print('not a claim')

def claim_clean(file):
    file = [w.replace('!','*') for w in file]
    file = [w.replace('\\|',':') for w in file]
    file = [w.replace('>',':') for w in file]
    file = [w.replace('[\r\n]',' ') for w in file]
    return file


################################################################################


### test file discovery 
## set this cell to whatever folder you are checking for newfiles
get_rooms = w.get_rooms()
room_info = w.get_room_info(350519)
test_room_room_folders = w.get_room_folders(350519)
##test_room_room_folders['subFolders'][1]['subFolders']

#test_837 newfiles folderid = 5482329, masterfolder = 5482328, processed =5482327
doc = w.get_documents(room_id = 350519,
                        folder_id = 5482329) ##test 837s new files folder
doc = pd.DataFrame(doc)
print(doc)


################################################################################

##creating a new folder in processed(file release folder)

w.create_folder(folder_id=5482327,
                room_id=350519,
                new_folder_name=str(file_release))

################################################################################

## figuring out the file path for that new folder so we can send the processed files correct release folder
test_room_room_folders = w.get_room_folders(350519)
for i in test_room_room_folders['subFolders']:
    if i['name'] == '837s':
        temp = i['subFolders']
        for x in temp:
            if x['name'] == 'Processed':
                temp_2 = x['subFolders']
                for y in temp_2:
                    if y['name'] == str(file_release):
                        processed_file_release_path = y['fullPath']
                        processed_file_release_id = y['id']
                
print(processed_file_release_path)
print(processed_file_release_id)

###test_room_room_folders['subFolders'][1]['subFolders'][2]


################################################################################

#gathering files in the foler // combining all claims into one and moving original files into processed folder

for index, row in doc.iterrows():
    if row['file_type'] == 'zip':
        zip_document = w.download_document(row['guid'])
        zip_bytes = io.BytesIO(zip_document)
        myzipfile = zipfile.ZipFile(zip_bytes)
        for name in myzipfile.namelist():
            foofile = myzipfile.open(name).read().decode("ascii")
            claim_combine(foofile)
        print('zip file')
        ##w.move_document(room_id=350519, document_id=row['guid'], folder_path='837s/Processed')
        w.move_document(room_id=350519, document_id=row['guid'], folder_path=processed_file_release_path)
    else:
        reg_file = w.download_document(row['guid']).decode("ascii")
        claim_combine(reg_file)
        print('reg file')
        ##w.move_document(room_id=350519, document_id=row['guid'], folder_path='837s/Processed')
        w.move_document(room_id=350519, document_id=row['guid'], folder_path=processed_file_release_path)
        
    
#################################################################################

##Taking the combined file lists and creating one single file and moving it to master bin folder


afile = claim_clean(afile)
bifile = claim_clean(bifile)
bpfile= claim_clean(bpfile)

if len(bifile) >= 1:
    with open('C:\\Users\\JacobMacKinnon\\Documents\\hi-data-upload-utility-1.6\\bin\\Test_files\\'+'Master_bi'+file_release, 'w' ) as f:
        for i in bifile:
            f.write(str(i))
else:
    print('Bi file is empty')
      
if len(bpfile) >= 1:
    with open('C:\\Users\\JacobMacKinnon\\Documents\\hi-data-upload-utility-1.6\\bin\\Test_files\\'+'Master_bp'+file_release, 'w' ) as f:
        for i in bpfile:
            f.write(str(i))
            
else:
    print('Bp file is empty')
    
if len(afile) >= 1:
    with open('C:\\Users\\JacobMacKinnon\\Documents\\hi-data-upload-utility-1.6\\bin\\Test_files\\'+'Master_a'+file_release, 'w' ) as f:
        for i in bifile:
            f.write(str(i))
            
else:
    print('Afile is empty')
    
w.session_logout()