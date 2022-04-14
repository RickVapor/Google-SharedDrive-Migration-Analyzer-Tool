
import csv
import os
import argparse
from datetime import datetime
from time import sleep
from httplib2 import Http
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload

v = False
LOG_PATH = ""


def logger(statement):

    v = False
    file_path = "{}/Execuction_logs.txt".format(LOG_PATH)
    with open(file_path, 'a') as f:
        f.write(statement + "\n")
        f.close()
    if v:
        print(statement)


def find_children(service, fileid, folder_list=[], file_list=[], cant_move_list=[], page_token=None):
    timestamp = datetime.now()
    timestamp = timestamp.strftime("%Y-%m-%d %I-%M-%S")

    # The Below search will return an object of all the Folders in a structure
    try:
        while True:
            logger("[{}] - Looking up children for ID: {}".format(timestamp, fileid))

            files = service.files().list(q=" '{fileid}' in parents".format(fileid=fileid), fields="*",
                                         supportsAllDrives='true', includeTeamDriveItems='true',
                                         pageToken=page_token).execute()
            page_token = files.get('nextPageToken')
            files = files.get('files')
            if page_token is None:
                break

        # loop over all Children to sort.
        for item in files:

            if item['id'] and item['mimeType'] == 'application/vnd.google-apps.folder':
                logger("[{}] - Adding Folder ID: {} to the list to be copied.".format(timestamp, item['id']))
                folder_list.append(item)

                # Recursive call: if item is a folder than we need to search it for children and add those to the list.
                find_children(service, item['id'], folder_list, file_list, cant_move_list)
            else:
                move_rights = item['capabilities']

                if (move_rights['canMoveItemIntoTeamDrive'] and move_rights['canMoveItemOutOfDrive'] and
                        move_rights['canMoveItemWithinDrive'] and len(item['parents']) == 1):

                    logger("[{}] - Adding File ID: {} to the list to be moved.".format(timestamp, item['id']))
                    file_list.append(item)
                else:
                    logger("[{}] - Adding File ID: {} to unmovable list.".format(timestamp, item['id']))
                    cant_move_list.append(item)

    except HttpError as err:
        if err.resp.status == 500:  # backoff code
            print("g_entity={fileid}, message =  'Error 500...waiting and retrying...'".format(fileid=fileid))
            logger("g_entity={fileid}, message =  'Error 500...waiting and retrying...'".format(fileid=fileid))
            back_off(find_children(service, fileid, folder_list, file_list, cant_move_list))
        else:
            print("error while finding children")
            print(err)
            logger(str(err))

    return folder_list, file_list, cant_move_list


def find_file_info(service, file_id):
    timestamp = datetime.now()
    timestamp = timestamp.strftime("%Y-%m-%d %I-%M-%S")

    file_name = ""
    file_parent_id = ""
    file_parent_name = ""

    # This function will return the name of a file or folder based on the ID
    try:
        logger("[{}] - Finding file name for ID: {}".format(timestamp, file_id))
        file_info = service.files().get(fileId=str(file_id), fields='*', supportsAllDrives='true').execute()

        file_name = file_info.get('name')
        file_parent_id = file_info['parents'][0]
        file_parent_id = file_parent_id[0]
        file_owners = file_info['owners'][0]

        logger("[{}] - Finding file name for ID: {}".format(timestamp, file_id, file_name))

    except HttpError as err:
        if err.resp.status == 500:
            print("g_entity={fileid}, message =  'Error 500...waiting and retrying...'".format(fileid=file_id))
            logger("g_entity={fileid}, message =  'Error 500...waiting and retrying...'".format(fileid=file_id))
            file_name, file_parent_id, file_parent_name, file_owners = back_off(find_file_info(service, file_id.encode('utf-8')))
        else:
            print(err)
            print("g_entity={fileid}, message =  'Error {errorstatus}'".format(fileid=file_id,
                                                                               errorstatus=err.resp.status))
            logger("g_entity={fileid}, message =  'Error {errorstatus}'".format(fileid=file_id,
                                                                                errorstatus=err.resp.status))

    return file_name, file_parent_id, file_parent_name, file_owners


def create_folder_structure(service, upload_user, parent_id, parent_name, folder_list, new_folder_list={}):
    timestamp = datetime.now()
    timestamp = timestamp.strftime("%Y-%m-%d %I-%M-%S")

    current_root = parent_id

    for item in folder_list:

        folder_name = item.get('name')
        folder_name = folder_name.strip()

        original_id = item.get('id')
        og_parent_id = item.get('parents')

        # search folder list for parent.
        for folder in folder_list:
            if folder.get('id') == og_parent_id:
                parent_name = str(folder.get('name'))
                break

        prev_owner = item['owners'][0]
        prev_owner = prev_owner.get('emailAddress')

        # Creating new folder structure with folders of the same name at location.
        folder_id = create_drive_folder(service, upload_user, folder_name, current_root)
        new_folder_list[original_id] = {
            'ogparentid': og_parent_id,
            'parentid': parent_id,
            'name': folder_name,
            'id': folder_id,
            'type': original_id,
            'prev_owner': prev_owner,
            'parentname': parent_name}

    logger("[{}] - created flat folder structure at root folder ID: {} ".format(timestamp, current_root))
    return parent_id, new_folder_list


def set_parent(service, folder_list, item_list, currentroot, destination_id, type):
    parentstring = ""
    new_parent_id = None
    for item in item_list:
        try:
            timestamp = datetime.now()
            timestamp = timestamp.strftime("%Y-%m-%d %I-%M-%S")
            parent_id = item_list[item]['ogparentid']

            if item_list[item]['id'] is not currentroot:
                # search parent list and use first match
                for parent in parent_id:
                    if parent in folder_list:
                        new_parent_id = folder_list[parent]['id']
                        break

                if not new_parent_id:
                    # Fail over if for some reason the parent is not in the list of original folders.
                    new_parent_id = destination_id
                    print("Parent ID not in list Name: {} File ID: {} Parent ID: {}".format(item_list[item]['name'],
                                                                                            item, parent_id))

                if type  == 'folder':
                    logger("[{}] - Removing parent id: {} from Drive Folder: {}".format(timestamp, parent_id,
                                                                                        item_list[item]['name']))

                    logger("[{}] - Adding new parent to folder: {}  new parent id: {}".format(timestamp,
                                                                                              item_list[item]['name'],
                                                                                              new_parent_id))

                    service.files().update(fileId=item_list[item]['id'],
                                           removeParents=currentroot,
                                           addParents=new_parent_id,
                                           enforceSingleParent='true',
                                           supportsAllDrives='true').execute()

                else:
                    logger("Simulating moving file")
                    logger("[{}] - Removing parent id: {} from Drive file: {}".format(timestamp, parent_id,
                                                                                      item_list[item]['name']))

                    logger("[{}] - Moving File - Name: {} ID: {} Parent_ID: {}".format(timestamp,
                                                                                       item_list[item]['name'],
                                                                                       item_list[item]['id'],
                                                                                       parent_id))

                    # service.files().update(fileId=list[item]['id'], removeParents=parent_id, addParents=new_parent_id,
                      #                     enforceSingleParent='true', supportsAllDrives='true').execute()

        except HttpError as err:
            if err.resp.status == 500:
                back_off(set_parent(service, folder_list, item_list, currentroot, destination_id, type))
            elif err.resp.status == 403:
                print("403 Error! Folder Hierarchy too deep. Moving folders to destination root  "
                      "ID:{} Name:{} File ID: {} passing to next file.".format(destination_id,
                                                                               item_list[item]['name'],
                                                                               item))

                logger("403 Error! Folder Hierarchy too deep. Moving folders to destination root  "
                       "ID:{} Name:{} File ID: {} passing to next file.".format(destination_id,
                                                                                item_list[item]['name'],
                                                                                item))

            elif err.resp.status == 404:
                print("404 Error! File not Found.  File ID:{} passing to next file.".format(item))
                logger("404 Error! File not Found.  File ID:{} passing to next file.".format(item))

                pass
            else:
                print(err)
                logger(str(err))


def create_drive_folder(service, upload_user, folder_name, parent_id):
    timestamp = datetime.now()
    timestamp = timestamp.strftime("%Y-%m-%d %I-%M-%S")

    folder_id = ""
    try:
        folder_metadata = {'name': folder_name, 'mimeType': 'application/vnd.google-apps.folder',
                           'parents': [parent_id]}
        logger("[{}] - Creating folder - Name: {} Parent ID: {} Upload User: {}".format(timestamp, folder_name,
                                                                                        parent_id, upload_user))

        file = service.files().create(body=folder_metadata, supportsTeamDrives='True').execute()
        folder_id = file.get('id')

    except HttpError as err:
        if err.resp.status == 500:
            back_off(create_drive_folder(service, upload_user, folder_name, parent_id))
        else:
            folder_id = parent_id
    except Exception as err:
        print(err)
        logger(str(err))

    return folder_id


def organize_cant_moves(file_list, new_folder_list, og_folder_list, root_parent):
    timestamp = datetime.now()
    timestamp = timestamp.strftime("%Y-%m-%d %I-%M-%S")
    cant_move_file_list = {}

    for item in file_list:
        parent_id_list = []
        parent_name_list = []
        move_error = ""

        file_name = item.get('name')
        file_id = item.get('id')

        parent_id_list = item['parents']
        parentstring = ",".join(parent_id_list)

        for parent in parent_id_list:
            # adds the name if folder is in list, otherwise this is not necessary info.
            if parent in new_folder_list:
                parent_name = new_folder_list[parent]
                parent_name = parent_name.get('name')
                parent_name_list.append(parent_name)

        new_parent_id = new_folder_list[parent_id_list[0]]
        new_parent_id = new_parent_id.get('id')

        prev_owner = item['owners'][0]
        prev_owner = prev_owner.get('emailAddress')

        if "@umich.edu" not in prev_owner:
            logger("[{}] - Adding error type of 'External Owner' for File ID: {}".format(timestamp, file_id))
            move_error = "External Owners "
        if not item['capabilities']['canEdit']:
            logger("[{}] - Adding error type of 'Can Edit' for File ID: {}".format(timestamp, file_id))
            move_error += "Read Access "
        if move_error == "":
            logger("[{}] - Adding a pull of all meta data for unmovable File ID: {}".format(timestamp, file_id))
            move_error = str(item['capabilities'])

        # for files that cant move we are going to make 'type' carry some sort of error description since folders
        # are copied and not moved it is presumed that these are all files.
        logger("[{}] - Adding File - Name: {} ID: {} to unmovable file list.".format(timestamp, file_name, file_id))
        cant_move_file_list[file_id] = {
            'ogparentid': parentstring,
            'parentid': new_parent_id,
            'name': file_name,
            'id': file_id,
            'type': move_error,
            'prev_owner': prev_owner,
            'parentname': str(parent_name_list).strip()}

    return cant_move_file_list


def move_drive_files(file_list, new_folder_list, new_file_list={}):
    timestamp = datetime.now()
    timestamp = timestamp.strftime("%Y-%m-%d %I-%M-%S")

    for item in file_list:
        file_name = item.get('name')
        file_id = item.get('id')

        og_parent_id = item.get('parents')

        parent_name = og_parent_id[0]
        parent_name = new_folder_list[parent_name]
        parent_name = parent_name.get('name')

        new_parent = new_folder_list[og_parent_id[0]]

        new_parent_id = new_parent.get('id')

        prev_owner = item['owners'][0]
        prev_owner = prev_owner.get('emailAddress')

        logger("[{}] - Adding File - Name: {} ID: {} to list of files to be moved.".format(timestamp,
                                                                                           file_name,
                                                                                           file_id))

        new_file_list[file_id] = {
            'ogparentid': og_parent_id,
            'parentid': new_parent_id,
            'name': file_name,
            'id': file_id,
            'type': 'file',
            'prev_owner': prev_owner,
            'parentname': parent_name}

        logger("New Parent for {} is {}".format(file_name, new_parent_id))
    return new_file_list


def create_sheets(service, item_list, filename, path):
    timestamp = datetime.now()
    timestamp = timestamp.strftime("%Y-%m-%d %I-%M-%S")

    filename = "{}.csv".format(filename)
    headers = ['Name', "ID", "Original Owner", "Original Parent ID", "New Parent ID", "Parent Name", "Extra Info"]
    logger("[{}] - Creating Log Name: {}".format(timestamp, filename))

    with open('{}/{}'.format(path, filename), 'w+', encoding='UTF8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)

        for item in item_list:
            file_item = item_list[item]

            row = [file_item.get('name'),
                   file_item.get('id'),
                   file_item.get('prev_owner'),
                   file_item.get('ogparentid'),
                   file_item.get('parentid'),
                   file_item.get('parentname'),
                   file_item.get('type')]

            writer.writerow(row)
    return


def back_off(function, t=5):
    print('Error 500...waiting {time} seconds and retrying...'.format(time=t))
    logger('Error 500...waiting {time} seconds and retrying...'.format(time=t))

    sleep(t)
    try:
        return function
    except HttpError as err:
        return back_off(function, t * t)


def upload_sheet(drive_service, path, name, destination, upload_user):
    file_id = ''
    try:
        file_metadata = {
            'name': name,
            'mimeType': 'application/vnd.google-apps.spreadsheet',
            'parents': [destination],
            'owners': [upload_user]
        }
        media = MediaFileUpload(path,
                                mimetype='text/csv',
                                resumable=True)

        file = drive_service.files().create(body=file_metadata,
                                            media_body=media,
                                            supportsAllDrives=True,
                                            fields='id').execute()
        file_id = file.get('id')
    except HttpError as err:
        if err.resp.status == 500:
            back_off(upload_sheet(drive_service, path, name, destination, upload_user))

    except Exception as err:
        print(err)
        logger(err)

    return file_id


def move_ownership(service, file_id, owner, perm_id, parent_id, root='false'):

    metadata = {'role': 'owner', 'type': 'user', 'emailAddress': owner}
    try:
        if root == 'true':
            service.permissions().create(fileId=file_id,
                                         transferOwnership='true',
                                         moveToNewOwnersRoot='true',
                                         sendNotificationEmail='false',
                                         body=metadata).execute()
        else:
            service.permissions().create(fileId=file_id,
                                         transferOwnership='true',
                                         sendNotificationEmail='false',
                                         body=metadata).execute()

            print ("Moving to parent ID: {}".format(parent_id))
            service.files().update(fileId=file_id, addParents=parent_id)

        service.permissions().delete(fileId=file_id, permissionId=perm_id).execute()

    except HttpError as err:
        if err.resp.status == 500:
            back_off(move_ownership(service, file_id, owner, perm_id, parent_id, root))

    except Exception as err:
        print(err)
        logger(err)
    return


def main():
    global LOG_PATH
    helptext = "Future Help Text"

    parser = argparse.ArgumentParser(
        description='Creates shared accounts in collab services',
        epilog=helptext,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        '--source',
        '-s',
        help='The folder ID of the folder structure to copy. (ex "0B-mf8CerKaZceERvUnFFZllfMGs")'
    )

    parser.add_argument(
        '--destination',
        '-d',
        help='The folder ID of the source folder or root of the Shared drive. (ex "1L3hFIQECY_iJarX8HQt-x_MORcNdLjzr")'
    )

    parser.add_argument(
        '--user',
        '-u',
        help='The uniqname of the user who owns the folder structure.'
    )

    parser.add_argument(
        '-v',
        help='Visual toggle for more output.'
    )
    args = parser.parse_args()

    if not args.source:
        args.source = input("Enter folderID for the root folder in the source structure: ")
        args.destination = input("Enter the folderID for the destination: ")
        args.user = input("enter the uniqname of the requester (must have at least read permission for structure): ")

    new_folder_list = {}
    timestamp = datetime.now()
    timestamp = timestamp.strftime("%Y-%m-%d %I-%M-%S")

    # Make logging folders
    folder_path = "./logs/{}".format(timestamp)
    os.makedirs(folder_path)
    LOG_PATH = folder_path

    folder_parent_id = args.source
    destination_id = args.destination
    folder_owner = args.user

    owner_uniq = folder_owner
    folder_owner = folder_owner + '@umich.edu' if '@' not in folder_owner else folder_owner

    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        'resolute-clock-286716-225b871184f8.json', scopes=['https://www.googleapis.com/auth/drive'])

    delegated_credentials = credentials.create_delegated(folder_owner)

    http = Http()
    delegated_credentials.authorize(http)

    try:
        print("building service")
        logger("building service")
        service = build('drive', 'v3', http=http)
        print("service built")
        logger("building service")

        sheets_service = build('sheets', 'v4', credentials=credentials)
        print("Building Sheets Service")

    except HttpError as err:
        print("error in build 1")
        return err
    except Exception as err:
        if err.status == 401:
            print("401 error!")
            print(err)
        return err

    # if all arguments are available start workflow
    if folder_parent_id and destination_id and folder_owner:
        print("Finding Children for Folder ID: {}".format(folder_parent_id))
        folder_objects, file_objects, cant_move_objects = find_children(service, folder_parent_id)

        # upload user is set for any file uploads to drive.
        try:
            upload_user = '{user}'.format(user=folder_owner)
        except Exception as err:
            print("Error in get login name using default: {}".format({err}))
            upload_user = 'admin-mgoogle@umich.edu'

        # parentid is dummy data and unnecessary for the first entry.
        parentname, parent_id, parent_name, parent_owners = find_file_info(service, folder_parent_id)
        parentfolderid = create_drive_folder(service, folder_owner, parentname, destination_id)
        print("Created new parent folder - Name: {} ID: {}".format(parent_name, parentfolderid))

        new_folder_list[folder_parent_id] = {
            'parentid': destination_id,
            'name': parentname,
            'id': parentfolderid,
            'parentname': parent_name,
            'type': 'folder',
            'ogparentid': parent_id}

        # create flat folder structure
        print("Creating new folder Structure")
        source_root, new_folder_list = create_folder_structure(service, folder_owner, parentfolderid, parentname,
                                                               folder_objects, new_folder_list)

        print("Processing the list of files that can't be moved")
        cant_move_file_list = organize_cant_moves(cant_move_objects, new_folder_list, folder_objects, parent_id)

        print("Processing files that will be moved.")
        new_file_list = move_drive_files(file_objects, new_folder_list)

        print("Creating CSVs.")
        create_sheets(service, cant_move_file_list, "{}_unmovable".format(folder_owner), folder_path)
        create_sheets(service, new_file_list, "{}_files_to_move".format(folder_owner), folder_path)
        create_sheets(service, new_folder_list, "{}_folders_to_copy".format(folder_owner), folder_path)

        print("Setting parent attribute of sub folders.")
        set_parent(service, new_folder_list, new_folder_list, source_root, destination_id, type="folder")

        print("Moving Files.")
        set_parent(service, new_folder_list, new_file_list, source_root, destination_id, type="files")

        print("Link to new folder structure:  https://docs.google.com/drive/folders/{fileid}".format(
              fileid=parentfolderid))

        sheet_dest = create_drive_folder(service, folder_owner, "Migration Logs", parentfolderid)
        no_move_sheet = upload_sheet(service, "{}/{}_unmovable.csv".format(LOG_PATH, folder_owner),
                                     "Unmovable Files", sheet_dest, folder_owner)
        file_move_sheet = upload_sheet(service, "{}/{}_files_to_move.csv".format(LOG_PATH, folder_owner),
                                       "Movable Files", sheet_dest, folder_owner)
        folder_move_sheet = upload_sheet(service, "{}/{}_folders_to_copy.csv".format(LOG_PATH, folder_owner),
                                         "Copied Folders", sheet_dest, folder_owner)

        # upload logs to agents MyDrive.
        agent_login = "{}@umich.edu".format(os.getlogin())

        sheet_dest = create_drive_folder(service,
                                         agent_login,
                                         "{} {} Migration Logs".format(timestamp, folder_owner),
                                         'root')

        no_move_sheet = upload_sheet(service, "{}/{}_unmovable.csv".format(LOG_PATH, folder_owner),
                                     "Unmovable Files", sheet_dest, agent_login)
        a,b,c,no_move_perm = find_file_info(service, no_move_sheet)

        file_move_sheet = upload_sheet(service, "{}/{}_files_to_move.csv".format(LOG_PATH, folder_owner),
                                       "Movable Files", sheet_dest, agent_login)

        folder_move_sheet = upload_sheet(service, "{}/{}_folders_to_copy.csv".format(LOG_PATH, folder_owner),
                                         "Copied Folders", sheet_dest, agent_login)

        log_folder_name, log_folder_parent_id, log_folder_parent_name, log_folder_perm_id = find_file_info(service,
                                                                                                           sheet_dest)
        # move folder
        log_folder_perm_id = log_folder_perm_id['permissionId']
        move_ownership(service, sheet_dest, agent_login, log_folder_perm_id, 'root', 'true')

        # move sheets
        move_ownership(service, no_move_sheet, agent_login, log_folder_perm_id, sheet_dest)
        move_ownership(service, file_move_sheet, agent_login, log_folder_perm_id, sheet_dest)
        move_ownership(service, folder_move_sheet, agent_login, log_folder_perm_id, sheet_dest)

        print (no_move_sheet)

    else:
        print("Error, enter valid info during prompts")


if __name__ == '__main__':
    main()
