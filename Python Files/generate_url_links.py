import os
import time
import sys
import getpass
import pandas as pd
import openpyxl
currentdir = os.path.dirname(os.path.realpath(__file__))
parentdir = os.path.dirname(currentdir)
sys.path.append(parentdir)
import getpass
import base64
import sqlalchemy
from sqlalchemy import create_engine, inspect
from global_modules import print_color, engine_setup, run_sql_scripts, create_folder
from openpyxl import load_workbook
from PIL import Image
import io, uuid
from io import BytesIO
import dropbox
import shutil
import base64
import requests
import json



def dropbox_auth(token_file):
    APP_KEY = 'ef8vvhf6k039pxtpg'
    ACCESS_CODE_GENERATED = 'qiefeQo7daQ0UAAAAAAAABlDnaDxxCW_noZyDbsIydW4JI'
    APP_SECRET = 'gy0d4r1432defeeeffyvn'
    redirect_url = f'https://www.dropbox.com/oauth2/authorize?token_access_type=offline&response_type=code&client_id=8vvhf6k039pxefeftpg'

    print_color(token_file, color='y')

    refresh_token = None
    if os.path.exists(token_file):
        with open(token_file, 'r') as openfile:
            # Reading from json file
            refresh_data = json.load(openfile)

        # refresh_data = json.loads(token_file)
        print(refresh_data)
        if 'refresh_token' in refresh_data.keys():
            refresh_token = refresh_data.get('refresh_token')

    print(refresh_token)

    if refresh_token is not None:
        # refresh_token = refresh_token['refresh_token'].iloc[0]
        data = f'grant_type=refresh_token&refresh_token={refresh_token}'
    else:
        data = f'code={ACCESS_CODE_GENERATED}&grant_type=authorization_code'

    BASIC_AUTH = base64.b64encode(f'{APP_KEY}:{APP_SECRET}'.encode())

    headers = {
        'Authorization': f"Basic {BASIC_AUTH}",
        'Content-Type': 'application/x-www-form-urlencoded',
    }

    response = requests.post('https://api.dropboxapi.com/oauth2/token',
                             data=data,
                             auth=(APP_KEY, APP_SECRET))
    result = json.dumps(json.loads(response.text), indent=2)

    print_color(result, color='r')
    if "refresh_token" in json.loads(result).keys():
        with open(token_file, "w") as outfile:
            outfile.write(result)

    final_result = json.loads(result)
    access_token = final_result.get('access_token')

    return access_token



def copy_jpg_png_images(engine, original_folder, new_folder):

    create_folder(new_folder)
    existing_files = os.listdir(new_folder)
    all_files = os.listdir(original_folder)

    print_color(all_files)

    files_to_move = [x for x in all_files if x not in existing_files and x.split(".")[-1].lower() in ["jpg", "jpeg", "png"]]
    print_color(files_to_move, color='b')

    for each_file in files_to_move:
        src = f'{original_folder}\\{each_file}'
        dst =  f'{new_folder}\\{each_file}'
        shutil.copyfile(src, dst)

    print_color(f'All Image Files Have Been Moved', color='g')

    # map_module_setting(engine=engine, category='images', module='copy_jpg_png_images', sub_module='',
    #                    data_type='move images')


def get_url_links(engine, token_file):
    '''

    Step 1: Move All images to dropbox folder
    Step 2: Connect to dropbox account
    Step 3: get image link
    Step 4: store in database

    :return:
    '''

    token = dropbox_auth(engine, token_file)
    dbx = dropbox.Dropbox(token)

    engine.connect().execute(f'''create table if not exists image_data(
           names varchar(65),
           url varchar(255),
           primary key(names))''')

    list_of_urls = []
    folder_path = '/Images'

    table_name = 'image_data'
    if engine.dialect.has_table(engine, table_name):
        df = pd.read_sql(f'Select * from {table_name}', con=engine)
        existing_images = df['names'].unique().tolist()
    else:
        existing_images = []
    # print(dbx)
    # dbx.
    all_files = []
    result = dbx.files_list_folder(folder_path, recursive=True)
    files = result.entries
    # print(files)

    all_files.extend(files)
    while result.has_more:
        result = dbx.files_list_folder_continue(result.cursor)
        files = result.entries
        # print(files)

        all_files.extend(files)
    #     all_files.extend(files)
    print_color(len(all_files), color='r')
    for i, file in enumerate(all_files):
        file_name = f'{folder_path}/{file.name}'
        if file.name not in existing_images and file.name.split(".")[-1].lower() in ['jpg', 'jpeg', 'png']:


            print(file_name)
            shared_link = dbx.sharing_list_shared_links(file_name, direct_only=True).links
            print(shared_link)
            if len(shared_link) == 0:
                shared_link_metadata = dbx.sharing_create_shared_link_with_settings(file_name)
                url = shared_link_metadata.url
            else:
                url = shared_link[0].url

            url = url.replace("?dl=0","?raw=1")

            # list_of_urls.append([file.name, url])
            engine.connect().execute( f'''insert into {table_name} values ("{file.name}","{url}")''')
            print_color(f'{i} {len(all_files)} {file_name}: {url}', color='g')
