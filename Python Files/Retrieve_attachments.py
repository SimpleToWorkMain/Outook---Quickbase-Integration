import requests
import pandas as pd
import asyncio
from azure.identity import ClientSecretCredential
from msgraph import GraphServiceClient
import getpass
import dropbox
from generate_url_links import dropbox_auth
from dropbox.exceptions import AuthError
from quickbase_class import QuickbaseAPI
from global_modules import print_color


def get_attachment_id(access_token, user_id, message_id):
    endpoint = f"https://graph.microsoft.com/v1.0/users/{user_id}/mailFolders/Inbox/messages/{message_id}/attachments"
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    params = {
    }
    response = requests.get(endpoint, headers=headers, params=params)
    attachment_data = response.json().get("value", [])
    print_color(type(attachment_data), color='g')

    return attachment_data


def get_attachment_link(access_token, user_id, message_id, attachment_id):
    endpoint = f"https://graph.microsoft.com//v1.0/users//{user_id}//mailFolders//Inbox//messages//{message_id}//attachments//{attachment_id}//$value"
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    params = {
    }
    response = requests.get(endpoint, headers=headers, params=params)
    return(response.content)


def sort_data_to_send(result):
    message_id = [item['16']['value'] for item in result['data']]
    user_id = [item['35']['value'] for item in result['data']]
    data = {
        "Message Id": message_id,
        "User Id": user_id}
    df = pd.DataFrame(data)
    print(df.to_string())
    return df


def upload_to_dropbox(dbx, email_id, file_name, file_content):
    # try:


    response = dbx.files_upload(file_content,f'/{email_id}/{file_name}')

    print("File uploaded to Dropbox:", response.name)

    # Get the file URL
    file_url = dbx.sharing_create_shared_link(f'/{email_id}/{file_name}')
    file_url_share = file_url.url.replace('?dl=0', '?dl=1')
    # print("File URL:", file_url_share)
    return file_url_share
    # except AuthError as e:
    #     print("Authentication error:", e)
    # except Exception as e:
    #     print("Error uploading file to Dropbox:", e)
    #     return file_url


def retrieve_user_attachments(x, access_token, user_name, user_data_list):
    print_color(f'Attempting to Retrieve Attachments', color='y')
    # user_data_list = await retrieve_users(x)
    user = [user for user in user_data_list if user.get("user_principal_name") == user_name][0]
    print_color(user)

    user_name = user.get('user_principal_name')
    user_attachments_df, user_attachments_column_dict = QuickbaseAPI(hostname=x.qb_hostname, auth=x.qb_auth,
                                                       app_id=x.qb_app_id).get_qb_table_records(
        table_id=x.email_table_id,
        col_order=x.email_data.attachments_data.col_order,
        filter=[user_name, True, 0, ""],
        field_id=x.email_data.attachments_data.field_id,
        filter_type=x.email_data.attachments_data.filter_type,
        filter_operator=x.email_data.attachments_data.filter_operator)

    print_color(user_attachments_df, color='g')

    # data = data[data['Source_Email'] ==]
    #     # print_color(data, color='y')

    user_id_field = x.email_data.attachments_data.origin_user_id
    message_id_field = x.email_data.attachments_data.email_id
    record_id_field = x.email_data.attachments_data.record_id

    user_id_column = user_attachments_column_dict.get(user_id_field)
    message_id_column = user_attachments_column_dict.get(message_id_field)
    record_id_column = user_attachments_column_dict.get(record_id_field)

    if user_attachments_df.shape[0] >0:
        access_token_db = dropbox_auth(x.db_token_file)

        for i in range(user_attachments_df.shape[0]):
            user_id = user_attachments_df[user_id_column].iloc[i]
            message_id = user_attachments_df[message_id_column].iloc[i]
            related_email_id = str(user_attachments_df[record_id_column].iloc[i])
            print_color(f'{user_id} {message_id}', color='b')
            attachment_data = get_attachment_id(access_token=access_token, user_id=user_id, message_id=message_id)

            dbx = dropbox.Dropbox(access_token_db)

            # Upload the file
            try:
                dbx.files_create_folder(f'/{related_email_id}')
            except Exception as e:
                print_color(e, color='r')
                if "WriteConflictError" in str(e):
                    print_color('Folder Already Exists', color='y')


            print_color(len(attachment_data), color='g')
            data = []
            for each_attachment in attachment_data:

                # Extract the "id" from the first attachment (if any)
                attachment_id = each_attachment["id"]
                file_name = each_attachment["name"]

                print_color(file_name, color='r')
                file_content = get_attachment_link(access_token, user_id, message_id, attachment_id)
                file_url = upload_to_dropbox(dbx, related_email_id, file_name, file_content)
                file_url = file_url.replace("dl=0", "raw=1")
                print_color(file_url, color='b')

                body = {
                        x.email_data.attachments_upload.related_email_id: {"value": related_email_id},
                        x.email_data.attachments_upload.message_id: {"value": message_id},
                        x.email_data.attachments_upload.file_url: {"value": file_url},
                      x.email_data.attachments_upload.file_name: {"value": file_name}
                    }
                data.append(body)

            # print_color(data, color='y')

            QuickbaseAPI(hostname=x.qb_hostname, auth=x.qb_auth,
                 app_id=x.qb_app_id).create_qb_table_records(table_id=x.attachments_table_id,
                 user_token=x.qb_user_token, apptoken=None,
                 username=x.qb_username, password=x.qb_password,
                 filter_val=None, update_type='add_record', data=data,
                 reference_column=x.email_data.attachments_data.record_id,
                 filter_type="EX")


        # break

    else:
        print_color(f'No Attachments to Retrieve', color='r')