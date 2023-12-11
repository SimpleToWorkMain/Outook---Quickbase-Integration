import requests
import pandas as pd
import asyncio
from azure.identity import ClientSecretCredential
from msgraph import GraphServiceClient
from quickbase_class import QuickbaseAPI
from outlook_class import OutlookEmailAPI
from global_modules import print_color
from google_sheets_api import google_sheet_update

import json
import base64
import os

def get_authenticated_client(tenant_id, client_id, client_secret):
    scopes = [f'https://graph.microsoft.com/.default']

    # Create a DeviceCodeCredential
    credential = ClientSecretCredential(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret= client_secret)

    # Create and return a GraphServiceClient instance
    graph_client = GraphServiceClient(credential, scopes)
    return graph_client


def get_access_token(client_id, client_secret, tenant_id):
    token_url = f'https://login.microsoftonline.com/{tenant_id}/oauth2/token'
    data = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
        'resource': 'https://graph.microsoft.com'
    }
    response = requests.post(token_url, data=data)

    if response.status_code == 200:
        access_token = response.json()['access_token']
        return access_token
    else:
        raise Exception(f"Authentication failed with status code {response.status_code}: {response.text}")


async def get_users(graph_client):
    users = await graph_client.users.get()
    user_data_list = []
    for user in users.value:
        user_data = {
            "user_principal_name": user.user_principal_name,
            "id": user.id
        }
        user_data_list.append(user_data)
    print("This is a list of all users in this domain:")
    for user_data in user_data_list:
        print(user_data)
    return user_data_list


def get_QB_fields(table_id, headers_var, report_id, skip, top):
    headers = headers_var
    params = {
        'tableId': table_id,
        'skip': {skip},
        'top': {top}
    }
    r = requests.post(
        f'https://api.quickbase.com/v1/reports/{report_id}/run',
        params=params,
        headers=headers
    )
    data = r.json()
    return data


def download_attachments(headers_var, table_id, record_id, field_id, version_number):
    headers = headers_var
    response = requests.get(
        f'https://api.quickbase.com/v1/files/{table_id}/{record_id}/{field_id}/{version_number}',
        headers=headers
    )
    if (response.status_code==200):
        return response.content
    else:
        pass


def sort_data_to_send(result):
    send_to = [item['10']['value'] for item in result['data']]
    send_from=[item['9']['value'] for item in result['data']]
    body=[item['7']['value'] for item in result['data']]
    subject=[item['21']['value'] for item in result['data']]
    conversation_id=[item['19']['value'] for item in result['data']]
    record_id = [item['3']['value'] for item in result['data']]
    reply_all= [item['22']['value'] for item in result['data']]
    message_id= [item['23']['value'] for item in result['data']]
    file_1_name = [
        item['14']['value']['versions'][0]['fileName'] if '14' in item and 'value' in item['14'] and 'versions' in
                                                          item['14']['value'] and item['14']['value']['versions'] and
                                                          item['14']['value']['versions'][0] and 'fileName' in
                                                          item['14']['value']['versions'][0] else '' for item in
        result['data']]
    file_2_name = [
        item['15']['value']['versions'][0]['fileName'] if '15' in item and 'value' in item['15'] and 'versions' in
                                                          item['15']['value'] and item['15']['value']['versions'] and
                                                          item['15']['value']['versions'][0] and 'fileName' in
                                                          item['15']['value']['versions'][0] else '' for item in
        result['data']]
    file_3_name = [
        item['16']['value']['versions'][0]['fileName'] if '16' in item and 'value' in item['16'] and 'versions' in
                                                          item['16']['value'] and item['16']['value']['versions'] and
                                                          item['16']['value']['versions'][0] and 'fileName' in
                                                          item['16']['value']['versions'][0] else '' for item in
        result['data']]
    file_4_name = [
        item['17']['value']['versions'][0]['fileName'] if '17' in item and 'value' in item['17'] and 'versions' in
                                                          item['17']['value'] and item['17']['value']['versions'] and
                                                          item['17']['value']['versions'][0] and 'fileName' in
                                                          item['17']['value']['versions'][0] else '' for item in
        result['data']]
    file_5_name = [
        item['18']['value']['versions'][0]['fileName'] if '18' in item and 'value' in item['18'] and 'versions' in
                                                          item['18']['value'] and item['18']['value']['versions'] and
                                                          item['18']['value']['versions'][0] and 'fileName' in
                                                          item['18']['value']['versions'][0] else '' for item in
        result['data']]
    data = {
        "Send To": send_to,
        "Send From": send_from,
        "Body": body,
        "Subject": subject,
        "Conversation Id": conversation_id,
        "Message Id": message_id,
        "Record Id": record_id,
        "Reply all": reply_all,
        "File 1": file_1_name,
        "File 2": file_2_name,
        "File 3": file_3_name,
        "File 4": file_4_name,
        "File 5": file_5_name
    }

    df = pd.DataFrame(data)
    print(df.to_string())
    return df


def send_new_mail(access_token, user_id, send_to, subject, body, pdfs=[], df=None):
    endpoint = f"https://graph.microsoft.com/v1.0/users/{user_id}/sendMail"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}"
    }
    payload = {
        "message": {
            "subject": subject,
            "body": {
                "contentType": "Text",
                "content": body
            },
            "toRecipients": [
                {
                    "emailAddress": {
                        "address": send_to
                    }
                }
            ],
            "attachments": []  # Initialize an empty list for attachments
        }
    }

    for i, pdf in enumerate(pdfs):
        attachment = {
                "@odata.type": "#microsoft.graph.fileAttachment",
                "name": df[f'FILE{i+1}'],  # Replace with the actual file name
                "contentBytes": pdf
            }
        payload["message"]["attachments"].append(attachment)

    # if bytes:
    #     attachment = {
    #         "@odata.type": "#microsoft.graph.fileAttachment",
    #         "name": df['File 1'],  # Replace with the actual file name
    #         "contentBytes": bytes
    #     }
    #     payload["message"]["attachments"].append(attachment)
    #
    # if bytes_2:
    #     attachment = {
    #         "@odata.type": "#microsoft.graph.fileAttachment",
    #         "name": df['File 2'],  # Replace with the actual file name
    #         "contentBytes": bytes_2
    #     }
    #     payload["message"]["attachments"].append(attachment)
    #
    # if bytes_3:
    #     attachment = {
    #         "@odata.type": "#microsoft.graph.fileAttachment",
    #         "name": df['File 3'],  # Replace with the actual file name
    #         "contentBytes": bytes_3
    #     }
    #     payload["message"]["attachments"].append(attachment)
    #
    # if bytes_4:
    #     attachment = {
    #         "@odata.type": "#microsoft.graph.fileAttachment",
    #         "name": df['File 4'],  # Replace with the actual file name
    #         "contentBytes": bytes_4
    #     }
    #     payload["message"]["attachments"].append(attachment)
    #
    # if bytes_5:
    #     attachment = {
    #         "@odata.type": "#microsoft.graph.fileAttachment",
    #         "name": df['File 5'],  # Replace with the actual file name
    #         "contentBytes": bytes_5
    #     }
    #     payload["message"]["attachments"].append(attachment)

    print_color(payload, color='r')
    response = requests.post(endpoint, json=payload, headers=headers)

    if response.status_code == 202:
        print("Email sent successfully to:", send_to)
    else:
        print("Error sending email.")
        print(response.text)
    return response.status_code


def send_reply_mail(access_token, user_id, send_to, subject, body, message_id,  bytes, bytes_2, bytes_3, bytes_4, bytes_5, df):
    endpoint = f"https://graph.microsoft.com/v1.0/users/{user_id}/messages/{message_id}/reply"
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    payload = {
  "message": {
    "subject": subject,
    "body": {
      "contentType": "Text",
      "content": body
    },
    "toRecipients": [
      {
        "emailAddress": {
          "address": send_to,
        }
      }
    ],
      "attachments": []
  }
}
    if bytes:
        attachment = {
            "@odata.type": "#microsoft.graph.fileAttachment",
            "name": df['File 1'],  # Replace with the actual file name
            "contentBytes": bytes
        }
        payload["message"]["attachments"].append(attachment)

    if bytes_2:
        attachment = {
            "@odata.type": "#microsoft.graph.fileAttachment",
            "name": df['File 2'],  # Replace with the actual file name
            "contentBytes": bytes_2
        }
        payload["message"]["attachments"].append(attachment)

    if bytes_3:
        attachment = {
            "@odata.type": "#microsoft.graph.fileAttachment",
            "name": df['File 3'],  # Replace with the actual file name
            "contentBytes": bytes_3
        }
        payload["message"]["attachments"].append(attachment)

    if bytes_4:
        attachment = {
            "@odata.type": "#microsoft.graph.fileAttachment",
            "name": df['File 4'],  # Replace with the actual file name
            "contentBytes": bytes_4
        }
        payload["message"]["attachments"].append(attachment)

    if bytes_5:
        attachment = {
            "@odata.type": "#microsoft.graph.fileAttachment",
            "name": df['File 5'],  # Replace with the actual file name
            "contentBytes": bytes_5
        }
        payload["message"]["attachments"].append(attachment)
    response = requests.post(endpoint, json=payload, headers=headers)

    if response.status_code == 202:
        print("Email sent successfully to: "+send_to)
    else:
        print("Error sending email.")
        print(response.text)
    return response.status_code


def send_reply_all_mail(access_token, user_id, subject, body, message_id,  bytes, bytes_2, bytes_3, bytes_4, bytes_5, df):
    endpoint = f"https://graph.microsoft.com/v1.0/users/{user_id}/messages/{message_id}/replyAll"
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    payload = {
        "message": {
            "subject": subject,
            "body": {
                "contentType": "Text",
                "content": body
            },
            "attachments": []
        },
        "saveToSentItems": True
    }
    if bytes:
        attachment = {
            "@odata.type": "#microsoft.graph.fileAttachment",
            "name": df['File 1'],  # Replace with the actual file name
            "contentBytes": bytes
        }
        payload["message"]["attachments"].append(attachment)

    if bytes_2:
        attachment = {
            "@odata.type": "#microsoft.graph.fileAttachment",
            "name": df['File 2'],  # Replace with the actual file name
            "contentBytes": bytes_2
        }
        payload["message"]["attachments"].append(attachment)

    if bytes_3:
        attachment = {
            "@odata.type": "#microsoft.graph.fileAttachment",
            "name": df['File 3'],  # Replace with the actual file name
            "contentBytes": bytes_3
        }
        payload["message"]["attachments"].append(attachment)

    if bytes_4:
        attachment = {
            "@odata.type": "#microsoft.graph.fileAttachment",
            "name": df['File 4'],  # Replace with the actual file name
            "contentBytes": bytes_4
        }
        payload["message"]["attachments"].append(attachment)

    if bytes_5:
        attachment = {
            "@odata.type": "#microsoft.graph.fileAttachment",
            "name": df['File 5'],  # Replace with the actual file name
            "contentBytes": bytes_5
        }
        payload["message"]["attachments"].append(attachment)
    response = requests.post(endpoint, json=payload, headers=headers)

    if response.status_code == 202:
        print("Email reply sent successfully!")
    else:
        print("Error sending email reply.")
        print(response.text)
    return response.status_code


def update_records(table_id, headers_var, record_id, fid_to_change, result):
    headers=headers_var
    body={"to": table_id,
        "data": [
            {3: {"value": record_id},
                fid_to_change: {"value": result}}],
        "fieldsToReturn": [
            3,fid_to_change]}
    r = requests.post(
        'https://api.quickbase.com/v1/records',
        headers=headers,
        json=body
    )
    print(r.status_code)
    return r.status_code

# def create_upload_session(access_token, user_id, attachment_name, attachment_size):
#     endpoint = f"https://graph.microsoft.com/v1.0/users/{user_id}/messages/AAMkAGUwNjQ4ZjIxLTQ3Y2YtNDViMi1iZjc4LTMA=/attachments/createUploadSession"
#     headers = {
#         "Authorization": f"Bearer {access_token}",
#         "Content-Type": "application/json"
#     }
#     payload = {
#         "AttachmentItem": {
#             "attachmentType": "file",
#             "name": attachment_name,
#             "size": attachment_size
#         }
#     }
#
#     response = requests.post(endpoint, json=payload, headers=headers)
#     response_data = response.json()
#     print(response_data)
#     if response.status_code == 200:
#         upload_url = response_data["uploadUrl"]
#         return upload_url
#     else:
#         print("Error creating upload session:", response_data)
#         return None

async def send_emails(x, environment, user, all_users):
    access_token = get_access_token(client_id=x.azure_client_id,
                                    client_secret=x.azure_client_secret,
                                    tenant_id=x.azure_tenant_id)

    QuickBase_API = QuickbaseAPI(hostname=x.qb_hostname, auth=x.qb_auth, app_id=x.qb_app_id)

    df = QuickBase_API.get_report_records(
        table_id=x.outbox_table_id, report_id=6)
    print_color(df.columns, color='r')

    user_dict = {}
    for user in all_users:
        user_dict.update({user.get("user_principal_name").lower(): user.get("id")})

    print_color(all_users, color='y')
    print_color(df.columns, color='b')

    for i in range(df.shape[0]):
        sender = df['Sender'].iloc[i].lower()
        if environment == 'development':
            sender = 'ricky@simpletoworkllc.onmicrosoft.com'

        recipient = df['Recipient'].iloc[i].split(",")
        record_id = df['Record_ID_Num'].iloc[i]
        user_id = user_dict.get(sender)

        print_color(recipient, user_dict, user_id, color='b')
        conversation_id = df['conversation_id'].iloc[i]
        message_id = df['email_id'].iloc[i]
        subject = df['Subject'].iloc[i]
        body = df['Email_Template_Body'].iloc[i]
        reply_all = df['reply_all?'].iloc[i]
        reply_all = False if str(reply_all) == 'False' else True if str(reply_all) == 'True' else reply_all

        file_name_1 = df['FILE1'].iloc[i]
        file_name_2 = df['FILE2'].iloc[i]
        file_name_3 = df['FILE3'].iloc[i]
        file_name_4 = df['FILE4'].iloc[i]
        file_name_5 = df['FILE5'].iloc[i]
        print_color(file_name_1, color='y')
        file_name_detail_1 = None
        file_name_detail_2 = None
        file_name_detail_3 = None
        file_name_detail_4 = None
        file_name_detail_5 = None
        if len(file_name_1.get('versions')) >0:
            file_name_detail_1 = file_name_1.get('versions')[0].get('fileName')
        if len(file_name_2.get('versions')) > 0:
            file_name_detail_2 = file_name_2.get('versions')[0].get('fileName')
        if len(file_name_3.get('versions')) > 0:
            file_name_detail_3 = file_name_3.get('versions')[0].get('fileName')
        if len(file_name_4.get('versions')) > 0:
            file_name_detail_4 = file_name_4.get('versions')[0].get('fileName')
        if len(file_name_5.get('versions')) > 0:
            file_name_detail_5 = file_name_5.get('versions')[0].get('fileName')

        attachment_1 = QuickBase_API.download_files(table_id=x.outbox_table_id, record_id=record_id, field_id='14', version_number='1')
        attachment_2 = QuickBase_API.download_files(table_id=x.outbox_table_id, record_id=record_id, field_id='15', version_number='1')
        attachment_3 = QuickBase_API.download_files(table_id=x.outbox_table_id, record_id=record_id, field_id='16', version_number='1')
        attachment_4 = QuickBase_API.download_files(table_id=x.outbox_table_id, record_id=record_id, field_id='17', version_number='1')
        attachment_5 = QuickBase_API.download_files(table_id=x.outbox_table_id, record_id=record_id, field_id='18', version_number='1')

        attachment_dict = {}
        if file_name_detail_1 is not None:
            attachment_dict.update({file_name_detail_1: attachment_1})
        if file_name_detail_2 is not None:
            attachment_dict.update({file_name_detail_2: attachment_2})
        if file_name_detail_3 is not None:
            attachment_dict.update({file_name_detail_3: attachment_3})
        if file_name_detail_4 is not None:
            attachment_dict.update({file_name_detail_4: attachment_4})
        if file_name_detail_5 is not None:
            attachment_dict.update({file_name_detail_5: attachment_5})

        print_color(attachment_dict, color='b')
        print_color(reply_all, conversation_id, message_id, color='p')
        print_color(type(reply_all), reply_all is False, conversation_id, message_id, color='r')

        print_color(user_dict, color='g')



        print_color(record_id, sender, user_id,  recipient, subject, body, color='y')

        if reply_all is False:
            if conversation_id == '' and message_id == '':
                print_color("METHOD: send_new_mail", color='g')
            else:
                print_color("METHOD: send_reply_mail", color='g')
        else:
            print_color("METHOD: send_reply_all_mail", color='g')

        if reply_all is False:
            if conversation_id == '' and message_id == '':
                await OutlookEmailAPI(client_id=x.azure_client_id, client_secret=x.azure_client_secret,
                    tenant_id=x.azure_tenant_id).send_new_mail(user_id=user_id, subject=subject, body=body,
                                                               send_to=recipient, attachments=attachment_dict)
            else:
                print_color("METHOD: send_reply_mail", color='g')
                await OutlookEmailAPI(client_id=x.azure_client_id, client_secret=x.azure_client_secret,
                                      tenant_id=x.azure_tenant_id).send_reply_mail(user_id=user_id,
                                      message_id=message_id, subject=subject, body=body, send_to=recipient,
                                      attachments=attachment_dict)
        else:
            print_color("METHOD: send_reply_all_mail", color='g')
            await OutlookEmailAPI(client_id=x.azure_client_id, client_secret=x.azure_client_secret,
                                  tenant_id=x.azure_tenant_id).send_reply_all_mail(user_id=user_id,
                                   message_id=message_id, subject=subject,
                                   body=body, send_to=recipient,
                                   attachments=attachment_dict)


        data = [{
            "3": {"value": str(record_id)},
            "20": {"value": "True"}
        }]

        QuickBase_API.update_qb_table_records(table_id=x.outbox_table_id, data=data)
