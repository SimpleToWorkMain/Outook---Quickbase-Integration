import requests
import pandas as pd
import numpy as np
import asyncio
from azure.identity import ClientSecretCredential
from msgraph import GraphServiceClient
import getpass
import dropbox
import datetime
import time
import os
from generate_url_links import dropbox_auth
from dropbox.exceptions import AuthError
from bs4 import BeautifulSoup
from global_modules import print_color, ProgramCredentials, engine_setup, create_folder, run_sql_scripts
from quickbase_class import QuickbaseAPI
from Retrieve_attachments import retrieve_user_attachments

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
    # for user in user_data_list:
    #     print_color(user, color='g')
    return user_data_list


def get_inbox_messages(access_token, user_id, top, skip, start_date, end_date, in_out):
    print_color(f'Recruiting {top} {in_out} Messages for {user_id} Starting at {skip}')
    endpoint = f"https://graph.microsoft.com/v1.0/users/{user_id}/mailFolders/{in_out}/messages"
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    params = {
        "$top": top,    # Number of messages to retrieve
        "$skip": skip,  # Number of messages to skip (for pagination)
        "$filter": f"receivedDateTime ge {start_date} and receivedDateTime le {end_date}"
    }
    response = requests.get(endpoint, headers=headers, params=params)
    messages = []
    try:
        messages = response.json().get("value", [])
    except Exception as e:
        print_color(e, color='y')
        exit

    return messages


def get_message_body(access_token, user_id, message_id):
    endpoint = f"https://graph.microsoft.com/v1.0/users/{user_id}/messages/{message_id}"
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    params = {
        "$select": "body"
    }

    counter = 0
    extract_data = False
    while extract_data is False and counter < 5:
        try:
            response = requests.get(endpoint, headers=headers, params=params)
            result = response.json()
            extract_data = True
        except Exception as e:
            print_color(e, color='r')
            counter +=1
            time.sleep(1)


    body = None
    body_html = None
    if 'body' in result:
        if 'content' in result['body']:
            body_html = result['body']['content']
            # print_color(body_html, color='p')
            soup = BeautifulSoup(body_html, "html.parser")
            body = soup.get_text()

            # soup = BeautifulSoup(body_html, "lxml")
            # body = soup.get_text()
            # print_color(soup.html.unwrap(), color='p')
            # body = body_html


    return body, body_html


def read_inboxes_and_create_df(access_token, user, start_date, end_date, in_out, direction, qb_ids):
    # data = []
    batch_size = 100  # Number of emails to fetch in each batch
    # for user in user_data_list:
    user_id = user.get("id")
    user_principal_name = user.get("user_principal_name")
    start_date = start_date.strftime("%Y-%m-%d")
    end_date = end_date.strftime("%Y-%m-%d")
    skip = 0
    all_messages = {}
    all_message_ids = []


    while True:
        messages = get_inbox_messages(access_token, user_id, batch_size, skip, start_date, end_date, in_out)
        # all_messages.extend(messages)
        # print_color(messages, color='r')
        for message in messages:
            message_id = message.get("id", "")
            all_message_ids.append(message_id)
            all_messages.update({message_id: message})

        if not messages:
            break


            # break

        skip += batch_size

    # print_color(all_messages, color='g')
    missing_emails = compare_email_ids(qb_ids, all_messages)
    print_color(f'Missing Emails: {missing_emails}', color='p')

    # df = pd.DataFrame(all_messages)
    # print_color(df, color='g')
    # office_ids = list(df["Message id"].unique())
    # print_color(office_ids)
    data = []
    for i, missing_message in enumerate(missing_emails):
        message = all_messages.get(missing_message)
        # print_color(message, color='y')
        print_color(f'Recruiting {i+1}/{len(missing_emails)} {in_out}', color='y')
        subject = message.get("subject", "")
        sender = message.get("from", {}).get("emailAddress", {}).get("address", "")
        recipients = message.get("toRecipients", [])
        recipient_addresses = [recipient.get("emailAddress", {}).get("address", "") for recipient in recipients]
        received_time = message.get("receivedDateTime", "")
        cc_recipients = message.get("ccRecipients", [])
        cc_recipients_addresses = [recipient.get("emailAddress", {}).get("address", "") for recipient in cc_recipients]
        message_id = message.get("id", "")
        # bodyPreview = message.get("bodyPreview", "")
        body, body_html = get_message_body(access_token, user_id, message_id)
        conversation_id = message.get('conversationId', "")
        has_attachments = message.get('hasAttachments', "")

        data.append({
            "email": user_principal_name, "User": user_id, "Subject": subject, "Sender": sender,
            "Recipient": recipient_addresses,
            "CC_Recipient": cc_recipients_addresses,
            "ReceivedTime": received_time, "Message id": message_id, "Body": body,
            "Html": body_html,
            "Thread ID": conversation_id, "Has Attachments": has_attachments, "User Id": user_id, "Direction": direction
        })
        # break


    df = pd.DataFrame(data)
    return df


def get_qb_fields(table_id, headers_var, report_id, skip, top):
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


def compare_email_ids(qb_ids, office_ids):
    missing_emails=[]
    for id in office_ids:
        if id in qb_ids:
            print('this id is already in: '+ id)
        else:
            missing_emails.append(id)
            print(id+ ' added to df')
    return missing_emails


def insert_records(table_id, headers_var, df_to_input):
    headers = headers_var
    body = {
        "to": table_id,
        "data": [
            {"30": {"value": df_to_input[5]},
            "7": {"value": df_to_input[3]},
            "8": {"value": df_to_input[4]},
            "9": {"value": df_to_input[7]},
            "16": {"value": df_to_input[6]},
            "31": {"value": df_to_input[8]},
            "17": {"value": df_to_input[2]},
            "33": {"value": df_to_input[9]},
            "35":{"value": df_to_input[10]},
            "6": {"value": df_to_input[11]}
             }]}

    r = requests.post(
        'https://api.quickbase.com/v1/records',
        headers=headers,
        json=body
    )
    data=r.json


async def retrieve_users(x):
    graph_client = get_authenticated_client(tenant_id=x.azure_tenant_id,
                                            client_id=x.azure_client_id,
                                            client_secret=x.azure_client_secret)
    user_data_list = await get_users(graph_client)

    return user_data_list


async def retreive_emails(x, access_token, user_name):


    engine = engine_setup(project_name=x.project_name, hostname=x.hostname, username=x.username, password=x.password, port=x.port)

    user_data_list = await retrieve_users(x)
    user = [user for user in user_data_list if user.get("user_principal_name") == user_name][0]
    print_color(user)

    start_date = datetime.datetime.strptime("2023-01-01","%Y-%m-%d")
    end_date = datetime.datetime.now()

    user_name = user.get("user_principal_name")
    user_date_df, user_date_column_dict = QuickbaseAPI(hostname=x.qb_hostname, auth=x.qb_auth,
         app_id=x.qb_app_id).get_qb_table_records(table_id=x.user_email_table_id,
         col_order=x.email_data.user_email_dates.col_order,
         filter=user_name,
         field_id=x.email_data.user_email_dates.field_id,
         filter_type=x.email_data.user_email_dates.filter_type,
         filter_operator=x.email_data.user_email_dates.filter_operator)
    print_color(user_date_df, color='y')

    record_column = user_date_column_dict.get(x.email_data.user_email_dates.record_id)

    min_date = start_date
    max_date = end_date
    delta = (end_date - start_date).days + 1
    if user_date_df.shape[0] >0:
        recruited_dates = user_date_df['QB_Date'].unique().tolist()
        recruited_dates = [datetime.datetime.strptime(x, '%Y-%m-%d') for x in recruited_dates]
    else:
        recruited_dates = []
    print_color(recruited_dates, color='g')
    updated_recruited_dates = [x for x in recruited_dates if x < end_date - datetime.timedelta(days=2)]
    print_color(updated_recruited_dates, color='b')
    dates_to_recruit = []

    for i in range(delta):
        new_start_date = start_date + datetime.timedelta(days=i)
        if new_start_date not in updated_recruited_dates:
            dates_to_recruit.append(new_start_date)
    print_color(dates_to_recruit, color='g')

    user_column = user_date_column_dict.get(x.email_data.user_email_dates.user)
    date_column = user_date_column_dict.get(x.email_data.user_email_dates.date)
    # if user_date_df.shape[0] >0:
    #     min_date = datetime.datetime.strptime(user_date_df[(user_date_df[user_column] == user_name)][date_column].min(),"%Y-%m-%d") + datetime.timedelta(days=1)
    #     max_date = datetime.datetime.strptime(user_date_df[(user_date_df[user_column] == user_name)][date_column].max(),"%Y-%m-%d")
    # else:
    #     min_date = start_date
    #     max_date = end_date
    #
    # start_date = min_date if user_date_df.shape[0] ==0 else max_date - datetime.timedelta(days=1) if max_date >= (end_date - datetime.timedelta(days=1)) else max_date - datetime.timedelta(days=1)
    # # start_date = max_date - datetime.timedelta(days=1) if max_date.strftime("%Y-%m-%d") == (end_date - datetime.timedelta(days=1)).strftime("%Y-%m-%d") else max(min_date, max_date)
    # end_date = max(max_date, end_date)
    #
    # # start_date = start_date
    # # end_date = end_date
    # print_color(user_name, start_date, end_date, color='y')
    #
    # delta = (end_date - start_date).days + 1
    # print_color(delta, color='r')
    #

    export_folder = f'{x.project_folder}\\Data Folder'
    create_folder(export_folder)
    user_export_folder = f'{export_folder}\\{user_name.replace("@","_").replace(".com", "")}'
    create_folder(user_export_folder)

    # delta = 3
    for i, each_date in enumerate(dates_to_recruit):
        new_start_date = each_date
        new_end_date = each_date + datetime.timedelta(days=1)

        print_color(user_name, new_start_date, new_end_date, color='y')

        email_data_df, email_data_column_dict = QuickbaseAPI(hostname=x.qb_hostname, auth=x.qb_auth,
                                    app_id=x.qb_app_id).get_qb_table_records(table_id=x.email_table_id,
                                    col_order=x.email_data.all_email_fields.col_order,
                                     filter=[new_start_date, new_end_date, user_name],
                                     field_id=x.email_data.all_email_fields.field_id,
                                     filter_type=x.email_data.all_email_fields.filter_type,
                                     filter_operator=x.email_data.all_email_fields.filter_operator)
        email_id_column = email_data_column_dict.get(x.email_data.all_email_fields.email_id)

        # print_color(email_data_df, color='r')
        # print_color(email_data_df.columns, color='r')
        # print_color(email_data_column_dict, color='r')
        if email_data_df.shape[0] > 0:
            qb_ids = list(email_data_df[email_id_column].unique())
        else:
            qb_ids = []
        print_color(f'qb_ids: {qb_ids}', color='y')
        df = read_inboxes_and_create_df(access_token, user, new_start_date, new_end_date, "Inbox", "Incoming", qb_ids)
        df_1 = read_inboxes_and_create_df(access_token, user, new_start_date, new_end_date, "SentItems", "Outgoing", qb_ids)
        # print_color(df, color='b')
        # print_color(df_1, color='b')
        #
        combined_df = pd.concat([df, df_1], ignore_index=True)
        if combined_df.shape[0] > 0:
            combined_df = combined_df.sort_values(by='ReceivedTime')

        data = []
        user_date_data = [{
                    x.email_data.user_email_dates.user: {"value":user_name},
                    x.email_data.user_email_dates.date: {"value":datetime.datetime.strftime(new_start_date,"%Y-%m-%d")}
                    }]
        combined_df.columns = [x.lower() for x in combined_df.columns]

        new_data = []

        print_color(user_date_df, color='r')

        for i in range(combined_df.shape[0]):
            direction = combined_df['direction'].iloc[i]
            source_inbox = combined_df['email'].iloc[i]
            from_source = combined_df['sender'].iloc[i]
            to_source = combined_df['recipient'].iloc[i]
            cc_recipients = combined_df['cc_recipient'].iloc[i]

            email_body = combined_df['body'].iloc[i]
            email_id = combined_df['message id'].iloc[i]
            subject = combined_df['subject'].iloc[i]
            date_time = combined_df['receivedtime'].iloc[i]
            conversation_id = combined_df['thread id'].iloc[i]
            has_attachment = str(combined_df['has attachments'].iloc[i])
            origin_user_id = combined_df['user id'].iloc[i]
            html = combined_df['html'].iloc[i]
            new_data.append([source_inbox, direction, from_source, to_source, cc_recipients, email_id, conversation_id, subject, date_time])

            data.append(
                {
                    x.email_data.all_email_fields.direction: {"value":direction},
                    x.email_data.all_email_fields.from_source: {"value":from_source},
                    x.email_data.all_email_fields.to_source: {"value":to_source},
                    x.email_data.all_email_fields.cc_recipients: {"value": cc_recipients},
                    x.email_data.all_email_fields.email_body: {"value": email_body},
                    x.email_data.all_email_fields.email_id: {"value":email_id},
                    x.email_data.all_email_fields.subject: {"value": subject},
                    x.email_data.all_email_fields.date_time: {"value": date_time},
                    x.email_data.all_email_fields.conversation_id: {"value": conversation_id},
                    x.email_data.all_email_fields.has_attachment: {"value": has_attachment},
                    x.email_data.all_email_fields.origin_user_id: {"value": origin_user_id},
                    x.email_data.all_email_fields.source: {"value": source_inbox},
                    # x.email_data.all_email_fields.html: {"value": html}
                })

        print_color(new_data, color='y')
        new_df = pd.DataFrame(new_data)
        if new_df.shape[0] >0:
            new_df.columns = ['source_inbox', 'direction', 'from_source', 'to_source', 'cc_recipients', 'email_id', 'conversation_id', 'subject', 'date_time']
            new_df.to_csv(f'{user_export_folder}\\{new_start_date.strftime("%Y-%m-%d")}.csv', index=True)

        if len(data) >0:
            QuickbaseAPI(hostname=x.qb_hostname, auth=x.qb_auth,
                         app_id=x.qb_app_id).create_qb_table_records(table_id=x.email_table_id,
                                 user_token=x.qb_user_token, apptoken=None,
                                username=x.qb_username, password=x.qb_password,
                                filter_val=None, update_type='add_record', data=data,
                                reference_column=x.email_data.all_email_fields.record_id,
                                filter_type="EX")

        if new_start_date in recruited_dates:
            print_color(f'{new_start_date} Has Already been recruited before will delete and re-add', color='y')
            print_color(user_date_df, color='g')
            print_color(date_column, color='g')
            current_user_data_df = user_date_df[(user_date_df[date_column] == new_start_date.strftime('%Y-%m-%d'))]
            print_color(current_user_data_df, color='g')
            record_id = current_user_data_df[record_column].iloc[0]
            delete_data = {"from": f"{x.user_email_table_id}",
                          "where": "{" + str(x.email_data.user_email_dates.record_id) + ".EX." + str(record_id) + "}"}

            print_color(delete_data, color='y')
            QuickbaseAPI(hostname=x.qb_hostname, auth=x.qb_auth,
                        app_id=x.qb_app_id).delete_qb_table_records(table_id=x.user_email_table_id, data = delete_data)

        QuickbaseAPI(hostname=x.qb_hostname, auth=x.qb_auth,
                     app_id=x.qb_app_id).create_qb_table_records(table_id=x.user_email_table_id,
                                                                 user_token=x.qb_user_token, apptoken=None,
                                                                 username=x.qb_username, password=x.qb_password,
                                                                 filter_val=None, update_type='add_record',
                                                                 data=user_date_data,
                                                                 reference_column=x.email_data.user_email_dates.record_id,
                                                                 filter_type="EX"
                                                                 )

        # break

    #     print(new_start_date)
    #
    #     if user_date_df.shape[0]>0:
    #         print_color(user_date_df[date_column], color='b')
    #         print_color(user_date_df[(user_date_df[date_column] == new_start_date.strftime('%Y-%m-%d'))], color='y')
    #         user_selected_date_df = user_date_df[(user_date_df[date_column] == new_start_date.strftime('%Y-%m-%d'))]
    #         print_color(user_selected_date_df, color='y')
    #         print_color(user_date_column_dict, color='r')
    #         if user_selected_date_df.shape[0] >0:
    #             print_color(f'Deleting Date from User Email Dates Table', color='y')
    #
    #             for i in range(user_selected_date_df.shape[0]):
    #                 record_id = user_selected_date_df[record_column].iloc[i]
    #                 delete_data = {"from": f"{x.user_email_table_id}",
    #                                "where": "{" + str(x.email_data.user_email_dates.record_id) + ".EX." + str(record_id) + "}"}
    #
    #                 print_color(delete_data, color='y')
    #                 QuickbaseAPI(hostname=x.qb_hostname, auth=x.qb_auth,
    #                              app_id=x.qb_app_id).delete_qb_table_records(table_id=x.user_email_table_id, data = delete_data)
    #
    # #
    #     print_color(user_date_data, color='g  ')



    scripts = []
    scripts.append(f'''delete from  log_user_time where username = "{user_name}" and date = curdate();''')
    run_sql_scripts(engine=engine, scripts=scripts)
    # retrieve_user_attachments(x=x, access_token=access_token, user_name=user_name, user_data_list=user_data_list)


# if __name__ == '__main__':
#     asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
#     asyncio.run(retreive_emails())
#

