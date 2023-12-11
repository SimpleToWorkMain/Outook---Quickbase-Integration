from global_modules import print_color
import requests
import datetime
import json
import pandas as pd
import base64
import os
import sys


class QuickbaseAPI():
    def __init__(self, hostname, auth, app_id):
        self.headers = {
            'QB-Realm-Hostname': hostname,
            'User-Agent': 'Windows NT 10.0; Win64; x64',
            'Authorization': f'QB-USER-TOKEN {auth}'
        }

        self.hostname = hostname
        self.auth = auth
        self.app_id = app_id

    def get_variable_value(self,apptoken=None, username=None, password=None, table_id=None, variable_value=None):
        r = requests.get(f'{self.hostname}/db/main?a=API_Authenticate&username={username}&password={password}&hours=24')
        ticket = str(r.content).split('</ticket>')[0].split('<ticket>')[-1]
        # print_color(ticket, color='y')

        url = f'{self.hostname}/db/{table_id}?a=API_GetDBVar&ticket={ticket}&apptoken={apptoken}&varname={variable_value}'
        # print(url)
        r = requests.get(url)
        value = str(r.content).split('</value>')[0].split('<value>')[-1]
        print_color(f'Variable Value: {variable_value}:{value}', color='p')

        return value

    def get_qb_table_records(self,  table_id=None,  col_order='*', filter=None, field_id=3, filter_type='CT', filter_operator=None):

        print_color(f'Recruiting Data For Table_id {table_id}', color='r')
        params = {
            'tableId': table_id,
        }
        r = requests.get(
            'https://api.quickbase.com/v1/fields',
            params=params,
            headers=self.headers
        )

        fields_data = r.json()
        # print_color(fields_data, color='y')
        # print(type(fields_data))
        # print(fields_data)

        field_dict = {}

        for each_item in fields_data:
            # print_color(each_item, color='g')
            # print_color(each_item.keys(), color='r')
            id = each_item.get('id')
            fieldType = each_item.get('fieldType')
            label = each_item.get('label')
            field_dict.update({id: [label, fieldType]})

        print_color(type(filter), color='r')


        if filter is None:
            body = {
                "from": table_id,
                "select": col_order}
        elif type(filter) == str:
            # if filter_type == "CT":
            where_setting = '{' + str(field_id) + f".{filter_type}.'" + str(filter) + "'}" + ''
            print_color(f'Fiter Setting: {where_setting}', color='b')
            body = {
                "from": table_id,
                "select": col_order,
                "where": where_setting
            }

        elif type(filter) == bool:
            where_setting = '{' + str(field_id) + f".{filter_type}.'" + str(filter) + "'}" + ''
            body = {
                "from": table_id,
                "select": col_order,
                "where": where_setting
            }
        elif type(filter) == list:
            print_color(filter, color='g')
            where_setting = ''
            for i, j in enumerate(filter):
                if j == "True":
                    val = True
                elif j == "False":
                    val = False
                else:
                    val = j



                if i == 0:
                    where_setting = '{' + str(field_id[i]) + f".{filter_type[i]}.'" + str(val) + "'}" + ''
                else:
                    where_setting += f'\n{filter_operator[i-1]} ' + '{' + str(field_id[i]) + f".{filter_type[i]}.'" + str(val) + "'}"

            print_color(f'Fiter Setting: {where_setting}', color='b')
            body = {
                "from": table_id,
                "select": col_order,
                "where": where_setting
            }

        print_color(f'Request Settings: {body}', color='g')
        r = requests.post(
            'https://api.quickbase.com/v1/records/query',
            headers=self.headers,
            json=body
        )

        # print_color(r.json(), color='r')

        data = eval(json.dumps(r.json()).replace("null", "None").replace("false", "False").replace("true", "True")).get(
            "data")
        print(data)
        df = pd.DataFrame(data)
        for each_col in df.columns:
            df[each_col] = df[each_col].apply(lambda x: x.get('value'))


        col_order_str = [str(x) for x in col_order]
        if df.shape[0] > 0:
            df = df[col_order_str]
            df_columns = list(df.columns)
            df_columns = [int(x) for x in df_columns]
            df.columns = [field_dict.get(x)[0].replace(" ", "_").replace("#", "_Num") for x in df_columns]

        all_columns = [x.lower() for x in df.columns]
        for i, col in enumerate(df.columns):
            print_color(f'{i}: Column ID {col_order[i]} ---- {col}', color='g')
            if col.lower() == 'date':
                df.rename(columns={col: 'QB_Date'}, inplace=True)

        column_dict = {}
        data_columns = df.columns

        for j, value in enumerate(fields_data):
            label =  value['label']
            label = label.replace(" ","_").replace("#", "_Num")
            if label.lower() == 'date':
                label = "QB_Date"

            # if label == 'Record ID#':
            #     label = 'Record_ID_Num'
            column_dict.update({value['id']:label})


        print(type(df))
        if df.shape[0]>0:
            df['Date'] = datetime.datetime.now().strftime('%Y-%m-%d')

        return df, column_dict

    def get_report_records(self, table_id=None, report_id=None):
        params = {
            'tableId': table_id,
        }
        report_path = f'https://api.quickbase.com/v1/reports/{report_id}/run'
        print(report_path)
        r = requests.post(report_path,
            params=params,
            headers=self.headers
        )

        # fields_data = r.json()
        # print_color(r.json(), color='r')
        response = eval(json.dumps(r.json()).replace("null", "None").replace("false", "False").replace("true", "True"))
        data = response.get("data")
        fields_data = response.get("fields")
        field_dict = {}

        for each_item in fields_data:
            # print_color(each_item, color='g')
            # print_color(each_item.keys(), color='r')
            id = each_item.get('id')
            fieldType = each_item.get('fieldType')
            label = each_item.get('label')
            field_dict.update({id: [label, fieldType]})

        # # print(data)
        df = pd.DataFrame(data)

        for each_col in df.columns:
            df[each_col] = df[each_col].apply(lambda x: x.get('value'))

        df_columns = list(df.columns)
        df_columns = [int(x) for x in df_columns]
        df.columns = [field_dict.get(x)[0].replace(" ", "_").replace("#", "_Num") for x in df_columns]
        # print_color(df, color='y')

        return df

    def update_qb_table_records(self,  table_id=None, data = [{}]):
        body = {"to": table_id, "data": data,
                "fieldsToReturn": [3]}

        print(body)

        r = requests.post(
            'https://api.quickbase.com/v1/records',
            headers=self.headers,
            json=body
        )

        print(json.dumps(r.json()))

        print_color(f'Record Updated', color='b')

    def delete_qb_table_records(self,  table_id=None, data = [{}]):
        body = data

        print(body)

        r = requests.delete(
            'https://api.quickbase.com/v1/records',
            headers=self.headers,
            json=body
        )

        print(json.dumps(r.json()))

        print_color(f'Records Deleted', color='b')

    def purge_table_records(self,table_id=None, user_token=None, apptoken=None,
                                username=None, password=None,
                                filter_val=None,
                                reference_column=None,
                            filter_type = "EX"
                            ):
        headers = self.headers

        r = requests.get(
            f'{self.hostname}/db/main?a=API_Authenticate&username={username}&password={password}&hours=24')
        ticket = str(r.content).split('</ticket>')[0].split('<ticket>')[-1]

        print(self.hostname,
              table_id,
              reference_column,
              filter_val,
              ticket,
              user_token,
              apptoken)

        purge_records_url = f"{self.hostname}/db/" + table_id + "?a=API_PurgeRecords&query={" + str(reference_column) + "."+ filter_type +".'" + str( filter_val) + "'}&ticket=" + ticket + "&user_token=" + user_token + "&apptoken=" + apptoken

        print_color(purge_records_url, color='g')
        r = requests.get(purge_records_url)
        print_color(r.content)

        print_color(f'Records Purged', color='b')

    def create_qb_table_records(self,  table_id=None, user_token=None, apptoken=None,
                                username=None, password=None,
                                filter_val=None, update_type=None, data=None,
                                reference_column=None,
                                filter_type="EX"
                                ):
        headers = self.headers

        r = requests.get(
            f'{self.hostname}/db/main?a=API_Authenticate&username={username}&password={password}&hours=24')
        ticket = str(r.content).split('</ticket>')[0].split('<ticket>')[-1]
        print_color(ticket, color='y')

        body = {"to": f'{table_id}', "data": data, "fieldsToReturn": ['3']}
        print_color(body, color='g')
        if update_type == 'purge_and_reset':
            print(self.hostname,
                  table_id,
                  reference_column,
                  filter_val,
                  ticket,
                  user_token,
                  apptoken)

            purge_records_url = f"{self.hostname}/db/" + table_id + "?a=API_PurgeRecords&query={" + str(reference_column) + "."+ filter_type +".'" + str(filter_val) + "'}&ticket=" + ticket + "&user_token=" + user_token + "&apptoken=" + apptoken

            print_color(purge_records_url, color='g')
            r = requests.get(purge_records_url)
            print_color(r.content)

            print_color(f'Records Purged', color='b')


            r = requests.post(
                'https://api.quickbase.com/v1/records',
                headers=headers,
                json=body
            )
            print_color(r.content, color='g')
            print(json.dumps(r.json()))

            print_color(f'Record For Proposal {filter_val} Updated', color='b')

        elif update_type == 'add_record':
            r = requests.post(
                'https://api.quickbase.com/v1/records',
                headers=headers,
                json=body
            )

            print_color(r.content, color='g')
            print(json.dumps(r.json()))

            print_color(f'Record For Proposal {filter_val} Updated', color='b')


    def download_files(self, table_id=None, record_id=None, field_id=None, version_number=None):
        response = requests.get(
            f'https://api.quickbase.com/v1/files/{table_id}/{record_id}/{field_id}/{version_number}',
            headers=self.headers
        )
        if (response.status_code == 200):
            return response.content
        else:
            pass

    def download_qb_file(self, table_id=None, record_id=None,
                         file_name = None, file_folder=None,
                         field_id=None, version_number=None):
        download_file = requests.get(
            f'https://api.quickbase.com/v1/files/{table_id}/{record_id}/{field_id}/{version_number}',
            headers=self.headers)
        contents = download_file.content
        try:
            # Decode the Base64 string to bytes
            decoded_data = base64.b64decode(contents)
            file_path = os.path.join(file_folder, file_name)
            # Write the decoded data to the file
            with open(file_path, "wb") as file:
                file.write(decoded_data)
                print("File " + str(record_id) + " saved successfully.")
        except Exception as e:
            print(f"Failed to save the file: {e}")

        return file_path

    def delete_qb_file(self, table_id=None, record_id=None,
                             field_id=None, version_number=None):
        response = requests.delete(
                f'https://api.quickbase.com/v1/files/{table_id}/{record_id}/{field_id}/{version_number}',
            headers=self.headers
        )

        print(json.dumps(response.json(), indent=4))

