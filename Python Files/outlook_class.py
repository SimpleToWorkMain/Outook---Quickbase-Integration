import requests
import pandas as pd
import asyncio
from azure.identity import ClientSecretCredential
from msgraph import GraphServiceClient
from msgraph.generated.models.file_attachment import FileAttachment
import base64
from global_modules import print_color


class OutlookEmailAPI():
    def __init__(self, client_id, client_secret, tenant_id):
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
        self.access_token = self.get_access_token()
        self.graph_client  = self.get_authenticated_client()
        self.headers = self.set_headers()

    def get_authenticated_client(self):
        scopes = [f'https://graph.microsoft.com/.default']

        # Create a DeviceCodeCredential
        credential = ClientSecretCredential(
            tenant_id=self.tenant_id,
            client_id=self.client_id,
            client_secret=self.client_secret)

        # Create and return a GraphServiceClient instance
        graph_client = GraphServiceClient(credential, scopes)
        return graph_client

    def get_access_token(self):
        token_url = f'https://login.microsoftonline.com/{self.tenant_id}/oauth2/token'
        data = {
            'grant_type': 'client_credentials',
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'resource': 'https://graph.microsoft.com'
        }
        response = requests.post(token_url, data=data)

        if response.status_code == 200:
            access_token = response.json()['access_token']
            return access_token
        else:
            raise Exception(f"Authentication failed with status code {response.status_code}: {response.text}")

    def set_headers(self):
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.access_token}"
        }
        return headers

    # def create_upload_attachements(self):
    #     request_body = CreateUploadSessionPostRequestBody(
    #         attachment_item=AttachmentItem(
    #             attachment_type=AttachmentType.File,
    #             name="flower",
    #             size=3483322,
    #         ),
    #     )

    async def upload_small_attachment(self, user_id, file_name, byte_data):


        attachment = {
                "@odata.type": "#microsoft.graph.fileAttachment",
                "name": file_name,  # Replace with the actual file name
                "contentBytes": byte_data.decode("utf-8")
            }

        # attachment = FileAttachment(
        #     odata_type="#microsoft.graph.fileAttachment",
        #     name=file_name,
        #     # content_type="text/plain",
        #     content_bytes=base64.urlsafe_b64decode(byte_data.decode("utf-8")),
        # )

        return attachment

        # request_body = FileAttachment(
        #     odata_type="#microsoft.graph.fileAttachment",
        #     name=file_name,
        #     content_bytes=base64.urlsafe_b64decode(byte_data),
        # )
        #
        # endpoint = f"https://graph.microsoft.com/v1.0/users/{user_id}/events/by_event_id/'event-id'/attachments"
        # # result = requests.post(endpoint, json=request_body, headers=self.headers)
        # result = await self.graph_client.users(user_id).events.by_event_id('event-id').attachments.post(body = request_body)
        # return result

    async def send_payload(self, endpoint, user_id, subject, body, send_to, attachments):
        payload = {
            "message": {
                "subject": subject,
                "body": {
                    "contentType": "Text",
                    "content": body
                },
                "toRecipients": [

                ],
                "attachments": []  # Initialize an empty list for attachments
            }
        }

        for each_recipient in send_to:
            payload["message"]["toRecipients"].append(
                {
                    "emailAddress": {
                        "address": each_recipient
                    }
                }

            )

        for key, val in attachments.items():
            file_name = key
            byte_data = val
            attachment_data = await  self.upload_small_attachment(user_id, file_name, byte_data)
            payload["message"]["attachments"].append(attachment_data)

        print_color(payload, color='b')

        response = requests.post(endpoint, json=payload, headers=self.headers)

        if response.status_code == 202:
            print("Email sent successfully to:", send_to)
        else:
            print("Error sending email.")
            print(response.text)
        return response.status_code

    async def send_new_mail(self, user_id, subject, body, send_to, attachments={}):
        if len(attachments.items()) > 0:
            # endpoint= f"https://graph.microsoft.com/v1.0/users/{user_id}/attachments/"
            endpoint = f"https://graph.microsoft.com/v1.0/users/{user_id}/sendMail"
        else:
            endpoint = f"https://graph.microsoft.com/v1.0/users/{user_id}/sendMail"

        await self.send_payload(endpoint, user_id, subject, body, send_to, attachments)

    async def send_reply_mail(self, user_id, message_id, subject, body, send_to, attachments={}):
        if len(attachments.items()) > 0:
            # endpoint= f"https://graph.microsoft.com/v1.0/users/{user_id}/attachments/"
            endpoint = f"https://graph.microsoft.com/v1.0/users/{user_id}/messages/{message_id}/reply"
        else:
            endpoint = f"https://graph.microsoft.com/v1.0/users/{user_id}/messages/{message_id}/reply"

        await self.send_payload(endpoint, user_id, subject, body, send_to, attachments)

    async def send_reply_all_mail(self, user_id, message_id, subject, body, send_to, attachments={}):
        if len(attachments.items()) > 0:
            # endpoint= f"https://graph.microsoft.com/v1.0/users/{user_id}/attachments/"
            endpoint = f"https://graph.microsoft.com/v1.0/users/{user_id}/messages/{message_id}/replyAll"
        else:
            endpoint = f"https://graph.microsoft.com/v1.0/users/{user_id}/messages/{message_id}/replyAll"

        await self.send_payload(endpoint, user_id, subject, body, send_to, attachments)
