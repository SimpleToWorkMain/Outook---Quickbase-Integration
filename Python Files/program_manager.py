from Retrieve_emails import retrieve_users, retreive_emails, get_access_token
from Retrieve_attachments import retrieve_user_attachments
from Send_emails import send_emails
import asyncio
from global_modules import print_color, ProgramCredentials, engine_setup, run_sql_scripts, create_folder
import getpass
import os
import sys
import time
import json
from concurrent.futures import ThreadPoolExecutor
from threading import BoundedSemaphore
import pandas as pd
from quickbase_class import QuickbaseAPI
import base64
import pprint
import datetime


class BoundedExecutor:
    """BoundedExecutor behaves as a ThreadPoolExecutor which will block on
    calls to submit() once the limit given as "bound" work items are queued for
    execution.
    :param bound: Integer - the maximum number of items in the work queue
    :param max_workers: Integer - the size of the thread pool
    """
    def __init__(self, bound, max_workers):
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.semaphore = BoundedSemaphore(bound + max_workers)

    """See concurrent.futures.Executor#submit"""
    def submit(self, fn, *args, **kwargs):
        self.semaphore.acquire()
        try:
            future = self.executor.submit(fn, *args, **kwargs)
        except:
            self.semaphore.release()
            raise
        else:
            future.add_done_callback(lambda x: self.semaphore.release())
            return future

    """See concurrent.futures.Executor#shutdown"""
    def shutdown(self, wait=True):
        self.executor.shutdown(wait)


def run_user_emails(environment=None, user=None):
    x = ProgramCredentials(environment)
    access_token = get_access_token(client_id=x.azure_client_id,
                                    client_secret=x.azure_client_secret,
                                    tenant_id=x.azure_tenant_id)

    print_color(access_token, color='g')

    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(retreive_emails(x=x, access_token=access_token, user_name=user))
    # asyncio.run(retrieve_user_attachments(x=x, access_token=access_token, user_name=user))


def retrieve_attachments(environment=None, user=None):
    x = ProgramCredentials(environment)
    access_token = get_access_token(client_id=x.azure_client_id,
                                    client_secret=x.azure_client_secret,
                                    tenant_id=x.azure_tenant_id)
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    # asyncio.run(retrieve_user_attachments(x=x, access_token=access_token, user_name=user))
    # retrieve_user_attachments(x, access_token, user)


def database_setup(x, engine, engine1):
    scripts = []
    scripts.append(f'Create Database if not exists {x.project_name};')
    run_sql_scripts(engine=engine, scripts=scripts)

    scripts = []
    scripts.append(f'''Create table if not exists log_user_time(
        id int auto_increment primary key,
        username varchar(255),
        date date    
    )''')
    run_sql_scripts(engine=engine1, scripts=scripts)


async def run_retrieve_emails_cmd(environment, user):
    start_time = time.time()

    x = ProgramCredentials(environment)
    engine = engine_setup(project_name=None, hostname=x.hostname, username=x.username, password=x.password,
                          port=x.port)
    engine1 = engine_setup(project_name=x.project_name, hostname =x.hostname, username=x.username, password=x.password,
                          port=x.port)

    database_setup(x, engine, engine1)

    user_data_list = await retrieve_users(x)

    cmd_type = 'cmd /c'

    venv_path = f'C:\\Users\\{getpass.getuser()}\\Desktop\\New Projects\\Test Project\Test Project\\venv\\scripts\\python.exe'
    channel_project_folder = f'C:\\Users\\{getpass.getuser()}\\Desktop\\New Projects\\Test Project\Test Project'

    print_color(len(user_data_list))


    engine1.connect().execute(f'Delete from log_user_time where date = curdate()')
    script = f'SELECT count(*) as count_all FROM log_user_time where date = curdate()'

    counter = 0
    total_count = 0
    user_count = len(user_data_list)
    batchsize = 5
    available_users = 5
    posted_users = pd.read_sql(script, con=engine1)['count_all'].iloc[0]
    print_color(posted_users, color='g')

    user_data_list = user_data_list

    remaining_user_data_list = user_data_list
    print_color(remaining_user_data_list)
    print_color(f'available_users: {available_users}', color='g')

    while len(remaining_user_data_list) >0:
        for user in remaining_user_data_list:
            print_color(available_users, user, color='r')
            if available_users >0:
                print_color(f'Running: {user}', color='p')
                user_name = user.get("user_principal_name")

                engine1.connect().execute(f'''Insert into log_user_time values (0, "{user_name}", curdate())''')
                available_users -= 1
                # posted_users = pd.read_sql(script, con=engine1)['count_all'].iloc[0]
                command_1 = f'"{channel_project_folder}\\Python Files\\program_manager.py" "run_user_emails" "{environment}" "{user_name}"'

                final_command = f'start \"\" {cmd_type} \" "{venv_path}" {command_1}'
                print_color(final_command, color='b')
                os.system(final_command)

                # print_color(remaining_user_data_list, color='r')
                user_data_list.remove(user)


            else:
                pass
                # print_color(f'Not Running: {user}', color='r')
        for i in range(0, 20, 5):
            print_color(f'Waiting {20 - i} Seconds to run next batch of users', color='r')
            time.sleep(5)
        posted_users = pd.read_sql(script, con=engine1)['count_all'].iloc[0]
        available_users = batchsize - posted_users
        print_color(f'available_users: {available_users}', color='g')
        remaining_user_data_list = user_data_list


    end_time = time.time()
    duration = end_time - start_time

    print_color(f'System took {duration / 60} Minutes to run')




async def run_retrieve_emails(environment, user):
    x = ProgramCredentials(environment)


    user_data_list = await retrieve_users(x)

    cmd_type = 'cmd /k'
    # cmd_type = 'cmd /c'



    for user in user_data_list:
        print_color(user, color='g')

    print_color(len(user_data_list))
    batchsize = 1

    executor = BoundedExecutor(1, 1)
    for user in user_data_list:
        args = [environment, user]
        executor.submit(run_user_emails, *args)

    # for i in range(0, len(user_data_list), batchsize):
    #     user_list = user_data_list[i: i + batchsize]
    #     for user in user_list:
    #         print_color(user, color='g')
    #         command_1 = f'"{channel_project_folder}\\Python Files\\program_manager.py" "run_user_emails" "{environment}" "{user}"'
    #         final_command = f'start \"\" {cmd_type} \" "{venv_path}" {command_1}'
    #         print_color(final_command, color='b')
    #         os.system(final_command)

        # time.sleep(10)
        # break


async def run_send_email(x, environment, user, user_data_list):
    await send_emails(x=x, environment=environment, user=user, all_users=user_data_list)
    # asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    # asyncio.run(send_emails(x=x, user=user, all_users=user_data_list))


async def run_send_emails(environment, user):
    x = ProgramCredentials(environment)
    user_data_list = await retrieve_users(x)
    print_color(user_data_list, color='b')
    user=None
    await run_send_email(x,environment,  user, user_data_list)



if __name__ == '__main__':
    # methods = ['run_user_emails', 'run_retrieve_emails', 'run_send_emails']

    if len(sys.argv) == 1:
        method = 'run_retrieve_emails_gmail'
        environment = 'development'
        user = 'test@sample.com'
    else:
        method = sys.argv[1]
        environment = sys.argv[2]
        user = sys.argv[3]

    # print_color(method, environment, user, color='b')

    if method == 'run_retrieve_emails':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(run_retrieve_emails_cmd(environment=environment, user=None))
    elif method == 'run_send_emails':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(run_send_emails(environment=environment, user=None))
    elif method == 'run_user_emails':
        run_user_emails(environment=environment, user=user)

    else:
        globals()[method](environment=environment, user=user)

