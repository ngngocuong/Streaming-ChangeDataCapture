import json 
from datetime import datetime
from typing import Tuple 
import psycopg2
import base64
import time


# class Data():
#     def __init__(self, data: json) -> None:
#         self.data = data
    
#     def get_data(self) -> Tuple:
#         user_key_value = int(self.data['dynamodb']['NewImage']['user_key']['N'])
#         user_id_value = self.data['dynamodb']['NewImage']['user_id']['S']
#         country = self.data['dynamodb']['NewImage']['country']['S']
#         last_name = self.data['dynamodb']['NewImage']['last_name']['S']
#         first_name = self.data['dynamodb']['NewImage']['first_name']['S']
#         create_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
#         update_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
#         row_effective_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
#         row_expiration_datetime = datetime(9999, 12, 30, 12, 0, 0).strftime('%Y-%m-%d %H:%M:%S')
#         if row_effective_datetime < row_expiration_datetime: current_row_indicator = "current"
#         return (user_key_value,user_id_value,first_name,last_name,country,create_datetime,\
#         update_datetime,row_effective_datetime,row_expiration_datetime,current_row_indicator)


def lambda_handler(event, context):
    print("Lambda function invoked !")
    for record in event["Records"]:
        decoded_data = json.loads(
            base64.b64decode(record["kinesis"]["data"]).decode("utf-8")
        )
        print("HERE IS A MESSAGE..")
        if decoded_data["eventName"] == "INSERT":
            print("INSERT IS RUNNING")
            inserted_data = decoded_data["dynamodb"]["NewImage"]
            print(inserted_data)
            print(inserted_data['country']['S'])
            user_key_value = int(inserted_data['user_key']['N'])
            user_id_value = inserted_data['user_id']['S']
            country = inserted_data['country']['S']
            first_name = inserted_data['first_name']['S']
            last_name = inserted_data['last_name']['S']
            create_datetime = update_datetime = row_effective_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            row_expiration_datetime = datetime(9999, 12, 30, 12, 0, 0).strftime('%Y-%m-%d %H:%M:%S')
            current_row_indicator = "current"

            data = (user_key_value,user_id_value,first_name,last_name,country,create_datetime, update_datetime,row_effective_datetime,row_expiration_datetime,current_row_indicator)
            print(data)

            url = (
                "postgresql://postgres:cuong2305@database-1.crikapyxcje2.us-east-1.rds.amazonaws.com:5432/user_database"
            )

            conn = psycopg2.connect(url)
            cur = conn.cursor()
            script = 'INSERT INTO user_dim(user_key,user_id,first_name,last_name,country,\
            create_datetime,update_datetime,row_effective_datetime,row_expiration_datetime,\
            current_row_indicator) \
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            cur.execute(script, data)
            conn.commit()
            cur.close()
            conn.close()

        elif decoded_data["eventName"] == "MODIFY":
            # Fix previous row
            print("UPDATE IS RUNNING")
            old_data = decoded_data["dynamodb"]["OldImage"]
            user_key_value = int(old_data['user_key']['N'])
            user_id_value = old_data['user_id']['S']

            data_pass_fix = ("expired",datetime.now().strftime('%Y-%m-%d %H:%M:%S'),user_id_value,user_key_value)

            print(data_pass_fix)
            url = (
                "postgresql://postgres:cuong2305@database-1.crikapyxcje2.us-east-1.rds.amazonaws.com:5432/user_database"
            )
            conn = psycopg2.connect(url)
            cur = conn.cursor()
            sql = 'UPDATE user_dim \
                SET current_row_indicator = %s,row_expiration_datetime = %s\
                WHERE user_id = %s and user_key = %s'   
            cur.execute(sql,data_pass_fix)
            conn.commit()
            cur.close()
            conn.close()

            time.sleep(1)
            # INSERT INTO TABLE
            new_data = decoded_data["dynamodb"]["NewImage"]
            user_key_value = int(new_data['user_key']['N']) + 1000
            user_id_value = new_data['user_id']['S']
            country = new_data['country']['S']
            first_name = new_data['first_name']['S']
            last_name = new_data['last_name']['S']
            create_datetime = update_datetime = row_effective_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            row_expiration_datetime = datetime(9999, 12, 30, 12, 0, 0).strftime('%Y-%m-%d %H:%M:%S')
            current_row_indicator = "current"

            data_pass_insert = (user_key_value, user_id_value,first_name,last_name,country,create_datetime,update_datetime,row_effective_datetime,row_expiration_datetime,current_row_indicator)
            print(data_pass_insert)


            url = (
                "postgresql://postgres:cuong2305@database-1.crikapyxcje2.us-east-1.rds.amazonaws.com:5432/user_database"
            )

            conn = psycopg2.connect(url)
            cur = conn.cursor()
            script = 'INSERT INTO user_dim(user_key,user_id,first_name,last_name,country,\
            create_datetime,update_datetime,row_effective_datetime,row_expiration_datetime,\
            current_row_indicator) \
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            cur.execute(script, data_pass_insert)
            conn.commit()
            cur.close()
            conn.close()
        
        else:
            print("Do nothing!!")

    return {"statusCode": 200, "body": json.dumps("Hello from Lambda!")}