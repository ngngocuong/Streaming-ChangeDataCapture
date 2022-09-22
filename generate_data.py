import random
import uuid
import boto3
from faker import Faker
import time

fake = Faker()

dynamodb_client = boto3.client('dynamodb')

lst_country = [
    "USA",
    "England",
    "VietNam",
    "Germany",
    "France",
    "Italy",
    "Japan",
    "Korea",
    "Singapo",
    "India"
]


def get_item(x, user_id) -> dict:
    return  {
                'user_key' : {'N' : str(x)},
                'user_id' : {'S' : user_id},
                'first_name': {'S': f'{fake.first_name()}'},
                'last_name': {'S': f'{fake.last_name()}'},
                'country' : {'S' : f'{random.choice(lst_country)}'}
            }

def generate_user(x):
    lst_user_id = []
    for action in range(10):
        user_id = f'{uuid.uuid4()}'
        lst_user_id.append((x,user_id))
        Item = (get_item(x,user_id))
        resp = dynamodb_client.put_item(TableName="user_dim", Item=Item)
        x += 1
        print("Generate user success!!")
    return lst_user_id


def update_item(lst_user_id):
    for update in range(2):
        pick = random.choice(lst_user_id)
        user_key, user_id = pick
        print(user_key , user_id)
        print(pick)
        response = dynamodb_client.update_item(
            TableName="user_dim",
            Key={
                'user_key' : {'N' : str(user_key)},
                'user_id' : {'S' : user_id}
            },
            ExpressionAttributeNames= {
                '#c' : 'country'
            },
            UpdateExpression="set #c = :count",
            ExpressionAttributeValues={
                ':count' : {
                    'S' : f'{random.choice(lst_country)}' 
                }
            }
        )
        print("Updating is success!!")
        time.sleep(1)

if __name__ == "__main__" :
    # x = 1000 # the user_key begin
    # lst_user_id = generate_user(x)
    # print(lst_user_id) # List about user_key and user_id to pick up and update randomly
    lst_user_id = [(1000, 'ad03e30e-b145-42af-97cf-93573aae27db'), (1001, '74a3ff18-6853-4d0e-89b5-4c0c8b577fbe'), (1002, '136d7dcd-b2a3-4af3-bfe9-325c42988572'), (1003, 'bbe53b54-d473-4297-b6dc-c1c2a578631d'), (1004, '3d72ae8b-45e1-48e1-969b-8c0fc2c7bdb5'), (1005, 'f1bb2f15-69dc-4cff-8c24-d97ad70bf710'), (1006, '90411cc3-8e22-4c26-b0fe-63e7ad97045d'), (1007, 'fd66f8a4-e983-4c1f-a28e-cfa1ada669a4'), (1008, '04470095-d6cb-462a-860a-f906f690c43f'), (1009, 'a179cf17-3eee-4b76-8b20-7f7d83bbd579')]
    update_item(lst_user_id)