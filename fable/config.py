# DB hostname to connect
"""
config for global variable in this project
"""
import os
from pymongo import MongoClient
import sys
import re
from subprocess import Popen, call, check_output
import json

# Default values if key is not specified in the yaml
DEFAULT_CONFIG = {
    'tmp_path': './tmp',
    'mongo_user': None,
    'mongo_pwd': None,
    'localserver_port': 24680,
    'mongo_db': 'fable'
}

def config(key):
    return var_dict.get(key)


def set_var(key, value):
    exec(f'{key.upper()} = {value}', globals())
    exec(f'{var_dict[key.upper()]} = {value}', globals())


def unset(key):
    # TODO: Also unset the defined variables
    try:
        del(var_dict[key])
    except: pass


def back_default():
    global var_dict
    var_dict = default_var_dict.copy()
    # TODO Update defined variables


def azure_kv(vault_name, secret_name):
    # * Details: https://docs.microsoft.com/en-us/azure/key-vault/secrets/quick-create-python?tabs=cmd
    KVUri = f"https://{vault_name}.vault.azure.net"

    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=KVUri, credential=credential)
    retrieved_secret = client.get_secret(secret_name)
    return retrieved_secret

def new_db():
    if 'mongo_url' not in var_dict:
        db = eval(f"MongoClient(MONGO_HOSTNAME, username=MONGO_USER, password=MONGO_PWD, authSource='admin').{MONGO_DB}")
    else:
        db =  eval(f"MongoClient('{MONGO_URL}').{MONGO_DB}")
    return db

var_dict = {}
default_var_dict = {}
if os.getenv('FABLE_CONFIG_KEYVAULT', ''):
    # * Use Azure's Key vault service to get the yaml string. VaultName and KeyName required
    from azure.keyvault.secrets import SecretClient
    from azure.identity import DefaultAzureCredential
    vault_name = os.getenv('FABLE_CONFIG_VAULTNAME')
    secret_name = os.getenv('FABLE_CONFIG_SECRETNAME')
    secret = azure_kv(vault_name, secret_name)
    config_json = json.loads(secret.value) 
else:
    CONFIG_PATH = os.getenv('FABLE_CONFIG_PATH', os.path.dirname(__file__))
    config_json = json.load(open(os.path.join(CONFIG_PATH, 'config.json'), 'r'))

var_dict.update(config_json)
locals().update({k.upper(): v for k, v in var_dict.items()})
if config_json.get('proxies') is not None:
    PROXIES = [{'http': ip, 'https': ip } for ip in \
            config_json.get('proxies')]
else: PROXIES = [{}]
# PROXIES = PROXIES + [{}]  # One host do not have to use proxy
var_dict['proxies'] = PROXIES

default_var_dict = var_dict.copy()

for dc, dc_value in DEFAULT_CONFIG.items():
    if dc not in var_dict:
        dc = dc.upper()
        locals().update({dc: dc_value})
        var_dict.update({dc: dc_value})

if 'mongo_url' not in var_dict:
    DB_CONN = eval(f"MongoClient(MONGO_HOSTNAME, username=MONGO_USER, password=MONGO_PWD, authSource='admin')")
    DB = eval(f"MongoClient(MONGO_HOSTNAME, username=MONGO_USER, password=MONGO_PWD, authSource='admin').{MONGO_DB}")
else:
    DB_CONN =  eval(f"MongoClient('{MONGO_URL}')")
    DB =  eval(f"MongoClient('{MONGO_URL}').{MONGO_DB}")
