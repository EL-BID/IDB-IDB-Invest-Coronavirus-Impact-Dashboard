import pymsteams
import yaml


def get_connection(path='configs/teams.yaml'):
    return pymsteams.connectorcard(yaml.load(open(path, 'r'))['channel'])

def post(content):
    
    con = get_connection()
    con.text(str(content))
    con.send()