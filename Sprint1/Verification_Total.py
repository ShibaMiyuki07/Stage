import pymongo
import sys
from datetime import datetime

#def getTotal_usage_jour_global_daily_usage(client,day):


def getTotal_usage_jout_daily_usage(client,day):
  pipeline = []

  

if __name__ == "__main__":
    client = pymongo.MongoClient("mongodb://oma_dwh:Dwh4%40OrnZ@192.168.61.199:27017/?authMechanism=DEFAULT")
    date = sys.argv[1]
    datetime = datetime.strptime(date,'%Y-%m-%d').date()
    