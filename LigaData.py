from github import Github 
import os
import pandas as p
import io
from pandas import DataFrame 
from datetime import datetime
import psycopg2
from sqlalchemy import create_engine
import time

def DataManipulation(repo,commits):
  file = repo.get_contents("/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv", ref="master")
  df = p.read_csv(io.StringIO(file.decoded_content.decode('utf-8')))
  print(df.head)
  #Data Manuplation
  #1
  i= len(df.columns)-1
  while i >=5:
   df[df.columns[i]]=df[df.columns[i]] - df[df.columns[i -1]]
   i-=1
  #2
  df=df.drop(['Lat','Long','Province/State'],axis=1)
  #3
  df= df.groupby(["Country/Region"]).sum().reset_index()

  #Create Tables to DB
  engine = create_engine('postgresql://postgres:Hala@localhost:5432/ligadata')
  df = df.melt(id_vars = 'Country/Region', var_name = 'Date', value_name = 'Total')
  ndf=df.rename(columns={'Country/Region': 'CountryRegion'})
  for i in range(len(ndf.columns) - 1):
    tmp_df = DataFrame (ndf[ndf.columns[i]].unique(),columns=[ndf.columns[i]])
    tmp_df.to_sql(ndf.columns[i], con=engine, if_exists='replace')
    tmp_dict=(tmp_df.to_dict())
    dict_tmp=tmp_dict[ndf.columns[i]]
    inv_map = {v: k for k, v in dict_tmp.items()}
    ndf[ndf.columns[i]].replace(inv_map, inplace=True)
  ndf.to_sql('Fact', con=engine, if_exists='replace')

  last_commit_data=[datetime.strptime(repo.get_git_commit(commits[0].sha).last_modified, '%a, %d %b %Y %H:%M:%S %Z').strftime('%b/%d/%Y %H:%M:%S'),
  repo.get_git_commit(commits[0].sha).author.name,
  repo.get_git_commit(commits[0].sha).message]
  
  last_commit_data_tmp=[last_commit_data]
  last_commit_data_df = DataFrame(last_commit_data_tmp,columns=['Update Date','Author','Message'])
  last_commit_data_df.to_sql('Commits', con=engine, if_exists='append', index=False)

#connect to github API
token = os.getenv('GITHUB_TOKEN', 'e8683f5f2f437b7c0eb70a274e3d2ce1190f1298')
g = Github(token)
repo = g.get_repo("CSSEGISandData/COVID-19")

try:
  
   connection = psycopg2.connect(database="ligadata", user='postgres', password='Hala', host='127.0.0.1', port= '5432')
   cursor = connection.cursor()
   postgreSQL_select_Query = 'select max(to_timestamp("Update Date",'+"'Mon/DD/YYYY HH24:MI:SS'"+')) as date from "Commits"'
   cursor.execute(postgreSQL_select_Query)
   db_last_commit_date = cursor.fetchall() 
   db_last_commit_date=time.mktime(db_last_commit_date[0][0].timetuple())
   commits = list(repo.get_commits('master'))
   api_last_commit_date=datetime.strptime(repo.get_git_commit(commits[0].sha).last_modified, '%a, %d %b %Y %H:%M:%S %Z')
   api_last_commit_date=time.mktime(api_last_commit_date.timetuple())
 
   if (api_last_commit_date>=db_last_commit_date):
     DataManipulation(repo,commits)
   else:
     print('DataBase is Already Updated To the Last Version Of The File')
        
except (Exception, psycopg2.Error) as error :
    print ("Error while fetching data from PostgreSQL", error)

finally:
    #closing database connection.
    if(connection):
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")

