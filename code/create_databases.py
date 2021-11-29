import pandas as pd
from sqlalchemy import create_engine

database_username = 'root'
database_password = 'sqlpass'
database_ip       = 'localhost'
database_name     = 'stage_area'
database_connection = create_engine('mysql+mysqlconnector://{0}:{1}@{2}'.
                                               format(database_username, database_password, 
                                                      database_ip))
conn = database_connection.connect()
conn.execute('DROP DATABASE IF EXISTS {}'.format(database_name))
conn.execute('CREATE DATABASE {}'.format(database_name))
conn.execute('DROP DATABASE IF EXISTS {}'.format('DW'))
conn.execute('CREATE DATABASE {}'.format('DW'))
conn.execute('SET GLOBAL connect_timeout=28800')
conn.execute('SET GLOBAL max_allowed_packet=1000000000')