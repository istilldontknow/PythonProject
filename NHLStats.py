import mysql.connector
import pandas as pd

	# Import .csv data
data = pd.read_csv(r'/workspace/PersonalProjectPytho/Scoring.csv')
df = pd.DataFrame(data)

	# Drop 16th and onwards columns
df.drop(df.iloc[:, 15:], inplace=True, axis=1)

	# Sort by Goals
sortedDF = df.sort_values('G', ascending = False)

	# Find Top Goals and corresponding Top Scorer
topGoals = df['G'].max()
topScorer = df['playerID'][df['G'] == topGoals].values[0]
print(topScorer)

	# Find Year
year = df['year'][df['G'] == topGoals].values[0]
print(year)
print('Top Scorer {} scored {} goals in year {}'.format(topScorer, topGoals, year))

	# Save first 100 rows to top100
top100 = sortedDF.head(100)

	# Rename column for compatibility issue with SQL
rtop100 = top100.rename(columns={"+/-" : "PlusMinus"})
print(rtop100)



try:
    connection = mysql.connector.connect(host='localhost',
                                         database='PythonDB',
                                         user='root',
                                         password='PythonDB')
    mySql_Create_Table_Query = """CREATE TABLE NHLStats ( 
                             playerID varchar(250),
                             year INT(4),
                             stint INT(1) ,
                             tmID VARCHAR(3) ,
                             lgID VARCHAR(3) ,
                             pos VARCHAR(255) ,
                             GP FLOAT ,
                             G FLOAT ,
                             A FLOAT ,
                             Pts FLOAT,
                             PIM FLOAT,
                             PlusMinus FLOAT,
                             PPG FLOAT,
                             PPA FLOAT, 
                             SHG FLOAT)
                             """

    cursor = connection.cursor()
    result = cursor.execute(mySql_Create_Table_Query)
    print("NHLStats Table created successfully ")
    
    for index, row in rtop100.fillna(0).iterrows():
    	 cursor.execute("INSERT INTO NHLStats (playerID, year, stint, tmID, lgID, pos, GP,  G, A, Pts, PIM, PlusMinus, PPG, PPA, SHG) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", 
                        [row.playerID, row.year, row.stint, row.tmID, row.lgID, row.pos, row.GP, row.G, row.A, row.Pts, row.PIM, row.PlusMinus, row.PPG, row.PPA, row.SHG])
    print('NHLStats .csv data inserted into MySQL successfully')
    connection.commit()

except mysql.connector.Error as error:
    print("Failed to insert top100 NHLStats into MySQL: {}".format(error))
finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection is closed")