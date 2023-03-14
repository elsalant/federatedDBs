import prestodb
import pandas as pd
import json

def queryPresto(sqlQuery):
    try:
       connectionPresto = prestodb.dbapi.connect(
            host='localhost',  # replace with the hostname of your Presto server
            port=8080,  # replace with the port number on which Presto is running
            user='eliot',  # replace with your username
            schema='public',  # replace with a description of your application
            catalog='postgresql'
        )
    except (Exception, prestodb.Error) as error:
            print("Error while connecting to Presto db", error)

    cur = connectionPresto.cursor()
    cur.execute(sqlQuery)
    records = cur.fetchall()
    flatJsonDF = pd.DataFrame()
    USE_THIS = True
    if USE_THIS:  # Create on JSON string from the list of 4 separate strings
        for record in records:
            jStr = '{'
            for entry in record:
                if not type(entry) is str:
                    continue
                jStr += entry[1:-1]+','
            jStr = jStr[:-1]
            jStr += '}'
            jsonDict= json.loads(jStr)
            flatJsonDF = pd.concat([flatJsonDF, pd.json_normalize(jsonDict)], ignore_index=True)

 #           data = json.loads(jsonData)
     #       flatJsonDF = flatJsonDF.append(pd.json_normalize(data), ignore_index=True)
  #          flatJsonDF = pd.concat([flatJsonDF, pd.json_normalize(data)], ignore_index=True)
    else:
        flatJsonDF = pd.DataFrame(records)
    connectionPresto.close()

    # Not all records will have the same fields.  Gaps in numeric fields will be filled by "NaN" which breaks the JSON
    # Replace this will a string that says "No data"
    flatJsonDF.fillna('No data', inplace=True)
    return(flatJsonDF)
