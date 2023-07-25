import time
from datetime import datetime

import pyodbc



def execSelectData(srv,req):
    if str(srv).upper()=="VLD":
        s = "192.168.203.61,1433\\SQLEXPRESS"
        user = "agr"
        psw = '23@@enviRo'

        cnxn= pyodbc.connect(driver='{SQL Server Native Client 11.0}', server=s, database="agr-dcontrol", uid=user, pwd=psw)
    elif str(srv).upper()=="OBS":
        cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                              "Server=DESKTOP-5CJPAFM\\SQLEXPRESS;"
                              "Database=agr-dcontrol;"
                              "Trusted_Connection=yes;")
    else:
        print("server name error (VLD/OBS):", srv)
        return [("server name error")]

    cursor = cnxn.cursor()

    try:
    #  print("execSelelect says:exequte sql query:\n", req)
      cursor.execute(req)
      rows = cursor.fetchall()
    except pyodbc.OperationalError as msg:
      time.sleep(0.3)
      cursor.execute(req)
      rows = cursor.fetchall()
      print("lock error. sleep 0.3 . and try once more ", msg)
    cnxn.close()
    return rows
##############################################
def execListOfUpdateReq(srv,reqList):

    if str(srv).upper() == "VLD":
        s = "192.168.203.61,1433\\SQLEXPRESS"
        user = "agr"
        psw = '23@@enviRo'

        cnxn = pyodbc.connect(driver='{SQL Server Native Client 11.0}', server=s, database="agr-dcontrol", uid=user,
                              pwd=psw)
    elif str(srv).upper() == "OBS":
        cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                              "Server=DESKTOP-5CJPAFM\\SQLEXPRESS;"
                              "Database=agr-dcontrol;"
                              "Trusted_Connection=yes;")
    elif str(srv).upper() == "RDS":
        # Database connection settings
        server = 'observationdb.cbq8ahnbfrlw.eu-north-1.rds.amazonaws.com'
        database = 'observationdb'
        username = 'levi'
        password = 'f43r3g244rr'

        # Create the connection string
        cnxn = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};'
                              f'SERVER={server};'
                              f'DATABASE={database};'
                              f'UID={username};'
                              f'PWD={password}')
    else:
        print("server name error (VLD/OBS):", srv)
        return [("server name error")]


    for req in reqList:
        cursor = cnxn.cursor()
        # This will skip and report errors
        # For example, if the tables do not yet exist, this will skip over
        # the DROP TABLE commands
        try:
            print("exequte sql query:\n", req)
            if srv == "RDS":
                print("RDS insertion.")
           # cursor.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
            cursor.execute(req)
            if srv == "RDS":
                print("RDS insertion Finish")
        except pyodbc.OperationalError as msg:
            print("Command skipped: ", msg)
        cursor.commit()
        cursor.close()
    cnxn.close()


###########################################################################################
def isIDinDBgridOnRDS(tabName,id):
    '''cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                              "Server=DESKTOP-5CJPAFM\\SQLEXPRESS;"
                              "Database=agr-dcontrol;"
                              "Trusted_Connection=yes;")'''
    server = 'observationdb.cbq8ahnbfrlw.eu-north-1.rds.amazonaws.com'
    database = 'observationdb'
    username = 'levi'
    password = 'f43r3g244rr'

    # Create the connection string
    cnxn = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};'
                          f'SERVER={server};'
                          f'DATABASE={database};'
                          f'UID={username};'
                          f'PWD={password}')
    cursor = cnxn.cursor()
    checkIfExistCom = f"select id from [{tabName}] where id = {id}"
    cursor.execute(checkIfExistCom)

    row = cursor.fetchone()

    if row == None or row[0] == None:
        print(f"id {id} for {tabName} is not in grid")
        cursor.close()
        cnxn.close()
        return False
    else:
        print(f"new id {id} for {tabName}  is in grid!!")
        cursor.close()
        cnxn.close()
        return True


    #################################################################
def getMonListFromStationsTableOnVLD(tabName):
   ## print ("getmonlist says: trying connect to db.  table ",tabName)
    s = "192.168.203.61,1433\\SQLEXPRESS"
    user = "agr"
    psw = '23@@enviRo'

    cnxn = pyodbc.connect(driver='{SQL Server Native Client 11.0}', server=s, database="agr-dcontrol", uid=user, pwd=psw)

    cursor = cnxn.cursor()
    req= f"select monitors from [dbo].[stations] where tag='{tabName}'"
    cursor.execute(req)
    row = cursor.fetchone()
  #  print (row)
    monstring = row [0]

    monlist= monstring.split(";")
    return monlist

   # monlist.pop(0)
   # monlist.pop(0)
 #   cnxn.close()
   # return result


  ##############################################################
def buildUpdateSqlReq(tabname, id,monList, valList):
    st = f"UPDATE [{tabname}] set "
    st = st + monList[0] + "=" + str(valList[0])
    if len(monList) > 1:
        for i in range(1, len(monList)):
            valStr=str(valList[i])
            if  "NON" in valStr.upper(): valStr="-9999"
            st = st + ", " + monList[i] + "=" + valStr
    st= st + f" WHERE id = {id}"
    return st


#########################
def buildSelectSqlReq(tabname,id, monList):
    str = f"SELECT "
    str = str + monList[0]
    if len(monList) > 1:
        for i in range(1, len(monList)):
            str = str + ", " + monList[i]
    str = str + f" FROM [{tabname}] WHERE id = {id}"
    return str

 ###########################################################
def getDateTimeByID(id):
    #example id=231218045 > 2023-12-18 04:50:00
    min = (id % 10) * 10
    id_1 = id // 10
    hour = (id_1 % 100)
    id_2 = id_1 // 100
    day = id_2 % 100
    id_3 = id_2 // 100
    month = id_3 % 100
    id_4 = id_3 // 100
    year = id_4 + 2000

    dt = datetime(year, month, day, hour, min)
    return dt
 ################################################
def makeTimeGridForIDOnRDS(tabName, id):
    statusTable="VLDstat"
    #lastTime = getLastTimeOfTab(tabName)

    #dt = datetime.now()
    # dt = fromDate
    #delta10days= timedelta(days=daysNum)

    # enddate= dt+delta10days
    '''
    cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                          "Server=DESKTOP-5CJPAFM\\SQLEXPRESS;"
                          "Database=agr-dcontrol;"
                          "Trusted_Connection=yes;")
    '''
    # Database connection settings
    server = 'observationdb.cbq8ahnbfrlw.eu-north-1.rds.amazonaws.com'
    database = 'observationdb'
    username = 'levi'
    password = 'f43r3g244rr'


    cnxn_aws = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};'
                              f'SERVER={server};'
                              f'DATABASE={database};'
                              f'UID={username};'
                              f'PWD={password}')

  #  cursor = cnxn.cursor()
    cursor_aws = cnxn_aws.cursor()
    tabVLDname= tabName+"v"

    nextdate= getDateTimeByID(id)
    datastateDef=-10
    sendstateDef=0
    vldstateDef=0
    #  while (nextdate< enddate):
    nextid= id

    if isIDinDBgridOnRDS(tabName,id):
        print("makeTimeGridForID says: id is already in grid.  Insert operation is not required")

    #  print(f"makeTimeGridToTables says: id {nextid} already in table {tabName} ")

    else :
        print (f"makeTimeGridForIDOnRDS says: id {id} is NOT in grid . Making new entry.")
        dateStr = nextdate.strftime("%Y-%m-%dT%H:%M:%S")
        com = f"INSERT INTO [{tabName}] (id,datetime) VALUES ({nextid},'{dateStr}')"
        comVLD = f"INSERT INTO [{tabVLDname}] (id,datetime) VALUES ({nextid},'{dateStr}')"
        comStatus = f"INSERT INTO [{statusTable}] (tableName,FK,datastate,vldstate,sendstate) VALUES ( '{tabName}',{nextid},{datastateDef},{vldstateDef},{sendstateDef})"
        try:
            print(com)
            #   cursor.execute(com)
            cursor_aws.execute(com)
            cursor_aws.commit()
            cursor_aws.close()
            cursor_aws = cnxn_aws.cursor()
            print("for aws com:",com)
            #    cursor.execute(comVLD)
            cursor_aws = cnxn_aws.cursor()
            cursor_aws.execute(comVLD)
            cursor_aws.commit()
            cursor_aws.close()
            print("for aws comVLD:", comVLD)
            #     cursor.execute(comStatus)
            print("try comStatus: ", comStatus)
            cursor_aws = cnxn_aws.cursor()
            cursor_aws.execute(comStatus)
            cursor_aws.commit()
            cursor_aws.close()
            print("for aws com Status:",comStatus)
            # cursor.commit()

            print("aws Commit")
        except Exception as e:
            print(f"Error: {str(e)}")
    #cnxn.close()
    try:

        cnxn_aws.close()
        print("Connection closed successfully.")
    except Exception as e:
        print(f"Error: {str(e)}")
#***********************************************************************
