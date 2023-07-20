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

      print("Command skipped: ", msg)
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
        username = 'admin'
        password = 'HjHtEpugt8esmznd07vZ'

        # Create the connection string
        cnxn = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};'
                              f'SERVER={server};'
                              f'DATABASE={database};'
                              f'UID={username};'
                              f'PWD={password}')
    else:
        print("server name error (VLD/OBS):", srv)
        return [("server name error")]
    cursor = cnxn.cursor()
    for req in reqList:
            # This will skip and report errors
            # For example, if the tables do not yet exist, this will skip over
            # the DROP TABLE commands
            try:
                print("exequte sql query:\n", req)
                if srv == "RDS":
                    print("RDS insertion")
                cursor.execute(req)
                if srv == "RDS":
                    print("RDS insertion Finish")
            except pyodbc.OperationalError as msg:
                print("Command skipped: ", msg)
    cursor.commit()


###########################################################################################
def isIDinDBgridOnOBS(tabName,id):
    cnxn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                              "Server=DESKTOP-5CJPAFM\\SQLEXPRESS;"
                              "Database=agr-dcontrol;"
                              "Trusted_Connection=yes;")

    cursor = cnxn.cursor()
    checkIfExistCom = f"select id from [{tabName}] where id= {id}"
    cursor.execute(checkIfExistCom)

    row = cursor.fetchone()

    if row == None or row[0] == None:
        print(f"id {id} for {tabName} is not in grid")
        return False
    else:
        print(f"new id {id} for {tabName}  is in grid!!")
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
            st = st + ", " + monList[i] + "=" + str(valList[i])
    st= st + f" WHERE id={id}"
    return st


#########################
def buildSelectSqlReq(tabname,id, monList):
    str = f"SELECT "
    str = str + monList[0]
    if len(monList) > 1:
        for i in range(1, len(monList)):
            str = str + ", " + monList[i]
    str = str + f" FROM [{tabname}] WHERE id={id}"
    return str

 ##########################################################








#***********************************************************************
