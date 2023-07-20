# This is a sample Python script.
from tools import execListOfUpdateReq,execSelectData,isIDinDBgridOnOBS,getMonListFromStationsTableOnVLD,buildUpdateSqlReq,buildSelectSqlReq
# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
def completatorMain():
    req = """SELECT TOP (20) 
         [TableName]
          ,[FK]   
          ,[VldState]   
      FROM [agr-dcontrol].[dbo].[VLDstat] where SendState=0 and VldState !=0 order by FK desc"""
    lst = execSelectData("vld", req)
    print(lst)
    cnOK = 0
    cnErr = 0
    updateListForOBS = []
    updateListForVLD = []

    for item in lst:
        tab = item[0]
        vldTab = tab + "v"
        id = item[1]
        if isIDinDBgridOnOBS(tab, id):
            vldStatREq = f"SELECT DataState,VldState from VLDstat where TableName='{tab}' AND FK= {id} "
            monList = getMonListFromStationsTableOnVLD(tab)
            #get monitors values from vld sql
            req = buildSelectSqlReq(tab, id, monList)
            vldReq = buildSelectSqlReq(vldTab, id, monList)
            statSel = execSelectData("VLD", vldStatREq)
            slct = execSelectData("VLD", req)
            vldSel = execSelectData("VLD", vldReq)
            valList = list(slct[0])
            vldValList = list(vldSel[0])
            #build update request and add to exec list
            req = buildUpdateSqlReq(tab,id, monList, valList)
            vldReq = buildUpdateSqlReq(vldTab,id, monList, vldValList)
            updateListForOBS.append(req)
            updateListForOBS.append(vldReq)
            # _______________
            dataStatVal = statSel[0][0]
            vldStatVal = statSel[0][1]
            statUpdateReq = f"UPDATE [dbo].[VLDstat]  SET [DataState]= {dataStatVal}, [vldState]= {vldStatVal} where [TableName]='{tab}' and [FK]= {id}"
            updateListForOBS.append(statUpdateReq)
            # ______________________
            req = f"UPDATE [dbo].[VLDstat] SET [SendState] = 1 where TableName='{tab}' and FK={id}"
            print(req)
            updateListForVLD.append(req)
        else:
            cnErr = 1
    execListOfUpdateReq("OBS", updateListForOBS)
    execListOfUpdateReq("RDS", updateListForOBS)
    execListOfUpdateReq("VLD", updateListForVLD)
    print("OK:")

# See PyCharm help at https://www.jetbrains.com/help/pycharm/