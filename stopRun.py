def setRunFlagOFF():
    import json
    import os

    statusFile = ".\\runStatus.json"
    with open(statusFile, 'r') as f:
        json_data = json.load(f)
    json_data["runFlag"] = 'stop'
    with open(statusFile, 'w') as f:
        json.dump(json_data, f,indent=2)
setRunFlagOFF()