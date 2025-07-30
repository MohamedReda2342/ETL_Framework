from deepdiff import DeepDiff
import json


# Load both JSON files
#def jsonDiff():
with open("D:\edu\wavZ\ETL_Framework\pages\jsonFIles\snapshot1.json", "r") as f1, open("D:\edu\wavZ\ETL_Framework\pages\jsonFIles\snapshot2.json", "r") as f2:
    data1 = json.load(f1)
    data2 = json.load(f2)

# Compare
    diff = DeepDiff(data1, data2, ignore_order=True)
    with open("D:\edu\wavZ\ETL_Framework\pages\jsonFIles\Diffrences.json","w") as f:
        json.dump(diff,f,indent=4)
    print(diff)
# Print differences
    
#    return diff