import json
import types
import importlib.machinery
import copy
loader = importlib.machinery.SourceFileLoader("NetworkEle", "C:/Users/madha/OneDrive/Desktop/Projects/Python/SystemDesign/netSim/netWorkEle/NetWorkEle.py")
NetworkEle = types.ModuleType(loader.name)
loader.exec_module(NetworkEle)

DC_NAME = "DC_NAME"
DATA = "DATA"
DC_COPY_NUM = "DC_COPY_NUM"
NODES = "NODES"
DC_LIST = "DC_LIST"
    
class JsonLoader:
    data = None
    dc_dict = None
    
    def __init__(self, json_path):
        self.initVals()
        json_file_obj = open(json_path)
        json_dict = json.load(json_file_obj)
        try:
            for dc_id in json_dict[DC_NAME]:
                temp_dc = NetworkEle.DataCenter(dc_id)
                for node_id in json_dict[DC_NAME][dc_id][NODES]:
                    temp_db = NetworkEle.NetDb()
                    temp_node = NetworkEle.NetNode(node_id, temp_db, dc_id)
                    temp_dc.addNode(temp_node, json_dict[DC_NAME][dc_id][NODES][node_id])
                self.addDcToDict(temp_dc, json_dict[DC_NAME][dc_id][DC_COPY_NUM])
            for key in json_dict[DATA]:
                self.data[key] = json_dict[DATA][key]
        except:
            print("Error Loading, please check if the JSON File is of the proper format:")
            self.printJSONFormat()
            
    def initVals(self):
        self.dc_dict = {
        "DC_LIST" : dict(),
        "DC_COPY_NUM" : dict()
        }
        self.data = dict()
            
    def addDcToDict(self, dc, dc_copy_num):
        dc_id = dc.getDcId()
        self.dc_dict["DC_LIST"][dc_id] = dc
        self.dc_dict[DC_COPY_NUM][dc_id] = dc_copy_num
        
    def printJSONFormat(self):
        print("""
              All capitals are constant string, small letters will be replaced by their value
              JSON FORMAT:
                  {
                      DC_NAME : 
                          {
                              (string)dc_id : {
                              DC_COPY_NUM : int(number of dc copies),
                              NODES : {{(string)node_id: int(number of node copies)},...}
                              },
                              .
                              .
                          }
                      DATA:
                      {
                          key : value,
                          .
                          .
                          }
                  }
            """
            )
            
    def getDcDict(self):
        return self.dc_dict
    
    def getData(self):
        return self.data
        
    