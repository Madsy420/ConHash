from netConHash.ConHash import ConHash
from jsonNetLoader.NetLoader import JsonLoader
# This class will be used to simulate the network
# the use of the network sim is to check how the data is allocated/reassigned to different nodes when
# we add/remove node, add new key, delete keys
class NetSim:

    # netConHash object connected to the simNetwork
    net_con_dc_hash = None
    # list of data centers in the network stored as {datacenter_id : datacenter}
    dc_list = None
    abandoned_keys = None

    def __init__(self):
        self.net_con_dc_hash = ConHash()
        self.dc_list = dict()
        self.abandoned_keys = set()

    def addDc(self, dc, virtual_copy_num=2):
        if dc.getDcId() not in self.dc_list:
            self.dc_list[dc.getDcId()] = dc
            ##print("adding " + dc.getDcId() + " to Network conHash " + str(virtual_copy_num) + " times")
            self.net_con_dc_hash.storeNodeHash(dc.getDcId(), virtual_copy_num)
        else:
            print("Data center with the ID {} already exist in network.".format(dc.getDcId()))

    def delDc(self, dc_id):
        if dc_id in self.dc_list:
            del self.dc_list[dc_id]
            self.net_con_dc_hash.delNodeFromConHash(dc_id)
            self.abandoned_keys.update(self.net_con_dc_hash.getAbandonedKeyIds())

    def delDcList(self, dc_id_list):
        for dc_id in dc_id_list:
            self.delDc(dc_id)

    def delNode(self, dc_id, node_id):
        if dc_id in self.dc_list:
            self.dc_list[dc_id].delNode(node_id)

    def delNodeList(self, dc_id, node_id_list):
        for node_id in node_id_list:
            self.delNode(dc_id, node_id)

    def putVal(self, key, val, target_node_id, target_dc_id):
        if target_dc_id not in self.dc_list:
            print("Data center with ID: {} does not exist!".format(target_dc_id))
        elif target_node_id not in self.dc_list[target_dc_id].getNodes():
            print("Node with ID {}, does not exist in Data Center with ID {}!".format(target_node_id,
                                                                                      target_dc_id))
        else:
            self.dc_list[target_dc_id].getNodes()[target_node_id].getDb().putVal(key, val)

    def getVal(self, key):
        dc_id_list = self.net_con_dc_hash.getNodesWithKey(key)

        if dc_id_list != None:
            dc_id = dc_id_list[0]
            node_id_list = self.dc_list[dc_id].getConHash().getNodesWithKey(key)
            if node_id_list != None:
                node_id = node_id_list[0]
                return self.dc_list[dc_id].getNodes()[node_id].getDb().getVal(key)

    def storeValIntoNet(self, key, val, dc_copy_num, node_copy_num):
        is_abort = False
        storage_nodes_data = dict()
        target_dc_id_list = self.net_con_dc_hash.storeKeyInConHashAndGetNodes(key, dc_copy_num)
        try:
            if target_dc_id_list != None:
                for target_dc_id in target_dc_id_list:
                    if(type(node_copy_num) == int ):
                        target_node_id_list = self.dc_list[target_dc_id].getConHash().storeKeyInConHashAndGetNodes(key,
                                                                                                                   node_copy_num)
                    else:
                        target_node_id_list = self.dc_list[target_dc_id].getConHash().storeKeyInConHashAndGetNodes(key,
                                                                                                               node_copy_num[target_dc_id])
                    if target_node_id_list != None:
                        storage_nodes_data[target_dc_id] = target_node_id_list
                    else:
                        is_abort = True
                        break
            else:
                is_abort = True

            if not is_abort:
                for target_dc_id in storage_nodes_data:
                    for target_node_id in storage_nodes_data[target_dc_id]:
                        self.putVal(key, val, target_node_id, target_dc_id)
            else:
                self.net_con_dc_hash.delKeyFromConHash(key)
                if target_dc_id_list != None:
                    for target_dc_id in target_dc_id_list:
                        self.dc_list[target_dc_id].getConHash().delKeyFromConHash(key)

        except KeyError:
            # issue is most probably from node_copy_num not having some
            # dc_id as key
            print("Some necessary data missing in variables")
            
    def loadJson(self, json_path):
        json_net_handler = JsonLoader(json_path)
        dc_dict = json_net_handler.getDcDict()
        ##print(dc_dict["DC_LIST"])
        for dc_id in dc_dict["DC_LIST"]:
            ##print(dc_dict["DC_LIST"][dc_id].nodes)
            self.addDc(dc_dict["DC_LIST"][dc_id], dc_dict["DC_COPY_NUM"][dc_id])
            
        jsonData = json_net_handler.getData()
        for key in jsonData:
            self.storeValIntoNet(key, jsonData[key], 2, 2)
            
JSON_PATH = "C:/Users/madha/OneDrive/Desktop/Projects/Python/SystemDesign/netSim/samplNetJson.json"
def main():
    net_sim_obj = NetSim()
    net_sim_obj.loadJson(JSON_PATH)
    print("Storage done")
    for dc_key in net_sim_obj.dc_list:
        dc = net_sim_obj.dc_list[dc_key]
        dc_con_hash = dc.getConHash()
        print("#########################################################")
        print("DC NO: " + dc.getDcId())
        for node_id in dc_con_hash.node_vs_keys:
            print("Node ID: " + str(node_id))
            print("Key hash in node: " + str(dc_con_hash.node_vs_keys[node_id]))
    
if __name__ == '__main__':
    main()
