import importlib.machinery
import types
loader = importlib.machinery.SourceFileLoader("ConHash", "C:/Users/madha/OneDrive/Desktop/Projects/Python/SystemDesign/netSim/netConHash/ConHash.py")
ConHashMod = types.ModuleType(loader.name)
loader.exec_module(ConHashMod)
ConHash = ConHashMod.ConHash

#  a simple dataBase simulation for each of the "node"(represented by netNode)
class NetDb:
    # data stored as key_value pair {key : value}
    data = None
    
    def __init__(self):
        self.data = dict()
        
    # checks if the key is available
    # key : str : key that needs to be checked
    def hasKey(self, key):
        if key in self.data:
            return True
        return False
    
    # get value stored mapped to key
    # key : str : key for which the value is needed
    # returns : anyType :  value mapped to given key
    def getVal(self, key):
        if self.hasKey(key):
            return self.data[key]
        print("Key does not exist in the DB.")
        return None
    
    # store key value pair
    # key : str : key to which the data will be mapped
    # val : anyType : value that is going to be mapped to the key
    def putVal(self, key, val):
        if self.hasKey(key):
            print("Replacing current value with new value.")
        else:
           ##print("Appending key value pair to DB.")
           pass
        self.data[key] = val
    
    # delete a key from db
    # key : str : key to be removed (and data deleted)
    def delValWithKey(self, key):
        if self.hasKey(key):
            print("Deleting key from DB: " + str(key))
            self.data.pop(key)
        else:
            print("Given key does not exist: " + str(key))
            
# a dataCenter sim, contains node
class DataCenter:
    # unique id within a network
    dc_id = None
    # {node_id : node} mapping of all nodes in dc
    nodes = None
    # consisten hashing for allocating key in the datacenter(among nodes)
    con_node_hash = None
    
    abandoned_keys = None
    
    def __init__(self, dc_id):
        self.dc_id = dc_id
        self.nodes = dict()
        self.con_node_hash = ConHash()
        self.abandoned_keys = set()
    
    # add a node to the data center
    # node : netNode : node to be added
    # virtual_copy_num : int : tells number of copies of node to be added to the
    #                          consistent hashing ( number of virtual nodes = virtual_copy_num-1)
    def addNode(self, node, virtual_copy_num=2):
        if node.getNodeId() not in self.nodes:
            self.nodes[node.getNodeId()] = node
            ##print(str(self.getDcId()) + ": adding " + node.getNodeId() + " to Network conHash " + str(virtual_copy_num) + " times")
            self.con_node_hash.storeNodeHash(node.getNodeId(), virtual_copy_num)
        else:
            print("node with node ID {} exists in data center {}".format(node.getNodeId(),
                                                                         self.getDcId()))
            
    def delNode(self, node_id):
        if node_id in self.nodes:
            del self.nodes[node_id]
            self.con_node_hash.delNodeFromConHash(node_id)
            self.abandoned_keys.update(self.con_node_hash.getAbandonedKeyIds())
            
    
    # get all nodes mapped to their node_id {node_id : node}
    # returns : dict
    def getNodes(self):
        return self.nodes
    
    # return : str : dc_id
    def getDcId(self):
        return self.dc_id
    
    # returns : netConHash : consistent hashing object of the datacenters, contains all the 
    #                        information necessary about key allocation to nodes
    def getConHash(self):
        return self.con_node_hash

# simulation of a node.
class NetNode:
    
    # unique id within a data center
    node_id = None
    # parent datacenter id
    dc_id = None
    # db class connected to the node
    connected_db = None
    
    def __init__(self, node_id, connected_db, dc_id):
        self.node_id = node_id
        self.connected_db = connected_db
        self.dc_id = dc_id
    
    # get connected db
    # returns : netDb
    def getDb(self):
        return self.connected_db
    
    # get node id
    # returns : str
    def getNodeId(self):
        return self.node_id
    
    # get parent dc id
    # returns : str
    def getDcId(self):
        return self.dc_id