from math import floor

class ConHash:
    # {node_hash : node_id}
    net_con_node_hash = None
    # {key_hash : key}
    net_con_key_hash = None
    # list of node_hash sorted in ascending order
    sorted_node_hash = None
    # {node_id : [key_hash1, key_hash2...]}
    node_vs_keys = None
    # {node_id : [node_hash1, node_hash2...]}
    node_vs_hash_map = None
    # {key_hash : [node_id1, node_id2...]}
    key_vs_nodes = None
    # {key_hash :
    #    {"key_id": key_id,
    #     "num_of_copies": number of times abandoned}
    # }
    abandoned_keys = None
    # {key_hash : copy_num}
    key_vs_copies = None

    def __init__(self):
        self.net_con_node_hash = dict()
        self.net_con_key_hash = dict()
        self.sorted_node_hash = None
        self.node_vs_keys = dict()
        self.node_vs_hash_map = dict()
        self.key_vs_nodes = dict()
        self.abandoned_keys = dict()
        self.key_vs_copies = dict()

    # store nodes into the consistent hash object
    # node_id : str : nodes are represented by node_id in the object
    # virtual_copy_num : int : number of copies including original of node in hash obj
    # returns : dict : rehashed keys with new nodes, due to the node addition
    def storeNodeHash(self, node_id, virtual_copy_num):
        # hash the object using default python function
        # TO_DO: potential option to override hash function usinf user set hash functions
        hash_val = self.customHash(node_id)
        # check if the hash is already in cache, if so, run hash on the hash value
        # this way we can make sure to get only unque values.
        while hash_val in self.net_con_node_hash:
            hash_val = self.customHash(str(hash_val)+"string_to_randomize")

        # following part corresponds to adding all values to variable/cache/dict
        self.net_con_node_hash[hash_val] = node_id

        if node_id not in self.node_vs_hash_map:
            self.node_vs_hash_map[node_id] = set()
        self.node_vs_hash_map[node_id].add(hash_val)

        if virtual_copy_num > 1:
            # recursively call the function copy - 1 times
            ##print("recursive calls")
            ##print(self.node_vs_hash_map[node_id])
            self.storeNodeHash(node_id, virtual_copy_num - 1)
        else:
            ##print("reassignment of keys..")
            ##print(self.node_vs_hash_map)
            # if done with all the recursive loops sort and store all node hashes
            self.setSortedNodeHashList()

            # if there are any keys in the network,
            # we need to find out the keys that needs reassignment after
            # the adding of new node
            if len(self.net_con_key_hash) > 0:
                keys_to_reallocate = dict()
                for node_hash in self.node_vs_hash_map[node_id]:
                    node_hash_index = self.sorted_node_hash.index(node_hash)
                    prev_node_hash_index = node_hash_index - 1

                    if prev_node_hash_index < 0:
                        prev_node_hash_index = len(self.sorted_node_hash) - 1

                    # make sure the previous index doesnt correspond to the same node.
                    # if it is, back step once more
                    while self.sorted_node_hash[prev_node_hash_index] in self.node_vs_hash_map[node_id]:
                        prev_node_hash_index -= 1
                        if prev_node_hash_index < 0:
                            prev_node_hash_index = len(self.sorted_node_hash) - 1

                    # find the keys that are effected by the node addition
                    # the key hash of that needs reallocation is between
                    # the two indices, prev_node_hash_index and node_hash_index (circular)
                    prev_node_hash = self.sorted_node_hash[prev_node_hash_index]
                    prev_node_id = self.net_con_node_hash[prev_node_hash]
                    temp_keys_to_reallocate = self.node_vs_keys[prev_node_id]
                    for key_hash in temp_keys_to_reallocate:
                        if prev_node_hash_index > node_hash_index:
                            if (key_hash > prev_node_hash and key_hash >= node_hash) or (key_hash < prev_node_hash and key_hash <= node_hash):
                                if key_hash not in keys_to_reallocate:
                                    keys_to_reallocate[key_hash] = self.key_vs_copies[key_hash]
                                    self.delKeyFromConHash(self.net_con_key_hash[key_hash])
                        else:
                            if key_hash > prev_node_hash and key_hash <= node_hash:
                                if key_hash not in keys_to_reallocate:
                                    keys_to_reallocate[key_hash] = self.key_vs_copies[key_hash]
                                    self.delKeyFromConHash(self.net_con_key_hash[key_hash])

                # add the keys to the abandoned key list
                for key_hash in keys_to_reallocate:
                    self.addToAbandonedKeys(key_hash, keys_to_reallocate[key_hash], False)
            
                # rehash and return the reallocated keys with the new node
                return self.rehashAbandonedKeysAndGetNodeDict()


    # calling this function will store the key in the hash and return all the nodes
    # it is stored as a list of node_id
    # key : str : key as in data's key value pair
    # copy_num : int : number of copies to be stored in the network
    # returns: list of node_id(str) or None
    def storeKeyInConHashAndGetNodes(self, key, copy_num):

        # get the node_id_list
        node_id_list = self.getNodeIdForKey(key, copy_num)

        # if the list is not none(usually means not enough nodes to store copies)
        # store everything into appropriate cache
        if node_id_list != None:
            key_hash = self.customHash(key)
            if key_hash not in self.net_con_key_hash:
                self.net_con_key_hash[key_hash] = key

            self.key_vs_copies[key_hash] = len(node_id_list)

            for node_id in node_id_list:
                if node_id not in self.node_vs_keys:
                    self.node_vs_keys[node_id] = set()
                self.node_vs_keys[node_id].add(key_hash)
                if key_hash not in self.key_vs_nodes:
                    self.key_vs_nodes[key_hash] = set()
                self.key_vs_nodes[key_hash].add(node_id)
        return node_id_list

    # use this function to remove a particular key from the network
    # it is non verbose function wont tell throw error if the key is not in the network.
    # key : str : key as in data's key value pair, to be removed
    def delKeyFromConHash(self, key):
        key_hash = self.customHash(key)
        if key_hash in self.key_vs_nodes:
            for node_id in self.key_vs_nodes[key_hash]:
                if key_hash in self.node_vs_keys[node_id]:
                    self.node_vs_keys[node_id].remove(key_hash)
            del self.key_vs_nodes[key_hash]
        if key_hash in self.abandoned_keys:
            del self.abandoned_keys[key_hash]
        if key_hash in self.net_con_key_hash:
            del self.net_con_key_hash[key_hash]
        if key_hash in self.key_vs_copies:
            del self.key_vs_copies[key_hash]

    # use this function to remove node from hash
    # node_id : str : node_id corresponding to node to be removed
    def delNodeFromConHash(self, node_id):
        # check if the node_id is actually in the hash obj
        if node_id in self.node_vs_hash_map:
            all_hash_for_node = self.node_vs_hash_map[node_id]
            # remove node from appropriate cache
            for node_hash in all_hash_for_node:
                if node_hash in self.net_con_node_hash:
                    del self.net_con_node_hash[node_hash]
            del self.node_vs_hash_map[node_id]
            if node_id in self.node_vs_keys:
                for key_hash in self.node_vs_keys[node_id]:

                    if key_hash in self.key_vs_nodes:
                        if node_id in self.key_vs_nodes[key_hash]:
                            self.key_vs_nodes[key_hash].remove(node_id)
                        if len(self.key_vs_nodes[key_hash]) == 0:
                            del self.key_vs_nodes[key_hash]

                    # add each of the keys in the node into the list of
                    # "abandoned keys" and increment count as needed
                    self.addToAbandonedKeys(key_hash, 1)

                del self.node_vs_keys[node_id]
            # re sort the hash list
            self.setSortedNodeHashList()
        else:
            # shows explicit message when the node id is not present in the hash object
            print("Node with node ID {} not in the consistent hash map.".format(node_id))

    # this function should be called after one or more node removal to re-allot the keys
    # to other nodes as needed(as many times as its removed) the keys are removed from
    # cache if they are succesfullly alloted a new node, if not the key will remain in abandoned cache
    # returns : dict : {key : [node_id1. node_id2,...]}
    def rehashAbandonedKeysAndGetNodeDict(self):
        # tell user there are no keys to be rehashed if the abandoned key cache is empty
        if len(self.abandoned_keys) == 0:
            print("No abandoned keys to rehash.")
            return None
        else:
            # otherwise:

            #initiate return variable {key : [node_id1. node_id2,...]}
            key_vs_new_node = dict()

            for key_hash in self.abandoned_keys:
                # try to store the keys
                new_node_id_list = self.storeKeyInConHashAndGetNodes(self.net_con_key_hash[key_hash],
                                                                     self.abandoned_keys[key_hash]["num_of_copies"])
                # remove from cache if succesful
                # and add it to the return variable.
                if new_node_id_list != None:
                    del self.abandoned_keys[key_hash]
                    key = self.net_con_key_hash[key_hash]
                    if key not in key_vs_new_node:
                        key_vs_new_node[key] = set()
                    key_vs_new_node[key].update(new_node_id_list)

            # let user know if the rehashing is succesful or if there are any key
            # that couldnt be rehashed
            if len(self.abandoned_keys) == 0:
                print("All values rehashed.")
            else:
                print("{} keys yet to be rehashed, not enough node space.".format(len(self.abandoned_keys)))
            return key_vs_new_node


    # core of the object, finds the appropriate node the key needs to be assigned to
    # if there are not enough nodes to assign the value to returns None.
    # and return the nodes its allocated
    # key : str : key to be assigned
    # copy_num : int : number of copies that needs to be stored(on separate node)
    # returns : list of node_id : [str, ...]
    def getNodeIdForKey(self, key, copy_num=2):
        # use default hash function to find the hash of the key passed
        # TO_DO: check potential options to add custom hash function instead of the default one.
        key_hash = self.customHash(key)
        num_nodes = len( self.sorted_node_hash)

        # this variable stores the number of nodes in which the nodes are not already present
        unique_num_nodes_without_key = len(self.node_vs_hash_map)
        if key_hash in self.key_vs_nodes:
            print("key already present: " + str(self.net_con_key_hash[key_hash]))
            unique_num_nodes_without_key -= len(self.key_vs_nodes[key_hash])

        # store the node_id that the key is going to be stored in this variable
        node_id_list = set()

        # if there are any nodes in the network and if there are enough nodes to give
        # requested number of copies of key storage space continue
        if num_nodes > 0 and copy_num <= unique_num_nodes_without_key:

            sorted_hash_storage_index = None

            # if there is only one node or if the key_hash is greater than the
            # biggest node_hash then it belongs to the first node.
            if num_nodes == 1 or key_hash >  self.sorted_node_hash[num_nodes-1]:
                node_id_list.add(self.net_con_node_hash[ self.sorted_node_hash[0]])
                sorted_hash_storage_index = 0

            # from this point we will be using modified binary search to find
            # the first node_hash to store our key in
            high = num_nodes-1
            low = 0
            mid = floor(num_nodes/2)

            temp_node_id = None

            # while we havint found the first node value:
            while sorted_hash_storage_index == None:
                # if the the node_hash at mid value is lesser than the key_hash:
                if  self.sorted_node_hash[mid] < key_hash:
                    # move search region to the higher half
                    low = mid
                    mid = floor((low + high)/2)
                    
                    if low == mid:
                        sorted_hash_storage_index = high
                        
                # else if its greater:
                elif  self.sorted_node_hash[mid] > key_hash:
                    # if mid > 0:
                    if mid-1 >= 0:
                        # if node_hash at mid-1 is lesser and mid greater than the
                        # key hash, then it should be assigned to the mid node_hash
                        if  self.sorted_node_hash[mid-1] < key_hash:
                            sorted_hash_storage_index = mid
                        # if key_hash = node_hash at mid-1 assign to mid-1
                        elif  self.sorted_node_hash[mid-1] == key_hash:
                            sorted_hash_storage_index = mid-1
                        # if both node_hash at mid-1 and mid are greater than key_hash
                        # time to search the lower half:
                        else:
                            high = mid-1
                            mid = floor((low + high)/2)
                    # if the mid-1 index is smaller than 0, mid is 0
                    # and we know key_hash is < node at mid
                    # which means it should be assigned to node at 0
                    else:
                        sorted_hash_storage_index = 0
                # if node_hash at mid is = key_hash assign it to the node
                else:
                    sorted_hash_storage_index = mid

            temp_node_id = self.net_con_node_hash[ self.sorted_node_hash[sorted_hash_storage_index]]
            
            # if the key is already in the node
            if (key_hash in self.key_vs_nodes) and (temp_node_id not in self.key_vs_nodes[key_hash]):
                node_id_list.add(temp_node_id)
                
            # this part takes care of storing the keys multiple times in adjacent node_hash
            # with a check to make sure none are stored more than once into a single node
            # exits only when it has found enough nodes.
            while len(node_id_list) != copy_num:

                # circular array logic, if the index is at the end,
                # the increment takes it to the start
                if sorted_hash_storage_index == (num_nodes-1):
                    sorted_hash_storage_index = 0
                else:
                    sorted_hash_storage_index += 1

                temp_node_id = self.net_con_node_hash[ self.sorted_node_hash[sorted_hash_storage_index]]
                # if the node_id is not already in the list add
                ##if (temp_node_id not in node_id_list) and (temp_node_id not in self.key_vs_nodes[key_hash]):
                if (temp_node_id not in node_id_list) and ((key_hash not in self.key_vs_nodes) or (key_hash in self.key_vs_nodes and temp_node_id not in self.key_vs_nodes[key_hash])) :
                    node_id_list.add(temp_node_id)

            return node_id_list

        # if there are not enough nodes, print the appropriate message
        elif copy_num > unique_num_nodes_without_key:
            print("Not enough nodes to store given copy num.\n Copy Num: {} \n Nodes num: {}".format(copy_num,
                                                                                                     unique_num_nodes_without_key))
        else:
            print("No nodes to store in!")

        return None

    # sort and store all the node_hash present in the obj
    def setSortedNodeHashList(self):
        self.sorted_node_hash = sorted(self.net_con_node_hash.keys())

    # use to get the nodes that has the key given
    # key : str : key for which you want the node list
    # returns : list : [nodes with particular key]
    def getNodesWithKey(self, key):
        key_hash = self.customHash(key)
        if key_hash in self.key_vs_nodes:
            return self.key_vs_nodes[self.customHash(key)]
        print("Key not available in the network.")
        return None

    # get all keys in a node
    # returns : list() of str : [key_id1, key_id2...]
    def getKeysInNode(self, node_id):
        node_hash_list = self.node_vs_hash_map[node_id]
        ret_key_list = set()
        for node_hash in node_hash_list:
            if node_hash in self.node_vs_keys:
                for key_hash in self.node_vs_keys:
                    ret_key_list.add(self.net_con_key_hash[key_hash])
        return ret_key_list

    # use to store all the keys in a disconnected node to abandoned_keys variable
    # key_hash : int : key_hash
    # num_of_copies : int : number of times the key was abandoned
    # is_inc : boolean : to increment the count given, or reset the value to the count.
    def addToAbandonedKeys(self, key_hash, num_of_copies, is_inc=True):
        if key_hash not in self.abandoned_keys:
            self.abandoned_keys[key_hash] = {
                    "key_id" : self.net_con_key_hash[key_hash],
                    "num_of_copies" : 0
                }
        if is_inc:
            self.abandoned_keys[key_hash]["num_of_copies"] += num_of_copies
        else:
            self.abandoned_keys[key_hash]["num_of_copies"] = num_of_copies

    # get all keys in the abandoned key list
    def getAbandonedKeyIds(self):
        key_id_set = set()
        for key_hash in self.abandoned_keys:
            key_id_set.add(self.abandoned_keys[key_hash]["key_id"])
        return key_id_set
    
    # a simple hashing function that makes use of python's general hashing function
    # only gives positive numbers(replacing this with any proper hashing function
    # should not effect the conHash class, and thus is flexible to changes)
    # any : val : value to hash
    # returns : str : hash_val
    def customHash(self, val):
        hash_val = hash(val)
        
        while hash_val < 0:
            val = str(val)+"rondomizerString"
            hash_val = hash(val)
            
        return hash_val
