This code is written to show the working of consistent hashing algorithm.
The code for consistent hashing can be found at .\netConhash\ConHash.py (this is the crux of this project!)

Rest of the code are driven by NetSim.py file, it represents a virtual network divided into data centers, that are further
divided into servers making use of the ConHash module to divide and store information.

The network is represented by the sampleNetJson.json, which is read using module in jsonNetReader to form the virtual 
representation of network.