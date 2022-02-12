# Distributed Operating System Principles Project 3

# Group Members - UFID :
  1. Kavya Gopal 
  2. Rema Veeranna Gowda 

# How to run the project
  1. Unzip the folder
  2. The folder has the following files: project3.fsx and Readme.md
  3. To run the project, the following command needs to be typed:
      dotnet fsi project3.fsx numNodes numRequest

Chord DHT Protocol implementation in Scala using Akka Actor model

A F# and Akka implementation of Chord protocol for lookup in DHT network. Allows user to decide number of nodes and number of message request made by each node. Each node will make 1 lookup request per second until it has made required number of requests.

# What is working:
Chord protocol is working with below functionalities:
1. Joining the nodes to the network
2. Message transfer among nodes in network
3. Updating finger table, stabilizing the network after each addition.
4. calculating the average number of hops and time taken to transfer message to destination

# Largest network for each topology and algorithm:

Largest network working is with 30,000 nodes as input to the network