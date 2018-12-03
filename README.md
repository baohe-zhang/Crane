# crane

Crane, a simple Storm-like streaming processing system, CS 425 MP4, implemented by Grp#29 Baohe Zhang & Kechen Lu. It implemented a Storm-like system, which includes spout-bolt task distribution-running and reliability schemes for failover. It also integrates the SDFS to having input files or results to be availible for all nodes in the cluster.

## Project Info

- Language: Golang
- Tested Platform: macOS 10.13, CentOS 7
- Code file structure

/bolt: Framework for users to write the bolt task and set initial information
/core: Main code relating to the crane framework, it includes the 
/core/supervisor: supervisor for distributing the tasks to /core/boltworkers or core/spoutwokers
/core/driver: Master node to process topology submitted from the client, and then build the topopogy, distribute then to supervisors with bolts or spouts
/topology: topology framework for user application to submit the topology as client
/examples: Three examples for the Crane system
/scripts: Helper scripts for updating git repo or start/stop process
/spout: spouts task interface for user application to submit 

## How-to

### Build Deamon and Deploy it

To build the Crane project, we will go to the `driver` directory and `supervisor` directory to build separately. Just run

```shell
$ cd ./core/driver
$ go build
$ cd ./core/supervisor
$ go build
```

They are core parts of Crane system which play the topology building and distributing role. To deploy on the each machine of the cluster, we have to git clone this repo like below. To simply the repo update and build in the VM cluster, we have a easy-to-use script, each time we push a new commit to remote repo then run.

```shell
$ git clone git@gitlab.engr.illinois.edu:baohez2/crane.git
$ ./scripts/update_all.sh
```

### Build application with Crane framework

We have examples in the /examples folder. There are three examples. For example, we can enter the example/join folder. Just run `go build` . Then when we want to submit a topology with bolts and tasks, just run this application.

### Run Daemon

To run our Crane daemon, go to the `./driver/`  or `./supervisor/` directory. We can use `./supervisor -h` to get command help for starting the supervisors. We run the driver(master) deamon like below

```shell
$ cd ./core/driver
$ ./driver
2018/12/02 22:02:20 Local Machine Info [172.22.156.95] [fa18-cs425-g29-01.cs.illinois.edu]

```

Then we may want to start the daemon of supervisor which would actually spawning worker pool to execute the spout or bolt tasks. Use ./supervisor -h to see the arguments. And we run the supervisor like below to connect the master node vm 1. 

```shell
$ cd ./core/supervisor
$ ./supervisor -h
flag needs an argument: -h
Usage of ./supervisor:
  -h string
    	Driver's IP address (default "127.0.0.1")
  -vm int
    	VM index in cluster
$ ./supervisor -vm 1
2018/12/02 22:18:09 Enter Cluster mode
2018/12/02 22:18:09 Local Machine Info [172.22.158.95] [fa18-cs425-g29-02.cs.illinois.edu]
2018/12/02 22:18:09 Send Join Request

```

### Run Client

To run client, just run the example user application in the examples. It would put the needed file first into the SDFS. And submit the topology to driver(master) node.

```shell
$ $ ./join 
Master IP:Port address  fa18-cs425-g29-01.cs.illinois.edu:5000
[2018-12-02 22:19:30.299910313 -0600 CST m=+0.035305081] put start
[./process.so process.so]
4460824 process.so
sKrcHkXk_U4hLpoWcUbuiO3GsU8z33038luxgmawG7w= 1543810770376949123 [{1543804514138927081 2887162465} {1543804489256858794 2887162975} {1543804523829315288 2887163489} {1543804499616544618 2887162464}]
OK
Send file finish
[put confirm from master] process.so put finished
[2018-12-02 22:19:31.544892986 -0600 CST m=+1.280287789] put finish

2018/12/02 22:19:31 Received message OK on socket 127.0.0.1:5050
2018/12/02 22:19:31 Receive Message from 127.0.0.1:5050: OK
```

Then if we have the supervisor running, it would show the log like below:

```shell
2018/12/02 22:20:29 Received message ["62","male"] on socket 172.22.158.95:6000
2018/12/02 22:20:29 Received message ["61","56"] on socket 172.22.158.95:6022
2018/12/02 22:20:29 Join Bolt Emit ([61 female 56])
2018/12/02 22:20:29 Going to send message on socket 172.22.158.95:39946
2018/12/02 22:20:29 Received message ["61","female","56"] on socket 172.22.158.95:6007
2018/12/02 22:20:30 Age Spout Emit ([63 16])
2018/12/02 22:20:30 Going to send message on socket 172.22.158.95:48666
2018/12/02 22:20:30 Gender Spout Emit ([64 female])
2018/12/02 22:20:30 Going to send message on socket 172.22.158.95:44954
2018/12/02 22:20:30 Received message ["63","female"] on socket 172.22.158.95:6000
2018/12/02 22:20:30 Received message ["62","56"] on socket 172.22.158.95:6022
2018/12/02 22:20:30 Join Bolt Emit ([62 male 56])
2018/12/02 22:20:30 Going to send message on socket 172.22.158.95:54284
2018/12/02 22:20:30 Received message ["62","male","56"] on socket 172.22.158.95:6018
2018/12/02 22:20:31 Age Spout Emit ([64 62])
2018/12/02 22:20:31 Going to send message on socket 172.22.158.95:48642
2018/12/02 22:20:31 Gender Spout Emit ([65 female])
2018/12/02 22:20:31 Going to send message on socket 172.22.158.95:44944
2018/12/02 22:20:31 Received message ["64","female"] on socket 172.22.158.95:6000
2018/12/02 22:20:31 Received message ["63","16"] on socket 172.22.158.95:6022
2018/12/02 22:20:31 Join Bolt Emit ([63 female 16])
2018/12/02 22:20:31 Going to send message on socket 172.22.158.95:52426
2018/12/02 22:20:31 Received message ["63","female","16"] on socket 172.22.158.95:6009
2018/12/02 22:20:31 Merge Bolt Emit ([63 female 16]), Collect 8 Tuples
2018/12/02 22:20:31 Merge Bolt Emit ([48 male 79]), Collect 9 Tuples
2018/12/02 22:20:31 Merge Bolt Emit ([43 male 5]), Collect 10 Tuples
2018/12/02 22:20:31 Merge Bolt Emit ([55 male 5]), Collect 11 Tuples
2018/12/02 22:20:31 Merge Bolt Emit ([24 male 14]), Collect 12 Tuples
2018/12/02 22:20:31 Age Spout Emit ([65 67])
```