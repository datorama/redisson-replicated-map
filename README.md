# redisson-replicated-map
## Replicated map implementation on top of Redission (Redis java client)

A replicated map is an eventually consistent, distributed map that eliminates the network and serialization overhead when compared to a standard distributed map (like Redission RMap). A replicated map does not partition data, instead it replicates the data to all the different clients.

## Getting started
```java
//create a Redisson Client instance to be used by our replicated map
Config config = new Config();
config.useSingleServer().setAddress("127.0.0.1:6379");
RedissonClient redisson = Redisson.create(config);

//create an instance of a replicated map
RedissonReplicatedMap<String, Integer> map = new RedissonReplicatedMap<String, Integer>(redisson, "myReplicatedMap");
map1.put("myKey", 1);
...

redisson.shutdown();
			
```
## Note that...
Each RedissonReplicatedMap instance basically registers to the messaging channel so every change to every instance will trigger a change in all other instances (in process and out of process) that are using the same replicated map name ("myReplicatedMap" in the above example)
