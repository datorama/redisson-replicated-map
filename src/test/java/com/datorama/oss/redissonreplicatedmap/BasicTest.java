package com.datorama.oss.redissonreplicatedmap;
import org.junit.Assert;
import org.junit.Test;
import org.redisson.Config;
import org.redisson.Redisson;
import org.redisson.RedissonClient;

import com.datorama.oss.redissonreplicatedmap.RedissonReplicatedMap;

public class BasicTest {

	private RedissonClient getRedissonClient() {
		Config config = new Config();
		config.useSingleServer().setAddress("127.0.0.1:6379");
		RedissonClient redisson = Redisson.create(config);

		return redisson;
	}

	@Test
	public void mapInitTest() throws InterruptedException {

		RedissonClient redisson = getRedissonClient();

		RedissonReplicatedMap<String, Integer> map1 = new RedissonReplicatedMap<String, Integer>(redisson, "m1");
		// first clean up (as the maps are persistence)
		map1.clear();

		// now test
		map1.put("k1", 1);
		map1.put("k2", 1);

		RedissonReplicatedMap<String, Integer> map2 = new RedissonReplicatedMap<String, Integer>(redisson, "m1");

		Thread.sleep(200);

		Assert.assertTrue(map2.size() == 2);

		redisson.shutdown();
	}

	@Test
	public void simplePutTest() throws InterruptedException {

		RedissonClient redisson = getRedissonClient();

		RedissonReplicatedMap<String, Integer> map1 = new RedissonReplicatedMap<String, Integer>(redisson, "m1");
		// first clean up (as the maps are persistence)
		map1.clear();

		// now test
		map1.put("k1", 1);

		RedissonReplicatedMap<String, Integer> map2 = new RedissonReplicatedMap<String, Integer>(redisson, "m1");

		map2.put("k2", 1);

		Thread.sleep(200);

		Assert.assertTrue(map1.size() == 2);

		redisson.shutdown();
	}

	@Test
	public void clearTest() throws InterruptedException {

		RedissonClient redisson = getRedissonClient();

		RedissonReplicatedMap<String, Integer> map1 = new RedissonReplicatedMap<String, Integer>(redisson, "m1");
		// first clean up (as the maps are persistence)
		map1.clear();

		// now test
		map1.put("k1", 1);

		RedissonReplicatedMap<String, Integer> map2 = new RedissonReplicatedMap<String, Integer>(redisson, "m1");

		map2.clear();

		Thread.sleep(200);

		Assert.assertTrue(map1.isEmpty());

		redisson.shutdown();
	}

	@Test
	public void removeTest() throws InterruptedException {

		RedissonClient redisson = getRedissonClient();

		RedissonReplicatedMap<String, Integer> map1 = new RedissonReplicatedMap<String, Integer>(redisson, "m1");
		// first clean up (as the maps are persistence)
		map1.clear();

		// now test
		map1.put("k1", 1);

		RedissonReplicatedMap<String, Integer> map2 = new RedissonReplicatedMap<String, Integer>(redisson, "m1");

		map2.remove("k1");

		Thread.sleep(200);

		Assert.assertTrue(map1.isEmpty());

		redisson.shutdown();
	}

	@Test
	public void getTest() throws InterruptedException {

		RedissonClient redisson = getRedissonClient();

		RedissonReplicatedMap<String, Integer> map1 = new RedissonReplicatedMap<String, Integer>(redisson, "m1");
		// first clean up (as the maps are persistence)
		map1.clear();

		// now test
		map1.put("k1", 1);

		RedissonReplicatedMap<String, Integer> map2 = new RedissonReplicatedMap<String, Integer>(redisson, "m1");

		Assert.assertTrue(map2.containsKey("k1"));

		map1.put("k1", 2);

		Thread.sleep(200);

		Assert.assertTrue(map2.get("k1").equals(2));

		redisson.shutdown();
	}

	@Test
	public void getTestWhileClusterIsDown() throws InterruptedException {

		RedissonClient redisson = getRedissonClient();

		RedissonReplicatedMap<String, Integer> map1 = new RedissonReplicatedMap<String, Integer>(redisson, "m1");
		// first clean up (as the maps are persistence)
		map1.clear();

		// now test
		map1.put("k1", 1);

		RedissonReplicatedMap<String, Integer> map2 = new RedissonReplicatedMap<String, Integer>(redisson, "m1");

		Assert.assertTrue(map2.containsKey("k1"));

		map1.put("k1", 2);

		redisson.shutdown();

		Thread.sleep(200);

		Assert.assertTrue(map2.get("k1").equals(2));

	}

}
