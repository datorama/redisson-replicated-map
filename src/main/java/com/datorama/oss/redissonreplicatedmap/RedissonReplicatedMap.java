package com.datorama.oss.redissonreplicatedmap;

import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.redisson.RedissonClient;
import org.redisson.core.MessageListener;
import org.redisson.core.RMap;
import org.redisson.core.RTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datorama.oss.redissonreplicatedmap.Message.Type;

public class RedissonReplicatedMap<K, V> implements Map<K, V> {

	private static Logger LOG = LoggerFactory.getLogger(RedissonReplicatedMap.class);

	private Map<K, V> internalMap = new ConcurrentHashMap<K, V>();
	private String mapName;
	private RedissonClient redissonClient;
	private RMap<K, V> distributedMap;
	private RTopic<Message> topic;
	private String instanceId;
	private ScheduledExecutorService executor;

	public RedissonReplicatedMap(RedissonClient redissonClient, String mapName) {
		init(redissonClient, mapName, -1, TimeUnit.SECONDS);
	}

	public RedissonReplicatedMap(RedissonClient redissonClient, String mapName, Integer syncInterval, TimeUnit timeUnit) {
		init(redissonClient, mapName, syncInterval, timeUnit);
	}

	public void init(RedissonClient redissonClient, String mapName, Integer syncInterval, TimeUnit timeUnit) {

		this.redissonClient = redissonClient;
		this.mapName = mapName;

		// set up automated sync
		if (syncInterval > 0) {
			executor = Executors.newScheduledThreadPool(1);
			executor.scheduleAtFixedRate(new Runnable() {
				@Override
				public void run() {
					LOG.debug("sync map: " + mapName + ", " + new Date());
					syncLocalMapWithRemote(false);
				}
			}, 1, syncInterval, timeUnit);
		}

		// instance ID must be unique
		UUID uuid = UUID.randomUUID();
		instanceId = uuid.toString();

		this.distributedMap = redissonClient.getMap(mapName);
		this.topic = redissonClient.getTopic(mapName + "_topic");
		this.topic.addListener(new MessageListener<Message>() {

			public void onMessage(String channel, Message message) {
				// ignore messages originated from this instance
				if (message.getSource().equals(instanceId)) {
					return;
				}
				LOG.debug("got message " + message.getType() + " on channel " + channel + " from source " + message.getSource());

				switch (message.getType()) {
					case PUT:
						putInternal((K) message.getKey(), (V) message.getValue(), false);
						break;
					case REMOVE:
						removeInternal((K) message.getKey(), false);
						break;
					case CLEAR:
						clearInternal(false);
						break;
					case PUT_ALL:
						putAllInternal((Map<? extends K, ? extends V>) message.getValue(), false);
						break;
				}
			}
		});

		// fill the local map
		syncLocalMapWithRemote(false);

	}

	public void cleanUp() {
		if (executor != null) {
			executor.shutdown();
			try {
				if (!executor.awaitTermination(2, TimeUnit.SECONDS)) {
					LOG.error("failed to kill all executor threads, forcing shutdownNow");
					executor.shutdownNow();
				}
			} catch (InterruptedException e) {
				LOG.error("cleanUp-> error", e);
			}
		}
	}

	public void syncRemoteMapWithLocal() {
		distributedMap.putAll(internalMap);
	}

	public void syncLocalMapWithRemote(boolean clearMap) {
		if (clearMap)
			internalMap.clear();
		// we use the readAll to reduce the number of network round trips
		for (Entry<K, V> entry : distributedMap.readAllEntrySet()) {
			internalMap.put(entry.getKey(), entry.getValue());
		}
	}

	protected V putInternal(K key, V value, boolean notify) {
		V v = internalMap.put(key, value);

		if (notify) {
			topic.publish(new Message(Type.PUT, key, value, instanceId));
		}

		return v;
	}

	public V put(K key, V value) {
		V v = putInternal(key, value, true);

		distributedMap.fastPut(key, value);

		return v;

	}

	protected V removeInternal(Object key, boolean notify) {
		V v = internalMap.remove(key);

		if (notify) {
			topic.publish(new Message(Type.REMOVE, key, null, instanceId));
		}

		return v;
	}

	public V remove(Object key) {
		V v = removeInternal(key, true);

		distributedMap.fastRemove((K) key);

		return v;
	}

	protected void putAllInternal(Map<? extends K, ? extends V> m, boolean notify) {
		internalMap.putAll(m);

		if (notify) {
			topic.publish(new Message(Type.PUT_ALL, null, m, instanceId));
		}
	}

	public void putAll(Map<? extends K, ? extends V> m) {
		putAllInternal(m, true);

		distributedMap.putAll(m);

	}

	protected void clearInternal(boolean notify) {
		internalMap.clear();
		if (notify) {
			topic.publish(new Message(Type.CLEAR, null, null, instanceId));
		}
	}

	public void clear() {
		clearInternal(true);

		distributedMap.clear();
	}

	public int size() {
		return internalMap.size();
	}

	public boolean isEmpty() {
		return internalMap.isEmpty();
	}

	public boolean containsKey(Object key) {
		return internalMap.containsKey(key);
	}

	public boolean containsValue(Object value) {
		return internalMap.containsValue(value);
	}

	public V get(Object key) {
		return internalMap.get(key);
	}

	public Set<K> keySet() {
		return internalMap.keySet();
	}

	public Collection<V> values() {
		return internalMap.values();
	}

	public Set<java.util.Map.Entry<K, V>> entrySet() {
		return internalMap.entrySet();
	}

	public String getMapName() {
		return mapName;
	}

	public void setMapName(String mapName) {
		this.mapName = mapName;
	}

	public RedissonClient getRedissonClient() {
		return redissonClient;
	}

	public void setRedissonClient(RedissonClient redissonClient) {
		this.redissonClient = redissonClient;
	}

	public String getInstanceId() {
		return instanceId;
	}

}
