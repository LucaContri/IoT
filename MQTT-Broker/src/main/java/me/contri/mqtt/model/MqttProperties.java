package me.contri.mqtt.model;

public class MqttProperties {

	private Storage storage;
	private int cache_size = 100;
	private int cache_flush_interval = 1000;
	
	public Storage getStorage() {
		return storage;
	}

	public void setStorage(Storage storage) {
		this.storage = storage;
	}

	public int getCache_size() {
		return cache_size;
	}

	public void setCache_size(int cache_size) {
		this.cache_size = cache_size;
	}
	
	public void setCache_size(String cache_size) {
		this.cache_size = Integer.parseInt(cache_size);
	}

	public int getCache_flush_interval() {
		return cache_flush_interval;
	}

	public void setCache_flush_interval(int cache_flush_interval) {
		this.cache_flush_interval = cache_flush_interval;
	}
	
	public void setCache_flush_interval(String cache_flush_interval) {
		this.cache_flush_interval = Integer.parseInt(cache_flush_interval);
	}
	
}
