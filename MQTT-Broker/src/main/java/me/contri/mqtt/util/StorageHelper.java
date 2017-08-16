package me.contri.mqtt.util;

import io.moquette.interception.messages.InterceptPublishMessage;

import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import me.contri.mqtt.model.MqttProperties;

public class StorageHelper extends DbHelper {

	private final Set<InterceptPublishMessage> cache = new HashSet<>();
	private long lastFlushed = 0;
	private AdminHelper admin;
	
	public StorageHelper(MqttProperties properties, AdminHelper admin) {
		super(properties);
		this.admin = admin;
	}
		
	public synchronized void storeMessage(InterceptPublishMessage msg) throws ClassNotFoundException, IllegalAccessException, InstantiationException, SQLException {
		cache.add(msg);
		if(cache.size()>=properties.getCache_size() || ((System.currentTimeMillis()-lastFlushed)>=properties.getCache_flush_interval())) {
			flushCache();
		}
	}
	
	public synchronized void flushCache() throws ClassNotFoundException, IllegalAccessException, InstantiationException, SQLException {
		if (cache.size() > 0) {
			String insert = "INSERT INTO events (created_by, createdDate, topic_id, payload) VALUES ";
			insert += cache.stream()
				.map(m -> {
					String userId = admin.getAgentId(m.getUsername());
					String topicId = admin.getTopicId(m.getTopicName());
					if (userId == null || topicId == null)
						return "";
					else
						return "(" +  userId + ", utc_timestamp(), " +  topicId + ", '" + m.getPayload().toString(Charset.defaultCharset()) +"')";})
				.filter(s -> !s.trim().equalsIgnoreCase(""))
				.collect(Collectors.joining(", "));
		
			executeInsert(insert);
			lastFlushed = System.currentTimeMillis();
			cache.clear();
		}
	}
}
