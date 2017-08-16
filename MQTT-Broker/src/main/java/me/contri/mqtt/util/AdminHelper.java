package me.contri.mqtt.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.commons.codec.binary.Hex;

import me.contri.mqtt.model.Agent;
import me.contri.mqtt.model.AgentType;
import me.contri.mqtt.model.Constants;
import me.contri.mqtt.model.MqttProperties;
import me.contri.mqtt.model.Permission;
import me.contri.mqtt.model.Topic;

public class AdminHelper extends DbHelper {
	private final MessageDigest messageDigest;
	private HashMap<String, Agent> agents;
	private HashMap<String, Topic> topics;
	private static final String getUsersSql = 
			"select "
			+ "a.Id, "
			+ "a.Name,"
			+ "a.Password, "
			+ "a.type, "
			+ "group_concat(distinct concat(t.name, ';', `at`.permission)) as 'permissions' "
			+ "from agents a "
			+ "left join agent_topic `at` on `at`.agent_id = a.Id "
			+ "left join topics t on `at`.topic_id = t.id "
			+ "group by a.id;";
	private static final String getTopicsSql = 
			"select * from topics";
	
	public AdminHelper(MqttProperties properties) throws ClassNotFoundException, IllegalAccessException, InstantiationException, SQLException, NoSuchAlgorithmException {
		super(properties);
		this.messageDigest = MessageDigest.getInstance("SHA-256");
		init();
	}
	
	private void init() throws SQLException, ClassNotFoundException, IllegalAccessException, InstantiationException {
		loadAgents();
		loadTopics();
	}
	
	private void loadAgents() throws ClassNotFoundException, IllegalAccessException, InstantiationException, SQLException {
		agents = new HashMap<>();
		ResultSet rs = executeSelect(getUsersSql, -1);
		while(rs.next()) {
			HashMap<String, Permission> topicsPermissions = new HashMap<>();
			Arrays
				.stream(rs.getString("permissions").split(","))
				.forEach(s -> { 
					String[] parts = s.split(";");
					if (parts.length==2)
						topicsPermissions.put(parts[0], Permission.getPermissionByName(parts[1])); 
					});
			agents.put(rs.getString("name"), 
					new Agent(
							rs.getString("id"), 
							rs.getString("name"), 
							rs.getString("password"), 
							AgentType.getAgentTypeByName(rs.getString("type")),
							topicsPermissions
							)
			);
		}
	}
	
	private void loadTopics() throws ClassNotFoundException, IllegalAccessException, InstantiationException, SQLException {
		topics = new HashMap<>();
		rs = executeSelect(getTopicsSql, -1);
		while(rs.next()) {
			topics.put(rs.getString("name"), new Topic(rs.getString("id"), rs.getString("name")));
		}
	}
	
	public String getAgentId(String userName) {
		if(agents.containsKey(userName))
			return agents.get(userName).getId();
		return null;
	}
	
	public String getTopicId(String topicName) {
		if(topics.containsKey(topicName))
			return topics.get(topicName).getId() ;
		return null;
	}
	
	public boolean canRead(String topic, String user, String client) {
		if(agents.containsKey(user))
			return agents.get(user).canRead(topic);
		
		return false;
	}
	
	public boolean canWrite(String topic, String user, String client) throws ClassNotFoundException, IllegalAccessException, InstantiationException, SQLException {
		boolean canWrite = false;
		if(agents.containsKey(user))
			canWrite = agents.get(user).canWrite(topic);
		
		if(topic.equals(Constants.TOPIC_INTERNAL_SECURITY_REFRESH) && canWrite)
			init();
		return canWrite;
	}
	
	public boolean login(String clientId, String username, byte[] password) {
		// Check Username / Password in DB using sqlQuery
    	if (username == null || password == null || !agents.containsKey(username)) {
            LOG.info("username or password was null or user unknown");
            return false;
        }
    	
        messageDigest.update(password);
        byte[] digest = messageDigest.digest();
        String encodedPasswd = new String(Hex.encodeHex(digest));
    	return agents.get(username).login(encodedPasswd);
	}
}
