package me.contri.mqtt.model;

import java.util.HashMap;

public class Agent {
	private String id, name, encodedPassword;
	private AgentType type;
	private HashMap<String, Permission> topicsPermissions = new HashMap<>();

	public Agent(String id, String name, String encodedPassword, AgentType type, HashMap<String, Permission> topicsPermissions) {
		super();
		this.id = id;
		this.name = name;
		this.encodedPassword = encodedPassword;
		this.type = type;
		this.topicsPermissions = topicsPermissions;
	}
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public AgentType getType() {
		return type;
	}

	public void setType(AgentType type) {
		this.type = type;
	}

	public HashMap<String, Permission> getTopicsPermissions() {
		return topicsPermissions;
	}

	public void setTopicsPermissions(
			HashMap<String, Permission> topicsPermissions) {
		this.topicsPermissions = topicsPermissions;
	}

	public boolean canRead(String topic) {
		return topicsPermissions.get(topic) != null
				&& (topicsPermissions.get(topic).equals(Permission.READ) 
				|| topicsPermissions.get(topic).equals(Permission.READ_WRITE));
	}
	
	public boolean canWrite(String topic) {
		return topicsPermissions.get(topic) != null
				&& (topicsPermissions.get(topic).equals(Permission.WRITE) 
				|| topicsPermissions.get(topic).equals(Permission.READ_WRITE));
	}
	
	public boolean login(String encodedPassword) {
		return this.encodedPassword.equals(encodedPassword);
	}
}
