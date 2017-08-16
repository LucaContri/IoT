package me.contri.mqtt.model;

public enum AgentType {
	SENSOR("sensor"),
	GATEWAY("gateway"),
	HUMAN("human");
	
	public String name;
	
	AgentType(String name) {
		this.name = name;
	}
	
	public static AgentType getAgentTypeByName(String agentTypeName) {
		if (agentTypeName == null)
			return null;
		for (AgentType agentType : AgentType.values()) {
			if (agentType.name.equalsIgnoreCase(agentTypeName))
				return agentType;
		}
		return null;
	}
}
