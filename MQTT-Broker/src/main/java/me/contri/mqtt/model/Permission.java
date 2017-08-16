package me.contri.mqtt.model;

public enum Permission {
	READ("Read"),
	WRITE("Write"),
	READ_WRITE("Read/Write");
	
	public String name;
	
	Permission(String name) {
		this.name = name;
	}
	
	public static Permission getPermissionByName(String permissionName) {
		if (permissionName == null)
			return null;
		for (Permission permission : Permission.values()) {
			if (permission.name.equalsIgnoreCase(permissionName))
				return permission;
		}
		return null;
	}
}
