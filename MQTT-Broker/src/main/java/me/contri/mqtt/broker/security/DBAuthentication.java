package me.contri.mqtt.broker.security;

import io.moquette.server.config.IConfig;
import io.moquette.spi.security.IAuthenticator;
import me.contri.mqtt.model.Constants;
import me.contri.mqtt.model.MqttProperties;
import me.contri.mqtt.model.Storage;
import me.contri.mqtt.util.AdminHelper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;

public class DBAuthentication implements IAuthenticator {
	@SuppressWarnings("unused")
	private static final Logger LOG = LoggerFactory.getLogger(DBAuthentication.class);
	private AdminHelper admin;
    
    public DBAuthentication(IConfig conf) throws ClassNotFoundException, IllegalAccessException, InstantiationException, SQLException, NoSuchAlgorithmException {
        this(
                conf.getProperty(Constants.DATASOURCE_ADMIN_DRIVER , ""),
                conf.getProperty(Constants.DATASOURCE_ADMIN_CONNECTION_STRING, ""),
                conf.getProperty(Constants.DATASOURCE_ADMIN_USER, ""),
                conf.getProperty(Constants.DATASOURCE_ADMIN_PASSWORD, ""));
    }


    public DBAuthentication(String driver, String jdbcUrl, String sqlUser, String sqlPassword) throws ClassNotFoundException, IllegalAccessException, InstantiationException, SQLException, NoSuchAlgorithmException {
    	MqttProperties adminProperties = new MqttProperties();
		adminProperties.setStorage(new Storage());
		adminProperties.getStorage().setConnectionURL(jdbcUrl);
		adminProperties.getStorage().setDriver(driver);
		adminProperties.getStorage().setUser(sqlUser);
		adminProperties.getStorage().setPassword(sqlPassword);
		
		admin = new AdminHelper(adminProperties);
    }

    @Override
    public synchronized boolean checkValid(String clientId, String username, byte[] password) {
    	return admin.login(clientId, username, password);
    }
}
