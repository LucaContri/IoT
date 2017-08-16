package me.contri.mqtt.broker.security;

import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;

import me.contri.mqtt.model.Constants;
import me.contri.mqtt.model.MqttProperties;
import me.contri.mqtt.model.Storage;
import me.contri.mqtt.util.AdminHelper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.moquette.server.config.IConfig;
import io.moquette.spi.impl.subscriptions.Topic;
import io.moquette.spi.security.IAuthorizator;

public class DBAuthorisation implements IAuthorizator {

	private static final Logger LOG = LoggerFactory.getLogger(DBAuthorisation.class);
	private AdminHelper admin;
    
    public DBAuthorisation(IConfig conf) throws ClassNotFoundException, IllegalAccessException, InstantiationException, SQLException, NoSuchAlgorithmException {
        this(
        		conf.getProperty(Constants.DATASOURCE_ADMIN_DRIVER , ""),
                conf.getProperty(Constants.DATASOURCE_ADMIN_CONNECTION_STRING, ""),
                conf.getProperty(Constants.DATASOURCE_ADMIN_USER, ""),
                conf.getProperty(Constants.DATASOURCE_ADMIN_PASSWORD, ""));
    }

    public DBAuthorisation(String driver, String jdbcUrl, String sqlUser, String sqlPassword) throws ClassNotFoundException, IllegalAccessException, InstantiationException, SQLException, NoSuchAlgorithmException {
    	MqttProperties adminProperties = new MqttProperties();
		adminProperties.setStorage(new Storage());
		adminProperties.getStorage().setConnectionURL(jdbcUrl);
		adminProperties.getStorage().setDriver(driver);
		adminProperties.getStorage().setUser(sqlUser);
		adminProperties.getStorage().setPassword(sqlPassword);
		
		admin = new AdminHelper(adminProperties);
    	
	}

	@Override
    public boolean canWrite(Topic topic, String user, String client) {
        try {
			return admin.canWrite(topic.toString(), user, client);
		} catch (ClassNotFoundException | IllegalAccessException
				| InstantiationException | SQLException e) {
			LOG.error("Error in DbAuthorisation.canWrite ", e);
		}
        return false;
    }

    @Override
    public boolean canRead(Topic topic, String user, String client) {
    	return admin.canRead(topic.toString(), user, client);
    }


}
