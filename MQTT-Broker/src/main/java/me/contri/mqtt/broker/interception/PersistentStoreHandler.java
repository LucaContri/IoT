package me.contri.mqtt.broker.interception;


import java.io.File;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;

import me.contri.mqtt.model.Constants;
import me.contri.mqtt.model.MqttProperties;
import me.contri.mqtt.model.Storage;
import me.contri.mqtt.util.AdminHelper;
import me.contri.mqtt.util.StorageHelper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.moquette.interception.InterceptHandler;
import io.moquette.interception.messages.InterceptAcknowledgedMessage;
import io.moquette.interception.messages.InterceptConnectMessage;
import io.moquette.interception.messages.InterceptConnectionLostMessage;
import io.moquette.interception.messages.InterceptDisconnectMessage;
import io.moquette.interception.messages.InterceptPublishMessage;
import io.moquette.interception.messages.InterceptSubscribeMessage;
import io.moquette.interception.messages.InterceptUnsubscribeMessage;
import io.moquette.server.Server;
import io.moquette.server.config.FileResourceLoader;
import io.moquette.server.config.IConfig;
import io.moquette.server.config.IResourceLoader;
import io.moquette.server.config.ResourceLoaderConfig;

public class PersistentStoreHandler implements InterceptHandler {

	private static final Logger LOG = LoggerFactory.getLogger(PersistentStoreHandler.class);
	private StorageHelper storage;
	private AdminHelper admin;
	
	public PersistentStoreHandler(Server server) throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException, SQLException, NoSuchAlgorithmException {
		init(getConfig());
	}
	
	private void init(IConfig config) throws ClassNotFoundException, IllegalAccessException, InstantiationException, SQLException, NoSuchAlgorithmException {
		MqttProperties adminProperties = new MqttProperties();
		adminProperties.setStorage(new Storage());
		adminProperties.getStorage().setConnectionURL(config.getProperty(Constants.DATASOURCE_ADMIN_CONNECTION_STRING));
		adminProperties.getStorage().setDriver(config.getProperty(Constants.DATASOURCE_ADMIN_DRIVER));
		adminProperties.getStorage().setUser(config.getProperty(Constants.DATASOURCE_ADMIN_USER));
		adminProperties.getStorage().setPassword(config.getProperty(Constants.DATASOURCE_ADMIN_PASSWORD));
		
		MqttProperties storageProperties = new MqttProperties();
		storageProperties.setStorage(new Storage());
		storageProperties.getStorage().setConnectionURL(config.getProperty(Constants.DATASOURCE_STORAGE_CONNECTION_STRING));
		storageProperties.getStorage().setDriver(config.getProperty(Constants.DATASOURCE_STORAGE_DRIVER));
		storageProperties.getStorage().setUser(config.getProperty(Constants.DATASOURCE_STORAGE_USER));
		storageProperties.getStorage().setPassword(config.getProperty(Constants.DATASOURCE_STORAGE_PASSWORD));
		storageProperties.setCache_size(config.getProperty(Constants.DATASOURCE_STORAGE_CACHE_MAX_SIZE, "100"));
		storageProperties.setCache_flush_interval(config.getProperty(Constants.DATASOURCE_STORAGE_CACHE_FLUSH_INTERVAL_MS, "1000"));
		
		admin = new AdminHelper(adminProperties);
		storage = new StorageHelper(storageProperties, admin);
	}
	
    private IConfig getConfig() throws IOException {
        File defaultConfigurationFile = defaultConfigFile();
        LOG.info("Starting Moquette server. Configuration file path={}", defaultConfigurationFile.getAbsolutePath());
        IResourceLoader filesystemLoader = new FileResourceLoader(defaultConfigurationFile);
        return new ResourceLoaderConfig(filesystemLoader);
        
    }

    private static File defaultConfigFile() {
        String configPath = System.getProperty("moquette.path", null);
        return new File(configPath, IConfig.DEFAULT_CONFIG);
    }
    
	@Override
	public String getID() {
		return PersistentStoreHandler.class.getName();
	}

	@Override
	public Class<?>[] getInterceptedMessageTypes() {
		return new Class[] {InterceptPublishMessage.class, InterceptConnectionLostMessage.class, InterceptDisconnectMessage.class};
	}

	@Override
	public void onConnect(InterceptConnectMessage arg0) {
		// Auto-generated method stub
	}

	@Override
	public void onConnectionLost(InterceptConnectionLostMessage arg0) {
		try {
			storage.flushCache();
		} catch (ClassNotFoundException | IllegalAccessException
				| InstantiationException | SQLException e) {
			LOG.error("Error flushing cache", e);
		}
	}

	@Override
	public void onDisconnect(InterceptDisconnectMessage arg0) {
		try {
			storage.flushCache();
		} catch (ClassNotFoundException | IllegalAccessException
				| InstantiationException | SQLException e) {
			LOG.error("Error flushing cache", e);
		}
	}

	@Override
	public void onMessageAcknowledged(InterceptAcknowledgedMessage arg0) {
		// Auto-generated method stub
	}

	@Override
	public void onPublish(InterceptPublishMessage msg) {
		LOG.debug("Storing intercepted message: " + msg.toString());
		try {
			storage.storeMessage(msg);
		} catch (ClassNotFoundException | IllegalAccessException
				| InstantiationException | SQLException e) {
			LOG.error("Error in storing intercepted msg", e);
		}
	}

	@Override
	public void onSubscribe(InterceptSubscribeMessage arg0) {
		// Auto-generated method stub
	}

	@Override
	public void onUnsubscribe(InterceptUnsubscribeMessage arg0) {
		// Auto-generated method stub
	}

}

