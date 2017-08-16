package me.contri.mqtt.util;



import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import me.contri.mqtt.model.MqttProperties;

public class DbHelper {
	
	protected static final Logger LOG = LoggerFactory.getLogger(DbHelper.class);
	protected MqttProperties properties;
	protected Connection conn = null;
	protected ResultSet rs = null;
	protected Statement st = null;

	public DbHelper(MqttProperties properties) {
		this.properties = properties;
	}

	
	protected boolean validate() {
		if (!this.testConnection())
			return false;
		return true;
	}

	private boolean testConnection() {
		try {
			LOG.debug("Testing the connectivity to the local DB");
			openConnection();
			LOG.debug("Successfully verified the local DB connectivity");
			return true;
		} catch (Exception e) {
			LOG.error("DB instantiation error", e);
		} finally {
			closeConnection();
		}
		return false;
	}

	protected boolean isConnectionValid() throws SQLException {
		return !((conn == null) || conn.isClosed());
	}
	protected void openConnection() throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		if (!isConnectionValid()) {
			String URL = properties
					.getStorage()
					.getConnectionURL();
			Class.forName(properties.getStorage().getDriver())
						.newInstance();
			conn = DriverManager.getConnection(URL, properties
						.getStorage().getUser(), properties
						.getStorage().getPassword());
				
			LOG.debug("DB Connection successfully established");	
		} 
		

	}

	public void closeConnection() {
		if (rs != null) {
			try {
				rs.close();
			} catch (SQLException ignore) {
			}
		}
		if (st != null) {
			try {
				st.close();
			} catch (SQLException ignore) {
			}
		}
		if (conn != null) {
			try {
				conn.close();
			} catch (Exception ignore) { /* ignore close errors */
			}
		}
		LOG.debug("DB Connection closed successfully");
	}

	public String executeScalar(String query) throws InstantiationException,
			IllegalAccessException, ClassNotFoundException, SQLException {
		openConnection();
		LOG.debug(query);
		try {
			st = conn.createStatement();
			st.setMaxRows(1);
			rs = st.executeQuery(query);
			if (rs.next()) {
				String retValue = rs.getString(1);
				;
				return retValue;
			}
		} catch (SQLException sqlEx) {
			throw sqlEx;
		} finally {
			closeConnection();
		}
		return null;
	}

	public int executeScalarInt(String query) throws InstantiationException,
			IllegalAccessException, ClassNotFoundException, SQLException {
		openConnection();
		LOG.debug(query);
		try {
			st = conn.createStatement();
			st.setMaxRows(1);
			rs = st.executeQuery(query);
			if (rs.next()) {
				int retValue = rs.getInt(1);
				return retValue;
			}
		} catch (SQLException sqlEx) {
			throw sqlEx;
		} finally {
			closeConnection();
		}
		return -1;
	}

	public int executeStatement(String query) throws SQLException,
			ClassNotFoundException, IllegalAccessException,
			InstantiationException {
		openConnection();
		// Removing non-ASCII
		// query = nonASCII.matcher(query).replaceAll("");
		LOG.debug(query);
		try {
			Statement s = conn.createStatement();
			int count;
			count = s.executeUpdate(query);
			s.close();
			return count;
		} catch (SQLException sqlEx) {
			throw sqlEx;
		} finally {
			closeConnection();
		}
	}

	public int executeInsert(String query) throws SQLException,
			ClassNotFoundException, IllegalAccessException,
			InstantiationException {
		openConnection();
		// Removing non-ASCII
		// query = nonASCII.matcher(query).replaceAll("");
		LOG.debug(query);
		try {
			Statement s = conn.createStatement();
			int last_insert_id = -1;
			s.executeUpdate(query);
			rs = s.executeQuery("select last_insert_id()");
			while (rs.next()) {
				last_insert_id = rs.getInt(1);
			}
			s.close();
			return last_insert_id;
		} catch (SQLException sqlEx) {
			throw sqlEx;
		} finally {
			closeConnection();
		}
	}

	public Connection getNewConnection() throws InstantiationException,
			IllegalAccessException, ClassNotFoundException, SQLException {
		Connection conn;
		String URL = properties
				.getStorage()
				.getConnectionURL();
		Class.forName("com.mysql.jdbc.Driver").newInstance();
		conn = DriverManager.getConnection(URL, properties
				.getStorage().getUser(), properties
				.getStorage().getPassword());
		LOG.debug("DB Connection successfully established");

		return conn;
	}

	public Connection getConnection() throws InstantiationException,
			IllegalAccessException, ClassNotFoundException, SQLException {
		openConnection();
		return conn;
	}

	public ResultSet executeSelect(String query, int maxRows)
			throws SQLException, ClassNotFoundException,
			IllegalAccessException, InstantiationException {

		openConnection();
		LOG.debug(query);
		try {
			st = conn.createStatement();
			if (maxRows > 0) {
				st.setMaxRows(maxRows);
			}
			rs = st.executeQuery(query);
			return rs;
		} catch (SQLException sqlEx) {
			throw sqlEx;
		} finally {
			// closeConnection();
		}
	}

	public static String[] getKeywords() {
		String[] keywords = { "ADD", "ALL", "ALTER", "ANALYZE", "AND", "AS",
				"ASC", "ASENSITIVE", "AUTO_INCREMENT", "BDB", "BEFORE",
				"BERKELEYDB", "BETWEEN", "BIGINT", "BINARY", "BLOB", "BOTH",
				"BY", "CALL", "CASCADE", "CASE", "CHANGE", "CHAR", "CHARACTER",
				"CHECK", "COLLATE", "COLUMN", "COLUMNS", "CONDITION",
				"CONNECTION", "CONSTRAINT", "CONTINUE", "CREATE", "CROSS",
				"CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "CURSOR",
				"DATABASE", "DATABASES", "DAY_HOUR", "DAY_MICROSECOND",
				"DAY_MINUTE", "DAY_SECOND", "DEC DECIMAL", "DECLARE",
				"DEFAULT", "DELAYED", "DELETE", "DESC", "DESCRIBE",
				"DETERMINISTIC", "DISTINCT", "DISTINCTROW", "DIV", "DOUBLE",
				"DROP", "ELSE", "ELSEIF", "ENCLOSED", "ESCAPED", "EXISTS",
				"EXIT", "EXPLAIN", "FALSE", "FETCH", "FIELDS", "FLOAT", "FOR",
				"FORCE", "FOREIGN", "FOUND", "FRAC_SECOND", "FROM", "FULLTEXT",
				"GRANT", "GROUP", "HAVING", "HIGH_PRIORITY",
				"HOUR_MICROSECOND", "HOUR_MINUTE", "HOUR_SECOND", "IF",
				"IGNORE", "IN", "INDEX", "INFILE", "INNER", "INNODB", "INOUT",
				"INSENSITIVE", "INSERT", "INT", "INTEGER", "INTERVAL", "INTO",
				"IO_THREAD", "IS", "ITERATE", "JOIN", "KEY KEYS", "KILL",
				"LEADING", "LEAVE", "LEFT", "LIKE", "LIMIT", "LINES", "LOAD",
				"LOCALTIME", "LOCALTIMESTAMP", "LOCK", "LONG", "LONGBLOB",
				"LONGTEXT", "LOOP", "LOW_PRIORITY", "MASTER_SERVER_ID",
				"MATCH", "MEDIUMBLOB", "MEDIUMINT", "MEDIUMTEXT", "MIDDLEINT",
				"MINUTE_MICROSECOND", "MINUTE_SECOND", "MOD", "NATURAL", "NOT",
				"NO_WRITE_TO_BINLOG", "NULL", "NUMERIC", "ON", "OPTIMIZE",
				"OPTION", "OPTIONALLY", "OR", "ORDER", "OUT", "OUTER",
				"OUTFILE", "PRECISION", "PRIMARY", "PRIVILEGES", "PROCEDURE",
				"PURGE", "READ", "REAL", "REFERENCES", "REGEXP", "RENAME",
				"REPEAT", "REPLACE", "REQUIRE", "RESTRICT", "RETURN", "REVOKE",
				"RIGHT", "RLIKE", "SECOND_MICROSECOND", "SELECT", "SENSITIVE",
				"SEPARATOR", "SET", "SHOW", "SMALLINT", "SOME", "SONAME",
				"SPATIAL", "SPECIFIC", "SQL", "SQLEXCEPTION", "SQLSTATE",
				"SQLWARNING", "SQL_BIG_RESULT", "SQL_CALC_FOUND_ROWS",
				"SQL_SMALL_RESULT", "SQL_TSI_DAY", "SQL_TSI_FRAC_SECOND",
				"SQL_TSI_HOUR", "SQL_TSI_MINUTE", "SQL_TSI_MONTH",
				"SQL_TSI_QUARTER", "SQL_TSI_SECOND", "SQL_TSI_WEEK",
				"SQL_TSI_YEAR", "SSL", "STARTING", "STRAIGHT_JOIN", "STRIPED",
				"TABLE", "TABLES", "TERMINATED", "THEN", "TIMESTAMPADD",
				"TIMESTAMPDIFF", "TINYBLOB", "TINYINT", "TINYTEXT", "TO",
				"TRAILING", "TRUE", "UNDO", "UNION", "UNIQUE", "UNLOCK",
				"UNSIGNED", "UPDATE", "USAGE", "USE", "USER_RESOURCES",
				"USING UTC_DATE", "UTC_TIME", "UTC_TIMESTAMP", "VALUES",
				"VARBINARY", "VARCHAR", "VARCHARACTER", "VARYING", "WHEN",
				"WHERE", "WHILE", "WITH", "WRITE", "XOR", "YEAR_MONTH",
				"ZEROFILL" };
		return keywords;
	}
}

