package it.sweetlab.db;

import it.sweetlab.util.IOUtils;

import java.io.File;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;

public class JNDIConf {

	private static boolean configured = false;

	/** Solitamente java:/comp/env */
	private static String jndiKey;

	/** Chiave sotto la quale e' mappato il nome del servizio JDBC tramite JNDI */
	private static String jdbcKey;

	/** Chiave che identifica l'appender per il logger */
	private static String loggerKey;

	/** Chiave che identifica l'appender per il logger */
	private static String sqlRepository;

	/** Se true imposta un valure di autocommit 
	 * (Per compatibilita' tra i Servlet Container) */
	private static boolean setAutocommit;
	/** Se setAutoCommit = true, imposto questo valore di autocommit. */
	private static boolean autocommit;

	private static boolean checkSQLFileReload;
	
	static {
    	// Lettura dei dati dal property file:
        try {
            InputStream is = JNDIConf.class.getResourceAsStream("/sqlrepository.properties");
            Properties props = new Properties();
            props.load(is);
            is.close();
            String sqlRepository = IOUtils.getResourceLocation("/sqlrepository.properties").replace("sqlrepository.properties", "");
            configure(
				props.getProperty("jndiKey"), 
				props.getProperty("jdbcKey"),
				props.getProperty("loggerKey"),
				sqlRepository,
				Boolean.parseBoolean(props.getProperty("checkSQLFileReload")),
				Boolean.parseBoolean(props.getProperty("setAutocommit")),
				Boolean.parseBoolean(props.getProperty("autocommit"))
            );
            if (StringUtils.isNotEmpty(props.getProperty("test.query"))) {
            	try {
            		DataLink dl = new DataLink();
            		if (dl.execQuery(props.getProperty("test.query")).isEmpty()) {
            			JNDIConf.configured = false;
            		}
            	} catch (Exception e) {
            		JNDIConf.configured = false;            		
            	}
            }
        }
        catch (Throwable t) {
            System.out.println("Errore ad inizializzare il processo!"); // Nothing
            t.printStackTrace();
        }		
	}
	
	public static void configure(
		String jndiKey, 
		String jdbcKey,
		String loggerKey,
		String sqlRepository,
		boolean checkSQLFileReload,
		boolean setAutocommit,
		boolean autocommit
	) {
		JNDIConf.jndiKey = jndiKey;
		JNDIConf.jdbcKey = jdbcKey;
		JNDIConf.loggerKey = loggerKey;
		JNDIConf.sqlRepository = sqlRepository.replace('/',File.separatorChar);
		JNDIConf.checkSQLFileReload = checkSQLFileReload;
		JNDIConf.configured = true;
		JNDIConf.setAutocommit = setAutocommit;
		JNDIConf.autocommit = autocommit;
	}

	public static boolean isConfigured() {
		return configured;
	}

	public static void setConfigured(boolean value) {
		configured = value;
	}

	public static String getJndiKey() {
		return jndiKey;
	}

	public static void setJndiKey(String value) {
		jndiKey = value;
	}

	public static String getJdbcKey() {
		return jdbcKey;
	}

	public static void setJdbcKey(String value) {
		jdbcKey = value;
	}

	public static String getLoggerKey() {
		return loggerKey;
	}

	public static void setLoggerKey(String value) {
		loggerKey = value;
	}

	public static String getSqlRepository() {
		return sqlRepository;
	}

	public static void setSqlRepository(String sqlRepository) {
		JNDIConf.sqlRepository = sqlRepository;
	}

	public static boolean isCheckSQLFileReload() {
		return checkSQLFileReload;
	}

	public static void setCheckSQLFileReload(boolean checkSQLFileReload) {
		JNDIConf.checkSQLFileReload = checkSQLFileReload;
	}

	public static boolean isAutocommit() {
		return autocommit;
	}

	public static void setAutocommit(boolean autocommit) {
		JNDIConf.autocommit = autocommit;
	}

	public static boolean isSetAutocommit() {
		return setAutocommit;
	}

	public static void setSetAutocommit(boolean setAutocommit) {
		JNDIConf.setAutocommit = setAutocommit;
	}
	
	public static void main(String[] args) {
		System.out.println("Istanziamo!!!");
	}
}