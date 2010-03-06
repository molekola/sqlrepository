package it.sweetlab.db.repository;

import it.sweetlab.db.JNDIConf;

import java.io.File;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServlet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.xml.DOMConfigurator;

public class InitServlet extends HttpServlet {

	private static final long serialVersionUID = 8339584469386783004L;
	public static final String LOG_CONF_FILE_NAME = "logConfFileName";
	public static final String BASIC_LOG_CONFIG_FILE = "basicLogConfigFile";
	
	Log logger;
	
	public void init() {

		// Prelevo i parametri di configurazione del sistema.
		ServletContext sc = getServletContext(); // Li aggiungo al servlet
		String mcHome = sc.getRealPath("/");

		// Inizializzo JNDI.
		initJNDI(mcHome);

		// Inizializzazione temporanea del logger
		String basicLogConfigFile = getInitParameter(BASIC_LOG_CONFIG_FILE);
		initLoggers(basicLogConfigFile, mcHome);

		logger = LogFactory.getLog(InitServlet.class);
		logger.info("Loggers Configured!");
		
	}

	private void initJNDI(String mcHome) {
		// Compongo il percorso del repository.
		if(mcHome.endsWith(File.separator))
			mcHome+=File.separatorChar;
		String sqlRepository = getInitParameter("sqlRepository"); 
		if(sqlRepository.startsWith("/WEB-INF")) // tolgo lo '/' iniziale
			sqlRepository = mcHome + sqlRepository;

		// Configuro il servizio JNDI
		JNDIConf.configure(
			getInitParameter("jndiKey"),
			getInitParameter("jdbcKey"),
			getInitParameter("loggerAppender"),
			sqlRepository,
			Boolean.valueOf(getInitParameter("checkSQLFileReload")).booleanValue(),
			Boolean.valueOf(getInitParameter("setAutocommit")).booleanValue(),
			Boolean.valueOf(getInitParameter("autocommit")).booleanValue()
		);

		System.out.println("\tcheckSQLFileReload:"
			+ Boolean.valueOf(getInitParameter("checkSQLFileReload")).booleanValue());
	}

	private void initLoggers(String path, String mcHome) {

		System.out.print("logFile:");
		System.out.println(path);
		
		if(path.startsWith("/WEB-INF"))
			path = mcHome + path;

	    // if the log4j-init-file is not set, then no point in trying
	    if(path!= null)
	      DOMConfigurator.configure(path);
	}

}
