package it.sweetlab.db;

import it.sweetlab.db.repository.BasicResolver;
import it.sweetlab.db.repository.JSONResolver;
import it.sweetlab.db.repository.Query;
import it.sweetlab.db.repository.Resolver;
import it.sweetlab.util.DateUtil;

import java.io.FileNotFoundException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DBLink {

	protected Log sysLog = LogFactory.getLog(DBLink.class);
	private DataSource dataSource;
	private Connection connection;
	private static DataSource __DATA_SOURCE;
	private static String __JNDI_CONTEXT;
	private static String __JDBC_NAME;
	private static int __OPENED_CONNECTION_COUNT = 0;
	private Resolver<List<Map<String,Object>>> resolver = new BasicResolver();

	// Set DataSource, using default JNDI Configuration
	public DBLink() throws Exception {
		initDataSource();
		this.dataSource = __DATA_SOURCE;
		connect();
	}
	
	protected void initDataSource() throws Exception {
		if (__DATA_SOURCE == null) {
	        try{
	        	Context initContext = new InitialContext();
	        	Context envContext = (Context) initContext.lookup(__JNDI_CONTEXT);
	            __DATA_SOURCE = (DataSource) envContext.lookup(__JDBC_NAME);
			} catch (Throwable e) {
				sysLog.error(String.format("Cannot retrieve: %s/%s", __JNDI_CONTEXT, __JDBC_NAME));
				throw new Exception(e);
			}
		}
	}
	
	// Set DataSource, using Custom DataSource Configuration
	public DBLink(DataSource dataSource) throws Exception {
		this.dataSource = dataSource;
		connect();
	}
	
	// Set DataSource, using an alternate JNDI Configuration
	public DBLink(String jndiContext, String jdbcName) throws Exception {
        try{
        	Context initContext = new InitialContext();
        	Context envContext = (Context) initContext.lookup(jndiContext);
            this.dataSource = (DataSource) envContext.lookup(jdbcName);
		} catch (Throwable e) {
			sysLog.error(String.format("Cannot retrieve: %s/%s", jndiContext, jdbcName));
			throw new Exception(e);
		}
        connect();
	}
	
	public void release() throws Exception {
		int i = __OPENED_CONNECTION_COUNT--;
		try{
			if (connection!=null) connection.close();
		} catch (Exception e) {
			sysLog.error(String.format("Unable to release connection N. %d", i));
			throw (e);
		}
	}

	public void connect() throws Exception {
		int i = __OPENED_CONNECTION_COUNT++;
		try {
			sysLog.info(String.format("Getting Connection N. %d", i));
			connection = dataSource.getConnection();
			sysLog.info(String.format("Connection N. %d OK", i));
            if (JNDIConf.isSetAutocommit()){
                connection.setAutoCommit(JNDIConf.isAutocommit());
            }
		} catch (SQLException e) {
			sysLog.error("Unable to enstablish a connection",e);
			throw (e);
		}
		
	}

	public ResultSet baseQuery(Query q) throws SQLException {
        sysLog = LogFactory.getLog(q.getClazz());
		return baseQuery(q.getSql(),q.getParameters());
	}

	public ResultSet baseQuery(String sql, List<Object> params) throws SQLException {
        if(sysLog.isDebugEnabled()) sysLog.debug(String.format("sql:\n%s\nparams:\n%s", sql, params));
		PreparedStatement psQuery = prepareStatement(sql, params);
		ResultSet rs = psQuery.executeQuery();
		return rs;
	}

	protected PreparedStatement prepareStatement(String sql, List<Object> params) throws SQLException {
		return setParameterToPreparedStatement(sql, params, createPreparedStatement(sql));
	}

	protected PreparedStatement createPreparedStatement(String sql) throws SQLException {
		return createPreparedStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
	}

	protected PreparedStatement createPreparedStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		return connection.prepareStatement(sql, resultSetType, resultSetConcurrency);
	}

	protected PreparedStatement setParameterToPreparedStatement(String sql, List<Object> params, PreparedStatement psQuery) throws SQLException {
		int i = 1;
		Iterator<Object> it = params.iterator();
		while (it.hasNext()) {
			Object o = it.next();
			if (o instanceof String) {
				psQuery.setString(i, (String) o);
			} 
			else if (o instanceof Integer) {
				psQuery.setInt(i, (Integer) o);
			} 
			else if (o instanceof Long) {
				psQuery.setLong(i, (Long) o);
			} 
			else if (o instanceof Double) {
				psQuery.setDouble(i, (Double) o);
			} 
			else if (o instanceof BigDecimal) {
				psQuery.setBigDecimal(i, (BigDecimal) o);
			} 
			else if (o instanceof Timestamp) {
				psQuery.setTimestamp(i, (Timestamp) o);
			} 
			else if (o instanceof java.util.Date) {
				psQuery.setDate(i, DateUtil.dateToSqlDate((java.util.Date) o));
			} 
			else if (o instanceof java.sql.Date) {
				psQuery.setDate(i, (java.sql.Date) o);
			} 
			else if (o == null) {
				psQuery.setNull(i, java.sql.Types.VARCHAR);
			} 
			else {
				psQuery.setString(i, o.toString());
			}
			i++;
		}
		return psQuery;
	}

	// Send Insert / Update / Delete or other commands to Database 
	public int command(Query query) throws SQLException, FileNotFoundException {return 0;}
	
	// Load Data from database and populate three object types:
	//  - JSON Object
	//  - Entity Object
	//  - List<HashMap<String, Object>> Object
	public List<Map<String,Object>> load(Query query) throws SQLException{
		return load(query.getSql(), query.getParameters());
	}

	public Object load(Query query, Resolver<?> resolver) throws SQLException{
		return load(query.getSql(), query.getParameters(), resolver);
	}
	
	public List<Map<String,Object>> load(String sql, List<Object> params) throws SQLException {
        if(sysLog.isDebugEnabled()) sysLog.debug(String.format("sql:\n%s\nparams:\n%s", sql, params));
		PreparedStatement psQuery = prepareStatement(sql, params);
		return resolver.resolve(psQuery.executeQuery());
	}

	public Object load(String sql, List<Object> params, Resolver<?> resolver) throws SQLException {
		if(sysLog.isDebugEnabled()) sysLog.debug(String.format("sql:\n%s\nparams:\n%s", sql, params));
		PreparedStatement psQuery = prepareStatement(sql, params);
		return resolver.resolve(psQuery.executeQuery());
	}
	
	public String loadJson(Query query) throws SQLException {
		return loadJson(query.getSql(), query.getParameters());
	}
	
	public String loadJson(String sql, List<Object> params) throws SQLException {
		Resolver<String> jsonResolver = new JSONResolver();
		if(sysLog.isDebugEnabled()) sysLog.debug(String.format("sql:\n%s\nparams:\n%s", sql, params));
		PreparedStatement psQuery = prepareStatement(sql, params);
		return jsonResolver.resolve(psQuery.executeQuery());
	}

	private static final String TEST_QUERY = "SELECT	H.HEADING_ID,  "
			+ "		H.NAME, "
			+ "		H.MARKETPLACE, "
			+ "		HO.CLAIM, "
			+ "		HO.ORGANIZATION, "
			+ "		H.NAME_SPACE, "
			+ "		H.HEADING_TYPE, "
			+ "		HA.HOST_ALIAS, "
			+ "		HO.LOCALE_STRATEGY, "
			+ "		HO.WEB_SITE, "
			+ "		HA.LOCALE, "
			+ "		HO.DEFAULT_CATEGORY_ID, "
			+ "		C.CATEGORY_NAME DEFAULT_CATEGORY_NAME, "
			+ "		H.END_USER_ACTIVE, "
			+ "		HO.FATHER_HEADING_ID, "
			+ "		HO.USE_FATHER, "
			+ " "
			+ "		HO.SHOW_SHARE_FACEBOOK, "
			+ "		HO.FACEBOOK, "
			+ "		HO.SHOW_SHARE_TWITTER, "
			+ "		HO.TWITTER, "
			+ "		HO.SHOW_SHARE_GOOGLE, "
			+ "		HO.GOOGLE, "
			+ "		HO.SHOW_SHARE_PINTEREST, "
			+ "		HO.PINTEREST, "
			+ "		HO.GOOGLE_ANALYTICS, "
			+ "		HO.SHOW_SHARE_LINKEDIN, "
			+ "		HO.LINKEDIN, "
			+ "		HO.SHOW_SHARE_YOUTUBE, "
			+ "		HO.YOUTUBE, "
			+ "		HO.SHOW_SHARE_RSS, "
			+ "		HO.CUSTOM_RSS, "
			+ "		HO.HEAD_HTML_INJECTION, "
			+ "		 "
			+ "		HO.STATIC_HOST_ALIAS, "
			+ "		HO.LIBS_HOST_ALIAS, "
			+ " "
			+ "		HO.THEME_VARIANT, "
			+ "		HO.LAYOUT_STRATEGY, "
			+ "		FH.NAME_SPACE FATHER_NAME_SPACE, "
			+ " "
			+ "		HO.PROFILE_IMAGE_ID IMAGE_ID, "
			+ "        I.AUTHOR IMAGE_AUTHOR, "
			+ "		I.CAPTION IMAGE_CAPTION, "
			+ "		I.CAPTURED IMAGE_CAPTURED, "
			+ "		I.EVENT IMAGE_EVENT, "
			+ "		I.IMAGE_PATH, "
			+ "		I.KEYWORDS IMAGE_KEYWORDS, "
			+ "		I.TITLE IMAGE_TITLE, "
			+ "		I.WIDTH IMAGE_WIDTH, "
			+ "		I.HEIGHT IMAGE_HEIGHT, "
			+ "		I.FOCAL_POINT IMAGE_FOCAL_POINT, "
			+ " "
			+ "		HO.FAVICON_IMAGE_ID FAVICON_ID, "
			+ "        FI.AUTHOR FAVICON_AUTHOR, "
			+ "		FI.CAPTION FAVICON_CAPTION, "
			+ "		FI.CAPTURED FAVICON_CAPTURED, "
			+ "		FI.EVENT FAVICON_EVENT, "
			+ "		FI.IMAGE_PATH FAVICON_PATH, "
			+ "		FI.KEYWORDS FAVICON_KEYWORDS, "
			+ "		FI.TITLE FAVICON_TITLE, "
			+ "		FI.WIDTH FAVICON_WIDTH, "
			+ "		FI.HEIGHT FAVICON_HEIGHT, "
			+ "		FI.FOCAL_POINT FAVICON_FOCAL_POINT, "
			+ " "
			+ "		HO.BACKGROUND_TYPE, "
			+ "		HO.BACKGROUND_IMAGE_ID BACKGROUND_ID, "
			+ "        BG.AUTHOR BACKGROUND_AUTHOR, "
			+ "		BG.CAPTION BACKGROUND_CAPTION, "
			+ "		BG.CAPTURED BACKGROUND_CAPTURED, "
			+ "		BG.EVENT BACKGROUND_EVENT, "
			+ "		BG.IMAGE_PATH BACKGROUND_PATH, "
			+ "		BG.KEYWORDS BACKGROUND_KEYWORDS, "
			+ "		BG.TITLE BACKGROUND_TITLE, "
			+ "		BG.WIDTH BACKGROUND_WIDTH, "
			+ "		BG.HEIGHT BACKGROUND_HEIGHT, "
			+ "		BG.FOCAL_POINT BACKGROUND_FOCAL_POINT, "
			+ " "
			+ "		CO.CONTENT_ID       PRIVACY_NOTES_CONTENT_ID, "
			+ "		CO.CONTENT_NAME     PRIVACY_NOTES_CONTENT_NAME, "
			+ "		CO.TITLE            PRIVACY_NOTES_TITLE, "
			+ "		CO.MAIN_CATEGORY_ID PRIVACY_NOTES_MAIN_CATEGORY_ID, "
			+ "		CLN.CATEGORY_NAME   PRIVACY_NOTES_MAIN_CATEGORY_NAME, "
			+ "		CO.SUB_CATEGORY_ID  PRIVACY_NOTES_SUB_CATEGORY_ID, "
			+ "		CSLN.CATEGORY_NAME  PRIVACY_NOTES_SUB_CATEGORY_NAME "
			+ " "
			+ "  FROM SM_HEADINGS H, SM_HEADING_OPTIONS HO  "
			+ "  LEFT JOIN SM_CATEGORIES C        ON HO.DEFAULT_CATEGORY_ID = C.CATEGORY_ID "
			+ "  LEFT JOIN SM_HEADING_OPTIONS FHO ON FHO.HEADING_ID = HO.FATHER_HEADING_ID "
			+ "  LEFT JOIN SM_HEADINGS FH         ON FH.HEADING_ID  = HO.FATHER_HEADING_ID "
			+ "  LEFT JOIN SM_HEADING_ALIAS HA    ON HA.HEADING_ID  = HO.HEADING_ID AND HA.DEFAULT_ALIAS = 1 "
			+ "  LEFT JOIN SM_CONTENTS CO         ON HO.HEADING_ID  = CO.HEADING_ID AND HO.PRIVACY_NOTES = CO.CONTENT_ID "
			+ "  LEFT JOIN SM_CATEGORIES CLN      ON CO.MAIN_CATEGORY_ID = CLN.CATEGORY_ID "
			+ "  LEFT JOIN SM_CATEGORIES CSLN     ON CO.SUB_CATEGORY_ID  = CSLN.CATEGORY_ID "
			+ "  LEFT JOIN SM_IMAGE_LIBRARY I     ON HO.PROFILE_IMAGE_ID  = I.IMAGE_ID "
			+ "  LEFT JOIN SM_IMAGE_LIBRARY FI    ON HO.FAVICON_IMAGE_ID  = FI.IMAGE_ID "
			+ "  LEFT JOIN SM_IMAGE_LIBRARY BG    ON HO.BACKGROUND_IMAGE_ID  = BG.IMAGE_ID "
			+ " WHERE H.HEADING_ID = HO.HEADING_ID "
			+ "   AND (26 IS NULL OR H.HEADING_ID = 26) ";

	
	public static void main(String[] args) throws Exception {
		DataSource ds = new SimpleJDBCDataSource("jdbc:mysql://localhost:3333/sevendaysweb", "com.mysql.jdbc.Driver", "sevendaysweb", "7daysw3b");
		DBLink db = new DBLink(ds);
		List<Map<String,Object>> result = db.load(TEST_QUERY, new ArrayList<Object>());
		System.out.println(result);

//		// TODO: Escape parameters
//		// TODO: Format Dates
//		String json = (String)db.loadJson("select * from sm_contents order by content_id", new ArrayList<Object>());
//		System.out.println(json);
	}
	
}
