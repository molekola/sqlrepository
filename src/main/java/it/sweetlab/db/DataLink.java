package it.sweetlab.db;

import it.sweetlab.data.DataContainer;
import it.sweetlab.db.repository.Query;
import it.sweetlab.util.DateUtil;
import it.sweetlab.util.TextUtils;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DataLink {

	// Costanti per DataLink e DB INIT
	public static final String JNDI_KEY = JNDIConf.getJndiKey();
	public static final String ORACLE_KEY = JNDIConf.getJdbcKey();
	public static final int RETURN_TYPE_VOID = 0;
	public static final int RETURN_TYPE_STRING = 1;
	public static final int RETURN_TYPE_HASH = 2;

	/**
	 * Verifico il numero di connessioni che sono aperte.
	 * */
	private static int connessioniAperte = 0;

	private Connection con;

    private static Log sysLog = LogFactory.getLog(DataLink.class);
	private static Log resLog = LogFactory.getLog(JNDIConf.getLoggerKey());

	private Context initContext = null;
	private Context envContext = null;
	private DataSource ds = null;

    private static Context sInitContext = null;
	private static Context sEnvContext = null;
	private static DataSource sDs = null;
	
    static {
        // Lookup alla risorsa jndi.
        try{
            sInitContext = new InitialContext();
            sEnvContext = (Context) sInitContext.lookup(JNDI_KEY);
            sDs = (DataSource) sEnvContext.lookup(ORACLE_KEY);
		} catch (Throwable e) {
			resLog.error("Cannot retrieve: " + JNDI_KEY + "/" + ORACLE_KEY, e);
		}
    }

    /**
     * Costruttore di default,
     * Utilizza la risorsa jndi standard e configurata in modalita' statica. */
	public DataLink() {
        // this(JNDI_KEY,ORACLE_KEY);
        sysLog = LogFactory.getLog(DataLink.class);
		connect(sDs);
	}

    public DataLink(Connection con) {
		setConnection(con);
	}

	/** 
     * Costruttore, Specifico le chiavi con cui referenziare la risorsa jndi.
	 * @param jndiKey
	 * @param dataSourceKey */
	public DataLink(String jndiKey, String dataSourceKey) {
        if(resLog.isInfoEnabled()){
            resLog.info("JNDI_KEY:" + jndiKey);
            resLog.info("JDBC_KEY:" + dataSourceKey);
            resLog.info("LOGGER  :" + JNDIConf.getLoggerKey());
        }
        sysLog = LogFactory.getLog(DataLink.class);
        lookup(jndiKey, dataSourceKey);
        connect(ds);
	}

   /** 
    * Costruttore JDBC, utilizza la connessione JDBC.
    * @param url
    * @param driver
    * @param username
    * @param password
    * @throws SQLException
    */
    public DataLink(String url, String driver, String username, String password)
    throws SQLException{
        try {
            Class.forName(driver);                // Instanzio la classe driver.
            Connection c = DriverManager.getConnection(      // Connessione
                url,
                username,
                password
            );
            if (JNDIConf.isSetAutocommit()){
                c.setAutoCommit(JNDIConf.isAutocommit());
            }
            setConnection(c);
        } catch (ClassNotFoundException e) {
            resLog.fatal("Non si trova il driver per la connessione: "+url,e);
        }
    }

    public void commit() {
		try {
			if (con != null)
				con.commit();
		} catch (SQLException e) {
			sysLog.error("Impossibile effettuare commit:", e);
		}
	}

    private void connect(DataSource ds) {
		int i = connessioniAperte++;
		try {
			resLog.info("Getting Connection N. " + i);
			con = ds.getConnection();
			resLog.info("Connection " + i + ":OK!");
			/*
			 * Imposta ad off l'autocommit di jdbc.
			 * Il commit puo essere effettuato solo alla fine della procedura 
			 * per lasciare la base dati consistente. 
             * Su WebSphere non va impostato. */
            if (JNDIConf.isSetAutocommit()){
                con.setAutoCommit(JNDIConf.isAutocommit());
            }
		} catch (SQLException e) {
			resLog.error("Impossibile stabilire una connessione",e);
		}
	}
    
	/**
     * @return PreparedStatement il prepared statement appena creato 
	 * 		@param sql : La stringa SQL da elaborare
	 * 		@param resultSetType : La modalita' di navigazione del cursore.
	 * 		@param resultSetConcurrency : Lo stato di apertura delle righe del 
	 * 									  resultset (Update o ReadOnly).
     */
    private PreparedStatement createPreparedStatement(
        String sql
    ) throws SQLException {
		return createPreparedStatement(
			sql, 
	    	ResultSet.TYPE_FORWARD_ONLY,
	    	ResultSet.CONCUR_READ_ONLY
		);
    }

	/**
     * @return PreparedStatement il prepared statement appena creato 
	 * 		@param sql : La stringa SQL da elaborare
	 * 		@param resultSetType : La modalita' di navigazione del cursore.
	 * 		@param resultSetConcurrency : Lo stato di apertura delle righe del 
	 * 									  resultset (Update o ReadOnly).
	 * 		@param resultSetType : La modalita' di navigazione del cursore.
	 * 		@param resultSetConcurrency : Lo stato di apertura delle righe del 
	 * 									  resultset (Update o ReadOnly).
     */
    private PreparedStatement createPreparedStatement(
        String sql,
        int resultSetType,
        int resultSetConcurrency
    ) throws SQLException {
		return con.prepareStatement(
			sql, 
	    	resultSetType,
	    	resultSetConcurrency
		);
    }
	
	/**
	 * Esegue in batch un comando SQL parametrico, partendo da un oggetto 
	 * 		@see it.sweetlab.db.Query
     * @result contiene gli indici degli statements che hanno riscontrato un 
     * eccezione di chiave primaria duplicata.
	 * Parametri:
	 * 		@param Query : Oggetto che contiene la stringa SQL e i parametri. 
	 * */
	public BatchController execAndVerfyBatchSQLStatement(
        Query q, 
        BatchController controller
    ) throws Exception {
		String sql = q.getSql();
		List params = q.getParameters();

        sysLog = LogFactory.getLog(q.getClazz());
        if (sysLog.isDebugEnabled()){
            sysLog.debug("   sql:\n" + sql);
            sysLog.debug("params:\n" + params);
        }

        controller.onBeforeStart();
        PreparedStatement psQuery = createPreparedStatement(sql);
        //List exceptions = new ArrayList();
        Iterator i = params.iterator();
        int idx = 0;
        while(i.hasNext()){
            try{
                List lParams = (List)i.next();
                sysLog.debug("Parameters:"+lParams);
                setParameterToPreparedStatement(sql, lParams, psQuery);
                int result = psQuery.executeUpdate();
                controller.onSuccessDo(idx, result);
                sysLog.debug("Statement Result:"+result);
            } catch (Exception e){
                controller.onExceptionDo(idx, e);
                /*
                if(e.getMessage().startsWith("ORA-00001")){
                    // Aggiungo l'indice della riga che ho inserito.
                    exceptions.add(new Integer(idx));
                } else {
                    throw e;
                }*/
            }
            idx++;
        }
        controller.onAfterEnd();
        return controller;
	}
	
	/**
	 * Esegue una query senza parametri
	 * @param query : la query da eseguire. */
	public ResultSet execBaseQuery(Query q) throws SQLException {
		String sql = q.getSql();
		List params = q.getParameters();
        sysLog = LogFactory.getLog(q.getClazz());
		return execBaseQuery(sql,params);
	}
	
	/**
	 * Esegue una query senza parametri
	 * @param query : la query da eseguire.
	 * Restituisce direttamente il resultset.
	 * @Exception SQLException si verifica quando si tenta di utilizzare una
	 * connessione nulla oppure quando si ha una SQLException */
	public ResultSet execBaseQuery(String query) throws SQLException {
		sysLog.debug("\n" + query);
		ResultSet rs = null;
		Statement stmt = null;
		try {
			stmt = con.createStatement();
	
			rs = stmt.executeQuery(query);
		} catch (SQLException e) {
			sysLog.error(e);
		}
		return rs;
	}

	/**
	 * Restituisce il resultset corrispondente al comando SQL in argomento:
	 * 
	 * @param sql
	 *            in argomento. Risolve ed assegna posizionalmente i parametri
	 *            passati in argomento
	 * @param sql:
	 *            il comado sql da eseguire.
	 * @param params :
	 *            I parametri da passare. Accetta tipi: String, Double, Integer,
	 *            java.util.Date, java.sql.Date, java.sql.Timestamp
	 * @Exception SQLException
	 *                si verifica quando si tenta di utilizzare una connessione
	 *                nulla oppure quando si ha una SQLException
	 */
	public ResultSet execBaseQuery(String sql, List params) throws SQLException {
        if(sysLog.isInfoEnabled()){
            sysLog.debug("   sql:\n" + sql);
            sysLog.debug("params:\n" + params);
        }
		PreparedStatement psQuery = prepareStatement(sql, params);
		ResultSet rs = psQuery.executeQuery();
		return rs;
	}


	/**
	 * Esegue un comando SQL parametrico, partendo da un oggetto 
	 * 		@see it.sweetlab.db.Query
	 * Parametri:
	 * 		@param Query : Oggetto che contiene la stringa SQL e i parametri. 
	 * */
	public void execBatchSQLStatement(Query q) throws SQLException {
		String sql = q.getSql();
		List params = q.getParameters();
        sysLog = LogFactory.getLog(q.getClazz());
        if (sysLog.isDebugEnabled()){
            sysLog.debug("   sql:\n" + sql);
            sysLog.debug("params:\n" + params);
        }

        PreparedStatement psQuery = createPreparedStatement(sql);
        
        Iterator i = params.iterator();
        while(i.hasNext()){
            List lParams = (List)i.next();
            sysLog.debug("Parameters:"+lParams);
            setParameterToPreparedStatement(sql, lParams, psQuery);
            int result = psQuery.executeUpdate();
            sysLog.info("Statement Result:"+result);
        }
	}

	/**
	 * Esegue una query di count e quindi si aspetta un int come primo campo 
	 * della prima riga del resultset, restituisce solo quello.
	 * 
	 * Come parametri si aspetta:
	 *   @param query: L'oggetto Query da eseguire.
	 * */
	public int execCount(Query q) throws SQLException {
		String sql = q.getSql();
		List params = q.getParameters();
        sysLog = LogFactory.getLog(q.getClazz());
		return execCount(sql,params);
	}

	/**
  	 * Esegue una query di count e restituisce il primo campo della pirma riga 
  	 * estratta. 
  	 * 		@param query: la query da elaborare.
  	 * */
	public int execCount(String query) throws SQLException {
		sysLog.debug("   sql:\n" + query);
		ResultSet resultSet = null;
		Statement statement = null;
		int result = 0;
	
		try {
			statement = con.createStatement();
			resultSet = statement.executeQuery(query);
			resultSet.next();
			result = resultSet.getInt(1);
			resultSet.close();
	
			statement.close();
		} catch (SQLException e) {
			sysLog.error(e);
			try {
				if (resultSet != null)
					resultSet.close(); // CHIUDO RESULTSET
				if (statement != null)
					statement.close(); // CHIUDO STATEMENT
			} catch (SQLException ee) {
				sysLog.error(ee);
			}
		} catch (Exception e) {
			sysLog.error(e);
		}
		return result;
	}

    /**
	 * Esegue una query di count e quindi si aspetta un int come primo campo 
	 * della prima riga del resultset, restituisce solo quello.
	 * 
	 * Come parametri si aspetta:
	 *   @param query: La query in forma stringa sql 
	 *   @param vParams: Un List di parametri come per le altre query parametriche.
	 * */
    public int execCount(String query, List vParams) throws SQLException {
        if(sysLog.isInfoEnabled()){
            sysLog.debug("   sql:\n" + query);
            sysLog.debug("params:\n" + vParams);
        }
		ResultSet resultSet = null;
		int result = 0;
	
		try  {
			resultSet = execBaseQuery(query,vParams);
			resultSet.next();
			result = resultSet.getInt(1);
			resultSet.getStatement().close(); // CHIUDO LO STATEMENT
			resultSet.close();				  // CHIUDO IL RESULTSET
											  // LA CONNESSIONE LA CHIUDE IL CHIAMANTE
		} catch (SQLException e){
			sysLog.error(e);
		} catch (Exception e) {
            sysLog.error(e);
		}
		
		return result;
    }


	/**
	* Il metodo execPLSQL esegue una qualunque query
	* PL/SQL, sia essa una stored procedure sia essa una function.
	*
	* @param returnType    Indica il tipo di output della query PL/SQL.
	*                      I valori di questo parametro sono contenuti
	*                      nella classe Costanti.java
	*                      Es.: Se la query PL/SQL da eseguire e' una stored
	*                      procedure, essa non restituira' alcun risultato,
	*                      quindi il parametro returnType andra' impostato a
	*                      Costanti.RETURN_TYPE_VOID.
	*                      Nel caso si voglia utilizzare una function che
	*                      restituisca un cursore, il parametro andra'
	*                      impostato a Costanti.RETURN_TYPE_HASH.
	*
	* @param nameProcFunc  E' la firma della query PL/SQL che va eseguita.
	*                      Esempio con 4 parametri di ingresso:
	*                      nomeSchema.nomePackage.nomeProcedura(?,?,?,?);
	*                      Esempio senza parametri:
	*                      nomeSchema.nomePackage.nomeProcedura;
	*
	* @param parameters    Contiene i parametri da dare in ingresso alla query
	*                      PL/SQL. Ogni suo elemento equivale ad un parametro.
	*                      Se la procedura o funzione non ha parametri in input
	*                      parameters sara' un vector vuoto.
	*
	* @return Object       E' il risultato dell'esecuzione della query PL/SQL.
	*                      Esso puo' anche assumere come valore "null" qualora
	*                      sia stata eseguita una stored procedure.
	* @throws java.sql.SQLException
	*/
	public Object execOPLSQL(
		int returnType,
		String nameProcFunc,
		List parameters
	) throws SQLException {
		Object result = null;
		CallableStatement callableStatement;
		int parameterIndex = 0;

		if (returnType == RETURN_TYPE_VOID) {
			callableStatement = con.prepareCall("Begin " + nameProcFunc + "; End;");
		} else {
			callableStatement =	con.prepareCall("Begin ?:= " + nameProcFunc + "; End;");
			parameterIndex++;
			/* Prima di eseguire la procedura bisogna specificare esplicitamente 
			 * di che tipo e' il valore di ritorno della procedura tramite il 
			 * metodo RegisterOutParameter che riceve come parametri la 
			 * posizione del parametro in uscita (parameterIndex++) e il tipo di 
			 * parametro restituito */
			switch (returnType) {
				case RETURN_TYPE_STRING :
					callableStatement.registerOutParameter(
						parameterIndex,
						Types.VARCHAR
					);
					break;
				case RETURN_TYPE_HASH :
					callableStatement.registerOutParameter(
						parameterIndex,
						2012 // Types.REF_CURSOR
					);
					break;
			}
		}
		/* Impostazione dei parametri in ingresso.
		 * I parametri sono contenuti nel vettore parameters passato al metodo 
		 * execPLSQL.
		 * In questo caso bisogna verificare il tipo di parametro paasato per 
		 * settarlo correttamente */
		for (Object parameter : parameters) {
			parameterIndex++;
			Object paramIn = parameter;
			if (paramIn instanceof String) {
				String string = (String) paramIn;
				callableStatement.setString(parameterIndex, string);
				/* Raw e' un tipo di dato utilizzato per memorizzare dati binari
				* (es immagini digitali). */
			} else if (paramIn instanceof byte[]) {
				byte[] byteArray = (byte[]) paramIn;
				callableStatement.setObject(parameterIndex, new String(byteArray));
			} else if (paramIn instanceof Integer) {
				Integer integer = (Integer) paramIn;
				callableStatement.setInt(parameterIndex, integer);
			} else if (paramIn instanceof Long) {
				Long value = (Long) paramIn;
				callableStatement.setLong(parameterIndex, value);
			} else if (paramIn instanceof Short) {
				Short value = (Short) paramIn;
				callableStatement.setShort(parameterIndex, value);
			} else if (paramIn instanceof Double) {
				Double value = (Double) paramIn;
				callableStatement.setDouble(parameterIndex, value);
			} else if (paramIn instanceof java.util.Date) {
				java.sql.Date value = new java.sql.Date(((java.util.Date) paramIn).getTime());
				callableStatement.setDate(parameterIndex, value);
			} else if (paramIn instanceof Float) {
				Float value = (Float) paramIn;
				callableStatement.setFloat(parameterIndex, value);
				/* Tipo dati Blob (Binary Large Object) per memorizzare una
				* grande quantita' di dati come semplici bytes.
				* La classe DCBlob.java viene utilizzata per traformare il BLOB
				* in un oggetto di tipo array di byte gestibile da codice java */
			} else if (paramIn instanceof DCBlob) {
				DCBlob value = (DCBlob) paramIn;
				Blob oraBlob = new SerialBlob(value.getBytes());
				callableStatement.setBlob(parameterIndex, oraBlob);
				/* Tipo dati Clob(Character Large Object) per memorizzare una
				* grande quantita' di dati espressi in caratteri.
				* La classe DCClob.java viene utilizzata per traformare il CLOB
				* in un oggetto di tipo String gestibile da codice java */
			} else if (paramIn instanceof DCClob) {
				DCClob value = (DCClob) paramIn;
				callableStatement.setClob(parameterIndex, new SerialClob(value.getCharArray()));
			}
		}
		// Istruzione per eseguire la procedura con i parametri specificati.
		callableStatement.execute();

		// Gestione dell'output della query PL/SQL in base al tipo restituito
		switch (returnType) {
			case RETURN_TYPE_VOID :
				result = null;
				break;

			/* Trimmo il dato, se e' stringa, prima di inserirlo in 
			 * DataContainer */
			case RETURN_TYPE_STRING :
				result = callableStatement.getString(1).trim();
				break;

			/* in questo caso la procedura restituisce un cursore ove i 
			 * campi verranno mappati in una hashMap */
			case RETURN_TYPE_HASH :
				ResultSet resultSet = (ResultSet) callableStatement.getObject(1);
				/* scorro il resultSet per prelevare il valore dei campi 
				 * restituiti dalla procedura e nello stesso tempo con un 
				 * ciclo for prelevo dal resultSetMetaData il nome del campo 
				 * corrispondente */
				result = getDataContainer(resultSet);
				break;
		}
		/* Ogni volta che una variabile CallebleStatement viene aperta deve 
		 * sempre essere chiusa */
		callableStatement.close();
		return result;
	}
	public DataContainer execPLSQL(
		int returnType,
		String nameProcFunc,
		List parameters
	) throws SQLException {
		return (DataContainer) execOPLSQL(returnType, nameProcFunc, parameters);
	}

	/**
	 * Esegue una Query SQL parametrica, partendo da un oggetto 
	 * @return 
	 * @throws java.sql.SQLException
	 * 		@see it.sweetlab.db.Query
	 * Parametri:
	 * 		@param q : Oggetto che contiene la stringa SQL e i parametri. */
	public DataContainer execQuery(Query q) throws SQLException {
		String sql = q.getSql();
		List params = q.getParameters();
		sysLog = LogFactory.getLog(q.getClazz());
		return execQuery(sql,params);
	}

	/**
	 * Esegue una Query SQL parametrica, partendo da un oggetto 
	 * @return 
	 * @throws java.sql.SQLException
	 * 		@see it.sweetlab.db.Query
	 * Parametri:
	 * 		@param q : Oggetto che contiene la stringa SQL e i parametri.
	 *		@param resultType: Tipo di risultato di ritorno 
	 * @throws InvocationTargetException 
	 * @throws IllegalAccessException 
	 * @throws InstantiationException */
	public List execQuery(Query q, Class resultType) throws SQLException, InstantiationException, IllegalAccessException, InvocationTargetException {
		String sql = q.getSql();
		List params = q.getParameters();
        sysLog = LogFactory.getLog(q.getClazz());
        return execQuery(sql,params,resultType);
	}

	/**
	 * Esegue una Query SQL parametrica, partendo da un oggetto 
	 * @return 
	 * @throws java.sql.SQLException
	 * 		@see it.sweetlab.db.Query
	 * Parametri:
	 * 		@param q : Oggetto che contiene la stringa SQL e i parametri. 
	 *		@param resultType: Tipo di risultato di ritorno
	 * 		@param begin : Indice iniziale del resultSet da estrarre. 
	 * 		@param end : Indice finale del resultSet da estrarre. 
	 * @throws InvocationTargetException 
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 * */
	public List execQuery(Query q, Class resultType, int begin, int end) throws SQLException, InstantiationException, IllegalAccessException, InvocationTargetException {
		String sql = q.getSql();
		List params = q.getParameters();
        sysLog = LogFactory.getLog(q.getClazz());
		return execQuery(sql,params, resultType, begin,end);
	}

    /**
	 * Esegue una Query SQL parametrica, partendo da un oggetto 
	 * @return 
	 * @throws java.sql.SQLException
	 * 		@see it.sweetlab.db.Query
	 * Parametri:
	 * 		@param q : Oggetto che contiene la stringa SQL e i parametri. 
	 * 		@param begin : Indice iniziale del resultSet da estrarre. 
	 * 		@param end : Indice finale del resultSet da estrarre. 
	 * */
	public DataContainer execQuery(Query q, int begin, int end) throws SQLException {
		String sql = q.getSql();
		List params = q.getParameters();
        sysLog = LogFactory.getLog(q.getClazz());
		return execQuery(sql,params,begin,end);
	}

	/**
	 * Esegue una query senza parametri
	 * @param query : la query da eseguire.
	 * @return 
	 * @throws java.sql.SQLException
	 * @Exception SQLException si verifica quando si tenta di utilizzare una
	 * connessione nulla oppure quando si ha una SQLException */
	public DataContainer execQuery(String query) throws SQLException {
		sysLog.debug("\n" + query);
		ResultSet resultSet = null;
		Statement statement = null;
		DataContainer dtCont = null;
		try {
			statement = con.createStatement();
			resultSet = statement.executeQuery(query);
			dtCont = getDataContainer(resultSet);
			statement.close();
		} catch (SQLException e) {
			sysLog.error(e);
			try {
				if (resultSet != null)
					resultSet.close(); // CHIUDO RESULTSET
				if (statement != null)
					statement.close(); // CHIUDO STATEMENT
			} catch (SQLException ee) {
				sysLog.error(ee);
			}
		} catch (Exception e) {
			sysLog.error(e);
		}
		return dtCont;
	}

	/**
	 * Restituisce il resultset corrispondente alla query in argomento:
	 * @param sql: il comado sql da eseguire.
	 * @param params : I parametri da passare. Accetta tipi: 
	 * 		String, 
	 * 		Double, 
	 * 		Integer,
	 *      java.util.Date, 
	 *      java.sql.Date, 
	 *      java.sql.Timestamp
	 * @return 
	 * @throws java.sql.SQLException
	 * @Exception SQLException si verifica quando si tenta di utilizzare una 
	 * 		connessione nulla oppure quando si ha una SQLException
	 */
	public DataContainer execQuery(String sql, List params) throws SQLException {
        if(sysLog.isInfoEnabled()){
            sysLog.debug("   sql:\n" + sql);
            sysLog.debug("params:\n" + params);
        }
		DataContainer result;
		PreparedStatement psQuery = prepareStatement(sql, params);
		result = getDataContainer(psQuery.executeQuery());
		return result;
	}

	/**
	 * Restituisce il resultset corrispondente alla query in argomento:
	 * @param sql: il comado sql da eseguire.
	 * @param params : I parametri da passare. Accetta tipi: 
	 * 		String, 
	 * 		Double, 
	 * 		Integer,
	 *      java.util.Date, 
	 *      java.sql.Date, 
	 *      java.sql.Timestamp
	 * @param resultType: Il tipo di oggetto di ritorno 
	 * @return  
	 * @throws java.sql.SQLException 
	 * @throws InvocationTargetException 
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 * @Exception SQLException si verifica quando si tenta di utilizzare una 
	 * 		connessione nulla oppure quando si ha una SQLException
	 */
	public List execQuery(String sql, List params, Class resultType) throws SQLException, InstantiationException, IllegalAccessException, InvocationTargetException {
		if(sysLog.isInfoEnabled()){
			sysLog.debug("   sql:\n" + sql);
			sysLog.debug("params:\n" + params);
		}
		List result;
		PreparedStatement psQuery = prepareStatement(sql, params);
		result = getResultList(psQuery.executeQuery(),resultType);
		return result;
	}

	/**
	 * Restituisce il resultset corrispondente alla query in argomento:
	 * @param sql: il comado sql da eseguire.
	 * @param params : I parametri da passare. Accetta tipi: 
	 * 		String, 
	 * 		Double, 
	 * 		Integer,
	 *      java.util.Date, 
	 *      java.sql.Date, 
	 *      java.sql.Timestamp
	 * @param begin : Indice iniziale nel ResultSet.
	 * @param resultType : Tipo risultato di ritorno.
	 * @param end : Indice finale nel ResultSet .
	 * @return 
	 * @throws java.sql.SQLException
	 * @throws InvocationTargetException 
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 * @Exception SQLException si verifica quando si tenta di utilizzare una 
	 * 		connessione nulla oppure quando si ha una SQLException
	 */
	public List execQuery(String sql, List params, Class resultType, int begin, int end)
	throws SQLException, InstantiationException, IllegalAccessException, InvocationTargetException {
		if(sysLog.isInfoEnabled()){
			sysLog.debug("   sql:\n" + sql);
			sysLog.debug("params:\n" + params);
			sysLog.debug(" begin:" + begin);
			sysLog.debug("   end:" + end);
		}
		List result;
		PreparedStatement psQuery = createPreparedStatement(
				sql,
				ResultSet.TYPE_SCROLL_INSENSITIVE, 
				ResultSet.CONCUR_READ_ONLY
		);
		psQuery = setParameterToPreparedStatement(
				sql, 
				params,
				psQuery
		);
		result = getDataContainer(psQuery.executeQuery(), resultType, begin, end);
		return result;
	}
	
	/**
	 * Restituisce il resultset corrispondente alla query in argomento:
	 * @param sql: il comado sql da eseguire.
	 * @param params : I parametri da passare. Accetta tipi: 
	 * 		String, 
	 * 		Double, 
	 * 		Integer,
	 *      java.util.Date, 
	 *      java.sql.Date, 
	 *      java.sql.Timestamp
	 * @param resultType: Il tipo di oggetto di ritorno 
	 * @param fieldId 
	 * @return  
	 * @throws java.sql.SQLException 
	 * @throws InvocationTargetException 
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 * @Exception SQLException si verifica quando si tenta di utilizzare una 
	 * 		connessione nulla oppure quando si ha una SQLException
	 */
	public Map execQuery(String sql, List params, Class resultType, String fieldId) throws SQLException, InstantiationException, IllegalAccessException, InvocationTargetException {
		if(sysLog.isInfoEnabled()){
			sysLog.debug("   sql:\n" + sql);
			sysLog.debug("params:\n" + params);
		}
		Map<String, Object> result;
		PreparedStatement psQuery = prepareStatement(sql, params);
		result = getResultMap(psQuery.executeQuery(),resultType, fieldId);
		return result;
	}
	
	/**
	 * Restituisce il resultset corrispondente alla query in argomento:
	 * @param sql: il comado sql da eseguire.
	 * @param params : I parametri da passare. Accetta tipi: 
	 * 		String, 
	 * 		Double, 
	 * 		Integer,
	 *      java.util.Date, 
	 *      java.sql.Date, 
	 *      java.sql.Timestamp
	 * @param begin : Indice iniziale nel ResultSet.
	 * @param end : Indice finale nel ResultSet .
	 * @return 
	 * @throws java.sql.SQLException 
	 * @Exception SQLException si verifica quando si tenta di utilizzare una 
	 * 		connessione nulla oppure quando si ha una SQLException
	 */
	public DataContainer execQuery(String sql, List params, int begin, int end)
        throws SQLException {
        if(sysLog.isInfoEnabled()){
            sysLog.debug("   sql:\n" + sql);
            sysLog.debug("params:\n" + params);
            sysLog.debug(" begin:" + begin);
            sysLog.debug("   end:" + end);
        }
		DataContainer result;
		PreparedStatement psQuery = createPreparedStatement(
            sql,
            ResultSet.TYPE_SCROLL_INSENSITIVE, 
            ResultSet.CONCUR_READ_ONLY
        );
        psQuery = setParameterToPreparedStatement(
            sql, 
            params,
            psQuery
        );
		result = getDataContainer(psQuery.executeQuery(), begin, end);
		return result;
	}
	
	public Map execQueryToMap(Query q, Class resultType, String fieldId) throws SQLException, InstantiationException, IllegalAccessException, InvocationTargetException {
		String sql = q.getSql();
		List params = q.getParameters();
        sysLog = LogFactory.getLog(q.getClazz());
        return execQuery(sql,params,resultType, fieldId);		
	}
	
	/** 
	 * Esegue una query e restituisce il risultato sotto forma di lista del solo
	 * elemento presente nella colonna indicata in argomento.
	 * @param q
	 * @param column
	 * @return  The first column of the resultset */
	public List<Object> execQueryToSingleColumn(Query q, String column){
		if (StringUtils.isEmpty(column)) throw new NullPointerException("Column name could not be null.");
		String sql = q.getSql();
		List<Object> params = q.getParameters();
        sysLog = LogFactory.getLog(q.getClazz());
		ResultSet rs;
		List<Object> result = new ArrayList<Object>();
		try  {
			rs = execBaseQuery(sql,params);
			ResultSetMetaData meta = rs.getMetaData();
			int colIdx = -1;
			int colType = -1;
			// Recupero i riferimenti alla colonna da estrarre.
			for(int i = 1;i<=meta.getColumnCount();i++){
				if (column.equalsIgnoreCase(meta.getColumnLabel(i))){
					colIdx = i;
					colType = meta.getColumnType(colIdx);
					break;
				}
			}

			// Popolo i risultati
			while (rs.next()){
				result.add(populateField(colType,colIdx,rs));
			}

			rs.getStatement().close(); // CHIUDO LO STATEMENT
			rs.close();				   // CHIUDO IL RESULTSET
									   // LA CONNESSIONE VIENE CHIUSA DAL CHIAMANTE
		} catch (SQLException e){
			sysLog.error(e);
		} catch (Exception e) {
            sysLog.error(e);
		}
        
        return result;
	}
	
	/**
	 * Esegue un comando SQL parametrico, partendo da un oggetto 
	 * 		@see it.sweetlab.db.Query
	 * Parametri:
	 * 		@param q : Oggetto che contiene la stringa SQL e i parametri. 
	 * @throws java.sql.SQLException
	 * */
	public void execSQLStatement(Query q) throws SQLException {
		String sql = q.getSql();
		List params = q.getParameters();
        sysLog = LogFactory.getLog(q.getClazz());
		execSQLStatement(sql,params);
	}
	
	/**
	 * Esegue un comando sql Insert,Update,Delete
	 * @param query query  da eseguire
	 * @throws SQLException si verifica quando si tenta di utilizzare una
	 * connessione nulla oppure quando si ha una SQLException */
	public void execSQLStatement(String query) throws SQLException {
		sysLog.debug("   sql:\n" + query);
		Statement stmt = null;
		try {
			stmt = con.createStatement();
			stmt.execute(query);
			stmt.close();
		} catch (SQLException e) {
			try {
				if (stmt != null)
					stmt.close();
			} catch (Exception ee) {
				sysLog.error(ee);
			}
			sysLog.error(e);
		}
	}
	
	/**
	 * Esegue un comando sql Insert,Update,Delete
	 * 
	 * @param sql :
	 *            query da eseguire
	 * @param params :
	 *            I parametri da passare. Accetta tipi: String, Double, Integer,
	 *            java.util.Date, java.sql.Date, java.sql.Timestamp
	 * @throws SQLException
	 *                si verifica quando si tenta di utilizzare una connessione
	 *                nulla oppure quando si ha una SQLException
	 */
    public void execSQLStatement( String sql, List params )
    throws SQLException {
        if(sysLog.isInfoEnabled()){
            sysLog.debug("   sql:\n" + sql);
            sysLog.debug("params:\n" + params);
        }
        PreparedStatement psQuery = prepareStatement(sql, params);
        int result = psQuery.executeUpdate();
        sysLog.debug("Statement Result:"+result);
    }

	public Connection getConnection() {
		return con;
	}
	
	/* Prelevo ResultSetMetaData che contiene la descrizione delle colonne del
	 * ResultSet. 
	 * L'idea e' di valorizzare con un valore differente da null il
	 * DataContainer, in base al tipo della colonna sul DB. */
	public DataContainer getDataContainer(ResultSet rSet) throws SQLException {
		ResultSetMetaData cols = rSet.getMetaData();
		DataContainer result = new DataContainer();

		while (rSet.next()) { // Scorro il resultset in verticale (Righe)....

			result.add(populateRow(cols,rSet));// ... ed in orizzontale(colonne).

		}

        if (sysLog.isDebugEnabled()){
            sysLog.debug("size:" + (result == null ? 0 : result.size()));
        }

		return result;
	}
	/**
	 * Restituisce un DataContainer di dati contenuti tra begin e end.
	 * Serve per lavorare sulla paginazione.
	 * @param rSet
	 * @param begin indica un indice java standard, come quello di un array o di
	 * una collection. Il primo elemento e' quindi 0. 
	 * @param resultType: Tipo di risultato di ritorno
	 * @param end final index
	 * @return 
	 * @throws java.sql.SQLException 
	 * @throws InvocationTargetException 
	 * @throws IllegalAccessException 
	 * @throws InstantiationException */
	public List getDataContainer(ResultSet rSet, Class resultType, int begin, int end)
	throws SQLException, InstantiationException, IllegalAccessException, InvocationTargetException {
		/* Prelevo ResultSetMetaData che contiene la descrizione delle colonne 
		 * del ResultSet. 
		 * L'idea e' di valorizzare con un valore differente da null il
		 * DataContainer, in base al tipo della colonna sul DB. */
		ResultSetMetaData cols = rSet.getMetaData();
		DataContainer result = new DataContainer();
		
		/* Incremento begin ed end per avere una progressione che parte da 1, in
		 * modo che dal resto delle apps sia trasparente e la numerazione sia come
		 * per gli array e le altre collection standard. 
		 * continueUntilLimit indica al resultSet se andare avanti.
		 * Se non ci sono piu' righe mi aspetto che il posizionamento assoluto
		 * restituisca false, invece di un'eccezione.
		 * In modo da saltare a pie' pari l'iterazione sulle righe e restituire
		 * quindi il DataContainer vuoto. */
		boolean continueUntilLimit;
		if (begin == 0) {
			rSet.beforeFirst();
			continueUntilLimit = true;
		} else
			continueUntilLimit = rSet.absolute(begin);
		// Scorro il resultset in verticale (Righe)....
		while (continueUntilLimit && rSet.next()) {
			// ... ed in orizzontale(colonne).
			result.add(populateRow(cols,rSet,resultType));
			
			if (begin++ == end) // Dovrebbe essere corretto l'incremento postfisso.
				continueUntilLimit = false;
			
			/* prefix: ++x
			 * postfix: x++
			 * Entrambe le forme incrementano x, ++x restituisce il nuovo valore di x,
			 * x++ restituisce il vecchio valore.
			 * Esempio:
			 * int x = 1;
			 * int y = ++x;  // x == 2, y == 2
			 *
			 * int x = 1;
			 * int y = x++;  // x == 2, y == 1 */
		}
		sysLog.debug("\tsize:" + result.size());
		return result;
	}

	/**
	 * Prelevo ResultSetMetaData che contiene la descrizione delle colonne del
	 * ResultSet. 
	 * L'idea e' di valorizzare con un valore differente da null il
	 * DataContainer, in base al tipo della colonna sul DB. 
	 * Si interrompe quando arriva alla fine del ResultSet o quando arrivato a 
	 * @param rSet
	 * 		@param limit.
	 * @return
	 * @throws java.sql.SQLException  */
	public DataContainer getDataContainer(ResultSet rSet, int limit)
		throws SQLException {
		ResultSetMetaData cols = rSet.getMetaData();
		DataContainer result = new DataContainer();
		boolean breakForLimit = false;
		int righeElaborate = 0;

		while (!breakForLimit&&rSet.next()){    // Scorro il resultset in 
												// verticale (Righe) ....
			result.add(populateRow(cols,rSet)); // ... ed in orizzontale 
												//     (colonne).
			if (++righeElaborate == limit)
				breakForLimit = true;

		}
        if (sysLog.isDebugEnabled()){
            sysLog.debug("size:" + (result == null ? 0 : result.size()));
        }
		return result;
	}
	/**
	 * @param rSet
	 * @param begin indica un indice java standard, come quello di un array o di
	 * una collection. Il primo elemento e' quindi 0.
	 * @param end
	 * @return Restituisce un DataContainer di dati contenuti tra begin e end.
	 *			Serve per lavorare sulla paginazione.
	 * @throws java.sql.SQLException */
	public DataContainer getDataContainer(ResultSet rSet, int begin, int end)
		throws SQLException {
		/* Prelevo ResultSetMetaData che contiene la descrizione delle colonne 
		 * del ResultSet. 
		 * L'idea e' di valorizzare con un valore differente da null il
		 * DataContainer, in base al tipo della colonna sul DB. */
		ResultSetMetaData cols = rSet.getMetaData();
		DataContainer result = new DataContainer();
	
		/* Incremento begin ed end per avere una progressione che parte da 1, in
		 * modo che dal resto delle apps sia trasparente e la numerazione sia come
		 * per gli array e le altre collection standard. 
		 * continueUntilLimit indica al resultSet se andare avanti.
		 * Se non ci sono piu' righe mi aspetto che il posizionamento assoluto
		 * restituisca false, invece di un'eccezione.
		 * In modo da saltare a pie' pari l'iterazione sulle righe e restituire
		 * quindi il DataContainer vuoto. */
		boolean continueUntilLimit;
		if (begin == 0) {
			rSet.beforeFirst();
			continueUntilLimit = true;
		} else
			continueUntilLimit = rSet.absolute(begin);
		// Scorro il resultset in verticale (Righe)....
		while (continueUntilLimit && rSet.next()) {
			// ... ed in orizzontale(colonne).
			result.add(populateRow(cols,rSet));
	
			if (begin++ == end) // Dovrebbe essere corretto l'incremento postfisso.
				continueUntilLimit = false;
	
			/* prefix: ++x
			 * postfix: x++
			 * Entrambe le forme incrementano x, ++x restituisce il nuovo valore di x,
			 * x++ restituisce il vecchio valore.
			 * Esempio:
			 * int x = 1;
			 * int y = ++x;  // x == 2, y == 2
			 *
			 * int x = 1;
			 * int y = x++;  // x == 2, y == 1 */
		}
		sysLog.debug("\tsize:" + result.size());
		return result;
	}

	/**
	 * @param rSet
	 * @param resultType: Tipo di oggetto di ritorno
	 * @return Prelevo ResultSetMetaData che contiene la descrizione delle colonne del
	 *			ResultSet. 
	 *			L'idea e' di valorizzare con un valore differente da null il
	 *			DataContainer, in base al tipo della colonna sul DB. 
	 * @throws java.sql.SQLException
	 * @throws java.lang.InstantiationException
	 * @throws java.lang.IllegalAccessException
	 * @throws java.lang.reflect.InvocationTargetException
	 */
	public List getResultList(ResultSet rSet, Class resultType) throws SQLException, InstantiationException, IllegalAccessException, InvocationTargetException {
		ResultSetMetaData cols = rSet.getMetaData();
		List result = new ArrayList();
		
		while (rSet.next()) { // Scorro il resultset in verticale (Righe)....
			
			result.add(populateRow(cols,rSet,resultType));// ... ed in orizzontale(colonne).
			
		}
		
		if (sysLog.isDebugEnabled()){
			sysLog.debug("size:" + (result == null ? 0 : result.size()));
		}
		
		return result;
	}

	/**
	 * @param rSet
	 * @param resultType: Tipo di oggetto di ritorno
	 * @param fieldId
	 * @return Prelevo ResultSetMetaData che contiene la descrizione delle colonne del
	 *			ResultSet. 
	 *			L'idea e' di valorizzare con un valore differente da null il
	 *			DataContainer, in base al tipo della colonna sul DB. 
	 * @throws java.sql.SQLException
	 * @throws java.lang.IllegalAccessException
	 * @throws java.lang.InstantiationException
	 * @throws java.lang.reflect.InvocationTargetException*/
	public Map getResultMap(ResultSet rSet, Class resultType, String fieldId) throws SQLException, InstantiationException, IllegalAccessException, InvocationTargetException {
		ResultSetMetaData cols = rSet.getMetaData();
		Map result = new HashMap();
		
		while (rSet.next()) { // Scorro il resultset in verticale (Righe)....
			
			result.put(rSet.getObject(fieldId),populateRow(cols,rSet,resultType));// ... ed in orizzontale(colonne).
			
		}
		
		if (sysLog.isDebugEnabled()){
			sysLog.debug("size:" + (result == null ? 0 : result.size()));
		}
		
		return result;
	}

	private void lookup(String jndiKey, String dataSourceKey){
        try{
            initContext = new InitialContext();
            envContext = (Context) initContext.lookup(jndiKey);
            ds = (DataSource) envContext.lookup(dataSourceKey);
		} catch (NamingException e) {
			sysLog.error("Cannot retrieve: " + JNDI_KEY + "/" + ORACLE_KEY,e);
		}
    }
	
	/**
     * @param sequenceName
	 * @return Esegue una sequence e restituisce il valore successivo. 
     *			Testato e funzionante solo con Oracle.
	 * @throws java.sql.SQLException */
	@Deprecated
    public int nextValue(String sequenceName) throws SQLException {
        String query = "SELECT "+sequenceName+".NEXTVAL FROM DUAL";
        return execCount(query);
    }
	
	/**
	 * @param q The Query to execute
	 * @param columnDef 0 (default KEY) e come valore @param columnDef 1 (default value)
	 * @return Estrae una query e restituisce una HashMap, con chiave 
	 * @throws java.sql.SQLException */
	public Map<String, Object> pivotQuery(Query q, String... columnDef) throws SQLException {
		String columnKey   = columnDef.length > 0 ? columnDef[0] : "KEY";
		String columnValue = columnDef.length > 1 ? columnDef[1] : "VALUE";
		Map<String, Object> result = new HashMap<String, Object>();
		ResultSet rs = execBaseQuery(q);
		while (rs.next()) {
			result.put(rs.getString(columnKey), rs.getObject(columnValue));
		}
		rs.close();
		return result;
	}

	/**
	 * Restituisco un object, per la colonna corrente */
	private Object populateField(int columnType, int columnIndex, ResultSet rSet) throws SQLException {
		
		ResultSetMetaData meta = rSet.getMetaData();
		
		switch (columnType) {
			case java.sql.Types.CHAR :
			case java.sql.Types.VARCHAR :
			case java.sql.Types.LONGVARCHAR :
				String s = rSet.getString(columnIndex);
				return s == null ? "" : s.trim();
			case java.sql.Types.DATE :
			case java.sql.Types.TIME :
			case java.sql.Types.TIMESTAMP :
				Timestamp ts = rSet.getTimestamp(columnIndex);
				if (ts != null)
					return new java.util.Date(ts.getTime());
				break;
			case java.sql.Types.BIGINT :
				return rSet.getLong(columnIndex);
			case java.sql.Types.DECIMAL :
			case java.sql.Types.INTEGER :
			case java.sql.Types.NUMERIC :
				Object o = rSet.getObject(columnIndex);
				if (o == null)
					return null;
				else if (meta.getScale(columnIndex) > 0)
					return rSet.getDouble(columnIndex);
				else // Restituisce 0 se e' null
					return rSet.getInt(columnIndex);
			case java.sql.Types.BLOB :
				Object b = rSet.getBlob(columnIndex);
				return b;
			case java.sql.Types.BOOLEAN :
				return rSet.getBoolean(columnIndex);
			default :
				Object obj = rSet.getObject(columnIndex);
				
				sysLog.info("Tipo dato sconosciuto: "
						+"[column: "+ meta.getColumnLabel(columnIndex)
						+", database type: "+ columnType
						+ ", java: " + (obj!=null?obj.getClass().getName():"null") +"]");
				
				return obj;
		}
		
		return null;
	}

	/**
	 * Popola la singola riga del DataContainer */
	private HashMap<String, Object> populateRow(ResultSetMetaData cols,ResultSet rSet) throws SQLException {
		HashMap<String, Object> riga = new HashMap<String, Object>();
		for (int i = 1; i <= cols.getColumnCount(); i++) {
			
			riga.put(cols.getColumnLabel(i), populateField(cols.getColumnType(i), i, rSet));
			
		}
		return riga;
	}

	/**
	 * Popola la singola riga del DataContainer 
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 * @throws InvocationTargetException */
	private Object populateRow(ResultSetMetaData cols,ResultSet rSet, Class<Object> c)
		throws SQLException, InstantiationException, IllegalAccessException, InvocationTargetException {
		Object bean = c.newInstance();
		HashMap<String, Object> riga = new HashMap<String, Object>();
		for (int i = 1; i <= cols.getColumnCount(); i++) {
			riga.put(TextUtils.dbToJava(cols.getColumnLabel(i)), populateField(cols.getColumnType(i), i, rSet));
		}
		BeanUtils.populate(bean, riga);
		return bean;
	}

    /**
     * Chiama @see setParameterToPreparedStatement(String,List,int,int) con 
     * 
     * @param sql : La stringa SQL da elaborare
     * @param params : I parametri da associare
     * @see ResultSet.TYPE_FORWARD_ONLY
     * @see ResultSet.CONCUR_READ_ONLY
     */
	private PreparedStatement prepareStatement(String sql, List params) throws SQLException {
		return setParameterToPreparedStatement(
			sql, 
			params, 
            createPreparedStatement(sql)
		);
	}
    
    /**
     * Rilascia la connessione in uso. */
	public void release() {
        int i = connessioniAperte--;
		try {
			if (con != null) {
				con.close();
				con = null;
				resLog.info("Connection closed N:" + i);
			}

			if (initContext != null){
				initContext.close();
				initContext = null;
			}

			if (envContext != null){
				envContext.close();
				envContext = null;
			}

			if (ds != null){
				ds = null;
			}

		} catch (SQLException e) {
			resLog.error("Impossibile rilasciare la connessione numero:" + i,e);
		} catch (NamingException e){
			resLog.error("Impossibile rilasciare la risorsa.", e );
		}
	}
    
    public void rollback() {
		try {
			if (con != null)
				con.rollback();
		} catch (SQLException e) {
			sysLog.error("Impossibile effettuare rollback:", e);
		}
	}
	
    public void setConnection(Connection con) {
		this.release();
		this.con = con;
	}

	/**
	 * Restituisce un PreparedStatement:
	 * 		@param sql : La stringa SQL da elaborare
	 * 		@param params : I parametri da associare
	 * 		@param psQuery : Il PreparedStatement da popolare con i parametri
	 *  */
	private PreparedStatement setParameterToPreparedStatement(
		String sql, 
		List params, 
        PreparedStatement psQuery
    ) throws SQLException {
	    int i = 1;
	    Iterator it = params.iterator();
	    while ( it.hasNext() ) {
	      Object o = it.next();
	      if (o instanceof java.lang.String){
	        psQuery.setString(i, (String)o);
	      } else if (o instanceof java.lang.Integer){
	        psQuery.setInt(i, ((Integer)o));
	      } else if (o instanceof java.lang.Long){
	        psQuery.setLong(i, ((Long)o));
	      } else if (o instanceof java.lang.Double){
	    	  psQuery.setDouble(i, ((Double)o));
	      } else if (o instanceof BigDecimal){
	        psQuery.setBigDecimal(i, ((BigDecimal)o));
		  } else if (o instanceof java.sql.Timestamp){
			psQuery.setTimestamp(i, (Timestamp)o);
	      } else if (o instanceof java.util.Date){
	        psQuery.setDate(i, DateUtil.dateToSqlDate((java.util.Date)o));
	      } else if (o instanceof java.sql.Date){
	        psQuery.setDate(i, (java.sql.Date)o);
	      } else if (o==null){
	        psQuery.setNull(i,java.sql.Types.VARCHAR);
	      } else {
	        psQuery.setString(i, o.toString());
	      }
	      i++;
	    }
		return psQuery;
	}
}