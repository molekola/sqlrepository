package it.sweetlab.db;

import it.sweetlab.data.DataContainer;
import it.sweetlab.util.DateUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class JDBCDataLink {

  public static final String JDBC_CONNECTION_STRING =
      "jdbc:searchserver:SearchServer_5.3";
  public static final String JDBC_DRIVER = "jdbc.searchserver.SSDriver";
  public static final String DEF_CHAR_SET = "SET CHARACTER_SET 'UTF8'";

  private Log logger = LogFactory.getLog(JDBCDataLink.class);

  private Connection con;

  public JDBCDataLink(String url, String driver, String charSet)
      throws SQLException{
	  this(url, driver, "", "", charSet);
  }
  
  /** 
   * @param url
   * @param driver
   * @param username
   * @param password
   * @param charSet
   * @throws SQLException
   */
  public JDBCDataLink(String url, String driver, String username, String password, String charSet)
  throws SQLException{
	  try {
		  Class.forName(driver);                     // Instanzio la classe driver.
		  con = DriverManager.getConnection(url,username,password); // Connessione.
		  setCharSet(charSet); // Eseguo lo statement che imposta il charset.
	  } catch (ClassNotFoundException e) {
		  logger.error("Non si trova il driver per la connessione: "+JDBC_DRIVER,e);
	  }
  }

  public JDBCDataLink() throws SQLException {
    this(JDBC_CONNECTION_STRING, JDBC_DRIVER, DEF_CHAR_SET);
  }

  public void setCharSet(String charSetCmd) throws SQLException{
    Statement statement = con.createStatement();
    statement.executeUpdate(charSetCmd);
    statement.close();
  }

  /**
   * Esegue una query e restituisce un ResultSet.
   * */
  public ResultSet execBaseQuery(String query) throws SQLException {
    logger.info("execBaseQuery:");
    logger.info("\tquery :\n\t"+query);
    Statement statement = con.createStatement();
    ResultSet resultSet = statement.executeQuery (query);
    return resultSet;
  }

  /**
   * Esegue una query e restituisce un ResultSet navigabile in avanti e indietro
   * e da la possibilita' di saltare
   * */
  public ResultSet execScrollableQuery(String query)
      throws SQLException {
    logger.info("execBaseQuery:");
    logger.info("\tquery :\n\t"+query);
    Statement statement = con.createStatement(
//        ResultSet.TYPE_SCROLL_INSENSITIVE,
//        ResultSet.CONCUR_READ_ONLY
    );
    ResultSet resultSet = statement.executeQuery (query);
    return resultSet;
  }

  /**
   * Restituisce il resultset corrispondente alla query @param query in
   * argomento. Risolve ed assegna posizionalmente i parametri passati in
   * argomento @param params. Il resultSet e' navigabile progressivamente in
   * avanti.
   * ATTENZIONE! SearchServer non supporta il PreparedStatement
   * */
  public ResultSet execBaseQuery(String query, Vector params)
      throws SQLException {
    logger.info("execBaseQuery:");
    logger.info("\tquery :\n\t"+query);
    logger.info("\tparams:\n\t"+params);
    PreparedStatement psQuery = con.prepareStatement(query);
    for( int i = 0; i<params.size(); i++){
      Object o = params.get(i);
      if (o instanceof java.lang.String){
        psQuery.setString(i+1, (String)o);
      } else if (o instanceof java.lang.Integer){
        psQuery.setInt(i+1, ((Integer)o).intValue());
      } else if (o instanceof java.lang.Double){
        psQuery.setDouble(i+1, ((Double)o).doubleValue());
      } else if (o instanceof java.util.Date){
        psQuery.setDate(i+1, DateUtil.dateToSqlDate((java.util.Date)o));
      } else if (o instanceof java.sql.Date){
        psQuery.setDate(i+1, (java.sql.Date)o);
      } else if (o==null){
        psQuery.setNull(i+1,java.sql.Types.VARCHAR);
      } else {
        psQuery.setString(i+1, o.toString());
      }
    }
    return psQuery.executeQuery();
  }

  /**
   * Restituisce il resultset corrispondente alla query @param query in
   * argomento. Risolve ed assegna posizionalmente i parametri passati in
   * argomento @param params. Il resultSet e' navigabile in modo assoluto, in
   * avanti ed indietro.
   * */
  public ResultSet execScrollableQuery(String query, Vector params)
      throws SQLException {
    logger.info("execBaseQuery:");
    logger.info("\tquery :\n\t"+query);
    logger.info("\tparams:\n\t"+params);
    PreparedStatement psQuery = con.prepareStatement(
        query,
        ResultSet.TYPE_SCROLL_INSENSITIVE,
        ResultSet.CONCUR_READ_ONLY
    );
    for( int i = 0; i<params.size(); i++){
      Object o = params.get(i);
      if (o instanceof java.lang.String){
        psQuery.setString(i+1, (String)o);
      } else if (o instanceof java.lang.Integer){
        psQuery.setInt(i+1, ((Integer)o).intValue());
      } else if (o instanceof java.lang.Double){
        psQuery.setDouble(i+1, ((Double)o).doubleValue());
      } else if (o instanceof java.util.Date){
        psQuery.setDate(i+1, DateUtil.dateToSqlDate((java.util.Date)o));
      } else if (o instanceof java.sql.Date){
        psQuery.setDate(i+1, (java.sql.Date)o);
      } else if (o==null){
        psQuery.setNull(i+1,java.sql.Types.VARCHAR);
      } else {
        psQuery.setString(i+1, o.toString());
      }
    }
    return psQuery.executeQuery();
  }

  /**
   * Esegue una query e restituise un DataContainer.
   * */
  public DataContainer execQuery(String sql)
      throws SQLException {
    ResultSet rSet = execBaseQuery(sql);           // Eseguo la query
    DataContainer result = getDataContainer(rSet); // Popolo il DataContainer
    rSet.getStatement().close();                   // Chiudo lo Statement
    rSet.close();                                  // Chiudo il resultSet
    return result;
  }

  /**
   * Esegue una query e restituise il DataContainer che corrisponde alla
   * porzione di ResultSet specificata da @param begin e @param end.
   * */
  public DataContainer execQuery(String sql, int begin, int end)
      throws SQLException {
    ResultSet rSet = execScrollableQuery(sql);     // Eseguo la query
    DataContainer result = getDataContainer(rSet,begin,end); // Popolo il
                                                             // DataContainer
    rSet.getStatement().close();                   // Chiudo lo Statement
    rSet.close();                                  // Chiudo il resultSet
    return result;
  }

  /**
   * Esegue una query parametrica e restituise un DataContainer.
   * */
  public DataContainer execQuery(String sql, Vector params )
      throws SQLException {
    ResultSet rSet = execBaseQuery(sql,params);    // Eseguo la query
    DataContainer result = getDataContainer(rSet); // Popolo il DataContainer
    rSet.getStatement().close();                   // Chiudo lo Statement
    rSet.close();                                  // Chiudo il resultSet
    return result;
  }


  /**
   * Esegue una query parametrica e restituise il DataContainer che corrisponde
   * alla porzione di ResultSet specificata da @param begin e @param end.
   * */
  public DataContainer execQuery(String sql, Vector params, int begin, int end)
      throws SQLException {
    ResultSet rSet = execScrollableQuery(sql, params);       // Eseguo la query
    DataContainer result = getDataContainer(rSet,begin,end); // Popolo il
                                                             // DataContainer
    rSet.getStatement().close();                   // Chiudo lo Statement
    rSet.close();                                  // Chiudo il resultSet
    return result;
  }

  public DataContainer getDataContainer(ResultSet rSet) throws SQLException{
    ResultSetMetaData cols = rSet.getMetaData();
    DataContainer result = new DataContainer();
    // Scorro il resultset in verticale (Righe)....
    while(rSet.next()){
      // ... ed in orizzontale(colonne).
      Map riga = new HashMap();
      for (int i = 1; i <= cols.getColumnCount(); i++){
        // Prelevo il descrittore della colonna i-esima
        String colName = cols.getColumnName(i);
        switch(cols.getColumnType(i)){
          case java.sql.Types.CHAR:
          case java.sql.Types.VARCHAR:
            String s = rSet.getString(i);
            riga.put(colName, s==null?"":s.trim());
            break;
          case java.sql.Types.DATE:
            Timestamp ts = rSet.getTimestamp(i);
            if (ts!=null)
              riga.put(colName,new java.util.Date(ts.getTime()));
            break;
          case java.sql.Types.INTEGER:
          case java.sql.Types.NUMERIC:
            Object o = rSet.getObject(i);
            if (o == null)
              riga.put(colName, null);
            else if (cols.getScale(i) > 0 )
              riga.put(colName, new Double(rSet.getDouble(i)));
            else                // Restituisce 0 se e' null
              riga.put(colName, new Integer(rSet.getInt(i)));
            break;
          case java.sql.Types.BLOB:
            Object b = rSet.getBlob(i);
            riga.put(colName, b);
            break;
          default :
            riga.put(colName, rSet.getObject(i));
            break;
        }
      }
      result.add(riga);
    }
    logger.debug("size:"+result.size());
    return result;
  }

  public DataContainer getDataContainer(ResultSet rSet, int begin, int end)
      throws SQLException{
    ResultSetMetaData cols = rSet.getMetaData();
    DataContainer result = new DataContainer();
    boolean continueUntilLimit;
    if (begin==0) {
      rSet.beforeFirst();
      continueUntilLimit = true;
    } else
      continueUntilLimit = rSet.absolute(begin);

    // Scorro il resultset in verticale (Righe)....
    while(continueUntilLimit&&rSet.next()){
      // ... ed in orizzontale(colonne).
      Map riga = new HashMap();
      for (int i = 1; i <= cols.getColumnCount(); i++){
        // Prelevo il descrittore della colonna i-esima
        String colName = cols.getColumnName(i);
        switch(cols.getColumnType(i)){
          case java.sql.Types.CHAR:
          case java.sql.Types.VARCHAR:
            String s = rSet.getString(i);
            riga.put(colName, s==null?"":s.trim());
            break;
          case java.sql.Types.DATE:
            Timestamp ts = rSet.getTimestamp(i);
            if (ts!=null)
              riga.put(colName,new java.util.Date(ts.getTime()));
            break;
          case java.sql.Types.INTEGER:
          case java.sql.Types.NUMERIC:
            Object o = rSet.getObject(i);
            if (o == null)
              riga.put(colName, null);
            else if (cols.getScale(i) > 0 )
              riga.put(colName, new Double(rSet.getDouble(i)));
            else                // Restituisce 0 se e' null
              riga.put(colName, new Integer(rSet.getInt(i)));
            break;
          case java.sql.Types.BLOB:
            Object b = rSet.getBlob(i);
            riga.put(colName, b);
            break;
          default :
            riga.put(colName, rSet.getObject(i));
            break;
        }
      }
      result.add(riga);

      if (begin++==end)
        continueUntilLimit = false;
    }

    logger.debug("\tsize:"+result.size());
    return result;
  }


  public void release(){
    try {
      if (con!=null&&!con.isClosed())
        con.close();
    } catch (SQLException e) {
      logger.error("Impossibile chiudere la connessione alla base dati: ",e);
    }
  }

  public void commit() throws SQLException{
    con.commit();
  }

  public void rollback() throws SQLException{
    con.rollback();
  }

}