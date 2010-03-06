package it.sweetlab.db.repository;

import it.sweetlab.db.JNDIConf;
import it.sweetlab.util.IOUtils;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Query implements Serializable, Cloneable {

	private static Log log = LogFactory.getLog(Query.class);

	private static final long serialVersionUID = 3044751432816357244L;

	public String sqlRepository;
	public static final String PARAMS_DELIMITER = ":";
    
    private Class clazz;

	/**
	 * Stringa SQL pronta per JDBC (Coi '?' al posto del nome del parametro).  
	 * */
	private String sql;

	/**
	 * Riferimento al file.  
	 * */
	private String sqlPath;

	/** 
	 * Data di modifica del file.
	 * */
	private long dataModifica;
	
	/**
	 * La lista di parametri posizionale, pronta per JDBC.
	 * */
	private List parameters;
	
	/**
	 * Il carattere in testa ai parametri. Parametrico, nel caso dovessi usare 
	 * un ":" all'interno di una Query.
	 * */
	private String paramsDelimiter = PARAMS_DELIMITER;
	
	private String regExp;

	/**
	 * Contiene l'abbinamento tra i parametri e gli indici in cui inserirli.
	 * */
	private HashMap parametersMappings;

	/**
	 * Numero totale di parametri trovati (due occorrenze dello stesso parametro
	 * valgono due).
	 * */
	private int numberOfParams;

	/**
	 * Costruttore completo.
	 * */
	public Query(
        String sqlRepository,
        String sqlFile, 
        Map params, 
        String paramsDelimiter, 
        Class clazz
    ){
		this.paramsDelimiter = paramsDelimiter;
		this.regExp = paramsDelimiter+"{1}[a-zA-Z0-9_-]*";
		this.sqlRepository = sqlRepository;
        this.clazz = clazz;
		scanSqlForParameters(loadFromFile(sqlFile),params);
	}

	/**
	 * Costruttore completo.
	 * */
	public Query(String sqlRepository, String sqlFile, Map params, Class clazz){
		this(sqlRepository,sqlFile,params, PARAMS_DELIMITER, clazz);
	}

	/**
	 * Costruttore completo.
	 * */
	public Query(String sqlFile, Map params, String paramsDelimiter, Class clazz){
		this(JNDIConf.getSqlRepository(),sqlFile,params,paramsDelimiter, clazz);
	}

	/**
	 * Costruttore.
	 * Si intende come delimitatore dei parametri il ":" 
	 * @see PARAMS_DELIMITER
	 * */
	public Query(String sql, Map params, Class clazz){
		this(JNDIConf.getSqlRepository(),sql,params,PARAMS_DELIMITER, clazz);
	}

    // Parametri Batch.
    
	/**
	 * Costruttore completo.
	 * */
	public Query(
        String sqlRepository,
        String sqlFile, 
        List params, 
        String paramsDelimiter, 
        Class clazz
    ){
		this.paramsDelimiter = paramsDelimiter;
		this.regExp = paramsDelimiter+"{1}[a-zA-Z0-9_-]*";
		this.sqlRepository = sqlRepository;
        this.clazz = clazz;
		scanSqlForParameters(loadFromFile(sqlFile),params);
	}

	/**
	 * Costruttore completo.
	 * */
	public Query(String sqlRepository, String sqlFile, List params, Class clazz){
		this(sqlRepository,sqlFile,params, PARAMS_DELIMITER, clazz);
	}

	/**
	 * Costruttore completo.
	 * */
	public Query(String sqlFile, List params, String paramsDelimiter, Class clazz){
		this(JNDIConf.getSqlRepository(),sqlFile,params,paramsDelimiter, clazz);
	}

	/**
	 * Costruttore.
	 * Si intende come delimitatore dei parametri il ":" 
	 * @see PARAMS_DELIMITER
	 * */
	public Query(String sql, List params, Class clazz){
		this(JNDIConf.getSqlRepository(),sql,params,PARAMS_DELIMITER, clazz);
	}
    
    private Query(
		String sqlRepository,
		String sql, 
		String sqlPath, 
		long dataModifica, 
		List parameters, 
		String paramsDelimiter,
		HashMap parametersMappings, 
		int numberOfParams,
        Class clazz
	) {
		super();
		this.sqlRepository = sqlRepository;
		this.sql = sql;
		this.sqlPath = sqlPath;
		this.dataModifica = dataModifica;
		this.parameters = parameters;
		this.paramsDelimiter = paramsDelimiter;
		this.parametersMappings = parametersMappings;
		this.numberOfParams = numberOfParams;
        this.clazz = clazz;
	}

	/**
	 * Scandisce la stringa SQL alla ricerca di parametri e crea una struttura 
	 * dati che indica : 
	 * 		nomeParametro - Vettore di posizioni in cui mettere il parametro. 
	 * */
	private void scanSqlForParameters(String sql, Map params){

		log.debug("scanSqlForParameters():"+params);
		log.debug("\tparams:"+params);
		
		if (sql==null||params==null){
			return;
		}
        scanSqlForParameters(sql);
		generateJDBCParameterList(params); 
		return;
	}

    /**
	 * Scandisce la stringa SQL alla ricerca di parametri e crea una struttura 
	 * dati che indica : 
	 * 		nomeParametro - Vettore di posizioni in cui mettere il parametro. 
	 * */
	private void scanSqlForParameters(String sql, List params){

        log.debug("scanSqlForParameters():"+params);
		log.debug("\tparams:"+params);
		
		if (sql==null||params==null){
			return;
		}
        scanSqlForParameters(sql);
        generateBatchParameterList(params);
		return;
	}

    public void generateBatchParameterList(List params) {
        Iterator i = params.iterator();
        this.parameters = new ArrayList();
        while(i.hasNext()){
    		parameters.add(generateParameterList((Map)i.next())); 
        }
    }

    private void scanSqlForParameters(String sql) {
		
        Matcher m = Pattern.compile( regExp , Pattern.MULTILINE).matcher(sql);
    	
        parametersMappings = new HashMap();
        numberOfParams = 0;
        
        while (m.find()){
        	String parola = m.group();

        	// E' un parametro!!!
        	parola=parola.replaceFirst(this.paramsDelimiter,"");
        	Vector indici = (Vector)parametersMappings.get(parola);

        	if(indici==null)
        		indici = new Vector();

        	indici.add(new Integer(numberOfParams));
        	parametersMappings.put(parola, indici);

        	numberOfParams++; // Incremento solo se trovo.
        }
        this.sql = sql.replaceAll(regExp, " ? ").trim();
    }

	public void generateJDBCParameterList(Map params) {
        this.parameters = generateParameterList(params);
	}

    public List generateParameterList(Map params){
		log.debug("Inizio -");
		Object[] oParams = new Object[numberOfParams];
		log.debug("\n\t\tDimensione Vector Parametri:"+numberOfParams);
		Iterator it = parametersMappings.keySet().iterator();
		while (it.hasNext()){
			String parametro = (String)it.next();
			Vector indici = (Vector)parametersMappings.get(parametro);
			Iterator iIndici = indici.iterator();
			Object o = params.get(parametro);
			while (iIndici.hasNext()){
				Integer indice = (Integer)iIndici.next();
				//log.debug("\t\t\tindice :"+indice.intValue());
				oParams[indice.intValue()]=o;
			}
		}
		log.debug("- Fine.");
        return Arrays.asList(oParams);
    }
	/**
	 * Restituisce true se la data di modifica del file e' cambiata.
	 * */
	public boolean isModified(){
		File f = new File(sqlPath);
		return f.lastModified()!=dataModifica;
	}
	
	/**
	 * Carica una stringa SQL da un file con nome espresso tipo package java.
	 * Il repository da cui parte a cercare il file e' espresso nella costante:
	 * @see SQL_REPOSITORY */
	private String loadFromFile(String fileName){
		sqlPath = convertToPath(fileName);
		File f = new File(sqlPath);
		if (!f.exists()) throw new QueryNotFoundException(fileName);
		this.dataModifica = f.lastModified();  // Setto la data di modifica
		return IOUtils.getFileContent(f);
	}

	private String convertToPath(String fileName){
		if (fileName==null) throw new QueryNotFoundException(null);
		/*if(log.isDebugEnabled()){
			log.debug("\tSQL_REPOSITORY:"+SQL_REPOSITORY);
			log.debug("\tfileName:"+fileName);
		}*/
		StringBuffer result = new StringBuffer();
		result.append(sqlRepository);
		if (!sqlRepository.endsWith(File.separator))
			result.append(File.separator);
		result.append(fileName.replace('.',File.separatorChar));
		result.append(".sql");
		if(log.isDebugEnabled()){
			log.debug("\tresult:"+result);
		}
		return result.toString();
	}

	public Object clone(){
		return new Query(
			this.sqlRepository,
			this.sql,
			this.sqlPath,
			this.dataModifica,
			null,
			this.paramsDelimiter,
			this.parametersMappings,
			this.numberOfParams, 
            this.clazz
		);
	}
	
	public String getSql() {
		return sql;
	}

	public List getParameters() {
		return parameters;
	}

	public void setParameters(List parameters) {
		this.parameters = parameters;
	}

	public long getDataModifica() {
		return dataModifica;
	}

	public void setDataModifica(long dataModifica) {
		this.dataModifica = dataModifica;
	}

    public Class getClazz() {
        return clazz;
    }

    public void setClazz(Class clazz) {
        this.clazz = clazz;
    }
}