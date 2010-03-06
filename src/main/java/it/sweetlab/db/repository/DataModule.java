package it.sweetlab.db.repository;

import it.sweetlab.db.JNDIConf;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DataModule implements Serializable{

	private static final long serialVersionUID = 2406771743812304127L;
	
	private static boolean checkSQLFileReload = JNDIConf.isCheckSQLFileReload();
	private static Log log = LogFactory.getLog(DataModule.class);
	
	private static Map repositories = new HashMap();

	/**
	 * Restituisce un oggetto Query esistente se e' presente nel repository in
	 * memoria e se non sono state apportate modifiche al file.
	 * Altrimenti restituisce un nuovo oggetto Query preso dal repository su 
	 * File System.
	 * */
	public static Query getQuery(
        Class c,
		String sqlPath, 
		Map params,
		String sqlRepository
	){
		Map repository = (Map)repositories.get(sqlRepository);
		if(repository == null) {
			repository = new HashMap();
			repositories.put(sqlRepository, repository);
		}
		Query q = (Query)repository.get(sqlPath);
		if ((q == null) || (q != null && checkSQLFileReload && q.isModified())){

			log.info("reloading query:\n"+sqlPath);

			q = new Query( sqlRepository, sqlPath, params, c );
			repository.put(sqlPath,q.clone()); // Salvo in memoria un clone dell'oggetto

		} else {

			log.info("return query:\n"+sqlPath);

			q = (Query)q.clone(); // Clono l'oggetto prima di abbinare i parametri.
			q.generateJDBCParameterList(params);

		}
		return q;
	}

    /**
	 * Restituisce un oggetto Query esistente se e' presente nel repository in
	 * memoria e se non sono state apportate modifiche al file.
	 * Altrimenti restituisce un nuovo oggetto Query preso dal repository su 
	 * File System.
	 * */
	public static Query getQuery(String sqlPath, Map params){ 
        // Per default, la classe usata da getQuery quando non ho la classe di appartenenza, uso Query.class.
		return getQuery(Query.class, sqlPath, params, JNDIConf.getSqlRepository());
	}

	public static Query getQuery(Class c, String name, Map params){
		String sqlPath=c.getPackage().getName()+"."+name;
        return getQuery(c, sqlPath, params, JNDIConf.getSqlRepository());
        //return getQuery(sqlPath,params);
	}

	public static Query getQuery(String sqlRepository, Class c, String name, Map params){
		String sqlPath=c.getPackage().getName()+"."+name;
		return getQuery(c, sqlPath,params,sqlRepository);
	}
	
	public static Query getQuery(Class c, Map params){
		String sqlPath=c.getPackage().getName()+"."+c.getName();
		return getQuery(c, sqlPath, params);
        //return getQuery(sqlPath,params);
	}
	public static Query getQuery(String sqlRepository, Class c, Map params){
		String sqlPath=c.getPackage().getName()+"."+c.getName();
		return getQuery(c, sqlPath,params,sqlRepository);
	}


	/**
	 * Restituisce un oggetto Query esistente se e' presente nel repository in
	 * memoria e se non sono state apportate modifiche al file.
	 * Altrimenti restituisce un nuovo oggetto Query preso dal repository su 
	 * File System.
     * Per elaborazione query di tipo batch.
	 * */
	public static Query getQuery(
        Class c,
		String sqlPath, 
		List params,
		String sqlRepository
	){
		Map repository = (Map)repositories.get(sqlRepository);
		if(repository == null) {
			repository = new HashMap();
			repositories.put(sqlRepository, repository);
		}
		Query q = (Query)repository.get(sqlPath);
		if ((q == null) || (q != null && checkSQLFileReload && q.isModified())){

			log.info("reloading query:\n"+sqlPath);

			q = new Query( sqlRepository, sqlPath, params, c );
			repository.put(sqlPath,q.clone()); // Salvo in memoria un clone dell'oggetto

		} else {

			log.info("return query:\n"+sqlPath);

			q = (Query)q.clone(); // Clono l'oggetto prima di abbinare i parametri.
			q.generateBatchParameterList(params);

		}
		return q;
	}

    /**
	 * Restituisce un oggetto Query esistente se e' presente nel repository in
	 * memoria e se non sono state apportate modifiche al file.
	 * Altrimenti restituisce un nuovo oggetto Query preso dal repository su 
	 * File System.
	 * */
	public static Query getQuery(String sqlPath, List params){ 
        // Per default, la classe usata da getQuery quando non ho la classe di appartenenza, uso Query.class.
		return getQuery(Query.class, sqlPath, params, JNDIConf.getSqlRepository());
	}

	public static Query getQuery(Class c, String name, List params){
		String sqlPath=c.getPackage().getName()+"."+name;
        return getQuery(c, sqlPath, params, JNDIConf.getSqlRepository());
	}

	public static Query getQuery(String sqlRepository, Class c, String name, List params){
		String sqlPath=c.getPackage().getName()+"."+name;
		return getQuery(c, sqlPath,params,sqlRepository);
	}
	
	public static Query getQuery(Class c, List params){
		String sqlPath=c.getPackage().getName()+"."+c.getName();
		return getQuery(c, sqlPath, params);
	}
    
	public static Query getQuery(String sqlRepository, Class c, List params){
		String sqlPath=c.getPackage().getName()+"."+c.getName();
		return getQuery(c, sqlPath,params,sqlRepository);
	}
    
	/**
	 * Invalida tutti i repository.
	 * */
	public static void invalidateRepository(){
		repositories.clear();
	}

	/**
	 * Invalida il repository selezionato.
	 * */
	public static void invalidateRepository(String sqlRepository){
		Map repository = (Map)repositories.get(sqlRepository);
		if (repository!=null) {
			repository.clear();
		}
	}
}
