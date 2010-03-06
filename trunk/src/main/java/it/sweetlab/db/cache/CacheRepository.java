package it.sweetlab.db.cache;

import it.sweetlab.db.DataLink;
import it.sweetlab.db.repository.DataModule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.collection.SynchronizedCollection;

public class CacheRepository {
	private static int cacheLimit = 200;
	private static Map cachedContents = new HashMap(cacheLimit);
	private static List cacheIndex = (List)SynchronizedCollection.decorate(new ArrayList());
	
	/** Aggiungo una voce in cache 
	 * @param id la chiave con cui memorizzare i dati, l'uuid o il canale.
	 * @param name il nome della query per intenderci. 
	 * @param content il risultato della query da cacheare. */
	public static void addCachedContent(String id, String name, Object content) {
		// Creo l'oggetto risultato della query, da cacheare.
		Map nameToCache = new HashMap(1);
		nameToCache.put(name, content);

		// Recupero l'oggetto ID, se non esiste lo creo
		Map idToCache = (Map)cachedContents.get(id);
		if (idToCache==null) idToCache = new HashMap(10);
		idToCache.put(id, nameToCache);

		// Se l'indice ha raggiunto il limite, rimuovo il piu' vecchio elemento (posizione 0)
		if (cacheIndex.size()>=cacheLimit) {
			cachedContents.remove(cacheIndex.remove(0));
		}

		// Aggiungo il nuovo oggetto alla cache
		cacheIndex.add(cachedContents.put(id, idToCache));
	}

	/**
	 * Estrae una voce dalla cache, da una coppia chiave, id. */
	public static Object getFromCache(String name, String id) {
		Map idToCache = (Map)cachedContents.get(id);
		if (idToCache==null) return null;
		Map nameToCache = (Map)idToCache.get(name);
		if (nameToCache==null) return null;
		return nameToCache.get(id);	
	}

	/**
	 * Estrae una voce dalla cache, date una coppia di nome, id.
	 * Se non c'e' la voce in cache, la aggiunge. */
	public static Object getFromCache(
		String name, 
		String id, 
		DataLink dl, 
		Class clazz,
		String sqlName,
		Map params
	) throws Exception {
		Object result = getFromCache(name, id);
		if (result==null) {
			result = dl.execQuery(
				DataModule.getQuery(
					clazz,
					sqlName,
					params
				)
			);
			addCachedContent(id, name, result);
		}
		return result;
	}

	/**
	 * Estrae una voce dalla cache, date una coppia di nome, id.
	 * Se non c'e' la voce in cache, la aggiunge. */
	public static Object getFromCache(
		String name, 
		String id, 
		Class clazz,
		String sqlName,
		Map params
	) {
		DataLink dl = new DataLink();
		try {
			return getFromCache(name, id, dl, clazz, sqlName, params);
		} catch(Exception e) {
			dl.rollback();
			return null;
		} finally {
			dl.release();
		}
	}
}