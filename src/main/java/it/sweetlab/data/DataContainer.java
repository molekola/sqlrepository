package it.sweetlab.data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DataContainer extends ArrayList implements Serializable{

	private static final long serialVersionUID = -2450707142428929565L;

    private static Log sysLog = LogFactory.getLog(DataContainer.class);

	private Map rows = null;

	public DataContainer() {
		rows = new HashMap();
	}

	public Object getAbsolute(int posizione, String nomeAttributo) {
		Map mappa = new HashMap();
		nomeAttributo = nomeAttributo.toUpperCase().trim();
		if ((posizione < this.size())
				&& (((HashMap) this.get(posizione)).containsKey(nomeAttributo))) {
			mappa = (HashMap) this.get(posizione);
			return mappa.get(nomeAttributo);
		} else
			return null;
	}

	public Map getRows() {
		return rows;
	}

	public void setRows(Map rows) {
		this.rows = rows;
	}

	/**
	 * Restituisce il primo elemento del DataContainer. Se la HashMap come primo
	 * elemento e' null, crea una nuova HashMap, vuota. Se si forzasse una cosa
	 * differente da HashMap, darebbe un ClassCastException. Serve soprattutto
	 * per le query che restituiscono solo un'elemento, una comodita'.
	 */
	public HashMap getFirst() {
		if (this.isEmpty())
			return new HashMap();
		else {
			HashMap result = (HashMap) this.get(0);
			return result == null ? new HashMap() : result;
		}
	}

  /**
   * contains(String[] keys, Object[] values) ritorna true se trova HashMap al
   * suo interno che, a parita' di indice, corrispondano a tutte le coppie
   * chiave - valore.
   *
   * Per questo motivo e' necessario che sia l'array di chiavi, che l'array di
   * valori abbiano lo stesso numero di elementi.
   *
   * **/

  public boolean contains(String[] keys, Object[] values){

    if (
      (keys   == null)     ||
      (values == null)     ||
      (keys.length == 0)   ||
      (values.length == 0) ||
      (keys.length != values.length)
    ) return false;

    for (int i = 0 ; i < size() ; i++ ){
      if (contains(keys, values, i)) return true;
    }

    return false;
  }

  public boolean contains(String keys, Object values){
    if ((keys == null) || (values == null)) return false;
    String[] k = {keys};
    Object[] o = {values};
    return contains(k,o);
  }

  /**
   * contains(String[] keys, Object[] values, int index) ritorna true se nell'
   * HashMap alla posizione i-esima del datacontainer, a parita' di indice,
   * corrispondano a tutte le coppie chiave - valore.
   *
   * Per questo motivo e' necessario che sia l'array di chiavi, che l'array di
   * valori abbiano lo stesso numero di elementi.
   *
   * **/

  public boolean contains(String[] keys, Object[] values, int index){
    if (
      (keys          == null          ) ||
      (values        == null          ) ||
      (keys.length   == 0             ) ||
      (values.length == 0             ) ||
      (keys.length   != values.length ) ||
      (size()        <  index         )
    ) return false;

    Map m = (HashMap) this.get(index);

    if (m  == null) return false;

    for (int i = 0 ; i < keys.length ; i++){
      if (
        !(
          (m.containsKey(keys[i])) &&
          (m.get(keys[i]) != null) &&
          (m.get(keys[i]).equals(values[i]))
        )
      ) return false;
    }

    return true;
  }

  public Object contains(String key, Object value, String anotherKey){

    if ( (key == null) || (value == null) ) return null;

    for (int i = 0 ; i < size() ; i++){
      Map m = (HashMap)get(i);
      if (
        m.containsKey(key) &&
        m.get(key).equals(value) &&
        m.containsKey(anotherKey) &&
        (m.get(anotherKey) != null)
      )
        return m.get(anotherKey);
    }

    return null;
  }

  public Object getFirst(String key, Object value){
    if ( (key == null) || (value == null) ) return null;

    for (int i = 0 ; i < size() ; i++){
      Map m = (HashMap)get(i);
      if (m.containsKey(key) && m.get(key).equals(value))
        return m;
    }

    return null;
  }

  /**
   * Restituisce un sottoinsieme di DataContainer,
   * tale che a @param key corrisponda @param value.
   * **/
  public DataContainer get(String key, Object value){
    if ( (key == null) || (value == null) ) return null;

    DataContainer result = new DataContainer();

    for (int i = 0 ; i < size() ; i++){
      Map m = (HashMap)get(i);
      if (m.containsKey(key) && (m.get(value) != null))
        result.add(m);
    }

    return result;
  }

  /**
   * Restituisce un sottoinsieme di DataContainer,
   * tale che a @param keys corrisponda @param values,
   * a parita' di indice, sugli array.
   * Es. m.get(keys[i]).equals(value[i])
   * **/
//  public DataContainer get(String[] keys, Object[] values, boolean[] condictions){
  public DataContainer get(String[] keys, Object[] values){

    if ( (keys == null) || (values == null) ) return null;

    DataContainer result = new DataContainer();

    for (int i = 0 ; i < size() ; i++){
      Map m = (HashMap)get(i);
      if (contains(keys, values, i))
        result.add(m);
    }

    return result;
  }

  /**
   * Restituisce un sottoinsieme di DataContainer,
   * tale che a @param keys corrisponda @param values,
   * a parita' di indice, sugli array.
   * Esclude a priori l'elemento alla posizione @param exclude.
   * Es. m.get(keys[i]).equals(value[i])
   * **/
  public DataContainer get( String[] keys,
                            Object[] values,
                            boolean[] condictions,
                            int exclude){

    if ( (keys == null) || (values == null) ) return null;

    DataContainer result = new DataContainer();

    for (int i = 0 ; i < size() ; i++){
      if (i == exclude) continue;
      Map m = (HashMap)get(i);
      if (contains(keys, values, condictions, i))
        result.add(m);
    }

    return result;
  }

  /**
   * contains(String[] keys, Object[] values, int index) ritorna true se nell'
   * HashMap alla posizione i-esima del datacontainer, a parita' di indice,
   * corrispondano a tutte le coppie chiave - valore.
   *
   * Per questo motivo e' necessario che sia l'array di chiavi, che l'array di
   * valori abbiano lo stesso numero di elementi.
   *
   * **/

  public boolean contains(String[] keys, Object[] values, boolean[] condictions, int index){
    if (
      (keys          == null          ) ||
      (values        == null          ) ||
      (condictions   == null          ) ||
      (keys.length   == 0             ) ||
      (values.length == 0             ) ||
      (condictions.length == 0        ) ||
      (keys.length != values.length   ) ||
      (condictions.length != values.length ) ||
      (condictions.length != keys.length ) ||
      (size()        <  index         )
    ) return false;

    Map m = (HashMap) this.get(index);

    for (int i = 0 ; i < keys.length ; i++){
      if (
        !(
          (
            m.containsKey(keys[i])
              && (m.get(keys[i]) != null)
              && (condictions[i] ? m.get(keys[i]).equals(values[i]) : !m.get(keys[i]).equals(values[i]))
          )
        )
      ) {
        sysLog.debug("DataContainer.contains.test["+i+"] : false");
        return false;
      }
        sysLog.debug("DataContainer.contains.test["+i+"] : true");
    }

    return true;
  }



  /**
   * Scorre il DataContainer e, se trova l'oggetto "value",
   * ne restituisce l'indice di riga corrispondente.
   * Restituisce -1 se non trova nulla.
   * Per esempio, in un DataContainer dt che contiene:
   *
   * dt:{[a=pippo, b=pluto, c=paperino],
   *     [a=foo,   b=bar,   d=xxxx]}
   *
   * dt.verticalObjectIndex("bar") = 1.
   *
   * e
   *
   * dt.verticalObjectIndex("paperino") = 0.
   *
   * e
   *
   * dt.verticalObjectIndex("minni") = -1.
   *
   * **/
  public int verticalObjectIndex(Object value) {
		for (int i = 0; i < this.size(); i++) {
			Map map = (HashMap) this.get(i);
			if (map.containsValue(value))
				return i;
		}
		return -1;
	}

/*
 * public int verticalNextObjectIndex(Object value, Object visitedKey, Object
 * visitedValue){ for (int i = 0 ; i < this.size() ; i++){ Map map =
 * (HashMap)this.get(i); if (map.containsValue(value)) { if
 * (sysLog.isDebugEnabled()){
 * sysLog.debug("DataContainer.verticalNextObjectIndex.value :"+value);
 * sysLog.debug("DataContainer.verticalNextObjectIndex.map :"+map);
 * sysLog.debug("DataContainer.verticalNextObjectIndex.visitedKey
 * :"+visitedValue);
 * sysLog.debug("DataContainer.verticalNextObjectIndex.visitedValue
 * :"+visitedValue); } if (! ((map.containsKey(visitedKey)) &&
 * (map.get(visitedKey).equals(visitedValue))) ){ map.put(visitedKey,
 * visitedValue); return i; } } } return -1; }
 */

/*  public HashMap verticalNextObject(Object value, int currentIndex){
    for (int i = 0 ; i < this.size() ; i++){
      HashMap map = (HashMap)this.get(i);
      if (map.containsValue(value) && (currentIndex != i)) {
        return map;
      }
    }
    return null;
  }*/

  /**
   * @param testMaxKey contiene la chiave sulla quale testare il massimo valore.
   * @param testMaxKey DEVE contenere un numero intero, altrimenti solleva
   * un'eccezione.
   * */
  public HashMap verticalHigherObject(Object value, String key, int currentIndex, Object testMaxKey){
    int index  = -1;
    int maxVal = -1;
    sysLog.debug("DataContainer.verticalNextObject("+value+","+key+","+currentIndex+","+testMaxKey+")");

    if (currentIndex < (this.size() -1)){
      for (int i = 0 ; i < currentIndex ; i++){
        HashMap map = (HashMap)this.get(i);

        if (sysLog.isDebugEnabled()){
          sysLog.debug("DataContainer.verticalNextObject.i                            :"+i);
          sysLog.debug("DataContainer.verticalNextObject.index                        :"+index);
          sysLog.debug("DataContainer.verticalNextObject.maxVal                       :"+maxVal);
          sysLog.debug("DataContainer.verticalNextObject.map.get("+key+")         :"+map.get(key));
          sysLog.debug("DataContainer.verticalNextObject.containsKey("+key+")     :"+map.containsKey(key));
          sysLog.debug("DataContainer.verticalNextObject.map.get    ("+key+")=="+value+":"+(map.get(key) == value));
          sysLog.debug("DataContainer.verticalNextObject.currentIndex != i           :"+(currentIndex != i));
        }

        if (map.containsKey(key) && (map.get(key).equals(value))) {
          int thisVal = ((Integer)map.get(testMaxKey)).intValue();
          sysLog.debug("DataContainer.verticalNextObject.thisVal :"+thisVal);
          if ( thisVal > maxVal  ){
            maxVal = thisVal;
            index = i;
          }
        }

      }
    }

    if (index == -1) return null;
    else return (HashMap)this.get(index);
  }

  /**
   * incrementa il contenuto della chiave @param enumerateKey, progressivamente,
   * per ogni coppia "chiave | valore" uguale ai parametri @param value e
   * @param key.
   *
   * In poche parole conta il numero di coppie "chiave | valore" uguali
   * all'interno del DataContainer e ne assegna il valore progressivamente
   * alla chiave @param enumerateKey. Se non esiste, la crea per ogni riga,
   * altrimenti la ricopre.
   *
   * il conteggio parte da @param beginNumber.
   * **/

	public void enumerateEqualKeys(
		String key, 
		Object value,
		Object enumerateKey, 
		int beginNumber
	) {
		int lastValue = beginNumber;
		for (int i = 0; i < this.size(); i++) {
			HashMap map = (HashMap) this.get(i);
			if (map.containsKey(key) && (map.get(key).equals(value))) {
				map.put(enumerateKey, new Integer(lastValue));
				lastValue = ((Integer) map.get(enumerateKey)).intValue() + 1;
			}
		}
	}

  /***************************************************************************
	 * incrementa il contenuto della chiave
	 * 
	 * @param enumerateKey,
	 *            progressivamente, per ogni coppia "chiave | valore" uguale ai
	 *            parametri
	 * @param value
	 *            e
	 * @param key.
	 * 
	 * In poche parole conta il numero di coppie "chiave | valore" uguali
	 * all'interno del DataContainer e ne assegna il valore progressivamente
	 * alla chiave
	 * @param enumerateKey.
	 *            Se non esiste, la crea per ogni riga, altrimenti la ricopre.
	 * 
	 * il conteggio parte da
	 * @param beginNumber.
	 **************************************************************************/
  public int countEqualKeys(String key, Object value){

    int lastValue = 0;

    for (int i = 0 ; i < this.size() ; i++){

      HashMap map = (HashMap)this.get(i);
      if (map.containsKey(key) && (map.get(key).equals(value))) {
        lastValue++;
      }

    }

    return lastValue;

  }

  /*****************************************************************************
   * Conta il numero di elementi con
   *
   * @param chiave[i]
   * @param valore[i]
   *
   * uguali.
   ****************************************************************************/
  public int countEqualKeys(String[] key, Object[] value){

    int lastValue = 0;

    for (int i = 0 ; i < this.size() ; i++){

      if (contains(key, value, i)) {
        lastValue++;
      }

    }

    return lastValue;

  }

  /*****************************************************************************
   * nextValueOf restituisce il valore massimo + 1 corrispondente al parametro
   *
   * @param keyMax,
   *          a parita' di
   * @param key
   *          e
   * @param value.
   ****************************************************************************/

  public int nextValueOf(String key, Object value, String keyMax){

    if (sysLog.isDebugEnabled()){
      sysLog.debug("DataContainer.nextValueOf.params:");
      sysLog.debug("\tString key    :"+key    );
      sysLog.debug("\tObject value  :"+value  );
      sysLog.debug("\tString keyMax :"+keyMax );
    }

    int lastValue = 0;

    for (int i = 0 ; i < this.size() ; i++){

      HashMap map = (HashMap)this.get(i);

      if ((map.get(key) != null) &&
         (map.get(key).equals(value)) &&
         (((Integer)map.get(keyMax)).intValue() > lastValue)) {
        lastValue = ((Integer)map.get(keyMax)).intValue();
      }

    }

    lastValue++;

    return lastValue;

  }

  /*****************************************************************************
   * restituisco il valore di nKei maggiore, + 1.
   ****************************************************************************/
  public int countMajorValue(String key, Object value, String nKey){

//    if (sysLog.isDebugEnabled()) {
//      sysLog.debug("DataContainer.countMajorValue().params:");
//      sysLog.debug("\tkey   :"+key   );
//      sysLog.debug("\tvalue :"+value );
//      sysLog.debug("\tnKey  :"+nKey  );
//    }
    int lastValue = 0;

    for (int i = 0 ; i < this.size() ; i++){

      //sysLog.debug("DataContainer.countMajorValue().for:");
      HashMap map = (HashMap)this.get(i);
      if (map.containsKey(key) &&
          (map.get(key).equals(value)) &&
          (map.get(nKey) != null) &&
          (map.get(nKey) instanceof Integer)
          ){

        int x = ((Integer)map.get(nKey)).intValue();

        if (x >= lastValue)
          lastValue = x+1;

      }
    }
    return lastValue;
  }


  public void remove(int eCanc, String key, Object value){

    while (!isEmpty() && (eCanc < size())){

      Map m = (HashMap)get(eCanc);

      if ((m.get(key) != null) && (((String)m.get(key)).equals(value))){
        remove(eCanc);
      } else break;

    }

  }

  /**
   * incrementa il contenuto della chiave @param enumerateKey, progressivamente,
   * per ogni coppia "chiave | valore" uguale ai parametri @param value e
   * @param key, solo se l'oggetto non contiene la chiave @param nullKey .
   *
   * il conteggio parte da @param beginNumber.
   * **/
  public void enumerateEqualKeys(Object value, String key, String nullKey, Object enumerateKey, int beginNumber){
    if (sysLog.isDebugEnabled()){
      sysLog.debug("DataContainer.enumerateEqualKeys.params:");
      sysLog.debug("\tvalue        :"+value       );
      sysLog.debug("\tkey          :"+key         );
      sysLog.debug("\tnullKey      :"+nullKey     );
      sysLog.debug("\tenumerateKey :"+enumerateKey);
      sysLog.debug("\tbeginNumber  :"+beginNumber );
    }
    int lastValue = beginNumber;
    for (int i = 0 ; i < this.size() ; i++){
      HashMap map = (HashMap)this.get(i);
      if (sysLog.isDebugEnabled()){
        sysLog.debug("DataContainer.enumerateEqualKeys.map.size                          :"+map.size());
        sysLog.debug("DataContainer.enumerateEqualKeys.map.containsKey("+key+")          :"+map.containsKey(key));
        sysLog.debug("DataContainer.enumerateEqualKeys.map.get("+key+")                  :"+map.get(key));
//        sysLog.debug("DataContainer.enumerateEqualKeys.map.get("+key+").equals("+value+"):"+map.get(key).equals(value));
//        sysLog.debug("DataContainer.enumerateEqualKeys.!map.containsKey("+nullKey+")     :"+!map.containsKey(nullKey));
      }
      if (map.containsKey(key) && (map.get(key) != null) && (map.get(key).equals(value)) && (!map.containsKey(nullKey))) {
        map.put(enumerateKey, new Integer(lastValue));
        lastValue = ((Integer)map.get(enumerateKey)).intValue() + 1;
      }
    }
  }

  /**
     * Permette di verificare l'esistenza di un attributo di nome
     * <em>attributeName</em> in un DataContainer.
     * @param attributeName � il nome dell'attributo.
     * @return true - se l'attributo cercato esiste;<br>
     *         false - se non esiste.
     */
  public boolean existAttribute(Object nameAttribute ,int posizioneRiga){
    Map map = (HashMap) this.get(posizioneRiga);
    if(map.containsKey((nameAttribute))) {
      return true;
    }else{
      return false;
    }
  }

  public void toStringDaCont()throws Exception{
//    String objectToString ="";
    for(int i=0;i<this.size();i++){
      sysLog.debug("DataContainer.toStringDataCont.posizione record  :"+i);
      Iterator itr = rows.values().iterator();
      while(itr.hasNext()){
        sysLog.debug("DataContainer.toStringDaCont()__contenuto field"+itr.next());
      }
    }
  }

  public String toString(){
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i<size(); i++){
      //sb.append("riga["+i+"]:<br />");
      sb.append("\n\triga["+i+"]:");
      //sb.append(this.get(i).toString());
      HashMap m = (HashMap)this.get(i);
      Iterator iKey = m.keySet().iterator();
      while(iKey.hasNext()){
        String s = (String)iKey.next();
        sb.append("\n\t");
        sb.append(s);
        sb.append(" : ");
        sb.append(m.get(s));
      }
    }
    String s = new String(sb);
    return s;
  }

  public void replace(int index, String key, Object obj){
    ((HashMap)this.get(index)).put(key, obj);
  }

  /**
   * @deprecated
   * **/
  public void addRecord (Map rows){
    add(rows);
  }

  public void add(Map rows){
    super.add(rows);
  }

  public int getNumeroRighe(){
    return super.size();
  }
}