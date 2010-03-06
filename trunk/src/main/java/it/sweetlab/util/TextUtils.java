package it.sweetlab.util;

public class TextUtils {

    /**
     * Convert a string from Database to Java, so "FIRST_NAME" becomes "firstName".
     *
     * @param in
     * @return
     */
    public static String dbToJava(String in){
    	StringBuffer buff = new StringBuffer(in.length());
    	char chars[] = in.toCharArray();
    	boolean capitalizeNextChar = false;
    	for (int x=0; x< chars.length; x++){
    		char c = chars[x];
    		if(c == '_'){
    			capitalizeNextChar = true;	
    		} else {
        		if (capitalizeNextChar) buff.append(Character.toUpperCase(c));
        		else buff.append(Character.toLowerCase(c));
        		capitalizeNextChar = false;	
    		}
    	}
    	return buff.toString();
    }
    
    /**
     * Convert a string from Database to Java, so "FIRST_NAME" becomes "setFirstName".
     *
     * @param in
     * @return
     */
    public static String dbToJavaSetter(String in){
    	return dbToJavaSetter("SET_" + in);
    }
    
    /**
     * Convert a string from Database to Java, so "FIRST_NAME" becomes "setFirstName".
     *
     * @param in
     * @return
     */
    public static String dbToJavaGetter(String in){
    	return dbToJavaSetter("GET_" + in);
    }

    
    /**
     * Convert a string from Java to Database, so "firstName" becomes "FIRST_NAME".
     *
     * @param in
     * @return
     */
    public static String javaToDb(String in){
    	StringBuffer buff = new StringBuffer(in.length());
    	char chars[] = in.toCharArray();
    	for (int x=0; x< chars.length; x++){
    		char c = chars[x];
    		if(Character.isUpperCase(c)){
    			if(buff.length() > 0) buff.append('_');
    		} 
        	buff.append(Character.toUpperCase(c));
    	}
    	return buff.toString();
    }
}
