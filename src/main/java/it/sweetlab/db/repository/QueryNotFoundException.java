/*
 * QueryNotFoundException.java
 *
 */

package it.sweetlab.db.repository;


/**
 * @author Davide */
public class QueryNotFoundException extends NullPointerException {
    
    private static final long serialVersionUID = 3294408054877266609L;
	/** Creates a new instance of QueryNotFoundException */
    public QueryNotFoundException () {
        super();
    }
    public QueryNotFoundException(String message){
        super(message);
    }
}