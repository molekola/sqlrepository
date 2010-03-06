/*
 * BatchControllerExample.java
 *
 * Created on 13 marzo 2007, 11.54
 *
 */

package it.sweetlab.db;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Davide Gurgone
 */
public class BatchControllerExample implements BatchController{
    
    private List rows;
    private List exceptions;
    
    public BatchControllerExample(List rows) {
        this.rows = rows;
        this.exceptions = new ArrayList();
    }

    public void onAfterEnd() {
    }

    public void onBeforeStart() {
    }

    public void onExceptionDo(int index, Exception e) throws Exception {
        if(e.getMessage().startsWith("ORA-00001")){
            // Aggiungo la riga che ho inserito.
            exceptions.add(rows.get(index));
        } else {
            throw e;
        }
    }

    public void onSuccessDo(int index, int result) throws Exception {
    }
    
    public List getRows(){
        return rows;
    }

    public List getExceptions(){
        return exceptions;
    }

    
}
