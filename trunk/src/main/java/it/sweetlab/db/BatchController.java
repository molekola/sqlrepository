/*
 * BatchController.java
 *
 * Created on 13 marzo 2007, 11.41
 *
 */

package it.sweetlab.db;

/**
 *
 * @author Davide Gurgone
 */
public interface BatchController {
    
    /** Method called before start batch processing. */
    public void onBeforeStart();
    /** Method called if Exception raised. */
    public void onExceptionDo(int index, Exception e) throws Exception;
    /** Method called on single statement sucess. */
    public void onSuccessDo(int index, int result) throws Exception;
    /** Method called after process completed. */
    public void onAfterEnd();
}
