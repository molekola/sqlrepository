/*
 * GetQueryTag.java
 *
 * Created on 25 gennaio 2007, 10.47
 *
 */

package it.sweetlab.db.tags;

import it.sweetlab.db.DataLink;
import it.sweetlab.db.repository.DataModule;
import java.sql.SQLException;
import java.util.Map;
import javax.servlet.jsp.JspException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.taglibs.standard.lang.support.ExpressionEvaluatorManager;

/**
 *
 * @author Davide Gurgone
 */
public class GetQueryTag extends javax.servlet.jsp.tagext.TagSupport{
    
    private static final long serialVersionUID = 1L;
	private String sqlPath;
    private String params;
    private String beginIndex = null;
    private String endIndex = null;
    private String transaction = TransactionTag.DEFAULT_TRANSACTION_NAME;
    private String var = "quRowResult";
    
    private static Log log = LogFactory.getLog(GetQueryTag.class);
    
    public int doStartTag() throws JspException {
        
		sqlPath = (String) ExpressionEvaluatorManager.evaluate(             //ok
			"sqlPath",
			sqlPath, 
			String.class, 
			this,
			pageContext
		);
		var = (String) ExpressionEvaluatorManager.evaluate(                 //ok
			"var",
			var, 
			String.class, 
			this,
			pageContext
		);
		Map mParams = (Map) ExpressionEvaluatorManager.evaluate(            //ok
			"mParams",
			params, 
			Map.class, 
			this,
			pageContext
		);
        log.debug("mParams:"+mParams);
		Integer iBeginIndex = null;
        Integer iEndIndex = null;
        if (beginIndex != null && endIndex != null){
            iBeginIndex = (Integer) ExpressionEvaluatorManager.evaluate(//ok
                "beginIndex",
                beginIndex, 
                Integer.class, 
                this,
                pageContext
            );
            iEndIndex = (Integer) ExpressionEvaluatorManager.evaluate(  //ok
                "endIndex",
                endIndex, 
                Integer.class, 
                this,
                pageContext
            );
        }
		transaction = (String) ExpressionEvaluatorManager.evaluate(         //ok
			"transaction",
			transaction, 
			String.class, 
			this,
			pageContext
		);
        
		DataLink dl = (DataLink) pageContext.getAttribute(transaction);/*ExpressionEvaluatorManager.evaluate(       //ok
			transaction,
			transaction, 
			DataLink.class, 
			this,
			pageContext
		);*/

        try{
            if(iBeginIndex!=null&&iEndIndex!=null){
                pageContext.setAttribute(
                    var, 
                    dl.execQuery(
                        DataModule.getQuery(
                            sqlPath,
                            mParams
                        ),
                        iBeginIndex.intValue(),
                        iEndIndex.intValue()
                    )
                );
            } else {
                pageContext.setAttribute(
                    var, 
                    dl.execQuery(
                        DataModule.getQuery(
                            sqlPath,
                            mParams
                        )
                    )
                );
            }
        } catch (SQLException e) {
            throw new JspException(e);
        }
        return EVAL_BODY_INCLUDE;
    }

    public int doEndTag() throws JspException {
        return EVAL_PAGE;
    }

    public int doAfterBody() throws JspException {
        return EVAL_PAGE;
    }

    public void setBeginIndex(String beginIndex) {
        this.beginIndex = beginIndex;
    }

    public void setEndIndex(String endIndex) {
        this.endIndex = endIndex;
    }

    public void setParams(String params) {
        this.params = params;
    }

    public void setSqlPath(String sqlPath) {
        this.sqlPath = sqlPath;
    }

    public void setVar(String var) {
        this.var = var;
    }

}
