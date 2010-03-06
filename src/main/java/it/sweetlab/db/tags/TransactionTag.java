/*
 * TransactionTag.java
 *
 * Created on 25 gennaio 2007, 11.40
 *
 */

package it.sweetlab.db.tags;

import it.sweetlab.db.DataLink;

import javax.servlet.jsp.JspException;
import javax.servlet.jsp.tagext.TagSupport;

import org.apache.taglibs.standard.lang.support.ExpressionEvaluatorManager;
/**
 * @author Davide Gurgone
 */
public class TransactionTag extends TagSupport{
    private static final long serialVersionUID = 406922623477211625L;

	public static final String DEFAULT_TRANSACTION_NAME = "_jspDataLinkObject";
    
    private String var = DEFAULT_TRANSACTION_NAME;
    
    public int doStartTag() throws JspException {
        DataLink dl = new DataLink();
		var = (String) ExpressionEvaluatorManager.evaluate(
			"var",
			var, 
			String.class, 
			this,
			pageContext
		);
        pageContext.setAttribute(var, dl);
        return EVAL_BODY_INCLUDE;
    }

    public int doEndTag() throws JspException {
        DataLink dl = (DataLink)pageContext.getAttribute(var);
        dl.release();
        return EVAL_PAGE;
    }

    public void setVar(String var) {
        this.var = var;
    }

    

}
