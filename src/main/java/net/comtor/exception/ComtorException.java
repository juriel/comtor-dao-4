/*
 * ComtorException.java
 *
 * Created on July 19, 2007, 4:21 AM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package net.comtor.exception;

public class ComtorException extends Exception{
    
	private static final long serialVersionUID = 1L;
    
    private String message = null;
    private Throwable originalException = null;
        
    /** Creates a new instance of BusinessLogicException */
    public ComtorException(String message) {
        super(message);
    }
    
    /** Creates a new instance of BusinessLogicException */
    public ComtorException(Throwable throwable) {
        super(throwable);
        this.originalException = throwable;
    }
    
    public ComtorException(String message, Throwable throwable) {
        super(throwable);
        this.message = message;
        this.originalException = throwable;
    }    
    
    public String getMessage(){
        if (message != null){
            return message;
        }
        return super.getMessage();
    }
    
    public Throwable getOriginalException() {
		return originalException;
	}
    
    public String getOriginalMessage() {            
        if (originalException != null){
            return originalException.getMessage();
        }
        return "No original message";
    }
}
