/**
 * This code based on org.apache.cassandra.exceptions
 */
package org.bigfs.fs.exceptions;


public class BigFSException extends Exception
{ 
    /**
     * 
     */
    private static final long serialVersionUID = -2025127803834422571L;
    
    private final BigFSExceptionCode code;
    
    public BigFSException(String msg, BigFSExceptionCode code)
    {
        super(msg);
        this.code = code;
    }
    
    protected BigFSException(String msg, BigFSExceptionCode code, Throwable cause)
    {
        super(msg, cause);
        this.code = code;
    }

    public BigFSExceptionCode code()
    {
        return code;
    }
}
