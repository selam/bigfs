package org.bigfs.fs.exceptions;

import java.io.IOException;

public class FileFullException extends IOException
{

    private static final long serialVersionUID = 1L;
    
    public FileFullException() {
        super();
    }
    
    
    public FileFullException(String couse) {
        super(couse);
    }
    
    
    public FileFullException(Throwable couse) {
        super(couse);
    }
}
