package org.bigfs.fs.exceptions;

import java.util.HashMap;
import java.util.Map;



public enum BigFSExceptionCode
{
    
    SERVER_ERROR    (0x000B),

    BAD_CREDENTIALS (0x0100),

    // Axx: problem on files
    INVALID_FILENAME   (0xA000),
    PERMISSION_ERROR   (0xA001),
    FILE_EXISTS         (0xA002),
    FILE_NOT_EXISTS         (0xA003),
    
    //Bxx: Problem on directories
    DIRECTORY_NOT_FOUND   (0xB001),
    
    UNKOWN_ERROR(0xCCCC);
    
    public final int value;
    
    private static final Map<Integer, BigFSExceptionCode> valueToCode = new HashMap<Integer, BigFSExceptionCode>(BigFSExceptionCode.values().length);
    static
    {
        for (BigFSExceptionCode code : BigFSExceptionCode.values())
            valueToCode.put(code.value, code);
    }
    
    public static BigFSExceptionCode fromCode(int code)
    {
        return valueToCode.get(code);
    }

    private BigFSExceptionCode(int value)
    {
        this.value = value;
    }

}