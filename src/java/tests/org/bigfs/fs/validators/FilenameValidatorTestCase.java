package org.bigfs.fs.validators;

import junit.framework.TestCase;
import junit.framework.TestSuite;




public class FilenameValidatorTestCase extends TestCase 
{
   
    public void testIsValid()
    {
        assertTrue("regular filename", FilenameValidator.isValid("/home/bigfs.txt"));
        assertFalse("relative filename", FilenameValidator.isValid("./home/bigfs.txt"));
        assertFalse("path starts path sign", FilenameValidator.isValid(":/home/bigfs.txt"));
        assertFalse("path starting with ..", FilenameValidator.isValid("../home/bigfs.txt"));
        assertFalse("path including ..", FilenameValidator.isValid("/home/../bigfs.txt"));
        assertFalse("path including  .", FilenameValidator.isValid("/home/./bigfs.txt"));
        assertFalse("path including  path sign", FilenameValidator.isValid("/home:/bigfs.txt"));
        assertFalse("path including  path sign",FilenameValidator.isValid("/home/:/bigfs.txt"));
        assertTrue("filename including double dot", FilenameValidator.isValid("/home/bigfs..txt"));
        assertTrue("filename starting with dot",FilenameValidator.isValid("/home/.bigfs.txt"));
    }
    
    
    public static TestSuite suite() {
        return new TestSuite(FilenameValidatorTestCase.class);
    }

}

