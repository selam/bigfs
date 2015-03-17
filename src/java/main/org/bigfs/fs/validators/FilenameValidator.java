package org.bigfs.fs.validators;

import java.util.StringTokenizer;

public class FilenameValidator
{
    public static final String SEPARATOR = "/";
    
    public static boolean isValid(String src)
    {
        // Path must be absolute.
        if (!src.startsWith(SEPARATOR)) {
          return false;
        }

        // Check for ".." "." ":" "/"
        StringTokenizer tokens = new StringTokenizer(src, SEPARATOR);
        while(tokens.hasMoreTokens()) {
          String element = tokens.nextToken();
          if (element.equals("..") ||
              element.equals(".")  ||
              (element.indexOf(":") >= 0)  ||
              (element.indexOf("/") >= 0)) {
            return false;
          }
        }
        
        return true;        
    }

}
