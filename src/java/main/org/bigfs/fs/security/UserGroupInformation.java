/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Based on haddop org.apache.hadoop.security.UnixUserGroupInformation;
 */

package org.bigfs.fs.security;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UserGroupInformation
{
    private static final Logger logger = LoggerFactory.getLogger(UserGroupInformation.class);
    
    public static final String DEFAULT_USER = "yoda";
    public static final String DEFAULT_GROUP = "jedi";
    
    private String userName;
    private String[] groupNames;
    
    public UserGroupInformation() 
    {    
        this(DEFAULT_USER, DEFAULT_GROUP);
    }

    public UserGroupInformation(String userName, String groupName)
    {
        setUserGroupNames(userName, new String[]{groupName});
    }

    public UserGroupInformation(String userName, String[] groupNames) 
    {
        setUserGroupNames(userName, groupNames);
    }
    
    /** Return an array of group names
     */
    public String[] getGroupNames() 
    {
        return groupNames;
    }

    /** Return the user's name
     */
    public String getUserName() 
    {
        return userName;
    }
    
    
    /** Decide if two UGIs are the same
    *
    * @param other other object
    * @return true if they are the same; false otherwise.
    */
   public boolean equals(Object other) 
   {
        if (this == other) 
        {
           return true;
        }
 
        if (!(other instanceof UserGroupInformation)) 
        {
            return false;
        }
     
        UserGroupInformation otherUGI = (UserGroupInformation)other;
     
        // check userName
        if (userName == null) 
        {
            if (otherUGI.getUserName() != null) 
            {
                return false;
            }
        } 
        else 
        {
            if (!userName.equals(otherUGI.getUserName())) 
            {
                return false;
            }
        }
     
         // checkGroupNames
        if (groupNames == otherUGI.groupNames)
        {
             return true;
        }
        if (groupNames.length != otherUGI.groupNames.length) 
        {
            return false;
        }
        // check default group name
        if (groupNames.length>0 && !groupNames[0].equals(otherUGI.groupNames[0])) 
        {
            return false;
        }
         // check all group names, ignoring the order
         return new TreeSet<String>(Arrays.asList(groupNames)).equals(
             new TreeSet<String>(Arrays.asList(otherUGI.groupNames)));
   }

   /** Returns a hash code for this UGI. 
    * The hash code for a UGI is the hash code of its user name string.
    * 
    * @return  a hash code value for this UGI.
    */
   public int hashCode() 
   {
     return getUserName().hashCode();
   }
   
   /** Convert this object to a string
    * 
    * @return a comma separated string containing the user name and group names
    */
   public String toString() 
   {
     StringBuilder buf = new StringBuilder();
     buf.append(userName);
     for (String groupName : groupNames) 
     {
       buf.append(',');
       buf.append(groupName);
     }
     
     return buf.toString();
   }

   public String getName() {
     return toString();
   }    
    
    
    private void setUserGroupNames(String userName, String[] groupNames) 
    {
        if (userName==null || userName.length()==0 ||
            groupNames== null || groupNames.length==0) 
        {
          throw new IllegalArgumentException(
              "Parameters should not be null or an empty string/array");
        }
        
        for (int i=0; i<groupNames.length; i++) 
        {
          if(groupNames[i] == null || groupNames[i].length() == 0) 
          {
            throw new IllegalArgumentException("A null group name at index " + i);
          }
        }
        
        this.userName = userName;
        this.groupNames = groupNames;
    }   
    
    /** Deserialize this object
     * First check if this is a UGI in the string format.
     * If no, throw an IOException; otherwise
     * set this object's fields by reading them from the given data input
     *  
     *  @param in input stream
     *  @exception IOException is thrown if encounter any error when reading
     */
    public void readFields(DataInput in) throws IOException 
    {
        try 
        {
          //read this object
          userName = in.readUTF();
          int numOfGroups = in.readInt();
          groupNames = new String[numOfGroups];      
          for (int i = 0; i < numOfGroups; i++) 
          {
            groupNames[i] = in.readUTF();
          }
        }
        catch(IOException e)
        {
            logger.info(e.toString());
            throw e;
        }
    }

    /** Serialize this object
     * First write a string marking that this is a UGI in the string format,
     * then write this object's serialized form to the given data output
     * 
     * @param out output stream
     * @exception IOException if encounter any error during writing
     */
    public void write(DataOutput out) throws IOException {
      // write this object
      out.writeUTF(userName);
      out.writeInt(groupNames.length);
      for (String groupName : groupNames) 
      {
          out.writeUTF(groupName);
      }
    } 
    
    public static UserGroupInformation read(DataInput in) throws IOException
    {
        UserGroupInformation ugi = new UserGroupInformation();
        if(in.readBoolean()){
            ugi.readFields(in);
        }
        return ugi;
    }
}
