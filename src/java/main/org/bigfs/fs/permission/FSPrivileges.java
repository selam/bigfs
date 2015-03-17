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
 * Based on hadoop org.apache.hadoop.fs.permission.PermissionStatus
 */
package org.bigfs.fs.permission;

import java.io.DataInput;
import java.io.IOException;

import org.bigfs.fs.security.UserGroupInformation;

public class FSPrivileges
{
    private String username;
    
    private String groupname;
    
    private FSPermission permission;
    
    /** Constructor */
    public FSPrivileges(String user, String group, FSPermission permission)
    {
      username = user;
      groupname = group;
      this.permission = permission;
    }

    private FSPrivileges() {}

    /** Return user name */
    public String getUserName() 
    {
        return username;
    }

    /** Return group name */
    public String getGroupName() 
    {
        return groupname;
    }

    /** Return permission */
    public FSPermission getPermission() 
    {
        return permission;
    }

    public boolean canDo(UserGroupInformation ugi, FSAction action)
    {
        if(ugi.getUserName().equals(this.getUserName()))
        {
            return getPermission().getUserAction().implies(action);
        }
        else
        {
            for(String groupName: ugi.getGroupNames())
            {
                if(this.getGroupName().equals(groupName))
                {
                     return getPermission().getGroupAction().implies(action);
                }
            }
        }

        return getPermission().getOtherAction().implies(action);
    }

    /** {@inheritDoc} */
    public String toString() 
    {
      return username + ":" + groupname + ":" + permission;
    }
    
    
    /** {@inheritDoc} */
    public void readFields(DataInput in) throws IOException 
    {
        username = in.readUTF();
        groupname = in.readUTF();
        permission = FSPermission.read(in);
    }
    
    /**
     * Create and initialize a {@link FSPrivileges} from {@link DataInput}.
     */
    public static FSPrivileges read(DataInput in) throws IOException 
    {
        FSPrivileges p = new FSPrivileges();
        p.readFields(in);
        return p;
    }  
}


