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
 * Based on hadoop org.apache.hadoop.fs.permission.FsPermission
 */
package org.bigfs.fs.permission;

import org.bigfs.fs.security.UserGroupInformation;

import java.io.DataInput;
import java.io.IOException;

public class FSPermission
{
    //POSIX permission style
    private FSAction useraction = null;
    private FSAction groupaction = null;
    private FSAction otheraction = null;

    
    /**
     * Construct by the given {@link FSAction}.
     * @param u user action
     * @param g group action
     * @param o other action
     */ 
    public FSPermission(FSAction u, FSAction g, FSAction o) {set(u, g, o);}

    
    /**
     * Construct by the given mode.
     * @param mode
     * @see #toShort()
     */    
    public FSPermission(short mode) { fromShort(mode); }
    
    /**
     * Construct by the given mode.
     * @param mode
     * @see #toShort()
     */    
    public FSPermission(int mode) { fromShort((short)mode); } 
    
    /**
     * Copy constructor
     * 
     * @param other other permission
     */
    public FSPermission(FSPermission other) 
    {        
      this.useraction = other.useraction;
      this.groupaction = other.groupaction;
      this.otheraction = other.otheraction;      
    }
    
    
    private FSPermission(){}


    /** Return user {@link FSAction}. */
    public FSAction getUserAction() 
    {
        return useraction;
    }

    /** Return group {@link FSAction}. */
    public FSAction getGroupAction() 
    {
        return groupaction;
    }

    /** Return other {@link FSAction}. */
    public FSAction getOtherAction() 
    {
        return otheraction;
    }



    private void set(FSAction u, FSAction g, FSAction o) 
    {
        useraction = u;
        groupaction = g;
        otheraction = o;
    }
    
    /**
     * Encode the object to a short.
     */
    public short toShort() 
    {
        return Short.parseShort(String.format("%s%s%s", useraction.ordinal(), groupaction.ordinal(), otheraction.ordinal()));
    }
    
    public void fromShort(short n) 
    {
        FSAction[] v = FSAction.values();
        String s = String.valueOf(n);
        set(v[Short.parseShort(s.substring(0, 1))], v[Short.parseShort(s.substring(1, 2))], v[Short.parseShort(s.substring(2))]);
    }
  
    
    public int hashCode() 
    {
        return toShort();
    }
    
    /** Get the default permission. */
    public static FSPermission getDefault() 
    {
        return new FSPermission((short)0777);
    }

    /** {@inheritDoc} */
    public boolean equals(Object obj) 
    {
        return false;
    }
    
    /** {@inheritDoc} */
    public boolean equals(FSPermission that)
    {
        return this.useraction == that.useraction
                && this.groupaction == that.groupaction
                && this.otheraction == that.otheraction;
    }
    
    public void readFields(DataInput in) throws IOException {
        fromShort(in.readShort());
    }
    
    public String toString() {
        return useraction.SYMBOL + groupaction.SYMBOL + otheraction.SYMBOL;
    }
    
    /**
     * Create and initialize a {@link FSPermission} from {@link DataInput}.
     */
    public static FSPermission read(DataInput in) throws IOException 
    {
      FSPermission p = new FSPermission();
      p.readFields(in);
      return p;
    }
}
