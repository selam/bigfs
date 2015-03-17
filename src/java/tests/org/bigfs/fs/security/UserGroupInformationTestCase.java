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

import junit.framework.TestCase;

public class UserGroupInformationTestCase extends TestCase
{
    private UserGroupInformation ugi;
    
    public void setUp()
    {
        this.ugi = new UserGroupInformation();
    }
 
    public void testIsDefaults()
    {
        assertEquals("yoda", this.ugi.getUserName());
        assertEquals("yoda,jedi", this.ugi.getName());
        assertEquals("yoda,jedi", this.ugi.toString());        
        assertEquals(1, this.ugi.getGroupNames().length);
        assertEquals("jedi", this.ugi.getGroupNames()[0]);        
        assertTrue("check same group and username", this.ugi.equals(new UserGroupInformation()));
        assertFalse("check same group and username", this.ugi.equals(new UserGroupInformation("anakin", "jedi")));
        assertFalse("check same group and username", this.ugi.equals(new UserGroupInformation("yoda", new String[]{"jedi", "force"})));
    }
    
}
