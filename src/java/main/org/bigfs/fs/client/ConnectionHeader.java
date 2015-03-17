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
 * Based on org.apache.hadoop.ipc.ConnectionHeader
 * */

package org.bigfs.fs.client;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.bigfs.fs.security.UserGroupInformation;

public class ConnectionHeader
{
    private UserGroupInformation ugi = new UserGroupInformation();
    
    private final int protocol_magic = 0xBFDDACFF;
    
    public ConnectionHeader() {}
    
    public ConnectionHeader(UserGroupInformation ugi) {
        this.ugi = ugi;
    }
    
    public void readFields(DataInput in) throws IOException {
        boolean ugiPresent = in.readBoolean();
        if (ugiPresent) {
          ugi.readFields(in);
        } else {
          ugi = null;
        }
    }    
    
    public void write(DataOutput out) throws IOException {
        out.writeInt(protocol_magic);
        if (ugi != null) {
          out.writeBoolean(true);
          ugi.write(out);
        } else {
          out.writeBoolean(false);
        }
    }   
    
    public int getProtocol() {
        return protocol_magic;
    }

    public UserGroupInformation getUgi() {
        return ugi;
    }

    public String toString() {
        return String.format("%s-%s", protocol_magic,  ugi.toString());
    }
}
