package org.bigfs.fs.directory;

import java.util.Arrays;
import java.util.Map;

import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.bigfs.fs.config.BigFSConfiguration;
import org.bigfs.fs.security.UserGroupInformation;
import org.json.simple.JSONValue;

import de.jkeylockmanager.manager.LockCallback;

class BigFSCreateFile  implements LockCallback 
{   
    private final String fileName;         
    
    private final String parentDirectory; 
    
    private final UserGroupInformation ugi;
    
    private final Map<String, String> fileAttributes;
    
    public BigFSCreateFile(String parentDirectory, String fileName, Map<String, String> fileAttributes, UserGroupInformation ugi) 
    {
        this.parentDirectory = parentDirectory;
        this.fileName = fileName;
        this.fileAttributes = fileAttributes;
        this.ugi = ugi;        
    }
    
    
    
    @Override
    public void doInLock() throws Exception
    {  
        RowMutation fileMutation = new RowMutation(BigFSConfiguration.getKeyspace(), ByteBufferUtil.bytes(parentDirectory));
        ColumnPath cp = new ColumnPath(BigFSConfiguration.getColumnFamily()).setColumn(fileName.getBytes());
        
        
        fileMutation.add(new QueryPath(cp), ByteBufferUtil.bytes(
                String.format("f-%s-%s-%s-%s-%s", ugi.getUserName(), ugi.getGroupNames()[0], BigFSConfiguration.getDefaultFilePermission(), System.currentTimeMillis(),  
                        JSONValue.toJSONString(fileAttributes))
        ), System.currentTimeMillis());
        
        StorageProxy.mutate(Arrays.asList(fileMutation), BigFSConfiguration.getWriteConsistencyLevel());
    }        
}
