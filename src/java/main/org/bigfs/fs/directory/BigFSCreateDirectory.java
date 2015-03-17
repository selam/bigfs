package org.bigfs.fs.directory;

import java.util.Arrays;

import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.bigfs.fs.config.BigFSConfiguration;
import org.bigfs.fs.security.UserGroupInformation;

import de.jkeylockmanager.manager.LockCallback;

public class BigFSCreateDirectory  implements LockCallback 
{   
    // TODO: work as batch mutation
    private final String directoryName;         
    
    private final String parentDirectory; 
    
    private final UserGroupInformation ugi;
    
    public BigFSCreateDirectory(String parentDirectory, String directoryName, UserGroupInformation ugi) 
    {
        this.parentDirectory = parentDirectory;
        this.directoryName = directoryName;
        this.ugi = ugi;
    }
    
    
    @Override
    public void doInLock() throws Exception
    {  
       this.createDirectory();
       this.writeToParent();
    }       
    
    private void createDirectory() throws WriteTimeoutException, UnavailableException, OverloadedException
    {
        RowMutation change = new RowMutation(BigFSConfiguration.getKeyspace(), ByteBufferUtil.bytes(directoryName));
        ColumnPath cp = new ColumnPath(BigFSConfiguration.getColumnFamily()).setColumn(".".getBytes());
        change.add(new QueryPath(cp), ByteBufferUtil.bytes(
                String.format("d-%s-%s-%s-%s", ugi.getUserName(), ugi.getGroupNames()[0], BigFSConfiguration.getDefaultDirectoryPermission(), System.currentTimeMillis())
        ), System.currentTimeMillis());
        
        StorageProxy.mutate(Arrays.asList(change), BigFSConfiguration.getWriteConsistencyLevel());
    }
    
    private void writeToParent() throws WriteTimeoutException, UnavailableException, OverloadedException
    {
        RowMutation change = new RowMutation(BigFSConfiguration.getKeyspace(), ByteBufferUtil.bytes(parentDirectory));
        ColumnPath cp = new ColumnPath(BigFSConfiguration.getColumnFamily()).setColumn(directoryName.getBytes());
        change.add(new QueryPath(cp), ByteBufferUtil.bytes(
                String.format("d-%s-%s-%s-%s", ugi.getUserName(), ugi.getGroupNames()[0], BigFSConfiguration.getDefaultFilePermission(), System.currentTimeMillis())
        ), System.currentTimeMillis());
        
        StorageProxy.mutate(Arrays.asList(change), BigFSConfiguration.getWriteConsistencyLevel());
    }
}