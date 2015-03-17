package org.bigfs.fs.service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.SliceByNamesReadCommand;
import org.apache.cassandra.exceptions.IsBootstrappingException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.bigfs.fs.config.BigFSConfiguration;




public class BigFSStorageHelper
{
  
   public static List<Row> getBigFSFileColumn(String pathname, String fileName) throws ReadTimeoutException, UnavailableException, IsBootstrappingException, IOException
   {
       List<ReadCommand> readCommands = new ArrayList<ReadCommand>();
       ColumnParent columnParent = getColumnParent();
       
       Collection<ByteBuffer> columnNames = Arrays.asList(ByteBufferUtil.bytes(fileName));
       
       
       SliceByNamesReadCommand slice = new SliceByNamesReadCommand(BigFSConfiguration.getKeyspace(), ByteBufferUtil.bytes(pathname), columnParent, columnNames); 

       readCommands.add(slice);
       
       return StorageProxy.read(readCommands, ConsistencyLevel.ONE);
   }
   
  
   
   public static ColumnParent getColumnParent()
   {
       ColumnParent columnParent = new ColumnParent();
       columnParent.setColumn_family(BigFSConfiguration.getColumnFamily());
       
       return columnParent;
   }
}
