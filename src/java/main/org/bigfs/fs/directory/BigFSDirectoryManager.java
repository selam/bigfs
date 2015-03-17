package org.bigfs.fs.directory;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.RangeSliceCommand;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.db.filter.IDiskAtomFilter;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.ThriftValidation;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.bigfs.fs.config.BigFSConfiguration;
import org.bigfs.fs.exceptions.BigFSException;
import org.bigfs.fs.exceptions.BigFSExceptionCode;
import org.bigfs.fs.messages.BigFSCreateDirectoryMessage;
import org.bigfs.fs.messages.BigFSCreateDirectoryMessageResponse;
import org.bigfs.fs.messages.BigFSCreateFileMessage;
import org.bigfs.fs.messages.BigFSCreateFileMessageResponse;
import org.bigfs.fs.permission.FSAction;
import org.bigfs.fs.permission.FSPermission;
import org.bigfs.fs.permission.FSPrivileges;
import org.bigfs.fs.security.UserGroupInformation;
import org.bigfs.fs.service.BigFSStorageHelper;
import org.bigfs.fs.utils.Utils;
import org.bigfs.internode.message.IAsyncResult;
import org.bigfs.internode.message.MessageOut;
import org.bigfs.internode.service.MessagingService;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.jkeylockmanager.manager.KeyLockManager;
import de.jkeylockmanager.manager.KeyLockManagers;

public class BigFSDirectoryManager
{
    
    private static final Logger logger = LoggerFactory.getLogger(BigFSDirectoryManager.class);

    private static final KeyLockManager lockManager = KeyLockManagers.newLock();

    
    
    
    public static String getParent(String path) {
        int index = path.lastIndexOf(File.separator);
        if (index < 0 || index == 0) {   
            return path.substring(0, 1); //meaning /filename.txt
        }
        
        return path.substring(0, index);
    }    
    
    
   /**
    * Whether the pathname is valid.  Currently prohibits relative paths, 
    * and names which contain a ":" or "/" 
    * 
    * this path of code comes from org.apache.hadoop.hdfs.DFSUtil
    */
   public static boolean isValidName(String src) {
       
     if(src == null || src.length() == 0 || !src.startsWith("/")) {
         return false;
     }
     
     // Check for ".." "." ":" "/"
     StringTokenizer tokens = new StringTokenizer(src, "/");
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
   
      
   public static void createFile(String fileName,
            Map<String, String> fileAttributes, UserGroupInformation ugi) throws BigFSException
    {
        
        String parentDirectory = BigFSDirectoryManager.getValidateNameAndGetParent(fileName);
        List<InetAddress> targets = Utils.getKeyLocations(parentDirectory);
        if(Utils.inThatNode(targets))
        {
            createFileInternal(fileName, fileAttributes, ugi);
        }
        else
        {                
            for(InetAddress target: targets)
            {
                for(int tryCount = 0; tryCount<3; tryCount++)
                {
                    try 
                    {
                        BigFSCreateFileMessage message = new BigFSCreateFileMessage(
                                fileName, fileAttributes, ugi   
                        );
                        
                        MessageOut<BigFSCreateFileMessage> msgOut = new MessageOut<BigFSCreateFileMessage>(
                                message.getMessageGroup(), message, BigFSCreateFileMessage.serializer
                        );
                        
                        IAsyncResult<BigFSCreateFileMessageResponse> result = MessagingService.instance().send(msgOut, target);
                        
                        BigFSCreateFileMessageResponse response = result.get(1000, TimeUnit.MILLISECONDS);
                        if(response.getStatus())
                        {
                            throw response.getException();
                        }                        
                    }
                    catch(Exception e)
                    {
                        throw new BigFSException(e.getMessage(), BigFSExceptionCode.UNKOWN_ERROR);
                    }
                }
            }
        }
    }

   
   public static void createFileInternal(String fileName, Map<String, String> fileAttributes, UserGroupInformation ugi) throws BigFSException
   {
       String parentDirectory = BigFSDirectoryManager.getValidateNameAndGetParent(fileName);
       
       BigFSDirectoryManager.validateParentDirectoryPermission(parentDirectory, ugi, FSAction.WRITE_EXECUTE);
       
       FSPrivileges filePermission = BigFSDirectoryManager.getPrivileges(parentDirectory, fileName);

       if(filePermission != null) 
       {   
           throw new BigFSException(fileName+" exists", BigFSExceptionCode.FILE_EXISTS);
       }
       
       try 
       {
           lockManager.executeLocked(fileName, new BigFSCreateFile(parentDirectory, fileName, fileAttributes, ugi));
       }
       catch(Exception e)
       {
           throw new BigFSException(e.getMessage(), BigFSExceptionCode.UNKOWN_ERROR);
       }
   }
   
   
    public static void createDirectory(String directoryName,
            UserGroupInformation ugi) throws BigFSException
    {
       
        String parentDirectory = BigFSDirectoryManager.getValidateNameAndGetParent(directoryName);
        List<InetAddress> targets = Utils.getKeyLocations(parentDirectory);
        if(Utils.inThatNode(targets))
        {
            createDirectoryInternal(directoryName, ugi);
        }
        else
        {  
            for(InetAddress target: targets)
            {
                for(int tryCount = 0; tryCount<3; tryCount++)
                {
                    try 
                    {
                        BigFSCreateDirectoryMessage message = new BigFSCreateDirectoryMessage(
                                directoryName, ugi   
                        );
                        
                        MessageOut<BigFSCreateDirectoryMessage> msgOut = new MessageOut<BigFSCreateDirectoryMessage>(
                                message.getMessageGroup(), message, BigFSCreateDirectoryMessage.serializer
                        );
                        
                        IAsyncResult<BigFSCreateDirectoryMessageResponse> result = MessagingService.instance().send(msgOut, target);
                        
                        BigFSCreateDirectoryMessageResponse response = result.get(1000, TimeUnit.MILLISECONDS);
                        if(response.getStatus())
                        {
                            throw response.getException();
                        }                        
                    }
                    catch(Exception e)
                    {
                        throw new BigFSException(e.getMessage(), BigFSExceptionCode.UNKOWN_ERROR);
                    }
                }
            }
        }    
    }   
    
    public static void createDirectoryInternal(String directoryName,
            UserGroupInformation ugi) throws BigFSException
    {
        String parentDirectory = BigFSDirectoryManager.getValidateNameAndGetParent(directoryName);
        
        BigFSDirectoryManager.validateParentDirectoryPermission(parentDirectory, ugi, FSAction.WRITE_EXECUTE);
        
        try 
        {
            lockManager.executeLocked(directoryName, new BigFSCreateDirectory(parentDirectory, directoryName, ugi));
        }
        catch(Exception e)
        {
            throw new BigFSException(e.getMessage(), BigFSExceptionCode.UNKOWN_ERROR);
        }
    }
    
    
    // ask directory information from cassandra and return directory information
    private static FSPrivileges getPrivileges(String pathname, String filename) 
    {
        
        try
        {
            return getPrivileges(BigFSStorageHelper.getBigFSFileColumn(pathname, filename), filename);
        }
        catch (Exception e)
        {
        }
        
        return null;
    }
          
    
 // ask directory information from cassandra and return directory information
    private static FSPrivileges getPrivileges(List<Row> row, String filename) 
    {
        
        try
        {   
            if(row != null)
            {
                Row metadata = row.get(0);
                
                if(metadata != null && metadata.cf != null)
                {
                    try
                    {
                        IColumn column = metadata.cf.getColumn(ByteBufferUtil.bytes(filename));
                        
                        if(column != null)
                        {
                            String[] permissions = ByteBufferUtil.string(column.value()).split("-");
                        
                            return new FSPrivileges(
                                permissions[1], // user
                                permissions[2], // group
                                new FSPermission(Integer.parseInt(permissions[3]))
                            );
                        }
                    }
                    catch (CharacterCodingException e)
                    {                   
                        e.printStackTrace();
                    }
                }
            }
            
        }
        catch (Exception e)
        {
            // TODO özgün her bir exception'ı kendi nedeni ile handle etmek zorundayız.
            
            //e.printStackTrace();
        }
        
        return null;
    }
    
    
       
    private static String getValidateNameAndGetParent(String name) throws BigFSException
    {
        if(!BigFSDirectoryManager.isValidName(name))
        {
            throw new BigFSException(name+" is not valid", BigFSExceptionCode.INVALID_FILENAME);
        }
        // find parent directory name given filename
        return BigFSDirectoryManager.getParent(name);

    }
    
    private static FSPrivileges validateParentDirectoryPermission(String parentDirectory, UserGroupInformation ugi, FSAction action) throws BigFSException
    {
        // ask parent directory permissions to cassandra,
        FSPrivileges permissions = BigFSDirectoryManager.getPrivileges(parentDirectory, ".");
        if(permissions == null)
        {
            throw new BigFSException("directory "+parentDirectory+" not found", BigFSExceptionCode.DIRECTORY_NOT_FOUND);
        }
        // check directory permissions user is can create an file on that directory
        if(!permissions.canDo(ugi, action))
        {
            throw new BigFSException("permission error", BigFSExceptionCode.PERMISSION_ERROR);        
        }
        
        return permissions;
    }

    /**
     * List of directory content
     * 
     * @param path
     * @param out
     * @throws BigFSException
     * @throws IOException
     */
    public static void listDirectory(String path, DataOutputStream out) throws BigFSException, IOException
    {
        if(!BigFSDirectoryManager.isValidName(path))
        {
            throw new BigFSException(path+" is not valid", BigFSExceptionCode.INVALID_FILENAME);
        }
                
        SlicePredicate predicate = new SlicePredicate().setSlice_range(new SliceRange());
        
        String start_column = null;
        KeyRange range = new KeyRange(); 
        
        range.setStart_key(path.getBytes());
        range.setEnd_key(path.getBytes());
        
        predicate.getSlice_range().setStart("".getBytes());
        predicate.getSlice_range().setFinish("".getBytes());
       
        RowPosition end = RowPosition.forKey(range.end_key, StorageService.getPartitioner());
                
        AbstractBounds<RowPosition> bounds = new Bounds<RowPosition>(RowPosition.forKey(range.start_key, StorageService.getPartitioner()), end);

        Calendar today =  Calendar.getInstance();
        today.setTimeInMillis(System.currentTimeMillis());
        
        Calendar calendar = Calendar.getInstance(); 
        while(true)
        {   
            if(start_column != null)
            {
                predicate.getSlice_range().setStart(start_column.getBytes());
            }
            
            IDiskAtomFilter filter = ThriftValidation.asIFilter(predicate, UTF8Type.instance);
            
            RangeSliceCommand rangeSlice = new RangeSliceCommand(BigFSConfiguration.getKeyspace(),
                    BigFSConfiguration.getColumnFamily(), null, filter, bounds, range.row_filter, range.count, true, true);
            try
            {
                List<Row> rows = StorageProxy.getRangeSlice(rangeSlice, ConsistencyLevel.ONE);
                if(rows.size() > 0)
                {
                    Row row =rows.get(0);
                    SortedSet<ByteBuffer> set = row.cf.getColumnNames();
                    if(set.size() == 1)
                    {
                        ByteBuffer first_column_name = set.first();
                        IColumn first_column = row.cf.getColumn(first_column_name);
                        
                        if(ByteBufferUtil.string(first_column.name()).equals(start_column))
                        {
                            break;
                        }
                    }
                    for(ByteBuffer name: set)
                    {                    
                        IColumn column = row.cf.getColumn(name);
                        String output = getFileInformation(column, calendar, today);
                        
                        if(output == null)
                        {
                            continue;
                        }
                        
                        out.writeUTF(output);
                        
                        start_column = ByteBufferUtil.string(column.name());
                    }    
                }    
                
            }
            catch (ReadTimeoutException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            catch (UnavailableException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        
        out.writeInt(-1);
    }

    public static String getFileInformation(IColumn column, Calendar calendar, Calendar today) throws NumberFormatException, CharacterCodingException
    {        
        if(column.isMarkedForDelete())
        {
            return null;
        }
        
        String[] metadata = ByteBufferUtil.string(column.value()).split("-");
        calendar.setTimeInMillis(Long.parseLong(metadata[4]));
        String format = "HH:mm";
        if(today.get(Calendar.YEAR) > calendar.get(Calendar.YEAR))
        {
            format = "YYYY";
        }
        
        return String.format("%s%s %s %s %s %s", metadata[0].equals("f") ? "-" :  "f", new FSPermission(Short.parseShort(metadata[3])).toString(), metadata[1], metadata[2], new SimpleDateFormat("MMMM dd "+format).format(calendar.getTime()), ByteBufferUtil.string(column.name()));        
    }
    
    
    
    public static void getInodeInformation(String fileName, UserGroupInformation ugi, DataOutputStream out) throws BigFSException
    {                
        String parentDirectory = BigFSDirectoryManager.getValidateNameAndGetParent(fileName);
        
        
        BigFSDirectoryManager.validateParentDirectoryPermission(parentDirectory, ugi, FSAction.READ);
        
        List<Row> rows = null;
        try
        {
            rows = BigFSStorageHelper.getBigFSFileColumn(parentDirectory, fileName);
        }
        catch (Exception e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        if(rows.size() != 1)
        {
            throw new BigFSException(fileName+" not exists", BigFSExceptionCode.FILE_NOT_EXISTS);
        }
        
        FSPrivileges filePermission  = BigFSDirectoryManager.getPrivileges(rows, fileName);
        
        if(filePermission == null)
        {
            throw new BigFSException(fileName+" not exists", BigFSExceptionCode.FILE_NOT_EXISTS);
        }
        if(!filePermission.canDo(ugi, FSAction.READ)){
            throw new BigFSException(fileName, BigFSExceptionCode.PERMISSION_ERROR);

        }
        
        SortedSet<ByteBuffer> set = rows.get(0).cf.getColumnNames();
        IColumn column = rows.get(0).cf.getColumn(set.first());
        if(column.isMarkedForDelete())
        {
            throw new BigFSException(fileName+" not exists", BigFSExceptionCode.FILE_NOT_EXISTS);
        }
        
        String[] metadata;
        try
        {
            metadata = ByteBufferUtil.string(column.value()).split("-");
        }
        catch (CharacterCodingException e1)
        {
          return;
        }
        
        Calendar today =  Calendar.getInstance();
        
        today.setTimeInMillis(System.currentTimeMillis());
        
        Calendar calendar = Calendar.getInstance();
        String output = null;
        try
        {
            output = getFileInformation(column, calendar, today);
        }
        catch (NumberFormatException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch (CharacterCodingException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        if(output == null)
        {
            return;
        } 
        
        try
        {
            out.writeBoolean(true);
            out.writeUTF(output);
        }
        catch (IOException e)
        {
            return;
        }
        
        if(!metadata[5].equals("-"))
        {
            JSONParser parser = new JSONParser();
            Map<String, String> json = null;
            try
            {
                json = (Map<String, String>) parser.parse(metadata[5], new ContainerFactory());
            }
            catch (ParseException e1)
            {
               return;
            }
            
            try
            {   
                out.writeInt(json.size());
            }
            catch (IOException e)
            {
                logger.error(e.toString());
            }
            Iterator<Entry<String, String>> iter = json.entrySet().iterator();
            
            while(iter.hasNext())
            {
                Entry<String, String> entry = (Entry<String, String>) iter.next();
                try
                {                        
                    out.writeUTF(entry.getKey());
                    out.writeUTF(entry.getValue());
                }
                catch (IOException e)
                {   
                    logger.error(e.toString());
                }
                
            }
        }
        else
        {
            try
            {
                out.writeInt(0);
            }
            catch (IOException e)
            {
               
            }
        }
    }
    
    
    static class ContainerFactory implements org.json.simple.parser.ContainerFactory
    {
        public Map createObjectContainer() 
        {
            return new HashMap();
        }
        
        public List creatArrayContainer() 
        {
            return new LinkedList();
        }
                            
    }
}
