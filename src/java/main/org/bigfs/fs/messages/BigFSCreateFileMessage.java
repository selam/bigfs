package org.bigfs.fs.messages;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.bigfs.fs.security.UserGroupInformation;
import org.bigfs.internode.message.IMessage;
import org.bigfs.internode.message.IVersionedSerializer;

public class BigFSCreateFileMessage implements IMessage
{
    

    public static BigFSCreateFileMessageSerializer serializer = new BigFSCreateFileMessageSerializer();

    public static String messageGroup = "BigFSFileMessages";
    public static int messageType = 101; // we start from 100
    
    
    private final String fileName;
    private final UserGroupInformation ugi;
    private final Map<String, String> fileAttributes;
    
    public BigFSCreateFileMessage(String filename, Map<String,String> fileAttributes, UserGroupInformation ugi)
    {
        this.fileName = filename;
        this.fileAttributes = fileAttributes;
        this.ugi = ugi;
    }    
    
    public String getFilename()
    {
        return this.fileName;
    }
    
    public Map<String, String> getFileAttributes()
    {
        return this.fileAttributes;
    }
    
    public UserGroupInformation getUserGroupInformation() 
    {
        return this.ugi;
    }
    
    @Override
    public String getMessageGroup()
    {
        // TODO Auto-generated method stub
        return messageGroup;
    }

    @Override
    public int getMessageTimeout()
    {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getMessageType()
    {
        // TODO Auto-generated method stub
        return messageType;
    }
    
    
    private static class BigFSCreateFileMessageSerializer implements IVersionedSerializer<BigFSCreateFileMessage> {

        @Override
        public void serialize(BigFSCreateFileMessage t, DataOutput out, int version) throws IOException
        {
            out.writeUTF(t.getFilename());
            Map<String, String> fileAttr = t.getFileAttributes();
            
            out.writeInt(fileAttr.size());
            
            Set<String> keySet = fileAttr.keySet();
            
            for(String key: keySet)
            {
                out.writeUTF(key);
                out.writeUTF(fileAttr.get(key));
            }
            
            t.getUserGroupInformation().write(out);
        }

        @Override
        public BigFSCreateFileMessage deserialize(DataInput in, int version) throws IOException
        {
            String fileName = in.readUTF();
            int len = in.readInt();
            
            Map<String, String> fileAttributes = new HashMap<String, String>();
            
            for(int i =0; i<=len; i++)
            {
                String key = in.readUTF(); 
                String value = in.readUTF();
                fileAttributes.put(key, value);
            }
            
            UserGroupInformation ugi = new UserGroupInformation();
            ugi.readFields(in);
            
            return new BigFSCreateFileMessage(fileName, fileAttributes, ugi);
        }
        
    }

}
