package org.bigfs.fs.messages;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.bigfs.fs.security.UserGroupInformation;
import org.bigfs.internode.message.IMessage;
import org.bigfs.internode.message.IVersionedSerializer;

public class BigFSCreateDirectoryMessage implements IMessage
{
    

    public static BigFSCreateDirectoryMessageSerializer serializer = new BigFSCreateDirectoryMessageSerializer();

    public static String messageGroup = "BigFSFileMessages";
    public static int messageType = 103; // we start from 100
    
    
    private final String directoryName;
    private final UserGroupInformation ugi;

    
    public BigFSCreateDirectoryMessage(String filename, UserGroupInformation ugi)
    {
        this.directoryName = filename;
        this.ugi = ugi;
    }    
    
    public String getDirectoryname()
    {
        return this.directoryName;
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
    
    
    private static class BigFSCreateDirectoryMessageSerializer implements IVersionedSerializer<BigFSCreateDirectoryMessage> {

        @Override
        public void serialize(BigFSCreateDirectoryMessage t, DataOutput out, int version) throws IOException
        {
            out.writeUTF(t.getDirectoryname());            
            
            t.getUserGroupInformation().write(out);
        }

        @Override
        public BigFSCreateDirectoryMessage deserialize(DataInput in, int version) throws IOException
        {
            String directoryName = in.readUTF();
            
            
            UserGroupInformation ugi = new UserGroupInformation();
            ugi.readFields(in);
            
            return new BigFSCreateDirectoryMessage(directoryName, ugi);
        }
        
    }

}
