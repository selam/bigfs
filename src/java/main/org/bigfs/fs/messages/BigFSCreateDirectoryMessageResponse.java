package org.bigfs.fs.messages;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.bigfs.fs.exceptions.BigFSException;
import org.bigfs.fs.exceptions.BigFSExceptionCode;
import org.bigfs.internode.message.IMessage;
import org.bigfs.internode.message.IVersionedSerializer;

public class BigFSCreateDirectoryMessageResponse implements IMessage
{
    

    public static BigFSCreateFileMessageResponseSerializer serializer = new BigFSCreateFileMessageResponseSerializer();

    public static String messageGroup = "BigFSFileMessages";
    public static int messageType = 104; // we start from 100
    
    
    private final boolean status; 
    
    private final BigFSException exception;
    
    public BigFSCreateDirectoryMessageResponse(boolean status)
    {
        this.status = status;
        this.exception = null;
    }    
    
    public BigFSCreateDirectoryMessageResponse(BigFSException exception)
    {
        this.status = false;
        this.exception = exception;
    }

    
    public boolean getStatus()
    {
        return this.status;
    }
    
    public BigFSException getException()
    {
        return this.exception;
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
    
    
    private static class BigFSCreateFileMessageResponseSerializer implements IVersionedSerializer<BigFSCreateDirectoryMessageResponse> {

        @Override
        public void serialize(BigFSCreateDirectoryMessageResponse t, DataOutput out, int version) throws IOException
        {
            out.writeBoolean(t.status);
            
            if(!t.status)
            {
               out.writeInt(t.exception.code().value);
               out.writeUTF(t.exception.getMessage());
            }
        }

        @Override
        public BigFSCreateDirectoryMessageResponse deserialize(DataInput in, int version) throws IOException
        {
            boolean status = in.readBoolean();
            if(status)
            {
                return new BigFSCreateDirectoryMessageResponse(status);
            }
            
            int code = in.readInt();
            String message = in.readUTF();
            
            return new BigFSCreateDirectoryMessageResponse(new BigFSException(message, BigFSExceptionCode.fromCode(code)));
        }
        
    }

}
