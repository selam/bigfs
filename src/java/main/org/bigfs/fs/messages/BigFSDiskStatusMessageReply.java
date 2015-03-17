package org.bigfs.fs.messages;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

import org.bigfs.fs.messages.BigFSDiskStatusMessage.BigFSDiskStatusMessageSerializer;
import org.bigfs.fs.monitor.BigFSDiskUsageDigest;
import org.bigfs.internode.message.IMessage;
import org.bigfs.internode.message.IVersionedSerializer;
import org.bigfs.internode.serialization.InetAddressSerialization;

public class BigFSDiskStatusMessageReply implements IMessage
{
    public static String messageGroup = "BigFSInternal";
    public static int messageType = 103; // we start from 100
    
    public static BigFSDiskStatusMessageReplySerializer serializer = new BigFSDiskStatusMessageReplySerializer();

    List<BigFSDiskUsageDigest> usages;
    
    public BigFSDiskStatusMessageReply(List<BigFSDiskUsageDigest> usages)
    {
        this.usages = usages;
    }
        
    public List<BigFSDiskUsageDigest> getDiskUsageDigests()
    {
        return this.usages;
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

    public static class BigFSDiskStatusMessageReplySerializer implements IVersionedSerializer<BigFSDiskStatusMessageReply> {

        @Override
        public void serialize( BigFSDiskStatusMessageReply message, DataOutput out, int version) throws IOException
        {
            List<BigFSDiskUsageDigest> usages = message.getDiskUsageDigests();
            int size =message.getDiskUsageDigests().size();
            out.writeInt(size);
            for(int i=0; i<size; i++)
            {
                BigFSDiskUsageDigest digest = usages.get(i);

                InetAddressSerialization.serialize(digest.getNodeAddress(), out);
                out.writeLong(digest.getFreeSpace());
                out.writeDouble(digest.getCpuLoad());
                out.writeInt(digest.getStartingTime());
                out.writeInt(digest.getLastCheckTime());
            }

        }

        @Override
        public  BigFSDiskStatusMessageReply deserialize(DataInput in, int version) throws IOException
        {

            int size = in.readInt();
            List<BigFSDiskUsageDigest> usages = new ArrayList<BigFSDiskUsageDigest>(size);
            for(int i=0; i<size; i++)
            {
                InetAddress node_address = InetAddressSerialization.deserialize(in);
                long free_space = in.readLong();
                double cpu_load = in.readDouble();
                int start_time = in.readInt();
                int last_check = in.readInt();
                usages.add(new BigFSDiskUsageDigest(node_address, free_space, cpu_load, start_time, last_check));
            }

            return new  BigFSDiskStatusMessageReply(usages);
        }
    }
}
