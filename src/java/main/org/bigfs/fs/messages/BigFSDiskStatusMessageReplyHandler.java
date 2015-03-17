package org.bigfs.fs.messages;

import java.util.ArrayList;
import java.util.List;

import org.bigfs.fs.monitor.BigFSDiskMonitor;
import org.bigfs.fs.monitor.BigFSDiskUsageDigest;
import org.bigfs.internode.message.IMessageHandler;
import org.bigfs.internode.message.MessageIn;

public class BigFSDiskStatusMessageReplyHandler implements IMessageHandler<BigFSDiskStatusMessageReply>
{
    
    public void processMessage(MessageIn<BigFSDiskStatusMessageReply> message)
    {
        BigFSDiskStatusMessageReply diskStatuses = message.getPayload();
        
        
        List<BigFSDiskUsageDigest> morenewUsages = new ArrayList<BigFSDiskUsageDigest>();
        
        BigFSDiskMonitor.instance.examineDiskUsage(diskStatuses.getDiskUsageDigests(), morenewUsages);            
    }

 

}
