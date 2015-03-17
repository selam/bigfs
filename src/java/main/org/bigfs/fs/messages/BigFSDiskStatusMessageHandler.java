package org.bigfs.fs.messages;

import java.util.ArrayList;
import java.util.List;

import org.bigfs.fs.monitor.BigFSDiskMonitor;
import org.bigfs.fs.monitor.BigFSDiskUsageDigest;
import org.bigfs.internode.message.IMessageHandler;
import org.bigfs.internode.message.MessageIn;
import org.bigfs.internode.message.MessageOut;
import org.bigfs.internode.service.MessagingService;

public class BigFSDiskStatusMessageHandler implements IMessageHandler<BigFSDiskStatusMessage>
{
    
    public void processMessage(MessageIn<BigFSDiskStatusMessage> message)
    {
        BigFSDiskStatusMessage diskStatuses = message.getPayload();
        
        
        List<BigFSDiskUsageDigest> morenewUsages = new ArrayList<BigFSDiskUsageDigest>();
        
        BigFSDiskMonitor.instance.examineDiskUsage(diskStatuses.getDiskUsageDigests(), morenewUsages);
        
        if(morenewUsages.size() > 0)
        {
            MessageOut<BigFSDiskStatusMessageReply> replyMessage = new MessageOut<BigFSDiskStatusMessageReply>(
                    BigFSDiskStatusMessageReply.messageGroup,
                    new BigFSDiskStatusMessageReply(morenewUsages),
                    BigFSDiskStatusMessageReply.serializer
            );
            
            MessagingService.instance().sendOneWay(replyMessage, message.from);
        }
    }

 

}
