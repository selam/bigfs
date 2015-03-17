package org.bigfs.fs.messages;

import org.bigfs.fs.directory.BigFSDirectoryManager;
import org.bigfs.fs.exceptions.BigFSException;
import org.bigfs.internode.message.IMessageHandler;
import org.bigfs.internode.message.MessageIn;
import org.bigfs.internode.message.MessageOut;
import org.bigfs.internode.service.MessagingService;

public class BigFSCreateDirectoryMessageHandler implements IMessageHandler<BigFSCreateDirectoryMessage>
{

    @Override
    public void processMessage(MessageIn<BigFSCreateDirectoryMessage> message)
    {   
        BigFSCreateDirectoryMessageResponse response;
        
        try
        {
            BigFSCreateDirectoryMessage command = message.getPayload();
            BigFSDirectoryManager.createDirectoryInternal(command.getDirectoryname(), command.getUserGroupInformation());
            response = new BigFSCreateDirectoryMessageResponse(true);
        }
        catch(BigFSException exception)
        {
            response = new BigFSCreateDirectoryMessageResponse(exception);
        }
        
        MessageOut<BigFSCreateDirectoryMessageResponse> responseMessage = new MessageOut<BigFSCreateDirectoryMessageResponse>(
                response.getMessageGroup(),
                response,
                BigFSCreateDirectoryMessageResponse.serializer
        );
        
        MessagingService.instance().sendReply(responseMessage, message.id, message.from);
    }

}
