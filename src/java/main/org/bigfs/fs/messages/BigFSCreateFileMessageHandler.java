package org.bigfs.fs.messages;

import org.bigfs.fs.directory.BigFSDirectoryManager;
import org.bigfs.fs.exceptions.BigFSException;
import org.bigfs.internode.message.IMessageHandler;
import org.bigfs.internode.message.MessageIn;
import org.bigfs.internode.message.MessageOut;
import org.bigfs.internode.service.MessagingService;

public class BigFSCreateFileMessageHandler implements IMessageHandler<BigFSCreateFileMessage>
{

    @Override
    public void processMessage(MessageIn<BigFSCreateFileMessage> message)
    {   
        BigFSCreateFileMessageResponse response;
        
        try
        {
            BigFSCreateFileMessage command = message.getPayload();
            BigFSDirectoryManager.createFileInternal(command.getFilename(), command.getFileAttributes(), command.getUserGroupInformation());
            response = new BigFSCreateFileMessageResponse(true);
        }
        catch(BigFSException exception)
        {
            response = new BigFSCreateFileMessageResponse(exception);
        }
        
        MessageOut<BigFSCreateFileMessageResponse> responseMessage = new MessageOut<BigFSCreateFileMessageResponse>(
                response.getMessageGroup(),
                response,
                BigFSCreateFileMessageResponse.serializer
        );
        
        MessagingService.instance().sendReply(responseMessage, message.id, message.from);
    }

}
