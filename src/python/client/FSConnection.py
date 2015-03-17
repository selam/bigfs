#!/usr/bin/env python
# -*- coding: utf-8 -*-
import socket, struct

from ConnectionHeader import ConnectionHeader
from DataInput import DataInput
from DataOutput import DataOutput
from FSClientExceptions import *

class Connection(object):

    COMMANDS = {
        "createFile": 1,
        "createDirectory": 2,
        "ls":         3,
        "getInfo":    4,
        "writeFile": 7
    }

    def __init__(self, userGroupInformation=None):
        self.connection_header = ConnectionHeader()
        self.socket = None
        self._in = None;
        self.out = None;
        if userGroupInformation is not None:
            self.set_user_group_information(userGroupInformation)

    def set_user_group_information(self, userGroupInformation):
        self.connection_header.setUserGroupInformation(userGroupInformation)

    def connect(self, server, port):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((server, port));
        self._in = DataInput(self.socket)
        self.out = DataOutput(self.socket)
        self.connection_header.write(self.out)


    def writeCommand(self, command):
        if command not in Connection.COMMANDS.keys():
            raise FSClientException("Command not found")

        self.out.writeInt(Connection.COMMANDS[command])

    def createFile(self, filename, information={}):
        self.writeCommand("createFile")
        self.out.writeUTF(filename);
        self.out.writeInt(len(information))
        for key,value in information.iteritems():
           self.out.writeUTF(key)
           self.out.writeUTF(value)

        _bool = self._in.readBoolean()
        if(_bool  == True):
           return True
        else:
           error_code=  self._in.readInt()
           error_message = self._in.readUTF()
           raise Exception("%s-%s" % (error_code, error_message))

    def createDirectory(self, directoryname):
        self.writeCommand("createDirectory")
        self.out.writeUTF(directoryname);

        _bool = self._in.readBoolean()
        if(_bool  == True):
           return True
        else:
           error_code=  self._in.readInt()
           error_message = self._in.readUTF()
           raise Exception("%s-%s" % (error_code, error_message))



    def list(self, directoryname):
        self.writeCommand("ls")
        self.out.writeUTF(directoryname);

        while(True):
            chunk = self._in.readUTF()
            if chunk == None:
              break
            yield chunk

    def getInfo(self, filename):
        self.writeCommand("getInfo")
        self.out.writeUTF(filename);
        _bool = self._in.readBoolean()

        if not _bool:
            error_code=  self._in.readInt()
            error_message = self._in.readUTF()
            raise Exception("%s-%s" % (error_code, error_message))

        yield self._in.readUTF()

        attrSize = self._in.readInt()
        attrs = {}
        for i in xrange(0, attrSize):
            attrs.update({self._in.readUTF(): self._in.readUTF()})

        yield attrs


if __name__ == '__main__':
    import time
    c = Connection();
    c.connect("127.0.0.1", 9161)
    """
    for i in xrange(0, 1000000):
        randomFile = "/"+str(int(time.time()))+"-"+str(i)+".txt";
        print randomFile, c.createFile(randomFile)

    for fileName in c.list("/"):
        print fileName
    """
    for i in c.getInfo("/1367329775-77.txt"):
       print i
