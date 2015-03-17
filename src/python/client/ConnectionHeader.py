#!/usr/bin/env python

from UserGroupInformation import UserGroupInformation

class ConnectionHeader(object):

    PROTOCOL_MAGIC = 0xBFDDACFF

    def __init__(self, ugi = None):
        self.ugi = ugi if ugi is not None else UserGroupInformation();

    def readFields(self, _input):
        if (_input.readBoolean()):
            ugi.readFields(_input);
        else:
          self.ugi = None;

    def write(self, out):
        out.writeInt(ConnectionHeader.PROTOCOL_MAGIC)
        if (self.ugi != None):
          out.writeBoolean(True);
          self.ugi.write(out);
        else:
          out.writeBoolean(False);



    def getProtocol(self):
        return ConnectionHeader.PROTOCOL_MAGIC;


    def getUgi(self):
        return self.ugi;


    def __str__(self):
        return "%s-%s" % (ConnectionHeader.PROTOCOL_MAGIC,  str(ugi));
