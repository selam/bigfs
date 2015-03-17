#!/usr/bin/env python
from struct import *

class DataInput(object):
    def __init__(self, socket):
        self.socket = socket

    def readBoolean(self):
        return unpack('>?', self.socket.recv(1))[0]

    def readChar(self,):
        return unpack('>c', self.socket.recv(1))[0]

    def readShort(self):
        return unpack('>s', self.socket.recv(2))[0]

    def readInt(self):
        r = self.socket.recv(4);
        return unpack('>i', r)[0]

    def readLong(self):
        return unpack('>l', self.socket.recv(4))[0]

    def readFloat(self):
        return unpack('>f', self.socket.recv(4))[0]

    def readDouble(self):
         return unpack('>d', self.socket.recv(8))[0]

    def readUTF(self):
        two_byte = self.socket.recv(2)
        _len = 0
        try:
            _len = unpack('>h', two_byte)[0]
        except:
            p = len(two_byte)
            eof = unpack(">"+str(len(two_byte))+"c", two_byte)[0]
            if eof == "\xff":
                return None
        if _len <= 0:
            return None

        return unpack(">"+str(_len)+"s", self.socket.recv(_len))[0]