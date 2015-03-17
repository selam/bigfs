#!/usr/bin/env python
from struct import *

class DataOutput(object):
    def __init__(self, socket):
        self.socket = socket

    def writeBoolean(self, bool):
        self.socket.send(pack('>?', bool))

    def writeChar(self, c):
        self.socket.send(pack('>c', c))

    def writeShort(self, s):
        self.socket.send(pack('>h', s))

    def writeInt(self, i):
        self.socket.send(pack('>I', i))

    def writeLong(self, l):
        self.socket.send(pack('>l', l))

    def writeFloat(self, f):
        self.socket.send(pack('>f', f))

    def writeDouble(self, d):
        self.socket.send(pack('>d', d))

    def writeUTF(self, string):
        self.socket.send(pack('>H', len(string.encode("utf8"))))
        self.socket.send(pack(">"+str(len(string.encode("utf8")))+"s", string.encode("utf8")))