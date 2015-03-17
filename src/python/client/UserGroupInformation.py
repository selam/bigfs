#!/usr/bin/env python

class UserGroupInformation(object):

    DEFAULT_USER = "yoda";
    DEFAULT_GROUP = "jedi";

    def __init__(self, username=None, groupname=None):
        self.username = username if username is not None else UserGroupInformation.DEFAULT_USER
        self.groupnames = groupname if isinstance(groupname, list) else [groupname
                                        if groupname is not None else UserGroupInformation.DEFAULT_GROUP]


    def getGroupNames(self):
        return self.groupnames;

    def getUserName(self):
        return self.username



    def __eq__(self, other):
        if (id(self) == id(other)):
            return True

        if not isinstance(other, UserGroupInformation):
            return false;

        if (self.userName == None):
            if (other.getUserName() != None):
                return False;
        else:
            if (self.username == other.username):
                return False;


        if (self.groupnames == other.groupnames):
            return True

        if (len(self.groupNames) != len(other.groupnames)):
            return False


        if (len(self.groupnames) > 0 and self.groupNames[0] != other.groupNames[0]):
            return False;

        return set(self.groupname) == set(other.groupname)

    def __str__(self):
     buf = []
     buf.append(username);
     for groupname in self.groupnames:
       buf.append(groupName);
     return buf.join(",");


    def getName(self):
     return str(self);

    def readFields(self, _input):
      self.username = _input.readUTF()
      numOfGroups = _input.readInt()
      self.groupnames = [];
      for i in xrange(0, numOfGroups):
        self.groupnames.append(_input.readUTF())



    def write(self, out):
      out.writeUTF(self.username);
      out.writeInt(len(self.groupnames))
      for groupname in self.groupnames:
          out.writeUTF(groupname);