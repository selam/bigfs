from FSAction import FSAction

class FSPermission(object):

    def __init__(self, useraction, groupaction, otheraction):
        self.useraction = useraction
        self.groupaction = groupaction
        self.otheraction = otheraction

    def getUserAction(self):
        return self.useraction;

    def getGroupAction(self):
        return self.groupaction;


    def getOtherAction(self):
        return self.otheraction;


    def toShort(self):
         return int("%s%s%s" % (self.useraction.ordinal(), self.groupaction.ordinal(), self.otheraction.ordinal()))

    def fromShort(self, n):
        v = FSAction.values();
        self.useraction = v[str(n)[0]]
        self.groupaction = v[str(n)[1]]
        self.otheraction = v[str(n)[3]];


    @staticmethod
    def getDefault():
        f = FSPermission();
        f.fromShort(777)
        return f


    def __eq__(self, that):
        return self.useraction == self.useraction and self.groupaction == that.groupaction and self.otheraction == that.otheraction;


    def __str__(self):

        return str(self.useraction) + str(self.groupaction) + str(self.otheraction);

"""
    public void readFields(DataInput in) throws IOException {
        fromShort(in.readShort());
    }


    /**
     * Create and initialize a {@link FSPermission} from {@link DataInput}.
     */
    public static FSPermission read(DataInput in) throws IOException
    {
      FSPermission p = new FSPermission();
      p.readFields(in);
      return p;
    }
}
"""

if __name__ == '__main__':
    f = FSPermission(FSAction.READ, FSAction.READ_WRITE, FSAction.NONE);
    f2 = FSPermission(FSAction.READ, FSAction.READ_WRITE, FSAction.EXECUTE);
    f3 = FSPermission(FSAction.READ, FSAction.READ_WRITE, FSAction.NONE);
    print f == f2
    print f == f3