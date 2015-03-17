import socket
from struct import *

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(("localhost", 9160));
s.send('\xBF\xDD\xAC\xFF');
FILE = "deneme2a.txt".encode("utf8")
F_LEN = len(FILE)
data = ""
data += pack('>i', 1)
data += pack('>i', 2)
data += pack('>H', F_LEN)
data += pack(">"+str(F_LEN)+"s", FILE)
data += pack(">i", 0)

s.send(data);

data = pack('>i', 7)
f = file('/home/timu/superloto.sql').read();
s.send(data)
s.send(pack('>'+str(len(f))+'s', f))

"""
x       pad byte        no value                 
c       char    string of length 1      1        
b       signed char     integer 1       (3)
B       unsigned char   integer 1       (3)
?       _Bool   bool    1       (1)
h       short   integer 2       (3)
H       unsigned short  integer 2       (3)
i       int     integer 4       (3)
I       unsigned int    integer 4       (3)
l       long    integer 4       (3)
L       unsigned long   integer 4       (3)
q       long long       integer 8       (2), (3)
Q       unsigned long long      integer 8       (2), (3)
f       float   float   4       (4)
d       double  float   8       (4)
s       char[]  string           
p       char[]  string           
P
"""
