import socket
import time
import struct
import hashlib
import random
class Client():
    def __init__(self,filename,listenport=8888,windowSize=100,timeout=10,isDebug=True):
        self.isDebug = isDebug
        self.timeout = timeout
        self.windowSize = windowSize  
        self.last_cleanup = time.time()
        self.port = listenport
        self.host = ''
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind((self.host,self.port))
        self.expected_seqno = 0
        self.target=None
        self.outfile=open(filename,'wb')

    def receive(self):
        return self.socket.recvfrom(4096)

    def send(self, message):
        self.socket.sendto(message, self.target)

    def make_packet(self,ack,sack):
        return struct.pack('BB',ack,sack)

    def split_message(self, message):
        
        seqno = message[0]
        flag=message[1]
        packets_no=struct.unpack('L',message[8:16])[0]
        if(flag==0):
            data=message[16:]
        else :
            data=None
        
        return seqno,flag,packets_no,data

    def start(self):
        receiverReceivedSet = [0]*256   # 用于记录接收到的分组
        buffer = [""]*256
        cur_seqno = 0
        while True:
            self.socket.settimeout(self.timeout)
            data=""
            isEnd=False
            while True:
                try:
                    data,address=self.socket.recvfrom(4096)
                    self.target=address
                    
                    # seqno,isEnd,data,checksum=self.split_message(data)
                    seqno,isEnd,packets_no,data=self.split_message(data)
                    
                    if self.isDebug:
                        # print(seqno,isEnd,data,checksum)
                        # print("receive data from ",address)
                        print("receive seqno:",seqno)
                        print("receive isEnd:",isEnd)
                        print("receive packets_no:",packets_no)
                    # m=hashlib.md5()
                    # if isEnd==0:
                    #     check='0'
                    # else :
                    #     check=m.update(data)
                    # 第一个包
                    # print("check:",check)
                    # print("checksum:",checksum)
                    # if seqno ==self.expected_seqno and checksum == check:
                    if cur_seqno==packets_no :
                        self.expected_seqno =(self.expected_seqno+1)%256
                        #todo
                        cur_seqno = cur_seqno + 1
                        ack=self.make_packet(seqno,self.expected_seqno)
                        self.send(ack)
                        if self.isDebug:
                            print("send ack:",seqno)
                            print("send sack:",self.expected_seqno)
                            print("cur_seqno",cur_seqno)

                        for i in range(self.expected_seqno,self.expected_seqno+self.windowSize):
                            if receiverReceivedSet[i%256]==1:
                                self.expected_seqno=(self.expected_seqno+1)%256
                                #todo
                                cur_seqno = cur_seqno + 1
                                print("cur_seqno",cur_seqno)
                                data +=buffer[i%256]
                                receiverReceivedSet[i%256]=0
                            else:
                                break
                        break         
                    # 收到窗口内的包，但不是第一个包，存在缓冲区里
                    elif packets_no > cur_seqno and packets_no < cur_seqno+self.windowSize:
                    # elif  (seqno-self.expected_seqno+256)%256 < self.windowSize :
                        if  isEnd==1:
                            receiverReceivedSet[seqno%256] = 0         # 若是最后一个包未按序到达，则丢弃并标记为未接受过
                            
                        else:
                            receiverReceivedSet[seqno%256] = 1         # 记录已经收到
                            buffer[seqno%256] = data
                            ack_pkt = self.make_packet(seqno, self.expected_seqno)
                            self.send(ack_pkt)
                            if self.isDebug:
                                print("send ack:",seqno)
                                print("send ack:",self.expected_seqno)
                        data=bytes('', encoding='utf-8')
                        break
                    elif(packets_no < cur_seqno):
                        ack_pkt = self.make_packet(seqno, self.expected_seqno)
                        self.send(ack_pkt)
                        if self.isDebug:
                            print("send ack:",seqno)
                        data=bytes('', encoding='utf-8')
                        break
                    else:
                        data=bytes('', encoding='utf-8')
                        break
                except socket.timeout:
                    if self.isDebug:
                        print("超时")
                    data=bytes('', encoding='utf-8') 
                    break  
            if isEnd==1:
                self.outfile.close()
                break
            self.outfile.write(data)
def main():
    client=Client('./client/'+str("test")+'.jpg')
    client.start()

if __name__=="__main__":
    main()