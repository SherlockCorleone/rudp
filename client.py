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
        if(flag==0):
            data=message[2:]
        else :
            data=None
        
        return seqno,flag,data

    def start(self):
        receiverReceivedSet = [0]*256   # 用于记录接收到的分组
        buffer = [""]*256 #缓冲区
        last_ack=self.make_packet(0,0)
        while True:
            self.socket.settimeout(self.timeout)
            data=""
            isEnd=False
            while True:
                try:
                    data,address=self.socket.recvfrom(4096)
                    self.target=address
                    
                    seqno,flag,data=self.split_message(data)
                    
                    if self.isDebug:
                        print("receive seqno:",seqno)
                        print("receive isEnd:",isEnd)
                    #序号是窗口第一个包,提交缓冲区
                    if seqno ==self.expected_seqno :
                        self.expected_seqno =(self.expected_seqno+1)%256
                        ack=self.make_packet(seqno,self.expected_seqno)
                        try:
                           self.send(ack)
                        except OSError:
                            print("网络阻塞")
                            continue
                        if self.isDebug:
                            print("send ack:",seqno)
                            print("send sack:",self.expected_seqno)
                        #提交
                        for i in range(self.expected_seqno,self.expected_seqno+self.windowSize):
                            if receiverReceivedSet[i%256]==1:
                                self.expected_seqno=(self.expected_seqno+1)%256
                                data +=buffer[i%256]
                                receiverReceivedSet[i%256]=0
                            else:
                                break
                        if flag==1:
                            isEnd=True
                            last_ack=ack
                        break      
                    # 收到窗口内的包，但不是第一个包，存在缓冲区里
                    elif  (seqno-self.expected_seqno+256)%256 < self.windowSize :
                        if  flag==1:
                            receiverReceivedSet[seqno%256] = 0         # 若是最后一个包未按序到达，则丢弃并标记为未接受过
                        else:
                            receiverReceivedSet[seqno%256] = 1         # 记录已经收到
                            buffer[seqno%256] = data
                            ack_pkt = self.make_packet(seqno, self.expected_seqno)
                            try:
                                self.send(ack)
                            except OSError:
                                print("网络阻塞")
                                continue
                            if self.isDebug:
                                print("send ack:",seqno)
                                print("send ack:",self.expected_seqno)
                        data=bytes('', encoding='utf-8')
                        break
                    #其他的包直接发送确认ACK
                    else:
                        ack_pkt = self.make_packet(seqno, self.expected_seqno)
                        self.send(ack_pkt)
                        if self.isDebug:
                            print("send ack:",seqno)
                        data=bytes('', encoding='utf-8')
                        break
                except socket.timeout:
                    if self.isDebug:
                        print("超时")
                    data=bytes('', encoding='utf-8') 
                    break  
            if isEnd:
                self.outfile.close()
                #避免ACK丢失,客户机退出前重传10次ACK
                for i in range(10):
                    self.send(last_ack)
                break
            self.outfile.write(data)
def main():
    client=Client('./'+str("output")+'.bin')
    client.start()

if __name__=="__main__":
    main()