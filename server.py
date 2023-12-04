import socket
import struct
import hashlib
import os
import time
import math
#sudo tc qdisc add dev ens33 root netem delay 100ms 10ms 30%
#sudo tc qdisc del dev ens33 root netem delay 100ms 10ms 30%
class Server:
    def __init__(self,dest,port,filename,timeout=0.5,isDebug=True):
        self.isDebug=isDebug
        self.dest=dest
        self.port=port
        self.timeout=timeout
        self.socket=socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
        self.windowSize=5 #窗口大小
        self.base=0 #窗口首元素序号
        self.nextSeq=0 #下一个包的序号
        self.acks=[]
        self.bufferSize=256 #缓冲区大小
        self.packets=[""] * self.bufferSize #待发送包缓冲区
        self.sendPackets = [1] * self.bufferSize #发送标记缓冲区
        self.ACKs = [1] * self.bufferSize #ACK标记缓冲区
        self.ssthresh=15 #慢启动阈值
        self.maxWindowSize=100 #int(self.bufferSize/2)-1 #最大窗口大小
        self.dataSize=1024 #数据包大小

        self.infile=open(filename,'rb')
        self.infile.seek(0, os.SEEK_END)
        self.lastSeq = math.ceil(self.infile.tell()/self.dataSize)+1 #最后一个包的序号，默认为文件添加一个结尾包
        self.infile.seek(0)

    # 文件读取迭代器
    def read_part(self, size):
        data = self.infile.read(size)

        if not data:
            return None  # 文件已经读取完毕
        return data
    
    # 文件关闭
    def read_close(self):
        self.infile.close()
    
    #打包
    def make_packet(self,flag,seqno,msg):
        if msg !=None:
            return struct.pack('BB',seqno,flag)+msg
        else :
            return struct.pack('BB',seqno,flag)
    # 填满缓冲区
    def fill_packets_buffer(self):
        #遍历缓冲区
        for i in range(self.bufferSize):
            cur_num=(self.base+i)%self.bufferSize
            #如果已经收到ack并且已经发送过了并且不在窗口内
            if self.ACKs[cur_num]==1 and self.sendPackets[cur_num]==1 and not (cur_num+self.bufferSize-self.base)%self.bufferSize<self.windowSize:
                self.sendPackets[cur_num]=0
                self.ACKs[cur_num]=0
                data=self.read_part(self.dataSize)
                if data==None:
                    self.packets[cur_num]=self.make_packet(1,cur_num,data)
                    self.read_close()
                    return True
                else:
                    self.packets[cur_num]=self.make_packet(0,cur_num,data)   
        return False
    def init_buffer(self):
        #遍历缓冲区
        for i in range(self.bufferSize):
            cur_num=(self.base+i)%self.bufferSize
            #如果已经收到ack并且已经发送过了
            if self.ACKs[cur_num]==1 and self.sendPackets[cur_num]==1:
                self.sendPackets[cur_num]=0
                self.ACKs[cur_num]=0
                data=self.read_part(self.dataSize)
                if data==None:
                    self.packets[cur_num]=self.make_packet(1,cur_num,data)
                    self.read_close()
                    return True
                else:
                    self.packets[cur_num]=self.make_packet(0,cur_num,data)
        return False
    #send函数
    def send(self,message):
        self.socket.sendto(message,(self.dest,self.port))
       
    #分割ack包
    def split_ack_packet(self, message):
        ack=message[0]
        sack=message[1]   
        return ack,sack
    
    def start(self):
        #记录包的序号
        cur_seqno = 0
        # 读取文件
        isReadEnd=self.init_buffer()
        
        # 发送
        while True:
            #发送窗口内的包并且这个包还未发送过
            while (self.nextSeq+self.bufferSize-self.base)%self.bufferSize<self.windowSize and cur_seqno<self.lastSeq and self.sendPackets[self.nextSeq]==0:	
                try:
                    self.send(self.packets[self.nextSeq])
                except OSError:
                    print("网络阻塞")
                    continue
                if self.isDebug:
                    print("发送包：",self.nextSeq)
                    print("cur_seqno:",cur_seqno)
                self.sendPackets[self.nextSeq]=1
                self.nextSeq = (self.nextSeq + 1) % self.bufferSize  
                cur_seqno+=1
            #处理ack和超时
            self.socket.settimeout(self.timeout)
            while True:
                try:
                    data,address=self.socket.recvfrom(4096)
                    ack_seqno,sack_seqno=self.split_ack_packet(data)
                    if self.isDebug:
                        print("收到ack：",ack_seqno)
                    
                    # 记录发送窗口内的ack
                    if(ack_seqno-self.base+self.bufferSize)%self.bufferSize<self.windowSize:
                        self.ACKs[ack_seqno]=1
                    
                    #最后一个包的ack
                    if(ack_seqno==(self.lastSeq-1)%self.bufferSize and isReadEnd):
                        break
                    
                    if(self.base==ack_seqno):
                        while (self.ACKs[self.base] == 1):
                            self.base = (self.base + 1) % self.bufferSize  # 窗口滑动
                        #慢启动
                        if(self.windowSize<self.ssthresh):
                            self.windowSize*=2
                        #拥塞避免
                        elif(self.windowSize>=self.ssthresh and self.windowSize<self.maxWindowSize):
                            self.windowSize+=1
                        if self.isDebug:
                            print("窗口大小：",self.windowSize)
                    #所有的ack都收到了
                    if self.base == self.nextSeq: 
                        self.socket.settimeout(None)
                        break
                except socket.timeout:
                    if self.isDebug:
                        print("超时，丢包")
                    #快速重传      
                    for i in range(self.base, self.base + self.windowSize):
                        if (self.sendPackets[i%self.bufferSize] == 1 and self.ACKs[i%self.bufferSize] == 0):
                            try:
                                self.send(self.packets[i%self.bufferSize])
                            except OSError:
                                print("网络阻塞")
                                continue
                            if self.isDebug:
                                print("重新发送包：",i%self.bufferSize)
                    self.ssthresh =int(self.windowSize / 2)
                    self.windowSize = self.ssthresh + 3
                    self.socket.settimeout(self.timeout)
            # 填缓冲区
            if isReadEnd==False:
                isReadEnd=self.fill_packets_buffer()
            #发送完毕
            if(cur_seqno>=self.lastSeq):
                break
def main():
    server=Server("192.168.127.130",8888,"./output.bin")
    server.start()

if __name__=="__main__":
    main()
