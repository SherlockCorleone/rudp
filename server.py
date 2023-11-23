import signal
import socket
import threading
# import timer
class Server:
    def __init__(self,dest,port,filename,timeout=0.5):
        self.dest=dest
        self.port=port
        self.timeout=timeout
        self.socket=socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
        self.windowSize=1 #窗口大小
        self.base=0 #窗口首元素序号
        self.nextSeq=0 #下一个包的序号
        self.acks=[]
        self.packets=[]
        self.infile=open(filename,'rb')
    # 文件读取迭代器
    def read_part(self, size):
        data = self.infile.read(size)
        if not data:
            return None  # 文件已经读取完毕
        return data
    def read_close(self):
        self.infile.close()

    # 根据相应大小获得对应的包
    def get_packets_by_size(self,size,seqno):
        packets = []
        for i in range(size):
            data=self.read_part(1024)
            if data==None:
                if i==0:
                    return None #如果是第一个包就是空的，那就返回空
                else:
                    return packets
            else:
                packets.append(self.make_packet("data",seqno+i,data))
        return packets

    def handle_response(self,response_packet):
        #todo 校验
        
        handle_pieces=response_packet.split('|')
        ack_type,ackno=handle_pieces
    #处理ack
    def handle_new_ack(self, ack):
        self.base = ack
    #处理超时
    def handle_timeout(self, seqno):
        self.nextSeq = seqno #seqno处超时，那下次就从seqno处开始发
    #send函数
    def send(self,message):
        self.socket.sendto(message,(self.dest,self.port))
    #打包
    def make_packet(self,msg_type,seqno,msg):
        body= str(msg_type)+"|"+str(seqno)+"|"+msg+"|"
        #todo 生成校验码
        checksum=1
        packet= body+checksum
        return packet
    def set_timer(self,seqno):
        timer=threading.Timer(self.timeout,self.handle_timeout,args=(seqno,))
        return timer
    # 超时处理
    def receive(self, timeout=None):
        self.socket.settimeout(timeout)
        try:
            return self.socket.recv(4096)
        except (socket.timeout, socket.error):
            return None
    def start(self):
        ack=0
        isReadEnd=False
        isSendEnd=False
        timers=[]
        while not isReadEnd or not isSendEnd:

            # 根据窗口大小读取文件 未发送的包小于窗口大小的两倍就读文件
            if not isReadEnd and len(self.packets)-self.base<self.windowSize*2:
                # 读取并打包窗口大小两倍的文件
                size=self.windowSize*2
                packets=self.get_packets_by_size(size,self.base)
                # 开始就没有
                if packets==None:
                    isReadEnd=True
                # 读到一半没有了
                elif len(packets)<size:
                    isReadEnd=True
                    self.packets.append(packets)
                else:
                    self.packets.append(packets) 
            while self.nextSeq<self.base+self.windowSize:
                #todo 
                if ack>len(self.packets):
                    break

                #发送数据包 并为每个包设置一个定时器
                self.send(self.packets[self.nextSeq])
                timer=self.set_timer(self.nextSeq)
                timer.start()
                timers.append(timer)
                self.nextSeq+=1
            #接受ack
            message =self.receive(self.timeout) 
            if message !=None:
                message=message.decode()
                msg_type,ack_data,data,checksum=self.split_packet(message)
                if msg_type=="ack":
                    ack=int(ack_data)
                elif msg_type =="sack":
                    ack_data=ack_data.splitt(";")
                    ack=int(ack_data[0])
                    acks[ack-1]=1
              
            


