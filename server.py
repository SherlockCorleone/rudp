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
        self.fileIteration=self.read_file_in_chunks(filename)#文件读取迭代对象
    # 文件读取迭代器
    def read_file_in_chunks(filename, chunk_size=1024):
        with open(filename, 'rb') as file:
            while True:
                chunk = file.read(chunk_size)
                if not chunk:
                    break
                yield chunk
    # 根据相应大小获得对应的包
    def get_packets_by_size(self,size,seqno):
        packets = []
        try:
            for i in range(size):
                file=next(self.fileIteration)
                if seqno==0:
                    packets.append(self.make_packet("start",seqno+i,file))
                else :
                    packets.append(self.make_packet("data",seqno+i,file))
        except StopIteration:
            pass
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
        # timers=timer.timer()
        isEnd=False
        timers=[]
        while ack!=len(self.packets):
            if not isEnd:
                # 读取并打包窗口大小两倍的文件
                self.packets.append(self.get_packets_by_size(self.windowSize*2,self.base))
                # todo 细节处理读取到文件末尾
                if len(self.packets) <self.windowSize*2:
                    break
                else:
                    isEnd   
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
              
            


