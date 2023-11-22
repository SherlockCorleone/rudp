def read_file_in_chunks(file_path, chunk_size=1024):
    with open(file_path, 'r') as file:
        while True:
            chunk = file.read(chunk_size)
            if not chunk:
                break
            yield chunk
file_path = './123.txt'
file_iterator = read_file_in_chunks(file_path)

# 第一次读取文件内容
chunk1 = next(file_iterator)
print(chunk1)

try:# 第二次读取文件内容
    chunk2 = next(file_iterator)
    print(chunk2)
except StopIteration:
    print ("文件已读完")
