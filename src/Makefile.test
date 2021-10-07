CC:=g++
CFLAGS+=-c -Wall -g -std=c++17
INCLUDES=-I./ -I./third_party/leveldb.1.22/leveldb/include

OBJS=main.o util.o vdfs.o vdfs.pb.o
LIBS=-lpthread -lleveldb -lprotobuf
LFLAGS=-L/usr/local/lib -L./third_party/leveldb.1.22/leveldb/b

main: $(OBJS)
	$(CC) $^ -o $@ $(LIBS) $(LFLAGS)

%.o: %.cc %.h
	$(CC) $(INCLUDES) $(CFLAGS) -o $@ $<

.PHONY:
proto:
	protoc -I. --cpp_out=. vdfs.proto

all:
	make proto && make main

clean:
	rm -rf *.o
	rm -rf main

