
all: zmq-journal-gatewayd zmq-journal-gatewayd-client clean 

zmq-journal-gatewayd: zmq-journal-gatewayd.o
	gcc -L/usr/local/lib zmq-journal-gatewayd.o -lzmq -lczmq -ljansson -lsystemd-journal -lsystemd-id128 -o zmq-journal-gatewayd 

zmq-journal-gatewayd.o: zmq-journal-gatewayd.c
	gcc -c zmq-journal-gatewayd.c -o zmq-journal-gatewayd.o

zmq-journal-gatewayd-client: zmq-journal-gatewayd-client.o
	gcc -L/usr/local/lib zmq-journal-gatewayd-client.o -lzmq -lczmq -ljansson -o zmq-journal-gatewayd-client 

zmq-journal-gatewayd-client.o: zmq-journal-gatewayd-client.c
	gcc -c zmq-journal-gatewayd-client.c -o zmq-journal-gatewayd-client.o


clean:
	rm *.o

