
all: libjsmn.a  zmq-journal-gatewayd zmq-journal-gatewayd-client clean 

zmq-journal-gatewayd: zmq-journal-gatewayd.o
	gcc zmq-journal-gatewayd.o jsmn.o -o zmq-journal-gatewayd -lzmq -lczmq

zmq-journal-gatewayd.o: zmq-journal-gatewayd.c
	gcc -c zmq-journal-gatewayd.c -o zmq-journal-gatewayd.o

zmq-journal-gatewayd-client: zmq-journal-gatewayd-client.o
	gcc zmq-journal-gatewayd-client.o jsmn.o -o zmq-journal-gatewayd-client -lzmq -lczmq

zmq-journal-gatewayd-client.o: zmq-journal-gatewayd-client.c
	gcc -c zmq-journal-gatewayd-client.c -o zmq-journal-gatewayd-client.o



# JSMN stuff
libjsmn.a: jsmn.o
	$(AR) rc $@ $^

%.o: %.c jsmn.h
	$(CC) -c $(CFLAGS) $< -o $@

test: jsmn_test
	./jsmn_test

jsmn_test: jsmn_test.o
	$(CC) -L. -ljsmn $< -o $@

jsmn_test.o: jsmn_test.c libjsmn.a

clean:
	rm -f jsmn.o jsmn_test.o
	rm -f jsmn_test
	rm -f jsmn_test.exe
	rm -f libjsmn.a
	rm *.o

.PHONY: all clean test
