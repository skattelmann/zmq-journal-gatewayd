#include <stdio.h>
#include <string.h>
#include <assert.h>
#include "czmq.h"
#include "zmq.h"

#define HEARTBEAT_INTERVAL 1000 // msecs
#define SERVER_HEARTBEAT_INTERVAL 1000 // msecs
#define HEARTBEAT "\001"
#define END_OF_RESPONSE "\002"

/* Do sth with the received message */
void response_handler(zmsg_t *response){
    zframe_t *frame;
    char *frame_data;
    int more;

    do{
        frame = zmsg_pop (response);
        more = zframe_more (frame);
        frame_data = (char *) zframe_data (frame);
        if( strcmp( frame_data, HEARTBEAT) ) printf("<< GOT HEARTBEAT FROM SERVER >>\n");
        else if( strcmp( frame_data, END_OF_RESPONSE) ) printf("<< FINISHED RESPONSE >>\n");
        else printf("FRAME DATA: %s\n", frame_data);
        zframe_destroy (&frame);
    }while(more);
    printf("\n\n");

}

main(void){

    /* initial setup */
    zctx_t *ctx = zctx_new ();
    void *client = zsocket_new (ctx, ZMQ_DEALER);
    zsocket_connect (client, "tcp://localhost:5555");

    /* send query */
    char *query_string = "{\"entries\": 5 , \"time1\" : 123 , \"time2\" : 789}";
    zstr_send (client, query_string, 0);
    printf("<< QUERY SENT >>\n");

    zmq_pollitem_t items [] = {
        { client, 0, ZMQ_POLLIN, 0 },
    };

    zmsg_t *response;
    int rc;

    /* receive response while sending heartbeats */
    uint64_t heartbeat_at = zclock_time () + HEARTBEAT_INTERVAL;
    while (true) {

        rc = zmq_poll (items, 1, SERVER_HEARTBEAT_INTERVAL * ZMQ_POLL_MSEC);
        if (rc == 0){
            printf("<< NO RESPONSE FROM SERVER ...  >>\n");
            break;
        }

        /* receive message and do sth with it */
        if (items[0].revents & ZMQ_POLLIN){ 
            response = zmsg_recv(client);
            response_handler(response);
            zmsg_destroy (&response);
        }

        if (zclock_time () >= heartbeat_at) {
            zstr_send (client, HEARTBEAT, 0);
            heartbeat_at = zclock_time () + HEARTBEAT_INTERVAL;
            printf("<< HEARTBEAT SENT >>\n");
        }

    }

    /* clear everything up */
    zsocket_destroy (ctx, client);
    zctx_destroy (&ctx);
    printf("<< EXIT >>\n");
    return 0;
}
