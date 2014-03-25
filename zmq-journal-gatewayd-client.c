/* a test client for zmq-journal-gatewayd */

/* this query string will be used for querying journal logs */
#define QUERY_STRING "{ \"format\" : \"json\" , \"since_timestamp\" : \"2014-03-21T13:30:12.000Z\" , \"follow\" : true }"

/* do you want heartbeating? this is necessary when you use the 'follow' functionality since the server has to know that you are still alive */
#define HEARTBEATING 1

#include <stdio.h>
#include <string.h>
#include <assert.h>
#include "czmq.h"
#include "zmq.h"

#define HEARTBEAT_INTERVAL 1000 // msecs, this states after which time you send a heartbeat
#define SERVER_HEARTBEAT_INTERVAL 5000 // msecs, this states how much time you give the server to answer a heartbeat
#define READY "\001"
#define END "\002"
#define HEARTBEAT "\003"
#define ERROR "\004"
#define TIMEOUT "\005"


/* Do sth with the received message */
int response_handler(zmsg_t *response){
    zframe_t *frame;
    char *frame_data;
    int more;

    do{
        frame = zmsg_pop (response);
        more = zframe_more (frame);
        frame_data = zframe_strdup(frame);
        if( strcmp( frame_data, END ) == 0 )
            return 1;
        if( strcmp( frame_data, ERROR ) == 0 )
            printf("<< ERROR >>\n");
        else if( strcmp( frame_data, HEARTBEAT ) == 0 )
            printf("<< HEARTBEAT >>\n");
        else if( strcmp( frame_data, TIMEOUT ) == 0 )
            printf("<< SERVER GOT NO HEARTBEAT >>\n");
        else if( strcmp( frame_data, READY ) == 0 )
            printf("<< SERVER GOT QUERY >>\n");
        else
            printf("New Message:\n%s\n\n\n", frame_data);
        zframe_destroy (&frame);
    }while(more);

    return 0;
}

main(void){

    /* initial setup */
    zctx_t *ctx = zctx_new ();
    void *client = zsocket_new (ctx, ZMQ_DEALER);
    zsocket_connect (client, "tcp://localhost:5555");

    /* send query */
    char *query_string = QUERY_STRING;
    zstr_send (client, query_string);

    zmq_pollitem_t items [] = {
        { client, 0, ZMQ_POLLIN, 0 },
    };

    zmsg_t *response;
    int rc;

    uint64_t heartbeat_at = zclock_time () + HEARTBEAT_INTERVAL;                // the absolute time after which a heartbeat is sent
    uint64_t server_heartbeat_at = zclock_time () + SERVER_HEARTBEAT_INTERVAL;  // the absolute time after which a server timeout occours, 
                                                                                // updated with every new message
                                                                                
    /* receive response while sending heartbeats (if necessary) */
    while (true) {

        rc = zmq_poll (items, 1, HEARTBEAT_INTERVAL * ZMQ_POLL_MSEC);
        if( rc == 0 && HEARTBEATING ){
            /* no message from server so far => send heartbeat */
            zstr_send (client, HEARTBEAT);
            heartbeat_at = zclock_time () + HEARTBEAT_INTERVAL;
        }
        else if ( rc > 0 ) 
            /* message from server arrived => update the timeout interval */
            server_heartbeat_at = zclock_time () +  SERVER_HEARTBEAT_INTERVAL;
        else if( rc == -1 ) 
            /* something went wrong */
            break;

        if(zclock_time () >= server_heartbeat_at){ 
            printf("<< SERVER TIMEOUT >>\n");
            break;
        }

        /* receive message and do sth with it */
        if (items[0].revents & ZMQ_POLLIN){ 
            response = zmsg_recv(client);
            rc = response_handler(response);
            zmsg_destroy (&response);
            /* end of log stream? */
            if (rc == 1){
                printf("<< GOT ALL LOGS! >>\n");
                break;
            }
        }

        /* the server also expects heartbeats while he is sending messages */
        if (zclock_time () >= heartbeat_at && HEARTBEATING) {
            zstr_send (client, HEARTBEAT);
            heartbeat_at = zclock_time () + HEARTBEAT_INTERVAL;
        }

    }

    /* clear everything up */
    zsocket_destroy (ctx, client);
    zctx_destroy (&ctx);
    printf("<< EXIT >>\n");
    return 0;
}
