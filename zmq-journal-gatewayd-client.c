/* a test client for zmq-journal-gatewayd */

#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <signal.h>

#include "czmq.h"
#include "zmq.h"

#define QUERY_STRING ""                         // default query string, every communication begins with sending a query string
#define HEARTBEATING 0                          // set to one if 'follow' is true 
#define CLIENT_SOCKET "tcp://localhost:5555"    // the socket the client should connect to
#define HEARTBEAT_INTERVAL 1000                 // msecs, this states after which time you send a heartbeat
#define SERVER_HEARTBEAT_INTERVAL 5000          // msecs, this states how much time you give the server to answer a heartbeat
#define READY "\001"                            // answer from the gateway for a successful query (after this message the requested logs will be sent)
#define END "\002"                              // message from the gateway, successfully sent all requested logs
#define HEARTBEAT "\003"                        // the client is in charge of sending HEARTBEATs according to the given intervals, these will be answered immediately with a HEARTBEAT
#define ERROR "\004"                            // message from gateway, invalid query?
#define TIMEOUT "\005"                          // sent by gateway if a HEARTBEAT was expected but not sent by the client
#define STOP "\006"                             // the client and gateway handler can be stopped (e.g. via ctrl-c), this will be confirmed by the gateway with a STOP 

static zctx_t *ctx;
static void *client;

static bool active = true;
void stop_handler(int dummy) {
    printf("\n<< sending STOP ... >>\n");
    zstr_send (client, STOP);
    char *frame_string = NULL;
    do {
        if (frame_string != NULL) 
            free(frame_string);
        frame_string = zstr_recv(client);
    }while( strcmp( frame_string, STOP ) != 0 );
    free(frame_string);

    printf("<< STOP confirmed >>\n");

    /* stop the client */
    active = false;
}

/* Do sth with the received message */
int response_handler(zmsg_t *response){
    zframe_t *frame;
    char *frame_data;
    int more;

    do{
        frame = zmsg_pop (response);
        more = zframe_more (frame);
        frame_data = zframe_strdup(frame);
        zframe_destroy (&frame);
        if( strcmp( frame_data, END ) == 0 ){
            printf("<< got all logs >>\n");
            free(frame_data);
            return 1;
        }
        if( strcmp( frame_data, ERROR ) == 0 ){
            printf("<< an error occoured - invalid json query string? >>\n");
            free(frame_data);
            return -1;
        }
        else if( strcmp( frame_data, HEARTBEAT ) == 0 )
            printf("<< HEARTBEAT >>\n");
        else if( strcmp( frame_data, TIMEOUT ) == 0 )
            printf("<< server got no heartbeat >>\n");
        else if( strcmp( frame_data, READY ) == 0 )
            printf("<< gateway accepted query >>\n\n");
        else
            printf("%s\n\n", frame_data);
        free(frame_data);
    }while(more);

    return 0;
}

int main ( int argc, char *argv[] ){

    /* initial setup */
    ctx = zctx_new ();
    client = zsocket_new (ctx, ZMQ_DEALER);
    zsocket_connect (client, CLIENT_SOCKET);

    /* for stopping the client and the gateway handler via keystroke (ctrl-c) */
    signal(SIGINT, stop_handler);

    /* send query */
    char *query_string = argv[1] != NULL ? argv[1] : QUERY_STRING;
    printf( "Press any key for sending the following query:\n%s\n", query_string);
    zstr_send (client, query_string);

    zmq_pollitem_t items [] = {
        { client, 0, ZMQ_POLLIN, 0 },
    };

    zmsg_t *response;
    int rc;

    uint64_t heartbeat_at = zclock_time () + HEARTBEAT_INTERVAL;                // the absolute time after which a heartbeat is sent
    uint64_t server_heartbeat_at = zclock_time () + SERVER_HEARTBEAT_INTERVAL;  // the absolute time after which a server timeout occours, 
                                                                                // updated with every new message (doesn't need to be a heartbeat)
                                                                                
    /* receive response while sending heartbeats (if necessary) */
    while (active) {

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
            if (rc != 0)
                break;
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
