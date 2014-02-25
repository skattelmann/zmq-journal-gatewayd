#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <time.h>
#include "czmq.h"
#include "zmq.h"
#include "jansson.h"

#define HEARTBEAT_INTERVAL 5*1000 // msecs
#define READY "1"
#define END "2"
#define HEARTBEAT "3"

typedef struct RequestMeta {
    // for response
    zframe_t *client_ID;
    zframe_t *handler_ID;
    char* client_ID_string;
    //output format
    const char *format;
    //time window for entries to be sent
    int since_timestamp;
    int until_timestamp;
    //cursor window for entries to be sent
    char *cursor1;
    char *cursor2;
    //follow entries, get just one entry, only boot with ID, show machine info
    bool follow;
    bool discrete;
    bool boot;
    int boot_id;
    bool machine;
    //send unique entries for specified fields
    bool unique_entries;
    char *field;
    //fields to be matched with
    char **field_matches;
} RequestMeta;

typedef struct Connection {
    zframe_t *client_ID;
    zframe_t *handler_ID;
}Connection;

RequestMeta *parse_json(zmsg_t* query_msg){

    zframe_t *client_ID = zmsg_pop (query_msg);
    zframe_t *query_frame = zmsg_pop (query_msg);

    char *query_string = zframe_strdup (query_frame);
    zframe_destroy (&query_frame);
    json_error_t error;
    json_t *json_args = json_loads(query_string, 0, &error);
    assert(json_args != NULL);
    free(query_string);

    char *print_query = json_dumps(json_args, JSON_INDENT(4));
    printf("\n%s\n", print_query);
    free(print_query);

    json_t *format = json_object_get(json_args, "format");
    json_t *since_timestamp = json_object_get(json_args, "since_timestamp");
    json_t *until_timestamp = json_object_get(json_args, "until_timestamp");

    RequestMeta *args = malloc( sizeof(RequestMeta) );
    args->client_ID = client_ID;
    args->client_ID_string = zframe_strhex (client_ID);
    args->format = json_string_value(format);
    args->since_timestamp = (int) json_number_value(since_timestamp);
    args->until_timestamp = (int) json_number_value(until_timestamp);

    return args;
}

static void *handler_routine (void *_args) {

    RequestMeta *args = (RequestMeta *) _args;
    zctx_t *ctx = zctx_new ();
    void *query_handler = zsocket_new (ctx, ZMQ_DEALER);
    int rc = zsocket_connect (query_handler, "ipc://backend");
    assert(rc == 0);

    /* send READY to the client */
    zmsg_t *ready = zmsg_new();
    zframe_t *client_ID = zframe_dup (args->client_ID);
    zframe_t *ready_frame = zframe_new ( READY, strlen(READY)+1 );
    zmsg_push (ready, ready_frame);
    zmsg_push (ready, client_ID);
    zmsg_send (&ready, query_handler);
    printf("<< READY SENT TO %s >>\n", args->client_ID_string);

    zmq_pollitem_t items [] = {
        { query_handler, 0, ZMQ_POLLIN, 0 },
    };

    /* just for debugging */
    struct timespec tim, tim2;
    tim.tv_sec  = 0;
    tim.tv_nsec = 200000000L;

    uint64_t heartbeat_at = zclock_time () + HEARTBEAT_INTERVAL;
    while (true) {

        rc = zmq_poll (items, 1, 0);
        assert(rc != -1);

        /* receive heartbeat and send it back */
        if (items[0].revents & ZMQ_POLLIN){
            printf("<< HANDLER GOT MESSAGE >>\n");
            char *heartbeat_string = zstr_recv (query_handler);

            //if( strcmp(heartbeat_string, HEARTBEAT) ){
                printf("<< HEARTBEAT RECEIVED >>\n");
                zmsg_t *heartbeat = zmsg_new();
                zframe_t *client_ID = zframe_dup (args->client_ID);
                zframe_t *heartbeat_frame = zframe_new ( HEARTBEAT, strlen(HEARTBEAT)+1 );
                zmsg_push (heartbeat, heartbeat_frame);
                zmsg_push (heartbeat, client_ID);
                zmsg_send (&heartbeat, query_handler);
                printf("<< HEARTBEAT ANSWERED >>\n");
                heartbeat_at = zclock_time () + HEARTBEAT_INTERVAL;
            //}
            free (heartbeat_string);
        }

        /* timeout from client */
        if (zclock_time () >= heartbeat_at) {
                zmsg_t *end = zmsg_new();
                zframe_t *client_ID = zframe_dup (args->client_ID);
                zframe_t *end_frame = zframe_new ( END, strlen(END)+1 );
                zmsg_push (end, end_frame);
                zmsg_push (end, client_ID);
                zmsg_send (&end, query_handler);
            printf("<< MISSING HEARTBEAT >>\n");
            break;
        }

        /* DO SOME WORK HERE */
        nanosleep(&tim , &tim2);
        printf(">>>> TICK\n");

    }

    zmq_close (query_handler);
    return NULL;

}

int main (void){

    void *ctx = zctx_new ();

    // Socket to talk to clients
    void *frontend = zsocket_new (ctx, ZMQ_ROUTER);
    assert(frontend);
    int rc = zsocket_bind (frontend, "tcp://*:5555");
    assert(rc == 5555);

    // Socket to talk to the query handlers
    void *backend = zsocket_new (ctx, ZMQ_ROUTER);
    assert(backend);
    rc = zsocket_bind (backend, "ipc://backend");
    assert(rc == 0);

    // Setup the poller for frontend and backend
    zmq_pollitem_t items[] = {
        {frontend, 0, ZMQ_POLLIN, 0},
        {backend, 0, ZMQ_POLLIN, 0},
    };

    zhash_t *connections = zhash_new ();
    Connection *lookup;
    zmsg_t *msg;
    RequestMeta *args; 
    while (1) {
        zmq_poll (items, 2, -1);

        if (items[0].revents & ZMQ_POLLIN) {
            msg = zmsg_recv (frontend);
            printf("<< RECEIVED NEW MESSAGE >>\n");

            zframe_t *client_ID = zmsg_first (msg);
            char *client_ID_string = zframe_strhex (client_ID);
            lookup = (Connection *) zhash_lookup(connections, client_ID_string);

            /* first case: new query */
            if( lookup == NULL ){
                printf("<< NEW CLIENT >>\n");
                args = parse_json(msg);
                assert(args);
                Connection *new_connection = (Connection *) malloc( sizeof(Connection) );
                new_connection->client_ID = client_ID;
                new_connection->handler_ID = NULL;
                zhash_update (connections, args->client_ID_string, new_connection);
                printf("<< NEW ID: %s >>\n", args->client_ID_string);
                assert(rc == 0);
                zthread_new (handler_routine, (void *) args);
            }
            /* second case: heartbeat sent by client */
            else{
                printf("<< HEARTBEAT SENT BY CLIENT: %s >>\n", client_ID_string);
                zframe_t *heartbeat_frame = zmsg_last (msg);
                assert(heartbeat_frame != NULL);
                zmsg_t *heartbeat = zmsg_new ();
                    
                assert(lookup->handler_ID != NULL);
                zframe_t *handler_ID = zframe_dup (lookup->handler_ID);

                assert(handler_ID != NULL);
                zmsg_push (heartbeat, heartbeat_frame);
                zmsg_push (heartbeat, handler_ID);
                zmsg_send (&heartbeat, backend);
            }
        }

        if (items[1].revents & ZMQ_POLLIN) {
            printf("<< SOMETHING SENT TO CLIENT >>\n");
            zmsg_t *response = zmsg_recv (backend);

            zframe_t *handler_ID = zmsg_pop (response);
            zframe_t *client_ID = zmsg_first (response);
            zframe_t *handler_response = zmsg_last (response);

            char *client_ID_string = zframe_strhex (client_ID);
            char *handler_response_string = zframe_strdup (handler_response);
            lookup = (Connection *) zhash_lookup(connections, client_ID_string);

            if( lookup->handler_ID == NULL ){
                lookup->handler_ID = handler_ID;
                printf("<< ADDED NEW HANDLER ID >>\n");
            }
            else zframe_destroy (&handler_ID);

            if( strcmp( handler_response_string, END ) == 0 ){
                zhash_delete (connections, client_ID_string);
                printf("<< DELETED ITEM FROM HASH >>\n");
            }

            free(handler_response_string);
            zmsg_send (&response, frontend);
        }

    }

}

