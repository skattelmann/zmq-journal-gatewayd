
#include <stdio.h>
#include <string.h>
#include <assert.h>

#include "czmq.h"
#include "zmq.h"
#include "jansson.h"

typedef struct RequestMeta {
    //output format
    int format;
    //time window for entries to be sent
    int since_timestamp;
    int until_timestamp2;
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

typedef struct Args{
    zframe_t *client_addr;
    json_t *json_args;
}Args;

Args *parse_json(zmsg_t* query_msg){
    int rc;
    int i;
    zframe_t *client_addr = zmsg_pop (query_msg);
    zframe_t *query_frame = zmsg_pop (query_msg);

    char *query_string = zframe_strdup (query_frame);
    zframe_destroy (&query_frame);
    json_error_t error;
    json_t *json_args = json_loads(query_string, 0, &error);
    assert(json_args != NULL);
    free(query_string);

    Args *args = malloc( sizeof(Args) );
    args->client_addr = client_addr;
    args->json_args = json_args;

    return args;
}

static void *handler_routine (void *args) {
    zctx_t *ctx = zctx_new ();
    void *query_handler = zsocket_new (ctx, ZMQ_DEALER);
    int rc = zsocket_connect (query_handler, "ipc://backend");
    assert(rc == 0);

    json_t *json_args = ((Args *) args)->json_args;

    printf("<<PARSED INPUT>>\n");
    json_dumpf(json_args, stdout, JSON_INDENT(4));
    printf("\n");

    zframe_t *client_addr = ((Args *) args)->client_addr;
    zframe_t *response1 = zframe_new ("pong", 6);

    printf("<<HANDLER SENDS BACK>>\n");
    zframe_send (&client_addr, query_handler, 1);
    zframe_send (&response1, query_handler, 0);

    zmq_close (query_handler);
    return NULL;
}

int main (void)
{
    void *ctx = zctx_new ();

    // Socket to talk to clients
    void *frontend = zsocket_new (ctx, ZMQ_ROUTER);
    assert(frontend);
    int rc = zsocket_bind (frontend, "tcp://*:5555");
    assert(rc == 5555);

    // Socket to talk to the query handlers
    void *backend = zsocket_new (ctx, ZMQ_DEALER);
    assert(backend);
    rc = zsocket_bind (backend, "ipc://backend");
    assert(rc == 0);

    // Setup the poller for frontend and backend
    zmq_pollitem_t items[] = {
        {frontend, 0, ZMQ_POLLIN, 0},
        {backend, 0, ZMQ_POLLIN, 0},
    };

    zmsg_t *msg;
    Args *json_args; 
    while (1) {
        zmq_poll (items, 2, -1);

        if (items[0].revents & ZMQ_POLLIN) {
            msg = zmsg_recv (frontend);
            printf("<<RECEIVED NEW QUERY>>\n");
            json_args = parse_json(msg);
            assert(json_args);
            zthread_new (handler_routine, json_args);
        }

        if (items[1].revents & ZMQ_POLLIN) {
            zmsg_t *response = zmsg_new ();
            response = zmsg_recv (backend);
            zmsg_send (&response, frontend);
            return 0;
        }

    }

}

