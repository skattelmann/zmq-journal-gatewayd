
#include <stdio.h>
#include <string.h>
#include <assert.h>

#include "czmq.h"
#include "zmq.h"
#include "jsmn.h"

#define TOKEN_PRINT(t) \
    printf("start: %d, end: %d, type: %d, size: %d\n", (t).start, (t).end, (t).type, (t).size)

typedef struct Args
{
    char *cmd;
    int since;
}Args;

Args *parse_json(zmsg_t* query_msg, jsmn_parser* json_parser){
    int r;
    zframe_t *client_addr = zmsg_pop (query_msg);
    zframe_t *query_frame = zmsg_pop (query_msg);

    size_t frame_size = zframe_size (query_frame);
    char *query_string = zframe_strdup (query_frame);
    //printf("Query String: %s\n", query_string);
    assert(query_string[frame_size-1] == '\0');
    zframe_destroy (&query_frame);

    jsmntok_t tokens[frame_size];
    jsmn_init(json_parser);
    r = jsmn_parse(json_parser, query_string, tokens, frame_size);
    assert(r == JSMN_SUCCESS);

    //    int k;
    //    for(k=0;k<=tokens[0].size;k++) TOKEN_PRINT(tokens[k]);

    Args *parsed_args = malloc(sizeof(Args));
    int cmd_len = tokens[1].end - tokens[1].start;
    int since_len = tokens[2].end - tokens[2].start;

    parse_string(parsed_args->cmd, query_string, tokens[1]);

    parsed_args->cmd = malloc( sizeof(char) * (cmd_len + 1) );
    strncpy(parsed_args->cmd, query_string + tokens[1].start, cmd_len);
    (parsed_args->cmd)[cmd_len] = '\0';

    char *since_string = malloc( sizeof(char) * (since_len + 1) );
    strncpy(since_string, query_string + tokens[2].start, since_len);
    since_string[since_len] = '\0';
    parsed_args->since = atoi(since_string); 
    free(since_string);

    free(query_string);
    return parsed_args;
}

static void *handler_routine (void *frames) {
    zctx_t *ctx = zctx_new ();
    void *query_handler = zsocket_new (ctx, ZMQ_DEALER);
    int rc = zsocket_connect (query_handler, "ipc://backend");
    assert(rc == 0);

    zframe_t *addr = * ((zframe_t **) frames);
    zframe_t *query = * (((zframe_t **) frames) + 1);

    zframe_t *response1 = zframe_new ("pong", 6);
    zframe_send (&addr, query_handler, 1);
    zframe_send (&response1, query_handler, 0);

    zframe_destroy (&query);
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

    jsmn_parser json_parser;

    zmsg_t *msg;
    Args *parsed_args; 
    while (1) {
        zmq_poll (items, 2, -1);

        if (items[0].revents & ZMQ_POLLIN) {
            msg = zmsg_recv (frontend);
            parsed_args = parse_json(msg, &json_parser);
            assert(parsed_args);

            printf("FIRST: %s\n", parsed_args->cmd);
            printf("SECOND: %d\n", parsed_args->since);
            return 0;

            //zthread_new (handler_routine, parsed_args);
        }

        if (items[1].revents & ZMQ_POLLIN) {
            zmsg_t *response = zmsg_new ();
            response = zmsg_recv (backend);
            zmsg_send (&response, frontend);
        }

    }

}

