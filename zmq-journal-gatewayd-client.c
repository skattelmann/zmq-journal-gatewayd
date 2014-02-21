
#include <stdio.h>
#include <string.h>
#include <assert.h>

#include "czmq.h"
#include "zmq.h"

int main(void)
{
    void *ctx = zctx_new ();
    void *client = zsocket_new (ctx, ZMQ_DEALER);
    zsocket_connect (client, "tcp://localhost:5555");

    char *string;
    zframe_t *frame;
    int more;

    char *query_string = "{\"entries\": 5 , \"time1\" : 123 , \"time2\" : 789}";
    zframe_t *query = zframe_new(query_string, strlen(query_string)+1);
    zframe_send (&query, client, 0);

    zmsg_t *response = zmsg_new ();
    response = zmsg_recv(client);
    printf("=== RECEIVED ===\n");

    do{
        frame = zmsg_pop (response);
        more = zframe_more (frame);
        string = (char *) zframe_data (frame);
        printf("FRAME: %s\n", string);
        zframe_destroy (&frame);
    }while(more);

    zmsg_destroy (&response);

    return 0;
}
