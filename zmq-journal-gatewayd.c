#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <time.h>
#include "czmq.h"
#include "zmq.h"
#include "jansson.h"

#define FRONTEND_SOCKET "tcp://*:5555"
#define BACKEND_SOCKET "ipc://backend"
#define HANDLER_HEARTBEAT_INTERVAL 5*1000 // msecs
#define READY "\001"
#define END "\002"
#define HEARTBEAT "\003"

typedef struct RequestMeta {
    zframe_t *client_ID;
    char* client_ID_string;
    const char *format;
    int since_timestamp;
    int until_timestamp;
    char *since_cursor;
    char *until_cursor;
    bool follow;
    bool discrete;
    bool boot;
    int boot_ID;
    bool machine;
    bool unique_entries;
    char *field;
    void **field_matches;
}RequestMeta;

typedef struct Connection {
    zframe_t *client_ID;
    zframe_t *handler_ID;
}Connection;

char *get_arg_string(json_t *json_args, char *key){
    json_t *json_string = json_object_get(json_args, key);
    if( json_string != NULL ){
        const char *string = json_string_value(json_string);
        char *string_cpy = (char *) malloc( sizeof(char) * (strlen(string)+1) );
        strcpy(string_cpy, string);
        json_decref(json_string);
        return string_cpy;
    }
    else{
        json_decref(json_string);
        return NULL;
    }
}

void **get_arg_array(json_t *json_args, char *key){
    json_t *json_array = json_object_get(json_args, key);
    if( json_array != NULL ){
        size_t size = json_array_size(json_array);
        size_t index;
        json_t *value;

        void **array = (void **) malloc( sizeof(char *) * size );
        json_array_foreach(json_array, index, value) {
            const char *json_array_value = json_string_value(value);
            array[index] = (char *) malloc( sizeof(char) * (strlen(json_array_value)+1) );
            strcpy(array[index], json_array_value);
        }

        json_decref(value);
        json_decref(json_array);
        return array;
    }
    else{
        json_decref(json_array);
        return NULL;
    }
}

int get_arg_bool(json_t *json_args, char *key){
    json_t *json_boolean = json_object_get(json_args, key);
    if( json_boolean != NULL ){
        int boolean;
        json_unpack(json_boolean, "b", &boolean);
        json_decref(json_boolean);
        return (boolean == 1) ? true : false;
    }
    else{
        json_decref(json_boolean);
        return false;
    }
}

get_arg_int(json_t *json_args, char *key){
    json_t *json_int = json_object_get(json_args, key);
    if( json_int != NULL ){
        int integer = json_number_value(json_int);
        json_decref(json_int);
        return integer;
    }
    else{
        json_decref(json_int);
        /* this will not be used */
        return -1;
    }
}

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

    /* fill args with arguments from json input */
    RequestMeta *args = malloc( sizeof(RequestMeta) );
    args->client_ID = client_ID;
    args->client_ID_string = zframe_strhex (client_ID);
    args->format = get_arg_string(json_args, "format");
    args->since_timestamp = get_arg_int(json_args, "since_timestamp");
    args->until_timestamp = get_arg_int(json_args, "until_timestamp");
    args->boot_ID = get_arg_int(json_args, "boot_ID");
    args->since_cursor = get_arg_string(json_args, "since_cursor");
    args->until_cursor = get_arg_string(json_args, "until_cursor");
    args->follow = get_arg_bool(json_args, "follow");
    args->discrete = get_arg_bool(json_args, "discrete");
    args->boot = get_arg_bool(json_args, "boot");
    args->machine = get_arg_bool(json_args, "machine");
    args->unique_entries = get_arg_bool(json_args, "unique_entries");
    args->field = get_arg_string(json_args, "field");
    args->field_matches = get_arg_array(json_args, "field_matches"); 

    json_decref(json_args);
    return args;
}

zmsg_t *build_msg_from_flag(zframe_t *ID, const char *flag){
    zmsg_t *msg = zmsg_new();
    zframe_t *ID_dup = zframe_dup (ID);
    zframe_t *flag_frame = zframe_new ( flag, strlen(flag) + 1 );
    zmsg_push (msg, flag_frame);
    zmsg_push (msg, ID_dup);
    return msg;
}

zmsg_t *build_msg_from_frame(zframe_t *ID, zframe_t *flag_frame){
    zmsg_t *msg = zmsg_new();
    zframe_t *ID_dup = zframe_dup (ID);
    zmsg_push (msg, flag_frame);
    zmsg_push (msg, ID_dup);
    return msg;
}

static void *handler_routine (void *_args) {

    RequestMeta *args = (RequestMeta *) _args;
    zctx_t *ctx = zctx_new ();
    void *query_handler = zsocket_new (ctx, ZMQ_DEALER);
    int rc = zsocket_connect (query_handler, BACKEND_SOCKET);

    /* send READY to the client */
    zmsg_t *ready = build_msg_from_flag(args->client_ID, READY);
    zmsg_send (&ready, query_handler);
    printf("<< READY SENT TO %s >>\n", args->client_ID_string);

    zmq_pollitem_t items [] = {
        { query_handler, 0, ZMQ_POLLIN, 0 },
    };

    /* just for debugging */
    struct timespec tim, tim2;
    tim.tv_sec  = 0;
    tim.tv_nsec = 200000000L;

    uint64_t heartbeat_at = zclock_time () + HANDLER_HEARTBEAT_INTERVAL;
    while (true) {

        rc = zmq_poll (items, 1, 0);
        assert(rc != -1);

        /* receive heartbeat and send it back */
        if (items[0].revents & ZMQ_POLLIN){
            char *heartbeat_string = zstr_recv (query_handler);
            if( strcmp(heartbeat_string, HEARTBEAT) == 0 ){
                printf("<< HEARTBEAT RECEIVED >>\n");
                zmsg_t *heartbeat = build_msg_from_flag(args->client_ID, HEARTBEAT);
                zmsg_send (&heartbeat, query_handler);
                printf("<< HEARTBEAT ANSWERED >>\n");
                heartbeat_at = zclock_time () + HANDLER_HEARTBEAT_INTERVAL;
            }
            free (heartbeat_string);
        }

        /* timeout from client */
        if (zclock_time () >= heartbeat_at) {
            zmsg_t *end = build_msg_from_flag(args->client_ID, END);
            zmsg_send (&end, query_handler);
            printf("<< CLIENT TIMEOUT >>\n");
            break;
        }

        /* DO SOME WORK HERE */
        nanosleep(&tim , &tim2);
        printf(">>>> TICK ...\n");

    }

    zmq_close (query_handler);
    return NULL;

}

int main (void){

    void *ctx = zctx_new ();

    // Socket to talk to clients
    void *frontend = zsocket_new (ctx, ZMQ_ROUTER);
    assert(frontend);
    int rc = zsocket_bind (frontend, FRONTEND_SOCKET);

    // Socket to talk to the query handlers
    void *backend = zsocket_new (ctx, ZMQ_ROUTER);
    assert(backend);
    rc = zsocket_bind (backend, BACKEND_SOCKET);

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
                zmsg_t *heartbeat = build_msg_from_frame(lookup->handler_ID, heartbeat_frame);
                zmsg_send (&heartbeat, backend);
                zframe_destroy (&client_ID);
            }
            free(client_ID_string);
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

