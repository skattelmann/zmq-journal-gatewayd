#include <stdio.h>
#include <string.h>
#include <alloca.h>
#include <assert.h>
#include <time.h>
#include <systemd/sd-journal.h>
#include <inttypes.h>
#include <signal.h>

#include "czmq.h"
#include "zmq.h"
#include "jansson.h"

#define FRONTEND_SOCKET "tcp://*:5555"
#define BACKEND_SOCKET "ipc://backend"
#define HANDLER_HEARTBEAT_INTERVAL 5*1000 // msecs
#define WAIT_TIMEOUT 100000

#define READY "\001"
#define END "\002"
#define HEARTBEAT "\003"
#define ERROR "\004"
#define TIMEOUT "\005"
#define STOP "\006"

/* DEBUGGING */
#define SLEEP 0 // 500000000L

static bool active = true;
void stop_gateway(int dummy) {
    printf("\n<< stopping gateway ... >>\n");
    /* stop the gateway */
    active = false;
}

typedef struct RequestMeta {
    zframe_t *client_ID;
    char* client_ID_string;
    const char *format;
    uint64_t since_timestamp;
    uint64_t until_timestamp;
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
    int num_field_matches;
    bool reverse; 
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
        return string_cpy;
    }
    else{
        return NULL;
    }
}

void set_matches(json_t *json_args, char *key, RequestMeta *args){
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

        args->num_field_matches = size;
        args->field_matches = array;
        return;
    }
    else{
        args->num_field_matches = 0;
        args->field_matches = NULL;
        return;
    }
}

bool get_arg_bool(json_t *json_args, char *key){
    json_t *json_boolean = json_object_get(json_args, key);
    if( json_boolean != NULL ){
        int boolean;
        json_unpack(json_boolean, "b", &boolean);
        return (boolean == 1) ? true : false;
    }
    else{
        return false;
    }
}

int get_arg_int(json_t *json_args, char *key){
    json_t *json_int = json_object_get(json_args, key);
    if( json_int != NULL ){
        int integer = json_number_value(json_int);
        return integer;
    }
    else{
        /* this will not be used */
        return -1;
    }
}

uint64_t get_arg_date(json_t *json_args, char *key){
    /* follows the human readable form "2012-04-23T18:25:43.511Z" */
    json_t *json_date = json_object_get(json_args, key);
    if( json_date != NULL ){
        const char *string = json_string_value(json_date);
        char string_cpy[strlen(string)+1];
        strcpy(string_cpy, string);

        /* decode the json date to unix epoch time, milliseconds are not considered */
        struct tm tm;
        time_t t;
        char *ptr = strtok(string_cpy, "T.");
        strptime(ptr, "%Y-%m-%d", &tm);
        ptr = strtok(NULL, "T.");
        strptime(ptr, "%H:%M:%S", &tm);
        tm.tm_isdst = -1;

        t = mktime(&tm) * 1000000;          // this time needs to be adjusted by 1000000 to fit the journal time

        return (uint64_t) t;
    }
    else{
        return -1;
    }
}

/* destructor for RequestMeta */
void RequestMeta_destruct ( RequestMeta *args ){
    free(args->client_ID_string);
    if (args->format != NULL) free( (void *) args->format);
    if (args->since_cursor != NULL) free(args->since_cursor);
    if (args->until_cursor != NULL) free(args->until_cursor);
    if (args->field != NULL ) free(args->field);
    
    free(args);
}

void Connection_destruct (void *_connection){
    Connection *connection = (Connection *) _connection;
    zframe_destroy( &(connection->handler_ID) );
    zframe_destroy ( &(connection->client_ID) );
    free( connection ) ;
}

RequestMeta *parse_json(zmsg_t* query_msg){
    zframe_t *client_ID = zmsg_pop (query_msg);
    zframe_t *query_frame = zmsg_pop (query_msg);

    char *query_string = zframe_strdup (query_frame);
    zframe_destroy (&query_frame);
    json_error_t error;
    json_t *json_args = json_loads(query_string, 0, &error);
    free(query_string);

    /* invalid query */
    if (json_args == NULL)
        return NULL;

    /* fill args with arguments from json input */
    RequestMeta *args = malloc( sizeof(RequestMeta) );
    args->client_ID = client_ID;
    args->client_ID_string = zframe_strhex (client_ID);
    args->format = get_arg_string(json_args, "format");
    args->since_timestamp = get_arg_date(json_args, "since_timestamp");
    args->until_timestamp = get_arg_date(json_args, "until_timestamp");
    args->boot_ID = get_arg_int(json_args, "boot_ID");
    args->since_cursor = get_arg_string(json_args, "since_cursor");
    args->until_cursor = get_arg_string(json_args, "until_cursor");
    args->follow = get_arg_bool(json_args, "follow");
    args->discrete = get_arg_bool(json_args, "discrete");
    args->boot = get_arg_bool(json_args, "boot");
    args->machine = get_arg_bool(json_args, "machine");
    args->unique_entries = get_arg_bool(json_args, "unique_entries");
    args->field = get_arg_string(json_args, "field");
    set_matches(json_args, "field_matches", args); 
    args->reverse = get_arg_bool(json_args, "reverse");

    /* there are some dependencies between certain attributes, these can be set here */
    if ( args->until_cursor != NULL || (int) args->until_timestamp != -1 )
        args->follow = false;
    if ( args->follow == true )
        args->reverse = true;

    json_decref(json_args);
    return args;
}

/* some small helpers */
zmsg_t *build_msg_from_frame(zframe_t *ID, zframe_t *flag_frame){
    zmsg_t *msg = zmsg_new();
    zframe_t *ID_dup = zframe_dup (ID);
    zframe_t *flag_dup = zframe_dup (flag_frame);
    zmsg_push (msg, flag_dup);
    zmsg_push (msg, ID_dup);
    return msg;
}
zmsg_t *build_entry_msg(zframe_t *ID, char *entry_string){
    zmsg_t *msg = zmsg_new();
    zframe_t *ID_dup = zframe_dup (ID);
    zmsg_pushstr (msg, entry_string);
    zmsg_push (msg, ID_dup);
    return msg;
}
void send_flag( zframe_t *ID, void *socket, zctx_t *ctx, char *flag){
    zmsg_t *msg = zmsg_new();
    zframe_t *ID_dup = zframe_dup (ID);
    zframe_t *flag_frame = zframe_new ( flag, strlen(flag) + 1 );
    zmsg_push (msg, flag_frame);
    zmsg_push (msg, ID_dup);

    /* ID will not be destroyed! */
    zmsg_send (&msg, socket);

    /* context with all sockets will be destroyed if given */
    if( ctx != NULL )
        zctx_destroy (&ctx);
}

void adjust_journal(RequestMeta *args, sd_journal *j){
    /* initial position will be seeked, don't forget to 'next'  or 'previous' the journal pointer */
    if ( args->reverse == false && args->until_cursor != NULL)
        sd_journal_seek_cursor( j, args->until_cursor );
    else if ( args->reverse == true && args->since_cursor != NULL)
        sd_journal_seek_cursor( j, args->since_cursor );
    else if( args->reverse == false && (int) args->until_timestamp != -1)
        sd_journal_seek_realtime_usec( j, args->until_timestamp );
    else if ( args->reverse == true && (int) args->since_timestamp != -1)
        sd_journal_seek_realtime_usec( j, args->since_timestamp );
    else if (args->reverse == false)
        sd_journal_seek_tail( j );
    else if (args->reverse == true)
        sd_journal_seek_head( j );

    /* field conditions */
    int i;
    for(i=0;i<args->num_field_matches;i++)
        sd_journal_add_match( j, args->field_matches[i], 0);
}

int check_args(RequestMeta *args, char *cursor, uint64_t realtime_usec, uint64_t monotonic_usec){
    if( ( args->reverse == false && args->since_cursor != NULL && strcmp(args->since_cursor, cursor) == 0 )
        || ( args->reverse == true && args->until_cursor != NULL && strcmp(args->until_cursor, cursor) == 0 )
        || ( args->reverse == false && (int) args->since_timestamp != -1 && args->since_timestamp > realtime_usec )
        || ( args->reverse == true && (int) args->until_timestamp != -1 && args->until_timestamp < realtime_usec ) )
        return 1;
    else
        return 0;
}

char *get_entry_string( sd_journal *j, RequestMeta *args){
    const void *data;
    size_t length;
    size_t total_length = 0;
    int counter = 0, i;
    
    /* first get the number of fields to allocate memory */
    SD_JOURNAL_FOREACH_DATA(j, data, length)
        counter++;
    char *entry_fields[counter+1+3];        // +3 for meta information, prefixed by '__'

    /* then insert the meta information, memory allocation first */
    char *cursor;
    uint64_t realtime_usec;
    char realtime_usec_string[65];          // 64 bit +1 for \0
    uint64_t monotonic_usec;
    char monotonic_usec_string[65];         // 64 bit +1 for \0
    sd_id128_t boot_id;
    
    sd_journal_get_cursor( j, &cursor );    // needs to be free'd afterwards
    sd_journal_get_realtime_usec( j, &realtime_usec );
    sprintf ( realtime_usec_string, "%" PRId64 , realtime_usec );
    sd_journal_get_monotonic_usec( j, &monotonic_usec, &boot_id);
    sprintf ( monotonic_usec_string, "%" PRId64 , monotonic_usec );
    
    /* check against args if this entry should be sent */
    if (check_args(args, cursor, realtime_usec, monotonic_usec) == 1){
        free(cursor);
        return NULL;
    }

    /* until now the prefixes for the meta information are missing */
    char *meta_information[] = { cursor, realtime_usec_string, monotonic_usec_string };
    const char *meta_prefixes[] = {"__CURSOR=", "__REALTIME_TIMESTAMP=" , "__MONOTONIC_TIMESTAMP=" };
    for(i=0; i<3; i++){
        entry_fields[i] = (char *) alloca( sizeof(char) * ( strlen(meta_prefixes[i]) + strlen(meta_information[i]) + 1));     
        ((char *)(entry_fields[i]))[0] = '\0';      // initial setup for strcat 
        strcat ( entry_fields[i], meta_prefixes[i] );
        strcat ( entry_fields[i], meta_information[i] );
        total_length += strlen(entry_fields[i])+1;  // +1 for \0
    }
    free(cursor);

    /* then get all fields */
    counter = 3;
    SD_JOURNAL_FOREACH_DATA(j, data, length){
        entry_fields[counter] = (char *) alloca( sizeof(char) * (length+1) );   // +1 for \0
        strncpy(entry_fields[counter], data, length);
        ((char *)(entry_fields[counter]))[length] = '\0';                       // set the \0 
        total_length += length+1;                                               // +1 for \0
        counter++;
    }

    /* the data fields are merged together according to the given output format */
    if( args->format == NULL || strcmp( args->format, "export" ) == 0 ){  
        char *entry_string = (char *) malloc( sizeof(char) * ( total_length + counter ));   // counter+1 for additional \n
        entry_string[0] = '\0';         // initial setup for strcat
        for(i=0; i<counter; i++){
            strcat ( entry_string, entry_fields[i] );
            strcat ( entry_string, "\n" );
        }
        return entry_string;
    }
    if( strcmp( args->format, "json" ) == 0 ){  
        char *entry_string = (char *) malloc( sizeof(char) * ( total_length + counter + 10*counter + 4 ));   // + counter for \n, +4 for {  }
        char *entry_string_token;
        entry_string[0] = '\0';         // initial setup for strcat
        strcat ( entry_string, "{\n" );
        for(i=0; i<counter; i++){
            /* every field string is splitted into two parts for the json format */
            strcat ( entry_string, "\t\"" );
            entry_string_token = strchr ( entry_fields[i], '=' );
            *entry_string_token = '\0';
            strcat ( entry_string, entry_fields[i] );
            strcat ( entry_string, "\" : \"" );
            strcat ( entry_string, entry_string_token+1 );
            strcat ( entry_string, "\"" );
            if (i != counter-1)
                strcat ( entry_string, ",\n" );
            else
                strcat ( entry_string, "\n" );
        }
        strcat ( entry_string, "}\n" );
        return entry_string;
    }
    else
        return ERROR;
}

static void *handler_routine (void *_args) {
    RequestMeta *args = (RequestMeta *) _args;
    zctx_t *ctx = zctx_new ();
    void *query_handler = zsocket_new (ctx, ZMQ_DEALER);
    int rc = zsocket_connect (query_handler, BACKEND_SOCKET);

    /* send READY to the client */
    printf("<< query accepted >>\n");
    send_flag( args->client_ID, query_handler, NULL, READY );

    zmq_pollitem_t items [] = {
        { query_handler, 0, ZMQ_POLLIN, 0 },
    };

    /* DEBUGGING */
    struct timespec tim1, tim2;
    tim1.tv_sec  = 0;
    tim1.tv_nsec = SLEEP;

    /* create and adjust the journal pointer according to the information in args */
    sd_journal *j;
    sd_journal_open(&j, SD_JOURNAL_LOCAL_ONLY);
    adjust_journal(args, j);

    uint64_t heartbeat_at = zclock_time () + HANDLER_HEARTBEAT_INTERVAL;
    while (true) {
        
        rc = zmq_poll (items, 1, 0);
        if( rc == -1 ){
            send_flag(args->client_ID, query_handler, ctx, ERROR);
            sd_journal_close( j );
            RequestMeta_destruct(args);
            return NULL;
        }

        if (items[0].revents & ZMQ_POLLIN){
            char *client_msg = zstr_recv (query_handler);
            if( strcmp(client_msg, HEARTBEAT) == 0 ){
                /* client sent heartbeat, only necessary when 'follow' is active */
                send_flag(args->client_ID, query_handler, NULL, HEARTBEAT);
                heartbeat_at = zclock_time () + HANDLER_HEARTBEAT_INTERVAL;
            }
            else if( strcmp(client_msg, STOP) == 0 ){
                /* client wants no more logs */
                send_flag(args->client_ID, query_handler, ctx, STOP);
                printf("<< confirmed STOP >>\n");
                sd_journal_close( j );
                RequestMeta_destruct(args);
                free (client_msg);
                return NULL;
            }
            free (client_msg);
        }

        /* timeout from client, only true when 'follow' is active and client does no heartbeating */
        if (zclock_time () >= heartbeat_at && args->follow) {
            send_flag(args->client_ID, query_handler, ctx, TIMEOUT);
            printf("<< CLIENT TIMEOUT >>\n");
            sd_journal_close( j );
            RequestMeta_destruct(args);
            return NULL;
        }

        /* move forwards or backwards? default is backwards */
        if( args->reverse == false )
            rc = sd_journal_previous(j);
        else
            rc = sd_journal_next(j);

        /* send new entry if possible */
        if( rc == 1 ){
            char *entry_string = get_entry_string( j, args ); 
            if (entry_string == NULL){
                send_flag(args->client_ID, query_handler, ctx, END);
                sd_journal_close( j );
                RequestMeta_destruct(args);
                return NULL;
            }
            else if ( strcmp(entry_string, ERROR) == 0 ){
                send_flag(args->client_ID, query_handler, ctx, ERROR);
                sd_journal_close( j );
                RequestMeta_destruct(args);
                return NULL;
            }
            /* no problems with the new entry, send it */
            else{
                printf("%s\n\n", entry_string);
                zmsg_t *entry_msg = build_entry_msg(args->client_ID, entry_string);
                free (entry_string);
                zmsg_send (&entry_msg, query_handler);
            }
        }
        /* end of journal and 'follow' active? => wait some time */
        else if ( rc == 0 && args->follow ){
            sd_journal_wait( j, (uint64_t) WAIT_TIMEOUT );
        }
        /* in case moving the journal pointer around produced an error */
        else if ( rc < 0 ){
            send_flag(args->client_ID, query_handler, ctx, ERROR);
            sd_journal_close( j );
            RequestMeta_destruct(args);
            return NULL;
        }
        /* query finished, send END and close the thread */
        else{
            send_flag(args->client_ID, query_handler, ctx, END);
            sd_journal_close( j );
            RequestMeta_destruct(args);
            return NULL;
        }
        nanosleep(&tim1 , &tim2);
    }
}

int main (void){

    zctx_t *ctx = zctx_new ();

    // Socket to talk to clients
    void *frontend = zsocket_new (ctx, ZMQ_ROUTER);
    assert(frontend);
    int rc = zsocket_bind (frontend, FRONTEND_SOCKET);

    // Socket to talk to the query handlers
    void *backend = zsocket_new (ctx, ZMQ_ROUTER);
    assert(backend);
    rc = zsocket_bind (backend, BACKEND_SOCKET);

    /* for stopping the gateway via keystroke (ctrl-c) */
    signal(SIGINT, stop_gateway);

    // Setup the poller for frontend and backend
    zmq_pollitem_t items[] = {
        {frontend, 0, ZMQ_POLLIN, 0},
        {backend, 0, ZMQ_POLLIN, 0},
    };

    zhash_t *connections = zhash_new ();
    Connection *lookup;
    zmsg_t *msg;
    RequestMeta *args; 
    while ( active ) {
        zmq_poll (items, 2, -1);

        if (items[0].revents & ZMQ_POLLIN) {
            msg = zmsg_recv (frontend);

            zframe_t *client_ID = zmsg_first (msg);

            char *client_ID_string = zframe_strhex (client_ID);
            lookup = (Connection *) zhash_lookup(connections, client_ID_string);

            /* first case: new query */
            if( lookup == NULL ){
                args = parse_json(msg);
                /* if query is valid open query handler and pass args to it */
                if (args != NULL){
                    Connection *new_connection = (Connection *) malloc( sizeof(Connection) );
                    new_connection->client_ID = client_ID;
                    new_connection->handler_ID = NULL;
                    zhash_update (connections, args->client_ID_string, new_connection);
                    zhash_freefn ( connections, args->client_ID_string, Connection_destruct );
                    zthread_new (handler_routine, (void *) args);
                }
                /* if args was invalid answer with error */
                else{
                    printf("<< got invalid query >>\n");
                    send_flag( client_ID, frontend, NULL, ERROR );
                    zframe_destroy (&client_ID);
                }
            }
            /* second case: heartbeat or stop sent by client */
            else{
                zframe_t *client_msg_frame = zmsg_last (msg);
                zmsg_t *client_msg = build_msg_from_frame(lookup->handler_ID, client_msg_frame);
                zmsg_send (&client_msg, backend);
            }
            zmsg_destroy ( &msg );
            free(client_ID_string);
        }

        if (items[1].revents & ZMQ_POLLIN) {
            zmsg_t *response = zmsg_recv (backend);

            zframe_t *handler_ID = zmsg_pop (response);
            zframe_t *client_ID = zmsg_first (response);
            zframe_t *handler_response = zmsg_last (response);

            char *client_ID_string = zframe_strhex (client_ID);
            char *handler_response_string = zframe_strdup (handler_response);
            lookup = (Connection *) zhash_lookup(connections, client_ID_string);

            /* the handler ID is inserted in the connection struct when he answers the first time */
            if( lookup->handler_ID == NULL ){
                lookup->handler_ID = handler_ID;
            }
            else zframe_destroy (&handler_ID);

            /* case handler ENDs or STOPs the query, regulary or because of error (e.g. missing heartbeat) */
            if( strcmp( handler_response_string, END ) == 0 
                    || strcmp( handler_response_string, ERROR ) == 0 
                    || strcmp( handler_response_string, STOP ) == 0 
                    || strcmp( handler_response_string, TIMEOUT ) == 0){
                zhash_delete (connections, client_ID_string);
                printf("<< query closed >>\n");
            }

            free(handler_response_string);
            free(client_ID_string);
            zmsg_send (&response, frontend);
        }
    }

    zhash_destroy (&connections);
    zctx_destroy (&ctx); 
    printf("<< ... gateway stopped >>\n");
    return 0;

}
