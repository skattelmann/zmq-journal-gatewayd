
/** general options, fit them to your needs  **/

#define FRONTEND_SOCKET "tcp://*:5555"          // used by the clients
#define BACKEND_SOCKET "ipc://backend"          // used by the query handlers
#define HANDLER_HEARTBEAT_INTERVAL 5*1000       // millisecs, defines the time interval in which the gateway will expect a heartbeat
#define WAIT_TIMEOUT 100000                     // microsecs, how long to wait in 'follow mode' when there is no new log; 
                                                // must be at most HANDLER_HEARTBEAT_INTERVAL since the gateway is not able 
                                                // to answer heartbeats in time when not.


/** definitions for internal communication between gateway and client **/

#define READY "\001"
#define END "\002"
#define HEARTBEAT "\003"
#define ERROR "\004"
#define TIMEOUT "\005"
#define STOP "\006"

#define QUERY_STRING ""                         // default query string, every communication begins with sending a query string
#define HEARTBEATING 0                          // set to '1' if 'follow' is true 
#define CLIENT_SOCKET "tcp://localhost:5555"    // the socket the client should connect to
#define HEARTBEAT_INTERVAL 1000                 // msecs, this states after which time you send a heartbeat
#define SERVER_HEARTBEAT_INTERVAL 5000          // msecs, this states how much time you give the server to answer a heartbeat


/** DEBUGGING, defines the time the gateway is waiting after sending one log **/
#define SLEEP 0 // 1500000L //  500000000L

