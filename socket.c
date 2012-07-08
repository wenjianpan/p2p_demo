#include <sys/types.h>
#include <sys/socket.h>
#include <stdio.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <netdb.h>
#include <string.h>
#include <sys/time.h>
#include <assert.h>
#include "list.h"
#include "stream.h"

typedef  int nodeState;
typedef  int boolean; 
#define  NODESTATE_NOTHING  0
#define  NODESTATE_AVAILABLE 1
#define  NODESTATE_CHOKE 2
#define  NODESTATE_WAITING  3

//typedef unsigned long u_int32_t;
struct _bufferMap {
	u_int32_t ourSessionID;
	u_int32_t segMask0;
	u_int32_t segMask1;
	u_int32_t segMask2;
	u_int32_t segMask3;
}; 

struct _bufferMap ourBufferMap;

/* 
 * bufferMap format
 *                         |<- one bit ->|   
 * +-----------------------+----------+----------+----------+-----+------------+
 * + four bytes session ID + segment0 + segment1 + segment2 + ... + segment127 +
 * +-----------------------+----------+----------+----------+-----+------------+
 * 4+128/8 = 20 bytes
*/
 
struct _peer {
	struct    list_head list;
	char      ipaddress[32];
	nodeState state;
	struct    timeval lastUpdateTime;
	kStream   ios;
};

boolean needUpdateInfoTable = 0;
int Clients = 0;
int ClientsAvailable = 0;
int Providers = 0;
int ProvidersAvailable = 0;

LIST_HEAD(ProvidersList);
LIST_HEAD(ClientsList);

#define BIGBUFFERSIZE 1024*1024
char BigBuffer0[BIGBUFFERSIZE], BigBuffer1[BIGBUFFERSIZE];
char *currBigBuf = NULL;

#define NOBLOCK 1
#define MAXIN 8
#define MAXOUT 7
#define MAXCLIENTS MAXOUT-2

#define MSG_HANDSHAKE            0x00
#define MSG_CHOKE                0x01
#define MSG_WANTNODEINFO         0x02
#define MSG_WANTEDNODEINFO       0x03
#define MSG_KEEPALIVE            0x04
#define MSG_IAMLEAVING           0x05
#define MSG_WANTSEG              0x06
#define MSG_WANTEDSEG            0x07
#define MSG_IHAVESEG             0x08
//#define MSG_SESSIONISDEAD        0x09

#define SOCKLEN_T int

#define DEBUG

int send_message( struct _peer *peer, char *buf, int len); 
void sendMsg(struct _peer *peer, char msgType);

void Error(const char *s)
{
	 printf("%s\n", s);
	 exit(1);
}

u_int32_t get4Bytes(unsigned char *ptr) { // as above, but doesn't advance ptr
    return (ptr[0]<<24)|(ptr[1]<<16)|(ptr[2]<<8)|ptr[3];
}

void convU32tChar(u_int32_t data, char *ptr)
{
    ptr[0] = (char)((data>>24)&0xff);
    ptr[1] = (char)((data>>16)&0xff);
    ptr[2] = (char)((data>>8)&0xff);
    ptr[3] = (char)(data&0xff);

}

void setBit(struct _bufferMap *p, int bitIndex)
{
	 //printf("set bit %d\n", bitIndex);

	 if(bitIndex > 127 || bitIndex < 0)
	    Error("bitIndex error! setBit error!");

	 if(bitIndex > 95) {// 96-127 
	    bitIndex-=96; 
	    p->segMask3 = p->segMask3 | (0x1<<bitIndex);
	 }
	 else if(bitIndex >=32 && bitIndex < 64) {
	 	  bitIndex-=32; // 32-63
	    p->segMask1 = p->segMask1 | (0x1<<bitIndex);
	 }
	 else if(bitIndex >=64 && bitIndex < 96) {
	 	  bitIndex-=64; // 64-95
	    p->segMask2 = p->segMask2 | (0x1<<bitIndex);
	 }
	 // else 0-31
	 else p->segMask0 = p->segMask0 | (0x1<<bitIndex);
}

u_int32_t testBit(struct _bufferMap *p, int bitIndex)
{
	 u_int32_t res=0;
	 
	 //printf("test bit %d\n", bitIndex);
	 if(bitIndex > 127 || bitIndex < 0)
	 	  Error("bitIndex error! testBit error!");

	 if(bitIndex > 95) {// 96-127 
	    bitIndex-=96; 
	    res = p->segMask3 & (0x1<<bitIndex);
	 }
	 else if(bitIndex >=32 && bitIndex < 64) {
	 	  bitIndex-=32; // 32-63
	    res = p->segMask1 & (0x1<<bitIndex);
	 }
	 else if(bitIndex >=64 && bitIndex < 96) {
	 	  bitIndex-=64; // 64-95
	    res = p->segMask2 & (0x1<<bitIndex);
	 }
	 // else 0-31
	 else res = p->segMask0 & (0x1<<bitIndex);

   return res;
}

void clearAllbits(struct _bufferMap *p)
{
	 p->segMask0 = 0;
	 p->segMask1 = 0;
	 p->segMask2 = 0;
	 p->segMask3 = 0;
}

void changeBuffer()
{
#ifdef DEBUG
         printf("change buffer\n");
#endif
	 clearAllbits(&ourBufferMap);
	 if(currBigBuf == BigBuffer0) currBigBuf = BigBuffer1;
	 else if(currBigBuf == BigBuffer1) currBigBuf = BigBuffer0;
}

#include <sys/timeb.h>
int getTimeOfDay(struct timeval *tp) 
{
  struct timeb tb;
  ftime(&tb);
  tp->tv_sec = tb.time;
  tp->tv_usec = 1000*tb.millitm;
  return 0;
}

void updateNodeTime(struct _peer *pNode)
{
	getTimeOfDay(&pNode->lastUpdateTime);
}

struct _peer* mallocNode()
{
   struct _peer *tmpNode = NULL;
   tmpNode = (struct _peer*)malloc(sizeof(struct _peer));	
   if(tmpNode == NULL) Error("malloc error"); 		
	 
	 tmpNode->state = NODESTATE_NOTHING;
	 kStream_create(&tmpNode->ios, -1);
	 getTimeOfDay(&tmpNode->lastUpdateTime);
   return tmpNode;
}

void freeNode(struct _peer *Node)
{
   if(Node) {
     if(Node->ios.fd > 0) {close(Node->ios.fd); Node->ios.fd=-1;}
     kStream_finit(&Node->ios);
     free((void *)Node); 
     Node = NULL;
   } else {
     Error("free NULL memory");	
   }
}

int setupSocket(int port, int makeNonBlocking) {

  int newSocket = socket(AF_INET, SOCK_STREAM, 0);
  if (newSocket < 0) {
    printf("unable to create stream socket: ");
    return newSocket;
  }

  if (port != 0) {
    struct sockaddr_in name;
    name.sin_family = AF_INET;
    name.sin_port = htons(port);
    name.sin_addr.s_addr = htonl(INADDR_ANY);
    if (bind(newSocket, (struct sockaddr*)&name, sizeof name) != 0) {
      char tmpBuffer[100];
      sprintf(tmpBuffer, "bind() error (port number: %d): ",
	      ntohs(port));
      printf(tmpBuffer);
      close(newSocket);    
      return -1;
    }
  }

  if (makeNonBlocking) {
    // Make the socket non-blocking:
    int curFlags = fcntl(newSocket, F_GETFL, 0);
    if (fcntl(newSocket, F_SETFL, curFlags|O_NONBLOCK) < 0) {
      printf("failed to make non-blocking: "); 
      close(newSocket); 
      return -1;
    }
  }

  return newSocket;
}

#define LISTEN_BACKLOG_SIZE 20

int setUpOurListenSocket(int ourPort) {
  int ourSocket = -1;

  do {
    ourSocket = setupSocket(ourPort, NOBLOCK);
    if (ourSocket < 0) break;

    // Allow multiple simultaneous connections:
    if (listen(ourSocket, LISTEN_BACKLOG_SIZE) < 0) {
      printf("listen() failed: ");
      break;
    }

    return ourSocket;
  } while (0);  

  if (ourSocket != -1) close(ourSocket);
  return -1;
}


int socketBlockUntilReadable(int socket, struct timeval* timeout) 
{
	// if timeout->tv_sec==0 && if timeout->tv_usec==0, it's polling 
  int result = -1;
  do {
    fd_set rd_set;
    FD_ZERO(&rd_set);
    if (socket < 0) break;
    FD_SET((unsigned) socket, &rd_set);
    const unsigned numFds = socket+1;
    
    result = select(numFds, &rd_set, NULL, NULL, timeout);
    if (timeout != NULL && result == 0) {
      break; // this is OK - timeout occurred
    } else if (result <= 0) {
      Error( "select() error: ");
      break;
    }
    
    if (!FD_ISSET(socket, &rd_set)) {
      Error("select() error - !FD_ISSET");
      break;
    }
  } while (0);

  return result;
}

void addPeerNode(struct list_head *list, struct _peer *Node)
{
	list_add(&Node->list, list);
}

#define TABLEMAXITEMS 10
#define BUFSIZE 10*1024
char InfoTable[1000]; /* this table stores the ip addresses of nodes to be connected to */
/*
  format(char/string): (Tab)(ipaddress0)(Tab)(ipaddress1)(Tab)(ipaddress2)...(0)
  if(InfoTable[0] = '\0') InfoTable is empty;
*/

void initInfoTable(char *ipaddr)
{
	/* this routine is called when the main() starts */
	/* copy the server ip adress (eg. argv[0]) to InfoTable */
  InfoTable[0] = '\t';
  InfoTable[1] = 0;
  printf("Server address: %s\n", ipaddr);
  strcat(InfoTable, ipaddr);
  //printf("InfoTable : %s\n", InfoTable);
}

char* selectOneIPAddrInInfoTable(char *buf)
{
		int i=0, tIndex;
	  
	  if(buf[0] == 0) return NULL;
	  	
	  while(buf[i]) {
		  if(buf[i] == '\t') tIndex = i;
		  i++;
	  }		
	  buf[tIndex] = 0;		
    //mmzero(&buf[tIndex], 17);
    return &buf[tIndex+1];
}

struct _peer* selectOneNodefromInfoTable()
{
	
	struct _peer *tmpNode;
	char *s;

  /* 
     if table is empty, needUpdateInfoTable=1, return NULL, else
     select the first ip adress from the table, delete it
     and return it 
  */
  //printf("%d\n", InfoTable[0]);
  if((s=selectOneIPAddrInInfoTable(InfoTable)) == NULL) {
  	
  	needUpdateInfoTable = 1;	
	  return NULL;
	}
	
	tmpNode = mallocNode();	  
	strcpy(tmpNode->ipaddress, s); 

#ifdef DEBUG
      printf("Selected ip adress %s\n", tmpNode->ipaddress);
#endif

  //if the table now is empty, needUpdateInfoTable=1;
	if(InfoTable[0] == 0) {
		needUpdateInfoTable = 1;
  }
 
	return tmpNode;
}

#define DEBUGNODEINFO
void sendNodeInfoTable(struct _peer *peer) /* to client */
{
	char buf[1024];
	struct list_head *curr;
	int items=0;
	
	memset(buf, 0, sizeof(buf));	
	buf[0] = MSG_WANTEDNODEINFO;

  list_for_each(curr, &ClientsList){
       struct _peer *PNode = list_entry(curr, struct _peer, list);
       if(items > TABLEMAXITEMS) break; 
       if(PNode->ios.fd != peer->ios.fd && PNode->state ==  NODESTATE_AVAILABLE) { 
       	 /* if equal, it's the client itself */
       	 /* else add PNode->ipaddress to buf */
       	 strcat(buf, "\t");
       	 strcat(buf, PNode->ipaddress);
         items++;
       }
  }

  /* send the buf to socket */
  if(items) { 
#ifdef DEBUGNODEINFO
    printf("send nodeInfo buf %s, items %d\n", buf, items);
#endif  	
  	send_message(peer, buf, items*17);
  }
}

void updateNodeInfoTable(char *buf)
{
	 char *currIPInBuf;
	 struct list_head *curr;
	 
	 memset(InfoTable, 0, sizeof(InfoTable));
   
	 while((currIPInBuf = selectOneIPAddrInInfoTable(buf))!= NULL ) {
 	  
	    /* if a IP address is already in the ProvidersList, ignore it */
	    list_for_each(curr, &ProvidersList){
        struct _peer *PNode = list_entry(curr, struct _peer, list);        
        if(strcmp(PNode->ipaddress, currIPInBuf)==0) {
            goto cont;
        }
      }
      
      /* add the ip address to the InfoTable */
      strcat(InfoTable, "\t");
      strcat(InfoTable, currIPInBuf);
#ifdef DEBUGNODEINFO        	
      printf("add:\n",currIPInBuf);
      printf("INfo table: %s\n", InfoTable);
#endif
      cont:
      	continue;
   }
   
	 needUpdateInfoTable = 0;
}

/*---------------------------------------------------------------------*/

struct _peer* getNewClient(int ourListenSocket) 
{
	 struct _peer *newClientNode;
	 int newClientSocket = -1;
	 struct timeval notime;
	 struct sockaddr_in clientAddr;
   SOCKLEN_T clientAddrLen = sizeof(clientAddr);
   
	 notime.tv_sec = 0;
	 notime.tv_usec = 0;
	 
	 /* listen to requestor, if request incomming */
	 
	 if(Clients<MAXOUT)
	   if((socketBlockUntilReadable(ourListenSocket, &notime)) > 0) {
	     /* accept it */
	     newClientSocket=accept(ourListenSocket,(struct sockaddr *)&clientAddr,&clientAddrLen);
	     if (newClientSocket < 0) {
         Error("accept() failed");;
         return NULL;
       }
       
       newClientNode = mallocNode();
       newClientNode->ios.fd = newClientSocket;

       long flags = fcntl( newClientSocket, F_GETFL);
       if (flags < 0) {
         printf("failed to make non-blocking: "); 
         close(newClientSocket); 
         return NULL;
       }
       flags |= O_NONBLOCK;
       if ( fcntl( newClientSocket, F_SETFL, flags)) {
         printf("failed to make non-blocking: "); 
         close(newClientSocket); 
         return NULL;
       }
            
       strcpy(newClientNode->ipaddress, inet_ntoa(clientAddr.sin_addr));
       //printf("%s\n",newClientNode->ipaddress);
       addPeerNode(&ClientsList, newClientNode);
	     Clients++;
#ifdef DEBUG
       printf("Clients now is %d\n", Clients);    
#endif 

       if(ClientsAvailable < MAXCLIENTS) {
	       /* build the connection: add the node(peer) to the ClientsList, ClientsAvailable++;
	        send handshake */       
	        ClientsAvailable++;
	        newClientNode->state = NODESTATE_AVAILABLE;
#ifdef DEBUG
          printf("new node added to client list, My ClientsAvailable %d \n", ClientsAvailable);
#endif
	        sendMsg(newClientNode, MSG_HANDSHAKE);
	     } else {
#ifdef DEBUG
          printf("I'm choke now\n");
#endif	     	
       	 /* I'm choke now, send choke, freeNode(newClientNode); */
       	  sendMsg(newClientNode, MSG_CHOKE);     	  
          newClientNode->state = NODESTATE_WAITING;
       }
	 }
   return newClientNode;
}

int connectToNode(struct _peer *providerNode)
{
	  int socket_fd;
	  struct sockaddr_in servaddr;
	  
	  socket_fd = socket(PF_INET,SOCK_STREAM,IPPROTO_TCP);
    bzero(&servaddr,sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = inet_addr(providerNode->ipaddress);
    servaddr.sin_port = htons(5150);
    
    //printf("%s\n",providerNode->ipaddress);
  
    if(connect(socket_fd,(struct sockaddr *)&servaddr, sizeof(servaddr)) == -1)
    {
      printf("Error in connecting...\n");
      close(socket_fd);
      return -1;
    }
    return socket_fd;
}

struct _peer* getNewProvider()
{
	 struct _peer *newProviderNode;
	 int newProviderSocket = -1;
	 
   if(Providers<MAXIN) {
      
       /*connect to one of the node from the tmp_list, tmp_list is the partners list of \
              the other node, see get_new_requestor and main procedure;*/
	    //newProviderNode = mallocNode();     
	    newProviderNode = selectOneNodefromInfoTable();
	    if(!newProviderNode) return NULL;
      newProviderSocket = connectToNode(newProviderNode);
      if(newProviderSocket < 0) 
         return NULL;

      newProviderNode->ios.fd = newProviderSocket;
      
      // change the socket NONBLOCK
      long flags = fcntl( newProviderSocket, F_GETFL);
      if (flags < 0) {
        printf("failed to make non-blocking: "); 
        close(newProviderSocket); 
        return NULL;
      }
      flags |= O_NONBLOCK;
      if ( fcntl( newProviderSocket, F_SETFL, flags)) {
        printf("failed to make non-blocking: "); 
        close(newProviderSocket); 
        return NULL;
      }

      Providers++;  
      /* build the connection: add the node(peer) to the ProvidersList */      
#ifdef DEBUG
      printf("new node added to Provider list \n");
      printf("Providers now is %d\n",Providers);
#endif 
	  	addPeerNode(&ProvidersList, newProviderNode);       
   }
   return newProviderNode;
}


void getNewPartners(int ourListenSocket) 
{
   getNewClient(ourListenSocket); /* media out */
   getNewProvider(); /* media in */
}

void announceToClients(int segID) // tell the clients that I have this segment
{
	 struct list_head *curr;
	 struct _peer *PNode;
	 char buf[6];
	 
	 buf[0] = MSG_IHAVESEG;
   convU32tChar(ourBufferMap.ourSessionID, &buf[1]);
   buf[5] = (char)segID;	 
	 // MSG_IHAVESEG message format
	 // +--------------+------------+-------+
	 // + MSG_IHAVESEG +  sessionID + segID +
	 // +    1 byte    +   4 bytes  + 1byte +
	 // +--------------+------------+-------+	
  
  list_for_each(curr, &ClientsList){ 
       PNode = list_entry(curr, struct _peer, list);
       if(PNode->state == NODESTATE_AVAILABLE)
          send_message(PNode, buf, sizeof(buf));         
  }
}

#define DEBUGTESTFILE
#ifdef DEBUGTESTFILE 
int savedFile;
#endif

void dealWithIfBufferFull()
{
	 // test if buffer is full
	 if(ourBufferMap.segMask0 == 0xffffffff && ourBufferMap.segMask1 == 0xffffffff && \
	 	     ourBufferMap.segMask2 == 0xffffffff && ourBufferMap.segMask3 == 0xffffffff ) {
	 	 printf("current buffer is full\n");
	   // write currBigBuf to player, for temp test, write to stdout 
#ifdef DEBUGTESTFILE 
     write(savedFile, currBigBuf, BIGBUFFERSIZE); 
#endif
	   
	   // changeBuffer();
	 }
}

void sendWantUpdateInfoTable()
{
	  struct list_head *curr;
	  struct _peer *PNode;
	  
    list_for_each(curr, &ProvidersList){ /* first look for a choke one */
       PNode = list_entry(curr, struct _peer, list);
       if(PNode->state == NODESTATE_CHOKE) goto findChokeNode;
    }
    list_for_each(curr, &ProvidersList){ /* if there is none, look for the first available one */
       PNode = list_entry(curr, struct _peer, list);
       if(PNode->state == NODESTATE_AVAILABLE) break;
    }
    
    findChokeNode:
       sendMsg(PNode, MSG_WANTNODEINFO);
    return; 
}

//////////////////////////////////////////////////////////////////////////////////////////
#define MAXMESSAGE 10*1024

/* 
 * returns 
 *  0 - no error
 * -1 - permanent error sending msg size 
 * -2 - permanent error sending message
 */
int send_message( struct _peer *peer, char *buf, int len) 
{
    int nslen = htonl(len);

    if (kStream_fwrite(&peer->ios, (void*)&nslen, sizeof(nslen)) < 0) {
	    printf("send_message error\n");
	    return -1;
    }
    if (len > 0) {
	    if (kStream_fwrite( &peer->ios, buf, len) < 0) {
	      printf("send_message error");
	      return -2;
	    }
    }

    return 0;
}

void sendMsg(struct _peer *peer, char msgType)
{
	 char ch = msgType;
	 send_message(peer, &ch, sizeof(char));
}


#define T_PROVIDER 0
#define T_CLIENT 1
/*
 * Return 1 if there are more messages waiting
 * Return 0 on success 
 * return -1 on error 
 * return -2 on unknown message.
 */
int handlePeerIncommingMsg(struct _peer *peer, int peerType) 
{
    int32_t nbo_len;
    int len;
    char msg[84];
    char *nmsg;
    int res = 0;
    int err;
    int segID;
    u_int32_t sessionID;

    err = kStream_fpeek( &peer->ios, (char *)&nbo_len, sizeof(nbo_len));
    if (err < 0) { 
	    //printf("recv error\n");
    	return -1;
    }
    assert(err == sizeof(nbo_len));
    len = ntohl(nbo_len);
    if (len <= 80) {
	     nmsg = msg;
    } else if (len > 0 && len <= MAXMESSAGE) {
	     nmsg = cxxmalloc(len+sizeof(int32_t));
    } else {	/* Too big a packet, kill the peer. */
	     printf("recv: too big packet\n");
	     return -1;
    }

    if(len > MAXMESSAGE || len < 0) Error("recv message error!");

#ifdef DEBUG
    //printf("recv message %d bytes\n", len);
#endif

    err = kStream_fread( &peer->ios, nmsg, len + sizeof(int32_t));
    if (err < 0) goto cleanup;
    if( err != len + (int)sizeof(int32_t)) Error("recv message error!");

    /* got message */
	  updateNodeTime(peer);
	  /* and find out the type of the msg */
	  if(peerType == T_PROVIDER) {
		switch(nmsg[4]) {
	  	case MSG_HANDSHAKE:
	  	   /* the MSG_HANDSHAKE msg is sent only by providers, the node is available */
	  	   ProvidersAvailable++;
#ifdef DEBUG
         printf("HANDSHAKE MSG from client socket: %d\n", peer->ios.fd);
         printf("My providers available %d \n", ProvidersAvailable);
#endif
	  	   peer->state = NODESTATE_AVAILABLE;        
	  	   break;
	  	case MSG_CHOKE:
	  	   /* the MSG_CHOKE msg is sent only by providers */	  	
	  	   /* the Providers is choke now, mask it */
#ifdef DEBUG
         printf("CHOKE MSG from client socket: %d\n", peer->ios.fd);
#endif	  	   
	  	   peer->state = NODESTATE_CHOKE;  /* still in the list for further use */
	  	   break;
	  	case MSG_WANTEDNODEINFO: /* Provider send me NODEINFOTABLE */
	  		 //printf("MSG_WANTEDNODEINFO\nbuf:%s\n", buf);
#ifdef DEBUGNODEINFO
         printf("from %d, recv nodeInfo buf %s\n", peer->ios.fd, nmsg);
#endif
	  		 updateNodeInfoTable(&nmsg[5]);
	  		 if(peer->state == NODESTATE_CHOKE) {
	  		   list_del(&peer->list); /* this choke node isn't needed anymore */
	  		   freeNode(peer);
	  		   Providers--;
	  		 }
	  		 break;
	  	case MSG_KEEPALIVE:
#ifdef DEBUGNODEINFO
         printf("from %d, recv MSG_KEEPALIVE\n", peer->ios.fd);
#endif	  		 
	  		 break;
	  	case MSG_IAMLEAVING:
	  		 if(peer->state == NODESTATE_AVAILABLE) ProvidersAvailable--;
	  		 list_del(&peer->list); 
	  		 freeNode(peer);
	  		 Providers--;	  		
	  		 break;
	  	case MSG_IHAVESEG:
	    case MSG_WANTEDSEG:

	    	 // case MSG_IHAVESEG
	  		 // if the sessionID is greater than ours, update the sessionID and change buffer 
	  		 // else if the seg is what I want, to fecth it(send MSG_WANTSEG)
	  		 // else ignore
	  		 // MSG_IHAVESEG message format
	  		 // +--------------+------------+-------+
	  		 // + MSG_IHAVESEG +  sessionID + segID +
	  		 // +    1 byte    +   4 bytes  + 1byte +
	  		 // +--------------+------------+-------+
	  		 // ^
	  		 // |
	  		 // &nmsg[4]
	  	   // case MSG_WANTEDSEG
	  		 // 1. recv it, if sessionID is less than ours, ignore, break; 
	  		 // 2. else if received sessionID is greater than ours, update the sessionID and 
	  		 //    change buffer, else if it's the same sessionID
	  		 // 3. write it to the right buffer, and announce that we have this seg  
	  		 
	  		 // 4. if the buffer is full, write it to player, then chage the buffer
	  		 // MSG_WANTEDSEG message format
	  		 // +---------------+-----------+-------+--------------+
	  		 // + MSG_WANTEDSEG + sessionID + segID + the seg data +
	  		 // +     1 byte    + 4 bytes   + 1byte +  8192 bytes  +
	  		 // +---------------+-----------+-------+--------------+
	  		 // ^
	  		 // |
	  		 // &nmsg[4]
	  		 
	  	   sessionID = get4Bytes((unsigned char*)&nmsg[5]);	
	  	   segID = (int)nmsg[9];
	  	   if(sessionID > ourBufferMap.ourSessionID ) {
	  	     	ourBufferMap.ourSessionID = sessionID;
	  	     	changeBuffer();	  	     	
	  	   }
	  	   else if(ourBufferMap.ourSessionID == sessionID)	{
#ifdef DEBUG
           printf("provider say have seg: %d-%d\n",ourBufferMap.ourSessionID, segID);
#endif
	  	     if(testBit(&ourBufferMap, segID)) {
	  	     	  // we already have this segment 
	  	     	  break; 
	  	  	 }
	  	   }
	  	   
	  	   if(nmsg[4] == MSG_IHAVESEG) { // ask the provider for this segment
#ifdef DEBUGNODEINFO
           printf("from %d, recv MSG_IHAVESEG\n", peer->ios.fd);
#endif
	 	       char tmpbuf[6];	 
	         tmpbuf[0] = MSG_WANTSEG;
           convU32tChar(ourBufferMap.ourSessionID, &tmpbuf[1]);
           tmpbuf[5] = (char)segID;
           send_message(peer, tmpbuf, sizeof(tmpbuf));
	  	   }
	  	   else { // buf[0] == MSG_WANTEDSEG
	  	   	 //save the seg data to the right buffer, started at &buf[6], 8192 bytes;
#ifdef DEBUG
           printf("get segment: %d-%d\n",ourBufferMap.ourSessionID, segID);
           printf("recv message %d bytes\n", len);
#endif	  	   	 
           memcpy(&currBigBuf[segID*8*1024], &nmsg[10], 8192);
	  	   	 setBit(&ourBufferMap, segID);
	  	   	 announceToClients(segID);
	  	   	 dealWithIfBufferFull();	   	 
	  	   }
	  	   
	  	   break;
	  	    
	  	default: 
	  	   break;
  	}
    } else if(peerType == T_CLIENT) {
    	switch(nmsg[4]) {
	  	case MSG_WANTNODEINFO:       /* client wants NODEINFOTABLE */
	  		sendNodeInfoTable(peer);
	  		break;
	  	case MSG_KEEPALIVE:
	  		break;
	  	case MSG_IAMLEAVING:
	  		if(peer->state == NODESTATE_AVAILABLE) ClientsAvailable--;
	  		list_del(&peer->list); 
	  		freeNode(peer);
	  		Clients--;
	  		break;
	  	case MSG_WANTSEG:
	  		// MSG_WANTSEG message format
	  		// +-------------+-----------+-------+
	  		// + MSG_WANTSEG + sessionID + segID +
	  		// +   1 byte    +  4 bytes -+ 1byte +
	  		// +-------------+-----------+-------+
	  		// ^
	  		// |
	  		// &nmsg[4] 
	  	  // if sessionID == ours, send the wanted seg (make sure we have that seg)
	  	  sessionID = get4Bytes((unsigned char*)&nmsg[1]);
	  	  segID = (int)nmsg[9];	
	  	  if(ourBufferMap.ourSessionID == sessionID)	{
	  	  	// make sure we have the seg, then send the data
	  	  	if(testBit(&ourBufferMap, segID)) {
	  	  	   //send MSG_WANTEDSEG and seg data;
	  	  	   char tmpbuf[8*1024+6];
	           tmpbuf[0] = MSG_WANTEDSEG;
             convU32tChar(ourBufferMap.ourSessionID, &tmpbuf[1]);
             tmpbuf[5] = (char)segID;
             memcpy(&tmpbuf[6], &currBigBuf[segID*8*1024], 8192);
             send_message(peer, tmpbuf, sizeof(tmpbuf));
	  	  	}
	  	  }	
	  	  break;
	  	default:
	  		break;
	    }
    }

cleanup:
    /* cleanup */
    if (err < 0) {
	   /* if there has been an error, report it */
	    res = -1;
    } 

    if (res == 0) {
	    /* check if there is another message waiting */
	    err = kStream_fpeek( &peer->ios, (char *)&nbo_len, sizeof(nbo_len));
	    if (err == sizeof(nbo_len)) {
	      int tlen;
	      tlen = ntohl(nbo_len) + sizeof(nbo_len);

            if (tlen < 0 || tlen > MAXMESSAGE) {
		           /* out of sync with peer, or packet too large */
		           printf("recv_peermsg error, too large packet\n");
		           return -1;
	          }
	      if(tlen > MAXMESSAGE || tlen < 0) Error("Clean up error"); 
	      if (kStream_iqlen( &peer->ios) >= tlen) res = 1;
	    }
    }

    if (len > 80) {
	     cxxfree(nmsg);
    }
    
    return res;
}

/////////////////////////////////////////////////////////////////////////////////////////


void dumpList(struct list_head *pList, const char* listName)
{
	  struct list_head *curr;
	  struct _peer *PNode;
	  
	  printf("Dump list %s\n", listName);
    list_for_each(curr, pList){ /* if there is none, look for the first available one */
       PNode = list_entry(curr, struct _peer, list);
       printf("%s\n",PNode->ipaddress);
    }
}

void keepAliveandTestNodeAbortion(struct timeval *pLastTimeTick)
{
  struct list_head *curr;
	struct _peer *PNode;

	struct timeval timeTick;
	getTimeOfDay(&timeTick);
	
	if(timeTick.tv_sec - pLastTimeTick->tv_sec >= 2) { // a period of 2s  
    list_for_each(curr, &ClientsList){ 
       PNode = list_entry(curr, struct _peer, list);
       if(timeTick.tv_sec - PNode->lastUpdateTime.tv_sec >= 5) {
       	 // if one node has no reponse in 5s, it's aborted? delete it
	  		if(PNode->state == NODESTATE_AVAILABLE) ClientsAvailable--;
	  		list_del(&PNode->list); 
	  		freeNode(PNode);
	  		Clients--;
       	continue;
       }
       sendMsg(PNode, MSG_KEEPALIVE);
    }
    list_for_each(curr, &ProvidersList){ 
       PNode = list_entry(curr, struct _peer, list);
       if(timeTick.tv_sec - PNode->lastUpdateTime.tv_sec >= 5) {
       	 // delete the node
	  		if(PNode->state == NODESTATE_AVAILABLE) ProvidersAvailable--;
	  		list_del(&PNode->list); 
	  		freeNode(PNode);
	  		Providers--;
       	 continue;
       }
       sendMsg(PNode, MSG_KEEPALIVE);
    }
    pLastTimeTick->tv_sec = timeTick.tv_sec;
  }
}

//for signal, for test
#include <signal.h>
static void
sig_int(int signo)
{
	struct list_head *curr;
	struct _peer *PNode;
	  
  list_for_each(curr, &ClientsList){ 
       PNode = list_entry(curr, struct _peer, list);
       sendMsg(PNode, MSG_IAMLEAVING);
  }
  list_for_each(curr, &ProvidersList){ 
       PNode = list_entry(curr, struct _peer, list);
       sendMsg(PNode, MSG_IAMLEAVING);
  }
  
  dumpList(&ProvidersList,"ProvidersList");
  dumpList(&ClientsList,"ClientsList"); 
#ifdef DEBUGTESTFILE 
  close(savedFile); 
#endif 	
	printf("sig_int finished\n");
	exit(0);
}

int main(int argc, char *argv[])
{
	int ourListenSocket;
	struct timeval timeTick;
	
	if(argc!=2) {
		 printf("Usage: %s serverIpAddress\n", argv[0]);
		 return 1;
	}
	
#ifdef DEBUGTESTFILE 
  if( (savedFile =open("t.mp3", O_WRONLY|O_CREAT)) <= 0) Error("test:open file error"); 
#endif

		//for sigaction, for signal, for test
	struct sigaction	act, oact;
	act.sa_handler = sig_int;
	sigemptyset(&act.sa_mask);
	act.sa_flags = 0;
	if(sigaction(SIGINT, &act, &oact)<0)
		return(-1);
		
	initInfoTable(argv[1]); // argv[1] is the server IP address
	memset((char*)&ourBufferMap, 0, sizeof(struct _bufferMap));
	currBigBuf = BigBuffer0;
	
	ourListenSocket = setUpOurListenSocket(5150);
	if(ourListenSocket < 0)
		Error("set up listen socket failed");
		
	getTimeOfDay(&timeTick);
	
	while(1) {
    if((Clients+Providers) < (MAXIN+MAXOUT))
       getNewPartners(ourListenSocket);
       
    /* handle each socket for incomming Msg */
    struct list_head *curr;
    list_for_each(curr, &ProvidersList){
       struct _peer *PNode = list_entry(curr, struct _peer, list);
       handlePeerIncommingMsg(PNode, T_PROVIDER);
    }
    list_for_each(curr, &ClientsList){
       struct _peer *CNode = list_entry(curr, struct _peer, list);
       handlePeerIncommingMsg(CNode, T_CLIENT);
    }
    
    /* if I need new node info from one random node in the provider list */
    
    if(needUpdateInfoTable) {
    	 sendWantUpdateInfoTable();
    }
    
    keepAliveandTestNodeAbortion(&timeTick);   
  }
  return 0;
}


