#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <stdarg.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>

#include <sys/types.h>
#include "stream.h"

#undef DEBUG_STREAM
#define CXXERR_NETWORK_ERROR 500
#ifndef max
#define max(x,y) ((x)>(y)?(x):(y))
#endif

void SYSDIE(const char *s)
{
	 printf("%s error!\n", s);
	 exit(1);
}

void *cxxcalloc( size_t nmemb, size_t size) {
    void *region = calloc( nmemb, size);
    if (!region) SYSDIE("calloc");
    return region;
}

void *cxxmalloc( size_t size) {
    void *region = malloc( size);
    if (!region) SYSDIE("malloc");
    return region;
}

void *cxxrealloc( void *ptr, size_t size) {

    void *region = realloc(ptr, size);
    if (!region) SYSDIE("realloc");
    return region;
}

void cxxfree( void *ptr) {
    free( ptr);
}

kStringBuffer* kStringBuffer_create( kStringBuffer* _sb) {
    kStringBuffer *sb;

    if (_sb) {
	    sb = _sb;
    } else {
	    sb = cxxmalloc( sizeof( kStringBuffer));
    }
    memset( sb, 0, sizeof( kStringBuffer));

    return sb;
}

void kStringBuffer_finit( kStringBuffer* sb) {
    if (sb) {
	    if ( sb->buf)
	      cxxfree( sb->buf);
	    sb->buf = NULL;
	    sb->bufsize = 0;
	    sb->cpos = 0;
    }
}

int kStringBuffer_grow( kStringBuffer *sb, int len) {
    sb->bufsize += len;
    sb->buf = cxxrealloc( sb->buf, sb->bufsize);
    if(!sb->buf) SYSDIE("memory null");
    return 0;
}

kStream* kStream_create( kStream *str, int sd) {
    if (!str) str = cxxcalloc( 1, sizeof(kStream));
    str->fd = sd;
    kStringBuffer_create( &str->ibuf);
    kStringBuffer_create( &str->obuf);
    return str;
}

void kStream_finit( kStream *str) {
    kStringBuffer_finit( &str->ibuf);
    kStringBuffer_finit( &str->obuf);
}

int sbcat( kStringBuffer *sb, char *str, int len) {
    if(len < 0) SYSDIE("sbcat(): len < 0");
    if (sb->cpos + len >= sb->bufsize) {
	kStringBuffer_grow( sb, max( len+1, 1024));
	if (!sb->buf) return -1;
    }
    memcpy(sb->buf + sb->cpos, str, len);
    sb->cpos += len;
    sb->buf[sb->cpos] = 0;
    return 0;
}

int sbtail( kStringBuffer *sb, int start_char) {
    if(!(start_char >= 0 && start_char <= sb->cpos)) SYSDIE("sbtail() error");
    if (start_char == 0) return 0;
    if (start_char < sb->cpos) {
	    memmove( sb->buf, sb->buf + start_char, sb->cpos - start_char);
    }
    sb->cpos -= start_char;
    sb->buf[sb->cpos] = 0;
    return 0;
}

void sbclear( kStringBuffer* sb) {
    kStringBuffer_finit( sb);
}

#ifdef DEBUG_STREAM
static void putch( int c) {
    if (c < 32 || c >= 127) printf("?");
    else putchar(c);
}

static void hexdump( char *buf, int len) {
    int addr, i,j;
    for (addr = 0; addr < len; addr+=20) {
	printf("%08x: ", addr);
	for (i = 0; i < 20; i+= 4) {
	    for (j = 0; j < 4; j++) {
		if (addr+i+j >= len) {
		    printf("..");
		} else {
		    printf("%02x", (unsigned char)buf[addr+i+j]);
		}
	    }
	    printf(" ");
	}
	printf(" |");
	for (i = 0; i < 20; i++) {
	    if (addr+i > len) {
		putch('.');
	    } else {
		putch((unsigned char)buf[addr+i+j]);
	    }
        } 
	printf("|\n");
    }
}
#endif

int kStream_read( kStream *str, char *buf, int max) 
{
    /* unbuffered stream reader */
    int nread;

    nread = recv( str->fd, buf, max, 0);

    if (nread > 0) {
	     str->error_count=0;
	     str->read_count += nread;
    }  else {
	     if (nread == 0) {
	       /* bug in linux implementation of recv()? */
	       errno = EAGAIN;
	       nread = -1;
	     } 
	
	     if (nread < 0) {
	       if (errno == EAGAIN) {
		        /* cap the number of EAGAINs on any socket 
		         * select shouldn't pick us when there is no data */
		        str->error_count++;
            /*	    	printf("Read 0 (count=%d)\n", str->error_count); */
		        if (str->error_count > 10) {
		           errno = CXXERR_NETWORK_ERROR;
		        }
	       } 
	       str->error = errno;
	       if (errno != EAGAIN) {
		         //fprintf(stderr, "%d: Read error\n", str->fd);
	       }
	     }
    }

#ifdef DEBUG_STREAM
    if (nread>0) {
	     printf("%d: read> (%d bytes)\n", str->fd, nread);
	     hexdump( buf, nread);
    }
#endif
    return nread;
}


int kStream_write( kStream *str, char *buf, int size) {
    /* unbuffered stream writer */
    int nwrite;

#ifdef DEBUG_STREAM
    printf("%d: write> (%d bytes)", str->fd, size);
    hexdump( buf, size);
    printf("'\n");
#endif

    nwrite = write( str->fd, buf, size);

    if (nwrite < 0) {
	str->error = errno;
	if (errno != EAGAIN) {
	    printf("%d: Write error\n", str->fd);
	}
    } else {
	str->write_count += nwrite;
#if 0
	printf("%d: written %d bytes\n", str->fd, str->write_count);
#endif
	if (nwrite < size) {
	    str->error = EAGAIN;
	}
    }
    return nwrite;
}

int kStream_in_addr( kStream *str) {
    return str->read_count - str->ibuf.cpos;
}

int kStream_out_addr( kStream *str) {
    return str->write_count + str->obuf.cpos;
}

int kStream_fpeek( kStream *str, char *buf, int size) {
    char tbuf[1024];
    int total = 0;
    int nread;
    int len;
    int err;

    /* Buffer ahead to the next newline */
    len = kStream_iqlen( str);
    while (len<size) {
        /* loop until all pending data has been read, or 'size' bytes are available  */
	nread = kStream_read( str, tbuf, sizeof( tbuf));
#if 0
	printf("stream: fpeek got %d bytes for %d total\n", nread, nread+len); 
#endif
	if (nread < 0) {
	    /* break if no more data available */
	    return nread;
	}
	total += nread;
	err = sbcat( &str->ibuf, tbuf, nread);
	if (err) return -1;
	len = kStream_iqlen( str);
    }

    /* Got enough data, now copy to the buffer */
    memcpy( buf, str->ibuf.buf, size);
    return size;
}

int kStream_fread( kStream *str, char *buf, int size) {
    int read = kStream_fpeek( str, buf, size);
    if (read>0) {
	sbtail( &str->ibuf, size);
    }
    return read;
}

int kStream_clear( kStream* str) {
    /* return 0 on success */
    sbclear( &str->obuf);
    return 0;
}

int kStream_flush( kStream* str) {
    /* return number of bytes still queued, or -1 on error */
    int nwrite;
    int res;
    nwrite = kStream_write( str, str->obuf.buf, str->obuf.cpos);
    if (nwrite > 0) {
	sbtail( &str->obuf, nwrite);
    } 
    res = str->obuf.cpos;
    if (nwrite <= 0 && str->error != EAGAIN) res = -1;
    return res;
}

int kStream_fwrite( kStream *str, char *buf, int len) {
    /* return number of bytes still queued, or -1 on error */
    if (sbcat( &str->obuf, buf, len)) {
        str->error = ENOMEM;
        return -1;
    }
    return kStream_flush( str);
}

int kStream_iqlen( kStream *str) {
    return str->ibuf.cpos;
}

int kStream_oqlen( kStream *str) {
    return str->obuf.cpos;
}

