// Some portions Copyright (c) 2004-2013 Sergey Lyubka
// Some portions Copyright (c) 2013 Cloudera Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

#ifdef __linux__
#define _XOPEN_SOURCE 600     // For flockfile() on Linux
#endif
#define _LARGEFILE_SOURCE     // Enable 64-bit file offsets
#define __STDC_FORMAT_MACROS  // <inttypes.h> wants this for C++
#define __STDC_LIMIT_MACROS   // C++ wants that for INT64_MAX

#ifdef __linux__
#include <sys/prctl.h>
#endif  // defined(__linux__)

#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <signal.h>
#include <fcntl.h>

#include <time.h>
#ifdef __MACH__
#include <mach/clock.h>
#include <mach/mach.h>
#endif

#include <stdlib.h>
#include <stdarg.h>
#include <assert.h>
#include <string.h>
#include <ctype.h>
#include <limits.h>
#include <stddef.h>
#include <stdio.h>

#include <sys/wait.h>
#include <sys/socket.h>
#include <sys/poll.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/time.h>
#include <stdint.h>
#include <inttypes.h>
#include <netdb.h>

#include <pwd.h>
#include <unistd.h>
#include <dirent.h>
#include <pthread.h>

#include <openssl/crypto.h>
#include <openssl/err.h>
#include <openssl/md5.h>
#include <openssl/sha.h>

#if defined(__MACH__)
#define SSL_LIB   "libssl.dylib"
#define CRYPTO_LIB  "libcrypto.dylib"
#else
#if !defined(SSL_LIB)
#define SSL_LIB   "libssl.so"
#endif
#if !defined(CRYPTO_LIB)
#define CRYPTO_LIB  "libcrypto.so"
#endif
#endif
#ifndef O_BINARY
#define O_BINARY  0
#endif // O_BINARY
#define closesocket(a) close(a)
#define sq_mkdir(x, y) mkdir(x, y)
#define sq_remove(x) remove(x)
#define sq_sleep(x) usleep((x) * 1000)
#define ERRNO errno
#define INVALID_SOCKET (-1)
#define INT64_FMT PRId64
typedef int SOCKET;

#include "squeasel.h"

#define SQUEASEL_VERSION "3.9"
#define PASSWORDS_FILE_NAME ".htpasswd"
#define CGI_ENVIRONMENT_SIZE 4096
#define MAX_CGI_ENVIR_VARS 64
#define SQ_BUF_LEN 8192
#define MAX_REQUEST_SIZE 16384
#define WORKER_THREAD_TIMEOUT_SECS 3
#define ARRAY_SIZE(array) (sizeof(array) / sizeof(array[0]))

#ifdef DEBUG_TRACE
#undef DEBUG_TRACE
#define DEBUG_TRACE(x)
#else
#if defined(DEBUG)
#define DEBUG_TRACE(x) do { \
  flockfile(stdout); \
  printf("*** %lu.%p.%s.%d: ", \
         (unsigned long) time(NULL), (void *) pthread_self(), \
         __func__, __LINE__); \
  printf x; \
  putchar('\n'); \
  fflush(stdout); \
  funlockfile(stdout); \
} while (0)
#else
#define DEBUG_TRACE(x)
#endif // DEBUG
#endif // DEBUG_TRACE

// Darwin prior to 7.0 and Win32 do not have socklen_t
#ifdef NO_SOCKLEN_T
typedef int socklen_t;
#endif // NO_SOCKLEN_T
#define _DARWIN_UNLIMITED_SELECT

#define IP_ADDR_STR_LEN 50  // IPv6 hex string is 46 chars

#if !defined(MSG_NOSIGNAL)
#define MSG_NOSIGNAL 0
#endif

#if !defined(SOMAXCONN)
#define SOMAXCONN 100
#endif

#if !defined(PATH_MAX)
#define PATH_MAX 4096
#endif

// Size of the accepted socket queue
#if !defined(MGSQLEN)
#define MGSQLEN 20
#endif

#define RETRY_ON_EINTR(ret, expr) do { \
  ret = expr; \
} while ((ret == -1) && (errno == EINTR));

static const char *http_500_error = "Internal Server Error";

#include <openssl/crypto.h>
#include <openssl/ssl.h>
#include <openssl/err.h>

// If these constants aren't defined, we still need them to compile and maintain backwards
// compatibility with pre-1.0.1 OpenSSL.
#ifndef SSL_OP_NO_TLSv1
#define SSL_OP_NO_TLSv1 0x04000000U
#endif
#ifndef SSL_OP_NO_TLSv1_1
#define SSL_OP_NO_TLSv1_1 0x10000000U
#endif
#ifndef SSL_OP_NO_TLSv1_2
#define SSL_OP_NO_TLSv1_2 0x08000000U
#endif

#define OPENSSL_MIN_VERSION_WITH_TLS_1_1 0x10001000L
#define OPENSSL_MIN_VERSION_WITH_TLS_1_3 0x10101000L

static const char *month_names[] = {
  "Jan", "Feb", "Mar", "Apr", "May", "Jun",
  "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
};

// Unified socket address. For IPv6 support, add IPv6 address structure
// in the union u.
union usa {
  struct sockaddr sa;
  struct sockaddr_in sin;
#if defined(USE_IPV6)
  struct sockaddr_in6 sin6;
#endif
};

// Describes a string (chunk of memory).
struct vec {
  const char *ptr;
  size_t len;
};

struct file {
  int is_directory;
  time_t modification_time;
  int64_t size;
  FILE *fp;
  const char *membuf;   // Non-NULL if file data is in memory
  // set to 1 if the content is gzipped
  // in which case we need a content-encoding: gzip header
  int gzipped;
};
#define STRUCT_FILE_INITIALIZER {0, 0, 0, NULL, NULL, 0}

// Describes listening socket, or socket which was accept()-ed by the master
// thread and queued for future handling by the worker thread.
struct socket {
  SOCKET sock;          // Listening socket
  union usa lsa;        // Local socket address
  union usa rsa;        // Remote socket address
  unsigned is_ssl:1;    // Is port SSL-ed
  unsigned ssl_redir:1; // Is port supposed to redirect everything to SSL port
};

char* SAFE_HTTP_METHODS[] = {
  "GET", "POST", "HEAD", "OPTIONS" };

// See https://www.owasp.org/index.php/Test_HTTP_Methods_(OTG-CONFIG-006) for details.
#ifdef ALLOW_UNSAFE_HTTP_METHODS
char* UNSAFE_HTTP_METHODS[] = { "DELETE" , "CONNECT", "PUT" };
#endif

// NOTE(lsm): this enum shoulds be in sync with the config_options below.
enum {
  CGI_EXTENSIONS, CGI_ENVIRONMENT, PUT_DELETE_PASSWORDS_FILE, CGI_INTERPRETER,
  PROTECT_URI, AUTHENTICATION_DOMAIN, SSI_EXTENSIONS, THROTTLE,
  ACCESS_LOG_FILE, ENABLE_DIRECTORY_LISTING, ERROR_LOG_FILE,
  GLOBAL_PASSWORDS_FILE, INDEX_FILES, ENABLE_KEEP_ALIVE, ACCESS_CONTROL_LIST,
  EXTRA_MIME_TYPES, LISTENING_PORTS, DOCUMENT_ROOT, SSL_CERTIFICATE, SSL_PRIVATE_KEY,
  SSL_PRIVATE_KEY_PASSWORD, SSL_GLOBAL_INIT, NUM_THREADS, RUN_AS_USER, REWRITE,
  HIDE_FILES, REQUEST_TIMEOUT, SSL_VERSION, SSL_CIPHERS, TLS_CIPHERSUITES, NUM_OPTIONS
};

static const char *config_options[] = {
  "cgi_pattern", "**.cgi$|**.pl$|**.php$",
  "cgi_environment", NULL,
  "put_delete_auth_file", NULL,
  "cgi_interpreter", NULL,
  "protect_uri", NULL,
  "authentication_domain", "mydomain.com",
  "ssi_pattern", "**.shtml$|**.shtm$",
  "throttle", NULL,
  "access_log_file", NULL,
  "enable_directory_listing", "yes",
  "error_log_file", NULL,
  "global_auth_file", NULL,
  "index_files",
    "index.html,index.htm,index.cgi,index.shtml,index.php,index.lp",
  "enable_keep_alive", "no",
  "access_control_list", NULL,
  "extra_mime_types", NULL,
  "listening_ports", "8080",
  "document_root",  NULL,
  "ssl_certificate", NULL,
  "ssl_private_key", NULL,
  "ssl_private_key_password", NULL,
  "ssl_global_init", "yes",
  "num_threads", "50",
  "run_as_user", NULL,
  "url_rewrite_patterns", NULL,
  "hide_files_patterns", NULL,
  "request_timeout_ms", "30000",
  "ssl_min_version", "tlsv1",
  "ssl_ciphers", NULL,
  "tls_ciphersuites", NULL,
  NULL
};

struct sq_context {
  volatile int stop_flag;         // Should we stop event loop
  int wakeup_fds[2];              // File descriptors used to wake up
                                  // master thread.

  SSL_CTX *ssl_ctx;               // SSL context
  char *config[NUM_OPTIONS];      // Squeasel configuration parameters
  struct sq_callbacks callbacks;  // User-defined callback function
  void *user_data;                // User-defined data

  struct socket *listening_sockets;
  int num_listening_sockets;

  int max_threads;           // Maximum number of threads to start.
  int num_free_threads;      // Number of worker threads currently not working
                             // on a request.
  volatile int num_threads;  // Number of threads
  pthread_mutex_t mutex;     // Protects (max|num|num_free)_threads
  pthread_cond_t  cond;      // Condvar for tracking workers terminations

  struct socket queue[MGSQLEN];   // Accepted sockets
  volatile int sq_head;      // Head of the socket queue
  volatile int sq_tail;      // Tail of the socket queue
  pthread_cond_t sq_full;    // Signaled when socket is produced
  pthread_cond_t sq_empty;   // Signaled when socket is consumed
};

struct sq_connection {
  struct sq_request_info request_info;
  struct sq_context *ctx;
  SSL *ssl;                   // SSL descriptor
  SSL_CTX *client_ssl_ctx;    // SSL context for client connections
  struct socket client;       // Connected client
  time_t birth_time;          // Time when request was received
  int64_t num_bytes_sent;     // Total bytes sent to client
  int64_t content_len;        // Content-Length header value
  int64_t consumed_content;   // How many bytes of content have been read
  char *buf;                  // Buffer for received data
  char *path_info;            // PATH_INFO part of the URL
  int must_close;             // 1 if connection must be closed
  int buf_size;               // Buffer size
  int request_len;            // Size of the request + headers in a buffer
  int data_len;               // Total size of data in a buffer
  int status_code;            // HTTP reply status code, e.g. 200
  int throttle;               // Throttling, bytes/sec. <= 0 means no throttle
  time_t last_throttle_time;  // Last time throttled data was sent
  int64_t last_throttle_bytes;// Bytes sent this second
};

// Directory entry
struct de {
  struct sq_connection *conn;
  char *file_name;
  struct file file;
};

const char **sq_get_valid_option_names(void) {
  return config_options;
}

static int is_file_in_memory(struct sq_connection *conn, const char *path,
                             struct file *filep) {
  size_t size = 0;
  if ((filep->membuf = conn->ctx->callbacks.open_file == NULL ? NULL :
       conn->ctx->callbacks.open_file(conn, path, &size)) != NULL) {
    // NOTE: override filep->size only on success. Otherwise, it might break
    // constructs like if (!sq_stat() || !sq_fopen()) ...
    filep->size = size;
  }
  return filep->membuf != NULL;
}

static int is_file_opened(const struct file *filep) {
  return filep->membuf != NULL || filep->fp != NULL;
}

static int sq_fopen(struct sq_connection *conn, const char *path,
                    const char *mode, struct file *filep) {
  if (!is_file_in_memory(conn, path, filep)) {
    filep->fp = fopen(path, mode);
  }

  return is_file_opened(filep);
}

static void sq_fclose(struct file *filep) {
  if (filep != NULL && filep->fp != NULL) {
    fclose(filep->fp);
  }
}

static int get_option_index(const char *name) {
  int i;

  for (i = 0; config_options[i * 2] != NULL; i++) {
    if (strcmp(config_options[i * 2], name) == 0) {
      return i;
    }
  }
  return -1;
}

const char *sq_get_option(const struct sq_context *ctx, const char *name) {
  int i;
  if ((i = get_option_index(name)) == -1) {
    return NULL;
  } else if (ctx->config[i] == NULL) {
    return "";
  } else {
    return ctx->config[i];
  }
}

static void sockaddr_to_string(char *buf, size_t len,
                                     const union usa *usa) {
  buf[0] = '\0';
#if defined(USE_IPV6)
  inet_ntop(usa->sa.sa_family, usa->sa.sa_family == AF_INET ?
            (void *) &usa->sin.sin_addr :
            (void *) &usa->sin6.sin6_addr, buf, len);
#else
  inet_ntop(usa->sa.sa_family, (void *) &usa->sin.sin_addr, buf, len);
#endif
}

static void cry(struct sq_connection *conn,
                PRINTF_FORMAT_STRING(const char *fmt), ...) PRINTF_ARGS(2, 3);

// Print error message to the opened error log stream.
static void cry(struct sq_connection *conn, const char *fmt, ...) {
  char buf[SQ_BUF_LEN], src_addr[IP_ADDR_STR_LEN];
  va_list ap;
  FILE *fp;
  time_t timestamp;

  va_start(ap, fmt);
  (void) vsnprintf(buf, sizeof(buf), fmt, ap);
  va_end(ap);

  // Do not lock when getting the callback value, here and below.
  // I suppose this is fine, since function cannot disappear in the
  // same way string option can.
  if (conn->ctx->callbacks.log_message == NULL ||
      conn->ctx->callbacks.log_message(conn, buf) == 0) {
    fp = conn->ctx == NULL || conn->ctx->config[ERROR_LOG_FILE] == NULL ? NULL :
      fopen(conn->ctx->config[ERROR_LOG_FILE], "a+");

    if (fp != NULL) {
      flockfile(fp);
      timestamp = time(NULL);

      sockaddr_to_string(src_addr, sizeof(src_addr), &conn->client.rsa);
      fprintf(fp, "[%010lu] [error] [client %s] ", (unsigned long) timestamp,
              src_addr);

      if (conn->request_info.request_method != NULL) {
        fprintf(fp, "%s %s: ", conn->request_info.request_method,
                conn->request_info.uri);
      }

      fprintf(fp, "%s", buf);
      fputc('\n', fp);
      funlockfile(fp);
      fclose(fp);
    }
  }
}

// Return fake connection structure. Used for logging, if connection
// is not applicable at the moment of logging.
static struct sq_connection *fc(const struct sq_context *ctx) {
  static struct sq_connection fake_connection;
  fake_connection.ctx = (struct sq_context*)ctx;
  return &fake_connection;
}

const char *sq_version(void) {
  return SQUEASEL_VERSION;
}

struct sq_request_info *sq_get_request_info(struct sq_connection *conn) {
  return &conn->request_info;
}

static void sq_strlcpy(register char *dst, register const char *src, size_t n) {
  for (; *src != '\0' && n > 1; n--) {
    *dst++ = *src++;
  }
  *dst = '\0';
}

static int lowercase(const char *s) {
  return tolower(* (const unsigned char *) s);
}

static int sq_strncasecmp(const char *s1, const char *s2, size_t len) {
  int diff = 0;

  if (len > 0)
    do {
      diff = lowercase(s1++) - lowercase(s2++);
    } while (diff == 0 && s1[-1] != '\0' && --len > 0);

  return diff;
}

static int sq_strcasecmp(const char *s1, const char *s2) {
  int diff;

  do {
    diff = lowercase(s1++) - lowercase(s2++);
  } while (diff == 0 && s1[-1] != '\0');

  return diff;
}

static char * sq_strndup(const char *ptr, size_t len) {
  char *p;

  if ((p = (char *) malloc(len + 1)) != NULL) {
    sq_strlcpy(p, ptr, len + 1);
  }

  return p;
}

static char * sq_strdup(const char *str) {
  return sq_strndup(str, strlen(str));
}

static const char *sq_strcasestr(const char *big_str, const char *small_str) {
  int i, big_len = strlen(big_str), small_len = strlen(small_str);

  for (i = 0; i <= big_len - small_len; i++) {
    if (sq_strncasecmp(big_str + i, small_str, small_len) == 0) {
      return big_str + i;
    }
  }

  return NULL;
}

// Like snprintf(), but never returns negative value, or a value
// that is larger than a supplied buffer.
// Thanks to Adam Zeldis to pointing snprintf()-caused vulnerability
// in his audit report.
static int sq_vsnprintf(struct sq_connection *conn, char *buf, size_t buflen,
                        const char *fmt, va_list ap) {
  int n;

  if (buflen == 0)
    return 0;

  n = vsnprintf(buf, buflen, fmt, ap);

  if (n < 0) {
    cry(conn, "vsnprintf error");
    n = 0;
  } else if (n >= (int) buflen) {
    cry(conn, "truncating vsnprintf buffer: [%.*s]",
        n > 200 ? 200 : n, buf);
    n = (int) buflen - 1;
  }
  buf[n] = '\0';

  return n;
}

static int sq_snprintf(struct sq_connection *conn, char *buf, size_t buflen,
                       PRINTF_FORMAT_STRING(const char *fmt), ...)
  PRINTF_ARGS(4, 5);

static int sq_snprintf(struct sq_connection *conn, char *buf, size_t buflen,
                       const char *fmt, ...) {
  va_list ap;
  int n;

  va_start(ap, fmt);
  n = sq_vsnprintf(conn, buf, buflen, fmt, ap);
  va_end(ap);

  return n;
}

// Skip the characters until one of the delimiters characters found.
// 0-terminate resulting word. Skip the delimiter and following whitespaces.
// Advance pointer to buffer to the next word. Return found 0-terminated word.
// Delimiters can be quoted with quotechar.
static char *skip_quoted(char **buf, const char *delimiters,
                         const char *whitespace, char quotechar) {
  char *p, *begin_word, *end_word, *end_whitespace;

  begin_word = *buf;
  end_word = begin_word + strcspn(begin_word, delimiters);

  // Check for quotechar
  if (end_word > begin_word) {
    p = end_word - 1;
    while (*p == quotechar) {
      // If there is anything beyond end_word, copy it
      if (*end_word == '\0') {
        *p = '\0';
        break;
      } else {
        size_t end_off = strcspn(end_word + 1, delimiters);
        memmove (p, end_word, end_off + 1);
        p += end_off; // p must correspond to end_word - 1
        end_word += end_off + 1;
      }
    }
    for (p++; p < end_word; p++) {
      *p = '\0';
    }
  }

  if (*end_word == '\0') {
    *buf = end_word;
  } else {
    end_whitespace = end_word + 1 + strspn(end_word + 1, whitespace);

    for (p = end_word; p < end_whitespace; p++) {
      *p = '\0';
    }

    *buf = end_whitespace;
  }

  return begin_word;
}

// Simplified version of skip_quoted without quote char
// and whitespace == delimiters
static char *skip(char **buf, const char *delimiters) {
  return skip_quoted(buf, delimiters, delimiters, 0);
}


// Return HTTP header value, or NULL if not found.
static const char *get_header(const struct sq_request_info *ri,
                              const char *name) {
  int i;

  for (i = 0; i < ri->num_headers; i++)
    if (!sq_strcasecmp(name, ri->http_headers[i].name))
      return ri->http_headers[i].value;

  return NULL;
}

const char *sq_get_header(const struct sq_connection *conn, const char *name) {
  return get_header(&conn->request_info, name);
}

// A helper function for traversing a comma separated list of values.
// It returns a list pointer shifted to the next value, or NULL if the end
// of the list found.
// Value is stored in val vector. If value has form "x=y", then eq_val
// vector is initialized to point to the "y" part, and val vector length
// is adjusted to point only to "x".
static const char *next_option(const char *list, struct vec *val,
                               struct vec *eq_val) {
  if (list == NULL || *list == '\0') {
    // End of the list
    list = NULL;
  } else {
    val->ptr = list;
    if ((list = strchr(val->ptr, ',')) != NULL) {
      // Comma found. Store length and shift the list ptr
      val->len = list - val->ptr;
      list++;
    } else {
      // This value is the last one
      list = val->ptr + strlen(val->ptr);
      val->len = list - val->ptr;
    }

    if (eq_val != NULL) {
      // Value has form "x=y", adjust pointers and lengths
      // so that val points to "x", and eq_val points to "y".
      eq_val->len = 0;
      eq_val->ptr = (const char *) memchr(val->ptr, '=', val->len);
      if (eq_val->ptr != NULL) {
        eq_val->ptr++;  // Skip over '=' character
        eq_val->len = val->ptr + val->len - eq_val->ptr;
        val->len = (eq_val->ptr - val->ptr) - 1;
      }
    }
  }

  return list;
}

// Perform case-insensitive match of string against pattern
static int match_prefix(const char *pattern, int pattern_len, const char *str) {
  const char *or_str;
  int i, j, len, res;

  if ((or_str = (const char *) memchr(pattern, '|', pattern_len)) != NULL) {
    res = match_prefix(pattern, or_str - pattern, str);
    return res > 0 ? res :
        match_prefix(or_str + 1, (pattern + pattern_len) - (or_str + 1), str);
  }

  i = j = 0;
  res = -1;
  for (; i < pattern_len; i++, j++) {
    if (pattern[i] == '?' && str[j] != '\0') {
      continue;
    } else if (pattern[i] == '$') {
      return str[j] == '\0' ? j : -1;
    } else if (pattern[i] == '*') {
      i++;
      if (pattern[i] == '*') {
        i++;
        len = (int) strlen(str + j);
      } else {
        len = (int) strcspn(str + j, "/");
      }
      if (i == pattern_len) {
        return j + len;
      }
      do {
        res = match_prefix(pattern + i, pattern_len - i, str + j + len);
      } while (res == -1 && len-- > 0);
      return res == -1 ? -1 : j + res + len;
    } else if (lowercase(&pattern[i]) != lowercase(&str[j])) {
      return -1;
    }
  }
  return j;
}

static int should_keep_alive(const struct sq_connection *conn) {
  const char *http_version = conn->request_info.http_version;
  const char *header = sq_get_header(conn, "Connection");

  // Start by checking our own internal request state and configuration.
  if (conn->must_close ||
      sq_strcasecmp(conn->ctx->config[ENABLE_KEEP_ALIVE], "yes") != 0) {
    return 0;
  }

  // Now consider the HTTP version and "Connection:" header.

  // If we couldn't parse an HTTP version, assume 1.0.
  //
  // We must tolerate situations when connection info is not set up, for
  // example if HTTP request parsing failed.
  int http_1_0 = !http_version || sq_strcasecmp(http_version, "1.0") == 0;
  if (!header) {
    // HTTP 1.1 assumes keep alive if the "Connection:" header is not set.
    return !http_1_0;
  }

  // With HTTP 1.0, keep alive only if the "Connection: keep-alive" header
  // exists. Otherwise, keep alive unless the "Connection: close" header exists.
  return (http_1_0 && sq_strcasecmp(header, "keep-alive") == 0) ||
         (!http_1_0 && sq_strcasecmp(header, "close") != 0);
}

static const char *suggest_connection_header(const struct sq_connection *conn) {
  return should_keep_alive(conn) ? "keep-alive" : "close";
}

static void send_http_error(struct sq_connection *, int, const char *,
                            PRINTF_FORMAT_STRING(const char *fmt), ...)
  PRINTF_ARGS(4, 5);


static void send_http_error(struct sq_connection *conn, int status,
                            const char *reason, const char *fmt, ...) {
  char buf[SQ_BUF_LEN];
  va_list ap;
  int len = 0;

  conn->status_code = status;
  if (conn->ctx->callbacks.http_error == NULL ||
      conn->ctx->callbacks.http_error(conn, status)) {
    buf[0] = '\0';

    // Errors 1xx, 204 and 304 MUST NOT send a body
    if (status > 199 && status != 204 && status != 304) {
      len = sq_snprintf(conn, buf, sizeof(buf), "Error %d: %s", status, reason);
      buf[len++] = '\n';

      va_start(ap, fmt);
      len += sq_vsnprintf(conn, buf + len, sizeof(buf) - len, fmt, ap);
      va_end(ap);
    }
    DEBUG_TRACE(("[%s]", buf));

    sq_printf(conn, "HTTP/1.1 %d %s\r\n"
              "Content-Length: %d\r\n"
              "Connection: %s\r\n\r\n", status, reason, len,
              suggest_connection_header(conn));
    conn->num_bytes_sent += sq_printf(conn, "%s", buf);
  }
}

static int sq_stat(struct sq_connection *conn, const char *path,
                   struct file *filep) {
  struct stat st;

  if (!is_file_in_memory(conn, path, filep) && !stat(path, &st)) {
    filep->size = st.st_size;
    filep->modification_time = st.st_mtime;
    filep->is_directory = S_ISDIR(st.st_mode);
  } else {
    filep->modification_time = (time_t) 0;
  }

  return filep->membuf != NULL || filep->modification_time != (time_t) 0;
}

static void set_close_on_exec(int fd) {
  fcntl(fd, F_SETFD, FD_CLOEXEC);
}

int sq_start_thread(sq_thread_func_t func, void *param) {
  pthread_t thread_id;
  pthread_attr_t attr;
  int result;

  (void) pthread_attr_init(&attr);
  (void) pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

#if USE_STACK_SIZE > 1
  // Compile-time option to control stack size, e.g. -DUSE_STACK_SIZE=16384
  (void) pthread_attr_setstacksize(&attr, USE_STACK_SIZE);
#endif

  result = pthread_create(&thread_id, &attr, func, param);
  pthread_attr_destroy(&attr);

  return result;
}

#ifndef NO_CGI
static pid_t spawn_process(struct sq_connection *conn, const char *prog,
                           char *envblk, char *envp[], int fdin,
                           int fdout, const char *dir) {
  pid_t pid;
  const char *interp;

  (void) envblk;

  if ((pid = fork()) == -1) {
    // Parent
    send_http_error(conn, 500, http_500_error, "fork(): %s", strerror(ERRNO));
  } else if (pid == 0) {
    // Child
    if (chdir(dir) != 0) {
      cry(conn, "%s: chdir(%s): %s", __func__, dir, strerror(ERRNO));
    } else if (dup2(fdin, 0) == -1) {
      cry(conn, "%s: dup2(%d, 0): %s", __func__, fdin, strerror(ERRNO));
    } else if (dup2(fdout, 1) == -1) {
      cry(conn, "%s: dup2(%d, 1): %s", __func__, fdout, strerror(ERRNO));
    } else {
      // Not redirecting stderr to stdout, to avoid output being littered
      // with the error messages.
      (void) close(fdin);
      (void) close(fdout);

      // After exec, all signal handlers are restored to their default values,
      // with one exception of SIGCHLD. According to POSIX.1-2001 and Linux's
      // implementation, SIGCHLD's handler will leave unchanged after exec
      // if it was set to be ignored. Restore it to default action.
      signal(SIGCHLD, SIG_DFL);

      interp = conn->ctx->config[CGI_INTERPRETER];
      if (interp == NULL) {
        (void) execle(prog, prog, NULL, envp);
        cry(conn, "%s: execle(%s): %s", __func__, prog, strerror(ERRNO));
      } else {
        (void) execle(interp, interp, prog, NULL, envp);
        cry(conn, "%s: execle(%s %s): %s", __func__, interp, prog,
            strerror(ERRNO));
      }
    }
    exit(EXIT_FAILURE);
  }

  return pid;
}
#endif // !NO_CGI

static int set_non_blocking_mode(SOCKET sock) {
  int flags;

  flags = fcntl(sock, F_GETFL, 0);
  (void) fcntl(sock, F_SETFL, flags | O_NONBLOCK);

  return 0;
}

int sq_get_bound_addresses(const struct sq_context *ctx, struct sockaddr_in ***addrs,
                           int *num_addrs) {
  int n = ctx->num_listening_sockets;
  int rc = 1;
  int i;

  struct sockaddr_in **addr_array = calloc(n, sizeof(struct sockaddr_in *));
  if (addr_array == NULL) {
    cry(fc(ctx), "%s: cannot allocate memory", __func__);
    goto cleanup;
  }
  *addrs = addr_array;

  for (i = 0; i < n; i++) {
    addr_array[i] = malloc(sizeof(struct sockaddr_storage));
    if (addr_array[i] == NULL) {
      cry(fc(ctx), "%s: cannot allocate memory", __func__);
      goto cleanup;
    }

    socklen_t len = sizeof(struct sockaddr_in *);
    if (getsockname(ctx->listening_sockets[i].sock, (struct sockaddr*)addr_array[i],
                    &len) != 0) {
      cry(fc(ctx), "%s: cannot get socket name: %s", __func__, strerror(errno));
      goto cleanup;
    }
  }

  *num_addrs = n;

  return 0;

  cleanup:
  if (addr_array) {
    for (i = 0; i < n; i++) {
      free(addr_array[i]);
    }
    free(addr_array);
  }
  return rc;
}


// Write data to the IO channel - opened file descriptor, socket or SSL
// descriptor. Return number of bytes written.
static int64_t push(FILE *fp, SOCKET sock, SSL *ssl, const char *buf,
                    int64_t len) {
  int64_t sent;
  int n, k;

  (void) ssl;  // Get rid of warning
  sent = 0;
  while (sent < len) {

    // How many bytes we send in this iteration
    k = len - sent > INT_MAX ? INT_MAX : (int) (len - sent);

#ifndef NO_SSL
    if (ssl != NULL) {
      n = SSL_write(ssl, buf + sent, k);
    } else
#endif
      if (fp != NULL) {
      n = (int) fwrite(buf + sent, 1, (size_t) k, fp);
      if (ferror(fp))
        n = -1;
    } else {
      RETRY_ON_EINTR(n, send(sock, buf + sent, (size_t) k, MSG_NOSIGNAL));
    }

    if (n <= 0)
      break;

    sent += n;
  }

  return sent;
}

// Wait for either 'fd' or 'wakeup_fds' to have readable data.
static void wait_for_readable_or_wakeup(struct sq_context *ctx,
    int fd, int timeout_ms) {
  struct pollfd pfd[2];
  pfd[0].fd = fd;
  pfd[0].events = POLLIN;
  pfd[1].fd = ctx->wakeup_fds[0];
  pfd[1].events = POLLIN;
  int poll_rc;
  RETRY_ON_EINTR(poll_rc, poll(pfd, 2, timeout_ms));
}

// Read from IO channel - opened file descriptor, socket, or SSL descriptor.
// Return negative value on error, or number of bytes read on success.
static int pull(FILE *fp, struct sq_connection *conn, char *buf, int len) {
  int nread;

  if (fp != NULL) {
    // Use read() instead of fread(), because if we're reading from the CGI
    // pipe, fread() may block until IO buffer is filled up. We cannot afford
    // to block and must pass all read bytes immediately to the client.
    RETRY_ON_EINTR(nread, read(fileno(fp), buf, (size_t) len));
#ifndef NO_SSL
  } else if (conn->ssl != NULL) {
    nread = SSL_read(conn->ssl, buf, len);
#endif
  } else {
    RETRY_ON_EINTR(nread, recv(conn->client.sock, buf, (size_t) len, 0));
    if (nread == -1 && errno != EAGAIN) {
      cry(conn, "error reading: %s", strerror(errno));
    }
  }

  return conn->ctx->stop_flag ? -1 : nread;
}

static int pull_all(FILE *fp, struct sq_connection *conn, char *buf, int len) {
  int n, nread = 0;

  while (len > 0 && conn->ctx->stop_flag == 0) {
    n = pull(fp, conn, buf + nread, len);
    if (n < 0) {
      nread = n;  // Propagate the error
      break;
    } else if (n == 0) {
      break;  // No more data to read
    } else {
      conn->consumed_content += n;
      nread += n;
      len -= n;
    }
  }

  return nread;
}

int sq_read(struct sq_connection *conn, void *buf, size_t len) {
  int n, buffered_len, nread;
  const char *body;

  // If Content-Length is not set, read until socket is closed
  if (conn->consumed_content == 0 && conn->content_len == 0) {
    conn->content_len = INT64_MAX;
    conn->must_close = 1;
  }

  nread = 0;
  if (conn->consumed_content < conn->content_len) {
    // Adjust number of bytes to read.
    int64_t to_read = conn->content_len - conn->consumed_content;
    if (to_read < (int64_t) len) {
      len = (size_t) to_read;
    }

    // Return buffered data
    body = conn->buf + conn->request_len + conn->consumed_content;
    buffered_len = &conn->buf[conn->data_len] - body;
    if (buffered_len > 0) {
      if (len < (size_t) buffered_len) {
        buffered_len = (int) len;
      }
      memcpy(buf, body, (size_t) buffered_len);
      len -= buffered_len;
      conn->consumed_content += buffered_len;
      nread += buffered_len;
      buf = (char *) buf + buffered_len;
    }

    // We have returned all buffered data. Read new data from the remote socket.
    n = pull_all(NULL, conn, (char *) buf, (int) len);
    nread = n >= 0 ? nread + n : n;
  }
  return nread;
}

int sq_write(struct sq_connection *conn, const void *buf, size_t len) {
  time_t now;
  int64_t n, total, allowed;

  if (conn->throttle > 0) {
    if ((now = time(NULL)) != conn->last_throttle_time) {
      conn->last_throttle_time = now;
      conn->last_throttle_bytes = 0;
    }
    allowed = conn->throttle - conn->last_throttle_bytes;
    if (allowed > (int64_t) len) {
      allowed = len;
    }
    if ((total = push(NULL, conn->client.sock, conn->ssl, (const char *) buf,
                      (int64_t) allowed)) == allowed) {
      buf = (char *) buf + total;
      conn->last_throttle_bytes += total;
      while (total < (int64_t) len && conn->ctx->stop_flag == 0) {
        allowed = conn->throttle > (int64_t) len - total ?
          (int64_t) len - total : conn->throttle;
        if ((n = push(NULL, conn->client.sock, conn->ssl, (const char *) buf,
                      (int64_t) allowed)) != allowed) {
          break;
        }
        sleep(1);
        conn->last_throttle_bytes = allowed;
        conn->last_throttle_time = time(NULL);
        buf = (char *) buf + n;
        total += n;
      }
    }
  } else {
    total = push(NULL, conn->client.sock, conn->ssl, (const char *) buf,
                 (int64_t) len);
  }
  return (int) total;
}

// Alternative alloc_vprintf() for non-compliant C runtimes
static int alloc_vprintf2(char **buf, const char *fmt, va_list ap) {
  va_list ap_copy;
  int size = SQ_BUF_LEN;
  int len = -1;

  *buf = NULL;
  while (len == -1) {
    if (*buf) free(*buf);
    *buf = malloc(size *= 4);
    if (!*buf) break;
    va_copy(ap_copy, ap);
    len = vsnprintf(*buf, size, fmt, ap_copy);
    va_end(ap_copy);
  }

  return len;
}

// Print message to buffer. If buffer is large enough to hold the message,
// return buffer. If buffer is to small, allocate large enough buffer on heap,
// and return allocated buffer.
static int alloc_vprintf(char **buf, size_t size, const char *fmt, va_list ap) {
  va_list ap_copy;
  int len;

  // Windows is not standard-compliant, and vsnprintf() returns -1 if
  // buffer is too small. Also, older versions of msvcrt.dll do not have
  // _vscprintf().  However, if size is 0, vsnprintf() behaves correctly.
  // Therefore, we make two passes: on first pass, get required message length.
  // On second pass, actually print the message.
  va_copy(ap_copy, ap);
  len = vsnprintf(NULL, 0, fmt, ap_copy);
  va_end(ap_copy);

  if (len < 0) {
    // C runtime is not standard compliant, vsnprintf() returned -1.
    // Switch to alternative code path that uses incremental allocations.
    va_copy(ap_copy, ap);
    len = alloc_vprintf2(buf, fmt, ap);
    va_end(ap_copy);
  } else if (len > (int) size &&
      (size = len + 1) > 0 &&
      (*buf = (char *) malloc(size)) == NULL) {
    len = -1;  // Allocation failed, mark failure
  } else {
    va_copy(ap_copy, ap);
    vsnprintf(*buf, size, fmt, ap_copy);
    va_end(ap_copy);
  }

  return len;
}

int sq_vprintf(struct sq_connection *conn, const char *fmt, va_list ap) {
  char mem[SQ_BUF_LEN], *buf = mem;
  int len;

  if ((len = alloc_vprintf(&buf, sizeof(mem), fmt, ap)) > 0) {
    len = sq_write(conn, buf, (size_t) len);
  }
  if (buf != mem && buf != NULL) {
    free(buf);
  }

  return len;
}

int sq_printf(struct sq_connection *conn, const char *fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  int ret_val = sq_vprintf(conn, fmt, ap);
  va_end(ap);
  return ret_val;
}

int sq_url_decode(const char *src, int src_len, char *dst,
                  int dst_len, int is_form_url_encoded) {
  int i, j, a, b;
#define HEXTOI(x) (isdigit(x) ? x - '0' : x - 'W')

  for (i = j = 0; i < src_len && j < dst_len - 1; i++, j++) {
    if (src[i] == '%' && i < src_len - 2 &&
        isxdigit(* (const unsigned char *) (src + i + 1)) &&
        isxdigit(* (const unsigned char *) (src + i + 2))) {
      a = tolower(* (const unsigned char *) (src + i + 1));
      b = tolower(* (const unsigned char *) (src + i + 2));
      dst[j] = (char) ((HEXTOI(a) << 4) | HEXTOI(b));
      i += 2;
    } else if (is_form_url_encoded && src[i] == '+') {
      dst[j] = ' ';
    } else {
      dst[j] = src[i];
    }
  }

  dst[j] = '\0'; // Null-terminate the destination

  return i >= src_len ? j : -1;
}

int sq_get_var(const char *data, size_t data_len, const char *name,
               char *dst, size_t dst_len) {
  const char *p, *e, *s;
  size_t name_len;
  int len;

  if (dst == NULL || dst_len == 0) {
    len = -2;
  } else if (data == NULL || name == NULL || data_len == 0) {
    len = -1;
    dst[0] = '\0';
  } else {
    name_len = strlen(name);
    e = data + data_len;
    len = -1;
    dst[0] = '\0';

    // data is "var1=val1&var2=val2...". Find variable first
    for (p = data; p + name_len < e; p++) {
      if ((p == data || p[-1] == '&') && p[name_len] == '=' &&
          !sq_strncasecmp(name, p, name_len)) {

        // Point p to variable value
        p += name_len + 1;

        // Point s to the end of the value
        s = (const char *) memchr(p, '&', (size_t)(e - p));
        if (s == NULL) {
          s = e;
        }
        assert(s >= p);

        // Decode variable into destination buffer
        len = sq_url_decode(p, (size_t)(s - p), dst, dst_len, 1);

        // Redirect error code from -1 to -2 (destination buffer too small).
        if (len == -1) {
          len = -2;
        }
        break;
      }
    }
  }

  return len;
}

int sq_get_cookie(const char *cookie_header, const char *var_name,
                  char *dst, size_t dst_size) {
  const char *s, *p, *end;
  int name_len, len = -1;

  if (dst == NULL || dst_size == 0) {
    len = -2;
  } else if (var_name == NULL || (s = cookie_header) == NULL) {
    len = -1;
    dst[0] = '\0';
  } else {
    name_len = (int) strlen(var_name);
    end = s + strlen(s);
    dst[0] = '\0';

    for (; (s = sq_strcasestr(s, var_name)) != NULL; s += name_len) {
      if (s[name_len] == '=') {
        s += name_len + 1;
        if ((p = strchr(s, ' ')) == NULL)
          p = end;
        if (p[-1] == ';')
          p--;
        if (*s == '"' && p[-1] == '"' && p > s + 1) {
          s++;
          p--;
        }
        if ((size_t) (p - s) < dst_size) {
          len = p - s;
          sq_strlcpy(dst, s, (size_t) len + 1);
        } else {
          len = -3;
        }
        break;
      }
    }
  }
  return len;
}

static void convert_uri_to_file_name(struct sq_connection *conn, char *buf,
                                     size_t buf_len, struct file *filep) {
  struct vec a, b;
  const char *rewrite, *uri = conn->request_info.uri,
        *root = conn->ctx->config[DOCUMENT_ROOT];
  char *p;
  int match_len;
  char gz_path[PATH_MAX];
  char const* accept_encoding;

  // Using buf_len - 1 because memmove() for PATH_INFO may shift part
  // of the path one byte on the right.
  // If document_root is NULL, leave the file empty.
  sq_snprintf(conn, buf, buf_len - 1, "%s%s",
              root == NULL ? "" : root,
              root == NULL ? "" : uri);

  rewrite = conn->ctx->config[REWRITE];
  while ((rewrite = next_option(rewrite, &a, &b)) != NULL) {
    if ((match_len = match_prefix(a.ptr, a.len, uri)) > 0) {
      sq_snprintf(conn, buf, buf_len - 1, "%.*s%s", (int) b.len, b.ptr,
                  uri + match_len);
      break;
    }
  }

  if (sq_stat(conn, buf, filep)) return;

  // if we can't find the actual file, look for the file
  // with the same name but a .gz extension. If we find it,
  // use that and set the gzipped flag in the file struct
  // to indicate that the response need to have the content-
  // encoding: gzip header
  // we can only do this if the browser declares support
  if ((accept_encoding = sq_get_header(conn, "Accept-Encoding")) != NULL) {
    if (strstr(accept_encoding,"gzip") != NULL) {
      snprintf(gz_path, sizeof(gz_path), "%s.gz", buf);
      if (sq_stat(conn, gz_path, filep)) {
        filep->gzipped = 1;
        return;
      }
    }
  }

  // Support PATH_INFO for CGI scripts.
  for (p = buf + strlen(buf); p > buf + 1; p--) {
    if (*p == '/') {
      *p = '\0';
      if (match_prefix(conn->ctx->config[CGI_EXTENSIONS],
                       strlen(conn->ctx->config[CGI_EXTENSIONS]), buf) > 0 &&
          sq_stat(conn, buf, filep)) {
        // Shift PATH_INFO block one character right, e.g.
        //  "/x.cgi/foo/bar\x00" => "/x.cgi\x00/foo/bar\x00"
        // conn->path_info is pointing to the local variable "path" declared
        // in handle_request(), so PATH_INFO is not valid after
        // handle_request returns.
        conn->path_info = p + 1;
        memmove(p + 2, p + 1, strlen(p + 1) + 1);  // +1 is for trailing \0
        p[1] = '/';
        break;
      } else {
        *p = '/';
      }
    }
  }
}

// Check whether full request is buffered. Return:
//   -1  if request is malformed
//    0  if request is not yet fully buffered
//   >0  actual request length, including last \r\n\r\n
static int get_request_len(const char *buf, int buflen) {
  const char *s, *e;
  int len = 0;

  for (s = buf, e = s + buflen - 1; len <= 0 && s < e; s++)
    // Control characters are not allowed but >=128 is.
    if (!isprint(* (const unsigned char *) s) && *s != '\r' &&
        *s != '\n' && * (const unsigned char *) s < 128) {
      len = -1;
      break;  // [i_a] abort scan as soon as one malformed character is found;
              // don't let subsequent \r\n\r\n win us over anyhow
    } else if (s[0] == '\n' && s[1] == '\n') {
      len = (int) (s - buf) + 2;
    } else if (s[0] == '\n' && &s[1] < e &&
        s[1] == '\r' && s[2] == '\n') {
      len = (int) (s - buf) + 3;
    }

  return len;
}

// Convert month to the month number. Return -1 on error, or month number
static int get_month_index(const char *s) {
  size_t i;

  for (i = 0; i < ARRAY_SIZE(month_names); i++)
    if (!strcmp(s, month_names[i]))
      return (int) i;

  return -1;
}

static int num_leap_years(int year) {
  return year / 4 - year / 100 + year / 400;
}

// Parse UTC date-time string, and return the corresponding time_t value.
static time_t parse_date_string(const char *datetime) {
  static const unsigned short days_before_month[] = {
    0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334
  };
  char month_str[32];
  int second, minute, hour, day, month, year, leap_days, days;
  time_t result = (time_t) 0;

  if (((sscanf(datetime, "%d/%3s/%d %d:%d:%d",
               &day, month_str, &year, &hour, &minute, &second) == 6) ||
       (sscanf(datetime, "%d %3s %d %d:%d:%d",
               &day, month_str, &year, &hour, &minute, &second) == 6) ||
       (sscanf(datetime, "%*3s, %d %3s %d %d:%d:%d",
               &day, month_str, &year, &hour, &minute, &second) == 6) ||
       (sscanf(datetime, "%d-%3s-%d %d:%d:%d",
               &day, month_str, &year, &hour, &minute, &second) == 6)) &&
      year > 1970 &&
      (month = get_month_index(month_str)) != -1) {
    leap_days = num_leap_years(year) - num_leap_years(1970);
    year -= 1970;
    days = year * 365 + days_before_month[month] + (day - 1) + leap_days;
    result = days * 24 * 3600 + hour * 3600 + minute * 60 + second;
  }

  return result;
}

// Protect against directory disclosure attack by removing '..',
// excessive '/' and '\' characters
static void remove_double_dots_and_double_slashes(char *s) {
  char *p = s;

  while (*s != '\0') {
    *p++ = *s++;
    if (s[-1] == '/' || s[-1] == '\\') {
      // Skip all following slashes, backslashes and double-dots
      while (s[0] != '\0') {
        if (s[0] == '/' || s[0] == '\\') {
          s++;
        } else if (s[0] == '.' && s[1] == '.') {
          s += 2;
        } else {
          break;
        }
      }
    }
  }
  *p = '\0';
}

static const struct {
  const char *extension;
  size_t ext_len;
  const char *mime_type;
} builtin_mime_types[] = {
  {".html", 5, "text/html"},
  {".htm", 4, "text/html"},
  {".shtm", 5, "text/html"},
  {".shtml", 6, "text/html"},
  {".css", 4, "text/css"},
  {".js",  3, "application/x-javascript"},
  {".ico", 4, "image/x-icon"},
  {".gif", 4, "image/gif"},
  {".jpg", 4, "image/jpeg"},
  {".jpeg", 5, "image/jpeg"},
  {".png", 4, "image/png"},
  {".svg", 4, "image/svg+xml"},
  {".txt", 4, "text/plain"},
  {".torrent", 8, "application/x-bittorrent"},
  {".wav", 4, "audio/x-wav"},
  {".mp3", 4, "audio/x-mp3"},
  {".mid", 4, "audio/mid"},
  {".m3u", 4, "audio/x-mpegurl"},
  {".ogg", 4, "audio/ogg"},
  {".ram", 4, "audio/x-pn-realaudio"},
  {".xml", 4, "text/xml"},
  {".json",  5, "text/json"},
  {".xslt", 5, "application/xml"},
  {".xsl", 4, "application/xml"},
  {".ra",  3, "audio/x-pn-realaudio"},
  {".doc", 4, "application/msword"},
  {".exe", 4, "application/octet-stream"},
  {".zip", 4, "application/x-zip-compressed"},
  {".xls", 4, "application/excel"},
  {".tgz", 4, "application/x-tar-gz"},
  {".tar", 4, "application/x-tar"},
  {".gz",  3, "application/x-gunzip"},
  {".arj", 4, "application/x-arj-compressed"},
  {".rar", 4, "application/x-arj-compressed"},
  {".rtf", 4, "application/rtf"},
  {".pdf", 4, "application/pdf"},
  {".swf", 4, "application/x-shockwave-flash"},
  {".mpg", 4, "video/mpeg"},
  {".webm", 5, "video/webm"},
  {".mpeg", 5, "video/mpeg"},
  {".mov", 4, "video/quicktime"},
  {".mp4", 4, "video/mp4"},
  {".m4v", 4, "video/x-m4v"},
  {".asf", 4, "video/x-ms-asf"},
  {".avi", 4, "video/x-msvideo"},
  {".bmp", 4, "image/bmp"},
  {".ttf", 4, "application/x-font-ttf"},
  {NULL,  0, NULL}
};

const char *sq_get_builtin_mime_type(const char *path) {
  const char *ext;
  size_t i, path_len;

  path_len = strlen(path);

  for (i = 0; builtin_mime_types[i].extension != NULL; i++) {
    ext = path + (path_len - builtin_mime_types[i].ext_len);
    if (path_len > builtin_mime_types[i].ext_len &&
        sq_strcasecmp(ext, builtin_mime_types[i].extension) == 0) {
      return builtin_mime_types[i].mime_type;
    }
  }

  return "text/plain";
}

// Look at the "path" extension and figure what mime type it has.
// Store mime type in the vector.
static void get_mime_type(struct sq_context *ctx, const char *path,
                          struct vec *vec) {
  struct vec ext_vec, mime_vec;
  const char *list, *ext;
  size_t path_len;

  path_len = strlen(path);

  // Scan user-defined mime types first, in case user wants to
  // override default mime types.
  list = ctx->config[EXTRA_MIME_TYPES];
  while ((list = next_option(list, &ext_vec, &mime_vec)) != NULL) {
    // ext now points to the path suffix
    ext = path + path_len - ext_vec.len;
    if (sq_strncasecmp(ext, ext_vec.ptr, ext_vec.len) == 0) {
      *vec = mime_vec;
      return;
    }
  }

  vec->ptr = sq_get_builtin_mime_type(path);
  vec->len = strlen(vec->ptr);
}

static int is_big_endian(void) {
  static const int n = 1;
  return ((char *) &n)[0] == 0;
}

// Stringify binary data. Output buffer must be twice as big as input,
// because each byte takes 2 bytes in string representation
static void bin2str(char *to, const unsigned char *p, size_t len) {
  static const char *hex = "0123456789abcdef";

  for (; len--; p++) {
    *to++ = hex[p[0] >> 4];
    *to++ = hex[p[0] & 0x0f];
  }
  *to = '\0';
}

// Return stringified MD5 hash for list of strings. Buffer must be 33 bytes.
char *sq_md5(char buf[33], ...) {
  unsigned char hash[16];
  const char *p;
  va_list ap;
  MD5_CTX ctx;

  MD5_Init(&ctx);

  va_start(ap, buf);
  while ((p = va_arg(ap, const char *)) != NULL) {
    MD5_Update(&ctx, (const unsigned char *) p, (unsigned) strlen(p));
  }
  va_end(ap);

  MD5_Final(hash, &ctx);
  bin2str(buf, hash, sizeof(hash));
  return buf;
}

// Check the user's password, return 1 if OK
static int check_password(const char *method, const char *ha1, const char *uri,
                          const char *nonce, const char *nc, const char *cnonce,
                          const char *qop, const char *response) {
  char ha2[32 + 1], expected_response[32 + 1];

  // Some of the parameters may be NULL
  if (method == NULL || nonce == NULL || nc == NULL || cnonce == NULL ||
      qop == NULL || response == NULL) {
    return 0;
  }

  // NOTE(lsm): due to a bug in MSIE, we do not compare the URI
  // TODO(lsm): check for authentication timeout
  if (// strcmp(dig->uri, c->ouri) != 0 ||
      strlen(response) != 32
      // || now - strtoul(dig->nonce, NULL, 10) > 3600
      ) {
    return 0;
  }

  sq_md5(ha2, method, ":", uri, NULL);
  sq_md5(expected_response, ha1, ":", nonce, ":", nc,
      ":", cnonce, ":", qop, ":", ha2, NULL);

  return sq_strcasecmp(response, expected_response) == 0;
}

// Use the global passwords file, if specified by auth_gpass option,
// or search for .htpasswd in the requested directory.
static void open_auth_file(struct sq_connection *conn, const char *path,
                           struct file *filep) {
  char name[PATH_MAX];
  const char *p, *e, *gpass = conn->ctx->config[GLOBAL_PASSWORDS_FILE];
  struct file file = STRUCT_FILE_INITIALIZER;

  if (gpass != NULL) {
    // Use global passwords file
    if (!sq_fopen(conn, gpass, "r", filep)) {
      cry(conn, "fopen(%s): %s", gpass, strerror(ERRNO));
    }
    // Important: using local struct file to test path for is_directory flag.
    // If filep is used, sq_stat() makes it appear as if auth file was opened.
  } else if (sq_stat(conn, path, &file) && file.is_directory) {
    sq_snprintf(conn, name, sizeof(name), "%s%c%s",
                path, '/', PASSWORDS_FILE_NAME);
    sq_fopen(conn, name, "r", filep);
  } else {
     // Try to find .htpasswd in requested directory.
    for (p = path, e = p + strlen(p) - 1; e > p; e--)
      if (e[0] == '/')
        break;
    sq_snprintf(conn, name, sizeof(name), "%.*s%c%s",
                (int) (e - p), p, '/', PASSWORDS_FILE_NAME);
    sq_fopen(conn, name, "r", filep);
  }
}

// Parsed Authorization header
struct ah {
  char *user, *uri, *cnonce, *response, *qop, *nc, *nonce;
};

// Return 1 on success. Always initializes the ah structure.
static int parse_auth_header(struct sq_connection *conn, char *buf,
                             size_t buf_size, struct ah *ah) {
  char *name, *value, *s;
  const char *auth_header;

  (void) memset(ah, 0, sizeof(*ah));
  if ((auth_header = sq_get_header(conn, "Authorization")) == NULL ||
      sq_strncasecmp(auth_header, "Digest ", 7) != 0) {
    return 0;
  }

  // Make modifiable copy of the auth header
  (void) sq_strlcpy(buf, auth_header + 7, buf_size);
  s = buf;

  // Parse authorization header
  for (;;) {
    // Gobble initial spaces
    while (isspace(* (unsigned char *) s)) {
      s++;
    }
    name = skip_quoted(&s, "=", " ", 0);
    // Value is either quote-delimited, or ends at first comma or space.
    if (s[0] == '\"') {
      s++;
      value = skip_quoted(&s, "\"", " ", '\\');
      if (s[0] == ',') {
        s++;
      }
    } else {
      value = skip_quoted(&s, ", ", " ", 0);  // IE uses commas, FF uses spaces
    }
    if (*name == '\0') {
      break;
    }

    if (!strcmp(name, "username")) {
      ah->user = value;
    } else if (!strcmp(name, "cnonce")) {
      ah->cnonce = value;
    } else if (!strcmp(name, "response")) {
      ah->response = value;
    } else if (!strcmp(name, "uri")) {
      ah->uri = value;
    } else if (!strcmp(name, "qop")) {
      ah->qop = value;
    } else if (!strcmp(name, "nc")) {
      ah->nc = value;
    } else if (!strcmp(name, "nonce")) {
      ah->nonce = value;
    }
  }

  // CGI needs it as REMOTE_USER
  if (ah->user != NULL) {
    conn->request_info.remote_user = sq_strdup(ah->user);
  } else {
    return 0;
  }

  return 1;
}

static char *sq_fgets(char *buf, size_t size, struct file *filep, char **p) {
  char *eof;
  size_t len;
  char *memend;

  if (filep->membuf != NULL && *p != NULL) {
    memend = (char *) &filep->membuf[filep->size];
    eof = (char *) memchr(*p, '\n', memend - *p); // Search for \n from p till the end of stream
    if (eof != NULL) {
      eof += 1; // Include \n
    } else {
      eof = memend; // Copy remaining data
    }
    len = (size_t) (eof - *p) > size - 1 ? size - 1 : (size_t) (eof - *p);
    memcpy(buf, *p, len);
    buf[len] = '\0';
    *p += len;
    return len ? eof : NULL;
  } else if (filep->fp != NULL) {
    return fgets(buf, size, filep->fp);
  } else {
    return NULL;
  }
}

// Authorize against the opened passwords file. Return 1 if authorized.
static int authorize(struct sq_connection *conn, struct file *filep) {
  struct ah ah;
  char line[256], f_user[256], ha1[256], f_domain[256], buf[SQ_BUF_LEN], *p;

  if (!parse_auth_header(conn, buf, sizeof(buf), &ah)) {
    return 0;
  }

  // Loop over passwords file
  p = (char *) filep->membuf;
  while (sq_fgets(line, sizeof(line), filep, &p) != NULL) {
    if (sscanf(line, "%[^:]:%[^:]:%s", f_user, f_domain, ha1) != 3) {
      continue;
    }

    if (!strcmp(ah.user, f_user) &&
        !strcmp(conn->ctx->config[AUTHENTICATION_DOMAIN], f_domain))
      return check_password(conn->request_info.request_method, ha1, ah.uri,
                            ah.nonce, ah.nc, ah.cnonce, ah.qop, ah.response);
  }

  return 0;
}

// Return 1 if request is authorised, 0 otherwise.
static int check_authorization(struct sq_connection *conn, const char *path) {
  char fname[PATH_MAX];
  struct vec uri_vec, filename_vec;
  const char *list;
  struct file file = STRUCT_FILE_INITIALIZER;
  int authorized = 1;

  list = conn->ctx->config[PROTECT_URI];
  while ((list = next_option(list, &uri_vec, &filename_vec)) != NULL) {
    if (!memcmp(conn->request_info.uri, uri_vec.ptr, uri_vec.len)) {
      sq_snprintf(conn, fname, sizeof(fname), "%.*s",
                  (int) filename_vec.len, filename_vec.ptr);
      if (!sq_fopen(conn, fname, "r", &file)) {
        cry(conn, "%s: cannot open %s: %s", __func__, fname, strerror(errno));
      }
      break;
    }
  }

  if (!is_file_opened(&file)) {
    open_auth_file(conn, path, &file);
  }

  if (is_file_opened(&file)) {
    authorized = authorize(conn, &file);
    sq_fclose(&file);
  }

  return authorized;
}

static void send_authorization_request(struct sq_connection *conn) {
  conn->status_code = 401;
  sq_printf(conn,
            "HTTP/1.1 401 Unauthorized\r\n"
            "Content-Length: 0\r\n"
            "WWW-Authenticate: Digest qop=\"auth\", "
            "realm=\"%s\", nonce=\"%lu\"\r\n\r\n",
            conn->ctx->config[AUTHENTICATION_DOMAIN],
            (unsigned long) time(NULL));
}

static int is_authorized_for_put(struct sq_connection *conn) {
  struct file file = STRUCT_FILE_INITIALIZER;
  const char *passfile = conn->ctx->config[PUT_DELETE_PASSWORDS_FILE];
  int ret = 0;

  if (passfile != NULL && sq_fopen(conn, passfile, "r", &file)) {
    ret = authorize(conn, &file);
    sq_fclose(&file);
  }

  return ret;
}

int sq_modify_passwords_file(const char *fname, const char *domain,
                             const char *user, const char *pass) {
  int found;
  char line[512], u[512], d[512], ha1[33], tmp[PATH_MAX];
  FILE *fp, *fp2;

  found = 0;
  fp = fp2 = NULL;

  // Regard empty password as no password - remove user record.
  if (pass != NULL && pass[0] == '\0') {
    pass = NULL;
  }

  (void) snprintf(tmp, sizeof(tmp), "%s.tmp", fname);

  // Create the file if does not exist
  if ((fp = fopen(fname, "a+")) != NULL) {
    (void) fclose(fp);
  }

  // Open the given file and temporary file
  if ((fp = fopen(fname, "r")) == NULL) {
    return 0;
  } else if ((fp2 = fopen(tmp, "w+")) == NULL) {
    fclose(fp);
    return 0;
  }

  // Copy the stuff to temporary file
  while (fgets(line, sizeof(line), fp) != NULL) {
    if (sscanf(line, "%[^:]:%[^:]:%*s", u, d) != 2) {
      continue;
    }

    if (!strcmp(u, user) && !strcmp(d, domain)) {
      found++;
      if (pass != NULL) {
        sq_md5(ha1, user, ":", domain, ":", pass, NULL);
        fprintf(fp2, "%s:%s:%s\n", user, domain, ha1);
      }
    } else {
      fprintf(fp2, "%s", line);
    }
  }

  // If new user, just add it
  if (!found && pass != NULL) {
    sq_md5(ha1, user, ":", domain, ":", pass, NULL);
    fprintf(fp2, "%s:%s:%s\n", user, domain, ha1);
  }

  // Close files
  fclose(fp);
  fclose(fp2);

  // Put the temp file in place of real file
  remove(fname);
  rename(tmp, fname);

  return 1;
}

static SOCKET conn2(const char *host, int port, int use_ssl,
                    char *ebuf, size_t ebuf_len) {
  struct sockaddr_in sin;
  struct hostent *he;
  SOCKET sock = INVALID_SOCKET;

  if (host == NULL) {
    snprintf(ebuf, ebuf_len, "%s", "NULL host");
    // TODO(lsm): use something threadsafe instead of gethostbyname()
  } else if ((he = gethostbyname(host)) == NULL) {
    snprintf(ebuf, ebuf_len, "gethostbyname(%s): %s", host, strerror(ERRNO));
  } else if ((sock = socket(PF_INET, SOCK_STREAM, 0)) == INVALID_SOCKET) {
    snprintf(ebuf, ebuf_len, "socket(): %s", strerror(ERRNO));
  } else {
    set_close_on_exec(sock);
    sin.sin_family = AF_INET;
    sin.sin_port = htons((uint16_t) port);
    sin.sin_addr = * (struct in_addr *) he->h_addr_list[0];
    if (connect(sock, (struct sockaddr *) &sin, sizeof(sin)) != 0) {
      snprintf(ebuf, ebuf_len, "connect(%s:%d): %s",
               host, port, strerror(ERRNO));
      closesocket(sock);
      sock = INVALID_SOCKET;
    }
  }
  return sock;
}



void sq_url_encode(const char *src, char *dst, size_t dst_len) {
  static const char *dont_escape = "._-$,;~()";
  static const char *hex = "0123456789abcdef";
  const char *end = dst + dst_len - 1;

  for (; *src != '\0' && dst < end; src++, dst++) {
    if (isalnum(*(const unsigned char *) src) ||
        strchr(dont_escape, * (const unsigned char *) src) != NULL) {
      *dst = *src;
    } else if (dst + 2 < end) {
      dst[0] = '%';
      dst[1] = hex[(* (const unsigned char *) src) >> 4];
      dst[2] = hex[(* (const unsigned char *) src) & 0xf];
      dst += 2;
    }
  }

  *dst = '\0';
}

// One simple rule: Strip everything between < and >
static void sq_strip_tags(const char *src, char *dst, size_t dst_len) {
  const char *end = dst + dst_len + 1;
  char tag = 0;
  for(; *src != '\0' && dst < end; src++) {
    if (*src == '<') {
      tag = 1;
    } else if (*src == '>') {
      tag = 0;
    } else if (tag == 0) {
      *dst++ = *src;
    }
  }
  *dst = '\0';
}


static void print_dir_entry(struct de *de) {
  char size[64], mod[64], href[PATH_MAX];

  if (de->file.is_directory) {
    sq_snprintf(de->conn, size, sizeof(size), "%s", "[DIRECTORY]");
  } else {
     // We use (signed) cast below because MSVC 6 compiler cannot
     // convert unsigned __int64 to double. Sigh.
    if (de->file.size < 1024) {
      sq_snprintf(de->conn, size, sizeof(size), "%d", (int) de->file.size);
    } else if (de->file.size < 0x100000) {
      sq_snprintf(de->conn, size, sizeof(size),
                  "%.1fk", (double) de->file.size / 1024.0);
    } else if (de->file.size < 0x40000000) {
      sq_snprintf(de->conn, size, sizeof(size),
                  "%.1fM", (double) de->file.size / 1048576);
    } else {
      sq_snprintf(de->conn, size, sizeof(size),
                  "%.1fG", (double) de->file.size / 1073741824);
    }
  }
  struct tm the_time;
  strftime(mod, sizeof(mod), "%d-%b-%Y %H:%M",
           localtime_r(&de->file.modification_time, &the_time));
  sq_url_encode(de->file_name, href, sizeof(href));
  de->conn->num_bytes_sent += sq_printf(de->conn,
      "<tr><td><a href=\"%s%s%s\">%s%s</a></td>"
      "<td>&nbsp;%s</td><td>&nbsp;&nbsp;%s</td></tr>\n",
      de->conn->request_info.uri, href, de->file.is_directory ? "/" : "",
      de->file_name, de->file.is_directory ? "/" : "", mod, size);
}

// This function is called from send_directory() and used for
// sorting directory entries by size, or name, or modification time.
// On windows, __cdecl specification is needed in case if project is built
// with __stdcall convention. qsort always requires __cdels callback.
static int compare_dir_entries(const void *p1, const void *p2) {
  const struct de *a = (const struct de *) p1, *b = (const struct de *) p2;
  const char *query_string = a->conn->request_info.query_string;
  int cmp_result = 0;

  if (query_string == NULL) {
    query_string = "na";
  }

  if (a->file.is_directory && !b->file.is_directory) {
    return -1;  // Always put directories on top
  } else if (!a->file.is_directory && b->file.is_directory) {
    return 1;   // Always put directories on top
  } else if (*query_string == 'n') {
    cmp_result = strcmp(a->file_name, b->file_name);
  } else if (*query_string == 's') {
    cmp_result = a->file.size == b->file.size ? 0 :
      a->file.size > b->file.size ? 1 : -1;
  } else if (*query_string == 'd') {
    cmp_result = a->file.modification_time == b->file.modification_time ? 0 :
      a->file.modification_time > b->file.modification_time ? 1 : -1;
  }

  return query_string[1] == 'd' ? -cmp_result : cmp_result;
}

static int must_hide_file(struct sq_connection *conn, const char *path) {
  const char *pw_pattern = "**" PASSWORDS_FILE_NAME "$";
  const char *pattern = conn->ctx->config[HIDE_FILES];
  return match_prefix(pw_pattern, strlen(pw_pattern), path) > 0 ||
    (pattern != NULL && match_prefix(pattern, strlen(pattern), path) > 0);
}

static int scan_directory(struct sq_connection *conn, const char *dir,
                          void *data, void (*cb)(struct de *, void *)) {
  char path[PATH_MAX];
  struct dirent *dp;
  DIR *dirp;
  struct de de;

  if ((dirp = opendir(dir)) == NULL) {
    return 0;
  } else {
    de.conn = conn;

    while ((dp = readdir(dirp)) != NULL) {
      // Do not show current dir and hidden files
      if (!strcmp(dp->d_name, ".") ||
          !strcmp(dp->d_name, "..") ||
          must_hide_file(conn, dp->d_name)) {
        continue;
      }

      sq_snprintf(conn, path, sizeof(path), "%s%c%s", dir, '/', dp->d_name);

      // If we don't memset stat structure to zero, mtime will have
      // garbage and strftime() will segfault later on in
      // print_dir_entry(). memset is required only if sq_stat()
      // fails. For more details, see
      // http://code.google.com/p/mongoose/issues/detail?id=79
      memset(&de.file, 0, sizeof(de.file));
      sq_stat(conn, path, &de.file);

      de.file_name = dp->d_name;
      cb(&de, data);
    }
    (void) closedir(dirp);
  }
  return 1;
}

static int remove_directory(struct sq_connection *conn, const char *dir) {
  char path[PATH_MAX];
  struct dirent *dp;
  DIR *dirp;
  struct de de;

  if ((dirp = opendir(dir)) == NULL) {
    return 0;
  } else {
    de.conn = conn;

    while ((dp = readdir(dirp)) != NULL) {
      // Do not show current dir (but show hidden files as they will also be removed)
      if (!strcmp(dp->d_name, ".") ||
          !strcmp(dp->d_name, "..")) {
        continue;
      }

      sq_snprintf(conn, path, sizeof(path), "%s%c%s", dir, '/', dp->d_name);

      // If we don't memset stat structure to zero, mtime will have
      // garbage and strftime() will segfault later on in
      // print_dir_entry(). memset is required only if sq_stat()
      // fails. For more details, see
      // http://code.google.com/p/mongoose/issues/detail?id=79
      memset(&de.file, 0, sizeof(de.file));
      sq_stat(conn, path, &de.file);
      if(de.file.modification_time) {
          if(de.file.is_directory) {
              remove_directory(conn, path);
          } else {
              sq_remove(path);
          }
      }

    }
    (void) closedir(dirp);

    rmdir(dir);
  }

  return 1;
}

struct dir_scan_data {
  struct de *entries;
  int num_entries;
  int arr_size;
};

// Behaves like realloc(), but frees original pointer on failure
static void *realloc2(void *ptr, size_t size) {
  void *new_ptr = realloc(ptr, size);
  if (new_ptr == NULL) {
    free(ptr);
  }
  return new_ptr;
}

static void dir_scan_callback(struct de *de, void *data) {
  struct dir_scan_data *dsd = (struct dir_scan_data *) data;

  if (dsd->entries == NULL || dsd->num_entries >= dsd->arr_size) {
    dsd->arr_size *= 2;
    dsd->entries = (struct de *) realloc2(dsd->entries, dsd->arr_size *
                                          sizeof(dsd->entries[0]));
  }
  if (dsd->entries == NULL) {
    // TODO(lsm): propagate an error to the caller
    dsd->num_entries = 0;
  } else {
    dsd->entries[dsd->num_entries].file_name = sq_strdup(de->file_name);
    dsd->entries[dsd->num_entries].file = de->file;
    dsd->entries[dsd->num_entries].conn = de->conn;
    dsd->num_entries++;
  }
}

static void handle_directory_request(struct sq_connection *conn,
                                     const char *dir) {
  int i, sort_direction;
  struct dir_scan_data data = { NULL, 0, 128 };

  if (!scan_directory(conn, dir, &data, dir_scan_callback)) {
    send_http_error(conn, 500, "Cannot open directory",
                    "Error: opendir(%s): %s", dir, strerror(ERRNO));
    return;
  }

  sort_direction = conn->request_info.query_string != NULL &&
    conn->request_info.query_string[1] == 'd' ? 'a' : 'd';

  conn->must_close = 1;
  sq_printf(conn, "%s",
            "HTTP/1.1 200 OK\r\n"
            "Connection: close\r\n"
            "Content-Type: text/html; charset=utf-8\r\n\r\n");

  conn->num_bytes_sent += sq_printf(conn,
      "<html><head><title>Index of %s</title>"
      "<style>th {text-align: left;}</style></head>"
      "<body><h1>Index of %s</h1><pre><table cellpadding=\"0\">"
      "<tr><th><a href=\"?n%c\">Name</a></th>"
      "<th><a href=\"?d%c\">Modified</a></th>"
      "<th><a href=\"?s%c\">Size</a></th></tr>"
      "<tr><td colspan=\"3\"><hr></td></tr>",
      conn->request_info.uri, conn->request_info.uri,
      sort_direction, sort_direction, sort_direction);

  // Print first entry - link to a parent directory
  conn->num_bytes_sent += sq_printf(conn,
      "<tr><td><a href=\"%s%s\">%s</a></td>"
      "<td>&nbsp;%s</td><td>&nbsp;&nbsp;%s</td></tr>\n",
      conn->request_info.uri, "..", "Parent directory", "-", "-");

  // Sort and print directory entries
  qsort(data.entries, (size_t) data.num_entries, sizeof(data.entries[0]),
        compare_dir_entries);
  for (i = 0; i < data.num_entries; i++) {
    print_dir_entry(&data.entries[i]);
    free(data.entries[i].file_name);
  }
  free(data.entries);

  conn->num_bytes_sent += sq_printf(conn, "%s", "</table></body></html>");
  conn->status_code = 200;
}

// Send len bytes from the opened file to the client.
static void send_file_data(struct sq_connection *conn, struct file *filep,
                           int64_t offset, int64_t len) {
  char buf[SQ_BUF_LEN];
  int to_read, num_read, num_written;

  // Sanity check the offset
  offset = offset < 0 ? 0 : offset > filep->size ? filep->size : offset;

  if (len > 0 && filep->membuf != NULL && filep->size > 0) {
    if (len > filep->size - offset) {
      len = filep->size - offset;
    }
    sq_write(conn, filep->membuf + offset, (size_t) len);
  } else if (len > 0 && filep->fp != NULL) {
    fseeko(filep->fp, offset, SEEK_SET);
    while (len > 0) {
      // Calculate how much to read from the file in the buffer
      to_read = sizeof(buf);
      if ((int64_t) to_read > len) {
        to_read = (int) len;
      }

      // Read from file, exit the loop on error
      if ((num_read = fread(buf, 1, (size_t) to_read, filep->fp)) <= 0) {
        break;
      }

      // Send read bytes to the client, exit the loop on error
      if ((num_written = sq_write(conn, buf, (size_t) num_read)) != num_read) {
        break;
      }

      // Both read and were successful, adjust counters
      conn->num_bytes_sent += num_written;
      len -= num_written;
    }
  }
}

static int parse_range_header(const char *header, int64_t *a, int64_t *b) {
  return sscanf(header, "bytes=%" INT64_FMT "-%" INT64_FMT, a, b);
}

static void gmt_time_string(char *buf, size_t buf_len, time_t *t) {
  strftime(buf, buf_len, "%a, %d %b %Y %H:%M:%S GMT", gmtime(t));
}

static void construct_etag(char *buf, size_t buf_len,
                           const struct file *filep) {
  snprintf(buf, buf_len, "\"%lx.%" INT64_FMT "\"",
           (unsigned long) filep->modification_time, filep->size);
}

static void fclose_on_exec(struct file *filep) {
  if (filep != NULL && filep->fp != NULL) {
    fcntl(fileno(filep->fp), F_SETFD, FD_CLOEXEC);
  }
}

static void handle_file_request(struct sq_connection *conn, const char *path,
                                struct file *filep) {
  char date[64], lm[64], etag[64], range[64];
  const char *msg = "OK", *hdr;
  time_t curtime = time(NULL);
  int64_t cl, r1, r2;
  struct vec mime_vec;
  int n;
  char gz_path[PATH_MAX];
  char const* encoding = "";

  get_mime_type(conn->ctx, path, &mime_vec);
  cl = filep->size;
  conn->status_code = 200;
  range[0] = '\0';

  // if this file is in fact a pre-gzipped file, rewrite its filename
  // it's important to rewrite the filename after resolving
  // the mime type from it, to preserve the actual file's type
  if (filep->gzipped) {
    snprintf(gz_path, sizeof(gz_path), "%s.gz", path);
    path = gz_path;
    encoding = "Content-Encoding: gzip\r\n";
  }

  if (!sq_fopen(conn, path, "rb", filep)) {
    send_http_error(conn, 500, http_500_error,
                    "fopen(%s): %s", path, strerror(ERRNO));
    return;
  }

  fclose_on_exec(filep);

  // If Range: header specified, act accordingly
  r1 = r2 = 0;
  hdr = sq_get_header(conn, "Range");
  if (hdr != NULL && (n = parse_range_header(hdr, &r1, &r2)) > 0 &&
      r1 >= 0 && r2 >= 0) {
    // actually, range requests don't play well with a pre-gzipped
    // file (since the range is specified in the uncmpressed space)
    if (filep->gzipped) {
      send_http_error(conn, 501, "Not Implemented", "range requests in gzipped files are not supported");
      return;
    }
    conn->status_code = 206;
    cl = n == 2 ? (r2 > cl ? cl : r2) - r1 + 1: cl - r1;
    sq_snprintf(conn, range, sizeof(range),
                "Content-Range: bytes "
                "%" INT64_FMT "-%"
                INT64_FMT "/%" INT64_FMT "\r\n",
                r1, r1 + cl - 1, filep->size);
    msg = "Partial Content";
  }

  // Prepare Etag, Date, Last-Modified headers. Must be in UTC, according to
  // http://www.w3.org/Protocols/rfc2616/rfc2616-sec3.html#sec3.3
  gmt_time_string(date, sizeof(date), &curtime);
  gmt_time_string(lm, sizeof(lm), &filep->modification_time);
  construct_etag(etag, sizeof(etag), filep);

  (void) sq_printf(conn,
      "HTTP/1.1 %d %s\r\n"
      "Date: %s\r\n"
      "Last-Modified: %s\r\n"
      "Etag: %s\r\n"
      "Content-Type: %.*s\r\n"
      "Content-Length: %" INT64_FMT "\r\n"
      "Connection: %s\r\n"
      "Accept-Ranges: bytes\r\n"
      "%s%s\r\n",
      conn->status_code, msg, date, lm, etag, (int) mime_vec.len,
      mime_vec.ptr, cl, suggest_connection_header(conn), range, encoding);

  if (strcmp(conn->request_info.request_method, "HEAD") != 0) {
    send_file_data(conn, filep, r1, cl);
  }
  sq_fclose(filep);
}

void sq_send_file(struct sq_connection *conn, const char *path) {
  struct file file = STRUCT_FILE_INITIALIZER;
  if (sq_stat(conn, path, &file)) {
    handle_file_request(conn, path, &file);
  } else {
    send_http_error(conn, 404, "Not Found", "%s", "File not found");
  }
}


// Parse HTTP headers from the given buffer, advance buffer to the point
// where parsing stopped.
static void parse_http_headers(char **buf, struct sq_request_info *ri) {
  int i;

  for (i = 0; i < (int) ARRAY_SIZE(ri->http_headers); i++) {
    ri->http_headers[i].name = skip_quoted(buf, ":", " ", 0);
    ri->http_headers[i].value = skip(buf, "\r\n");
    if (ri->http_headers[i].name[0] == '\0')
      break;
    ri->num_headers = i + 1;
  }
}

static int is_valid_http_method(const char *method) {
  for (unsigned int i = 0; i < sizeof(SAFE_HTTP_METHODS) / sizeof(char*); ++i) {
    if (!strcmp(method, SAFE_HTTP_METHODS[i])) return 1;
  }

#ifdef ALLOW_UNSAFE_HTTP_METHODS
  for (unsigned int i = 0; i < sizeof(UNSAFE_HTTP_METHODS) / sizeof(char*); ++i) {
    if (!strcmp(method, UNSAFE_HTTP_METHODS[i])) return 1;
  }
#endif

  return 0;
}

// Parse HTTP request, fill in sq_request_info structure.
// This function modifies the buffer by NUL-terminating
// HTTP request components, header names and header values.
static int parse_http_message(char *buf, int len, struct sq_request_info *ri) {
  int is_request, request_length = get_request_len(buf, len);
  if (request_length > 0) {
    // Reset attributes. DO NOT TOUCH is_ssl, remote_ip, remote_port
    ri->remote_user = ri->request_method = ri->uri = ri->http_version = NULL;
    ri->num_headers = 0;

    buf[request_length - 1] = '\0';

    // RFC says that all initial whitespaces should be ingored
    while (*buf != '\0' && isspace(* (unsigned char *) buf)) {
      buf++;
    }
    ri->request_method = skip(&buf, " ");
    ri->uri = skip(&buf, " ");
    ri->http_version = skip(&buf, "\r\n");

    // HTTP message could be either HTTP request or HTTP response, e.g.
    // "GET / HTTP/1.0 ...." or  "HTTP/1.0 200 OK ..."
    is_request = is_valid_http_method(ri->request_method);
    if ((is_request && memcmp(ri->http_version, "HTTP/", 5) != 0) ||
        (!is_request && memcmp(ri->request_method, "HTTP/", 5) != 0)) {
      request_length = -1;
    } else {
      if (is_request) {
        ri->http_version += 5;
      }
      parse_http_headers(&buf, ri);
    }
  }
  return request_length;
}

// Keep reading the input (either opened file descriptor fd, or socket sock,
// or SSL descriptor ssl) into buffer buf, until \r\n\r\n appears in the
// buffer (which marks the end of HTTP request). Buffer buf may already
// have some data. The length of the data is stored in nread.
// Upon every read operation, increase nread by the number of bytes read.
static int read_request(FILE *fp, struct sq_connection *conn,
                        char *buf, int bufsiz, int *nread) {
  int request_len, n = 0;

  request_len = get_request_len(buf, *nread);
  if (request_len == 0) {
    // If we are starting to read a new request, with nothing buffered,
    // wait for either the beginning of the request, or for the shutdown
    // signal.
    wait_for_readable_or_wakeup(conn->ctx, fp ? fileno(fp) : conn->client.sock,
        atoi(conn->ctx->config[REQUEST_TIMEOUT]));
  }

  while (conn->ctx->stop_flag == 0 &&
         *nread < bufsiz && request_len == 0 &&
         (n = pull(fp, conn, buf + *nread, bufsiz - *nread)) > 0) {
    *nread += n;
    assert(*nread <= bufsiz);
    request_len = get_request_len(buf, *nread);
  }

  return request_len <= 0 && n <= 0 ? -1 : request_len;
}

// For given directory path, substitute it to valid index file.
// Return 0 if index file has been found, -1 if not found.
// If the file is found, it's stats is returned in stp.
static int substitute_index_file(struct sq_connection *conn, char *path,
                                 size_t path_len, struct file *filep) {
  const char *list = conn->ctx->config[INDEX_FILES];
  struct file file = STRUCT_FILE_INITIALIZER;
  struct vec filename_vec;
  size_t n = strlen(path);
  int found = 0;

  // The 'path' given to us points to the directory. Remove all trailing
  // directory separator characters from the end of the path, and
  // then append single directory separator character.
  while (n > 0 && path[n - 1] == '/') {
    n--;
  }
  path[n] = '/';

  // Traverse index files list. For each entry, append it to the given
  // path and see if the file exists. If it exists, break the loop
  while ((list = next_option(list, &filename_vec, NULL)) != NULL) {

    // Ignore too long entries that may overflow path buffer
    if (filename_vec.len > path_len - (n + 2))
      continue;

    // Prepare full path to the index file
    sq_strlcpy(path + n + 1, filename_vec.ptr, filename_vec.len + 1);

    // Does it exist?
    if (sq_stat(conn, path, &file)) {
      // Yes it does, break the loop
      *filep = file;
      found = 1;
      break;
    }
  }

  // If no index file exists, restore directory path
  if (!found) {
    path[n] = '\0';
  }

  return found;
}

// Return True if we should reply 304 Not Modified.
static int is_not_modified(const struct sq_connection *conn,
                           const struct file *filep) {
  char etag[64];
  const char *ims = sq_get_header(conn, "If-Modified-Since");
  const char *inm = sq_get_header(conn, "If-None-Match");
  construct_etag(etag, sizeof(etag), filep);
  return (inm != NULL && !sq_strcasecmp(etag, inm)) ||
    (ims != NULL && filep->modification_time <= parse_date_string(ims));
}

static int forward_body_data(struct sq_connection *conn, FILE *fp,
                             SOCKET sock, SSL *ssl) {
  const char *expect, *body;
  char buf[SQ_BUF_LEN];
  int to_read, nread, buffered_len, success = 0;

  expect = sq_get_header(conn, "Expect");
  assert(fp != NULL);

  if (conn->content_len == -1) {
    send_http_error(conn, 411, "Length Required", "%s", "");
  } else if (expect != NULL && sq_strcasecmp(expect, "100-continue")) {
    send_http_error(conn, 417, "Expectation Failed", "%s", "");
  } else {
    if (expect != NULL) {
      (void) sq_printf(conn, "%s", "HTTP/1.1 100 Continue\r\n\r\n");
    }

    body = conn->buf + conn->request_len + conn->consumed_content;
    buffered_len = &conn->buf[conn->data_len] - body;
    assert(buffered_len >= 0);
    assert(conn->consumed_content == 0);

    if (buffered_len > 0) {
      if ((int64_t) buffered_len > conn->content_len) {
        buffered_len = (int) conn->content_len;
      }
      push(fp, sock, ssl, body, (int64_t) buffered_len);
      conn->consumed_content += buffered_len;
    }

    nread = 0;
    while (conn->consumed_content < conn->content_len) {
      to_read = sizeof(buf);
      if ((int64_t) to_read > conn->content_len - conn->consumed_content) {
        to_read = (int) (conn->content_len - conn->consumed_content);
      }
      nread = pull(NULL, conn, buf, to_read);
      if (nread <= 0 || push(fp, sock, ssl, buf, nread) != nread) {
        break;
      }
      conn->consumed_content += nread;
    }

    if (conn->consumed_content == conn->content_len) {
      success = nread >= 0;
    }

    // Each error code path in this function must send an error
    if (!success) {
      send_http_error(conn, 577, http_500_error, "%s", "");
    }
  }

  return success;
}

#if !defined(NO_CGI)
// This structure helps to create an environment for the spawned CGI program.
// Environment is an array of "VARIABLE=VALUE\0" ASCIIZ strings,
// last element must be NULL.
// However, on Windows there is a requirement that all these VARIABLE=VALUE\0
// strings must reside in a contiguous buffer. The end of the buffer is
// marked by two '\0' characters.
// We satisfy both worlds: we create an envp array (which is vars), all
// entries are actually pointers inside buf.
struct cgi_env_block {
  struct sq_connection *conn;
  char buf[CGI_ENVIRONMENT_SIZE]; // Environment buffer
  int len; // Space taken
  char *vars[MAX_CGI_ENVIR_VARS]; // char **envp
  int nvars; // Number of variables
};

static char *addenv(struct cgi_env_block *block,
                    PRINTF_FORMAT_STRING(const char *fmt), ...)
  PRINTF_ARGS(2, 3);

// Append VARIABLE=VALUE\0 string to the buffer, and add a respective
// pointer into the vars array.
static char *addenv(struct cgi_env_block *block, const char *fmt, ...) {
  int n, space;
  char *added;
  va_list ap;

  // Calculate how much space is left in the buffer
  space = sizeof(block->buf) - block->len - 2;
  assert(space >= 0);

  // Make a pointer to the free space int the buffer
  added = block->buf + block->len;

  // Copy VARIABLE=VALUE\0 string into the free space
  va_start(ap, fmt);
  n = sq_vsnprintf(block->conn, added, (size_t) space, fmt, ap);
  va_end(ap);

  // Make sure we do not overflow buffer and the envp array
  if (n > 0 && n + 1 < space &&
      block->nvars < (int) ARRAY_SIZE(block->vars) - 2) {
    // Append a pointer to the added string into the envp array
    block->vars[block->nvars++] = added;
    // Bump up used length counter. Include \0 terminator
    block->len += n + 1;
  } else {
    cry(block->conn, "%s: CGI env buffer truncated for [%s]", __func__, fmt);
  }

  return added;
}

static void prepare_cgi_environment(struct sq_connection *conn,
                                    const char *prog,
                                    struct cgi_env_block *blk) {
  const char *s, *slash;
  struct vec var_vec;
  char *p, src_addr[IP_ADDR_STR_LEN];
  int  i;

  blk->len = blk->nvars = 0;
  blk->conn = conn;
  sockaddr_to_string(src_addr, sizeof(src_addr), &conn->client.rsa);

  addenv(blk, "SERVER_NAME=%s", conn->ctx->config[AUTHENTICATION_DOMAIN]);
  addenv(blk, "SERVER_ROOT=%s", conn->ctx->config[DOCUMENT_ROOT]);
  addenv(blk, "DOCUMENT_ROOT=%s", conn->ctx->config[DOCUMENT_ROOT]);
  addenv(blk, "SERVER_SOFTWARE=%s/%s", "Squeasel", sq_version());

  // Prepare the environment block
  addenv(blk, "%s", "GATEWAY_INTERFACE=CGI/1.1");
  addenv(blk, "%s", "SERVER_PROTOCOL=HTTP/1.1");
  addenv(blk, "%s", "REDIRECT_STATUS=200"); // For PHP

  // TODO(lsm): fix this for IPv6 case
  addenv(blk, "SERVER_PORT=%d", ntohs(conn->client.lsa.sin.sin_port));

  addenv(blk, "REQUEST_METHOD=%s", conn->request_info.request_method);
  addenv(blk, "REMOTE_ADDR=%s", src_addr);
  addenv(blk, "REMOTE_PORT=%d", conn->request_info.remote_port);
  addenv(blk, "REQUEST_URI=%s", conn->request_info.uri);

  // SCRIPT_NAME
  assert(conn->request_info.uri[0] == '/');
  slash = strrchr(conn->request_info.uri, '/');
  if ((s = strrchr(prog, '/')) == NULL)
    s = prog;
  addenv(blk, "SCRIPT_NAME=%.*s%s", (int) (slash - conn->request_info.uri),
         conn->request_info.uri, s);

  addenv(blk, "SCRIPT_FILENAME=%s", prog);
  addenv(blk, "PATH_TRANSLATED=%s", prog);
  addenv(blk, "HTTPS=%s", conn->ssl == NULL ? "off" : "on");

  if ((s = sq_get_header(conn, "Content-Type")) != NULL)
    addenv(blk, "CONTENT_TYPE=%s", s);

  if (conn->request_info.query_string != NULL)
    addenv(blk, "QUERY_STRING=%s", conn->request_info.query_string);

  if ((s = sq_get_header(conn, "Content-Length")) != NULL)
    addenv(blk, "CONTENT_LENGTH=%s", s);

  if ((s = getenv("PATH")) != NULL)
    addenv(blk, "PATH=%s", s);

  if (conn->path_info != NULL) {
    addenv(blk, "PATH_INFO=%s", conn->path_info);
  }

  if ((s = getenv("LD_LIBRARY_PATH")) != NULL)
    addenv(blk, "LD_LIBRARY_PATH=%s", s);

  if ((s = getenv("PERLLIB")) != NULL)
    addenv(blk, "PERLLIB=%s", s);

  if (conn->request_info.remote_user != NULL) {
    addenv(blk, "REMOTE_USER=%s", conn->request_info.remote_user);
    addenv(blk, "%s", "AUTH_TYPE=Digest");
  }

  // Add all headers as HTTP_* variables
  for (i = 0; i < conn->request_info.num_headers; i++) {
    p = addenv(blk, "HTTP_%s=%s",
        conn->request_info.http_headers[i].name,
        conn->request_info.http_headers[i].value);

    // Convert variable name into uppercase, and change - to _
    for (; *p != '=' && *p != '\0'; p++) {
      if (*p == '-')
        *p = '_';
      *p = (char) toupper(* (unsigned char *) p);
    }
  }

  // Add user-specified variables
  s = conn->ctx->config[CGI_ENVIRONMENT];
  while ((s = next_option(s, &var_vec, NULL)) != NULL) {
    addenv(blk, "%.*s", (int) var_vec.len, var_vec.ptr);
  }

  blk->vars[blk->nvars++] = NULL;
  blk->buf[blk->len++] = '\0';

  assert(blk->nvars < (int) ARRAY_SIZE(blk->vars));
  assert(blk->len > 0);
  assert(blk->len < (int) sizeof(blk->buf));
}

static void handle_cgi_request(struct sq_connection *conn, const char *prog) {
  int headers_len, data_len, i, fdin[2], fdout[2];
  const char *status, *status_text;
  char buf[16384], *pbuf, dir[PATH_MAX], *p;
  struct sq_request_info ri;
  struct cgi_env_block blk;
  FILE *in = NULL, *out = NULL;
  struct file fout = STRUCT_FILE_INITIALIZER;
  pid_t pid = (pid_t) -1;

  prepare_cgi_environment(conn, prog, &blk);

  // CGI must be executed in its own directory. 'dir' must point to the
  // directory containing executable program, 'p' must point to the
  // executable program name relative to 'dir'.
  (void) sq_snprintf(conn, dir, sizeof(dir), "%s", prog);
  if ((p = strrchr(dir, '/')) != NULL) {
    *p++ = '\0';
  } else {
    dir[0] = '.', dir[1] = '\0';
    p = (char *) prog;
  }

  if (pipe(fdin) != 0 || pipe(fdout) != 0) {
    send_http_error(conn, 500, http_500_error,
        "Cannot create CGI pipe: %s", strerror(ERRNO));
    goto done;
  }

  pid = spawn_process(conn, p, blk.buf, blk.vars, fdin[0], fdout[1], dir);
  if (pid == (pid_t) -1) {
    send_http_error(conn, 500, http_500_error,
        "Cannot spawn CGI process [%s]: %s", prog, strerror(ERRNO));
    goto done;
  }

  // Make sure child closes all pipe descriptors. It must dup them to 0,1
  set_close_on_exec(fdin[0]);
  set_close_on_exec(fdin[1]);
  set_close_on_exec(fdout[0]);
  set_close_on_exec(fdout[1]);

  // Parent closes only one side of the pipes.
  // If we don't mark them as closed, close() attempt before
  // return from this function throws an exception on Windows.
  // Windows does not like when closed descriptor is closed again.
  (void) close(fdin[0]);
  (void) close(fdout[1]);
  fdin[0] = fdout[1] = -1;


  if ((in = fdopen(fdin[1], "wb")) == NULL ||
      (out = fdopen(fdout[0], "rb")) == NULL) {
    send_http_error(conn, 500, http_500_error,
        "fopen: %s", strerror(ERRNO));
    goto done;
  }

  setbuf(in, NULL);
  setbuf(out, NULL);
  fout.fp = out;

  // Send POST data to the CGI process if needed
  if (!strcmp(conn->request_info.request_method, "POST") &&
      !forward_body_data(conn, in, INVALID_SOCKET, NULL)) {
    goto done;
  }

  // Close so child gets an EOF.
  fclose(in);
  in = NULL;
  fdin[1] = -1;

  // Now read CGI reply into a buffer. We need to set correct
  // status code, thus we need to see all HTTP headers first.
  // Do not send anything back to client, until we buffer in all
  // HTTP headers.
  data_len = 0;
  headers_len = read_request(out, conn, buf, sizeof(buf), &data_len);
  if (headers_len <= 0) {
    send_http_error(conn, 500, http_500_error,
                    "CGI program sent malformed or too big (>%u bytes) "
                    "HTTP headers: [%.*s]",
                    (unsigned) sizeof(buf), data_len, buf);
    goto done;
  }
  pbuf = buf;
  buf[headers_len - 1] = '\0';
  parse_http_headers(&pbuf, &ri);

  // Make up and send the status line
  status_text = "OK";
  if ((status = get_header(&ri, "Status")) != NULL) {
    conn->status_code = atoi(status);
    status_text = status;
    while (isdigit(* (unsigned char *) status_text) || *status_text == ' ') {
      status_text++;
    }
  } else if (get_header(&ri, "Location") != NULL) {
    conn->status_code = 302;
  } else {
    conn->status_code = 200;
  }
  if (get_header(&ri, "Connection") != NULL &&
      !sq_strcasecmp(get_header(&ri, "Connection"), "keep-alive")) {
    conn->must_close = 1;
  }
  (void) sq_printf(conn, "HTTP/1.1 %d %s\r\n", conn->status_code,
                   status_text);

  // Send headers
  for (i = 0; i < ri.num_headers; i++) {
    sq_printf(conn, "%s: %s\r\n",
              ri.http_headers[i].name, ri.http_headers[i].value);
  }
  sq_write(conn, "\r\n", 2);

  // Send chunk of data that may have been read after the headers
  conn->num_bytes_sent += sq_write(conn, buf + headers_len,
                                   (size_t)(data_len - headers_len));

  // Read the rest of CGI output and send to the client
  send_file_data(conn, &fout, 0, INT64_MAX);

done:
  if (pid != (pid_t) -1) {
    kill(pid, SIGKILL);
  }
  if (fdin[0] != -1) {
    close(fdin[0]);
  }
  if (fdout[1] != -1) {
    close(fdout[1]);
  }

  if (in != NULL) {
    fclose(in);
  } else if (fdin[1] != -1) {
    close(fdin[1]);
  }

  if (out != NULL) {
    fclose(out);
  } else if (fdout[0] != -1) {
    close(fdout[0]);
  }
}
#endif // !NO_CGI

// For a given PUT path, create all intermediate subdirectories
// for given path. Return 0 if the path itself is a directory,
// or -1 on error, 1 if OK.
static int put_dir(struct sq_connection *conn, const char *path) {
  char buf[PATH_MAX];
  const char *s, *p;
  struct file file = STRUCT_FILE_INITIALIZER;
  int len, res = 1;

  for (s = p = path + 2; (p = strchr(s, '/')) != NULL; s = ++p) {
    len = p - path;
    if (len >= (int) sizeof(buf)) {
      res = -1;
      break;
    }
    memcpy(buf, path, len);
    buf[len] = '\0';

    // Try to create intermediate directory
    DEBUG_TRACE(("mkdir(%s)", buf));
    if (!sq_stat(conn, buf, &file) && sq_mkdir(buf, 0755) != 0) {
      res = -1;
      break;
    }

    // Is path itself a directory?
    if (p[1] == '\0') {
      res = 0;
    }
  }

  return res;
}

static void put_file(struct sq_connection *conn, const char *path) {
  struct file file = STRUCT_FILE_INITIALIZER;
  const char *range;
  int64_t r1, r2;
  int rc;

  conn->status_code = sq_stat(conn, path, &file) ? 200 : 201;

  if ((rc = put_dir(conn, path)) == 0) {
    sq_printf(conn, "HTTP/1.1 %d OK\r\n\r\n", conn->status_code);
  } else if (rc == -1) {
    send_http_error(conn, 500, http_500_error,
                    "put_dir(%s): %s", path, strerror(ERRNO));
  } else if (!sq_fopen(conn, path, "wb+", &file) || file.fp == NULL) {
    sq_fclose(&file);
    send_http_error(conn, 500, http_500_error,
                    "fopen(%s): %s", path, strerror(ERRNO));
  } else {
    fclose_on_exec(&file);
    range = sq_get_header(conn, "Content-Range");
    r1 = r2 = 0;
    if (range != NULL && parse_range_header(range, &r1, &r2) > 0) {
      conn->status_code = 206;
      fseeko(file.fp, r1, SEEK_SET);
    }
    if (!forward_body_data(conn, file.fp, INVALID_SOCKET, NULL)) {
      conn->status_code = 500;
    }
    sq_printf(conn, "HTTP/1.1 %d OK\r\nContent-Length: 0\r\n\r\n",
              conn->status_code);
    sq_fclose(&file);
  }
}

static void send_ssi_file(struct sq_connection *, const char *,
                          struct file *, int);

static void do_ssi_include(struct sq_connection *conn, const char *ssi,
                           char *tag, int include_level) {
  char file_name[SQ_BUF_LEN], path[PATH_MAX], *p;
  struct file file = STRUCT_FILE_INITIALIZER;

  // sscanf() is safe here, since send_ssi_file() also uses buffer
  // of size SQ_BUF_LEN to get the tag. So strlen(tag) is always < SQ_BUF_LEN.
  if (sscanf(tag, " virtual=\"%[^\"]\"", file_name) == 1) {
    // File name is relative to the webserver root
    (void) sq_snprintf(conn, path, sizeof(path), "%s%c%s",
        conn->ctx->config[DOCUMENT_ROOT], '/', file_name);
  } else if (sscanf(tag, " abspath=\"%[^\"]\"", file_name) == 1) {
    // File name is relative to the webserver working directory
    // or it is absolute system path
    (void) sq_snprintf(conn, path, sizeof(path), "%s", file_name);
  } else if (sscanf(tag, " file=\"%[^\"]\"", file_name) == 1 ||
             sscanf(tag, " \"%[^\"]\"", file_name) == 1) {
    // File name is relative to the currect document
    (void) sq_snprintf(conn, path, sizeof(path), "%s", ssi);
    if ((p = strrchr(path, '/')) != NULL) {
      p[1] = '\0';
    }
    (void) sq_snprintf(conn, path + strlen(path),
        sizeof(path) - strlen(path), "%s", file_name);
  } else {
    cry(conn, "Bad SSI #include: [%s]", tag);
    return;
  }

  if (!sq_fopen(conn, path, "rb", &file)) {
    cry(conn, "Cannot open SSI #include: [%s]: fopen(%s): %s",
        tag, path, strerror(ERRNO));
  } else {
    fclose_on_exec(&file);
    if (match_prefix(conn->ctx->config[SSI_EXTENSIONS],
                     strlen(conn->ctx->config[SSI_EXTENSIONS]), path) > 0) {
      send_ssi_file(conn, path, &file, include_level + 1);
    } else {
      send_file_data(conn, &file, 0, INT64_MAX);
    }
    sq_fclose(&file);
  }
}

#if !defined(NO_POPEN)
static void do_ssi_exec(struct sq_connection *conn, char *tag) {
  char cmd[SQ_BUF_LEN];
  struct file file = STRUCT_FILE_INITIALIZER;

  if (sscanf(tag, " \"%[^\"]\"", cmd) != 1) {
    cry(conn, "Bad SSI #exec: [%s]", tag);
  } else if ((file.fp = popen(cmd, "r")) == NULL) {
    cry(conn, "Cannot SSI #exec: [%s]: %s", cmd, strerror(ERRNO));
  } else {
    send_file_data(conn, &file, 0, INT64_MAX);
    pclose(file.fp);
  }
}
#endif // !NO_POPEN

static int sq_fgetc(struct file *filep, int offset) {
  if (filep->membuf != NULL && offset >=0 && offset < filep->size) {
    return ((unsigned char *) filep->membuf)[offset];
  } else if (filep->fp != NULL) {
    return fgetc(filep->fp);
  } else {
    return EOF;
  }
}

static void send_ssi_file(struct sq_connection *conn, const char *path,
                          struct file *filep, int include_level) {
  char buf[SQ_BUF_LEN];
  int ch, offset, len, in_ssi_tag;

  if (include_level > 10) {
    cry(conn, "SSI #include level is too deep (%s)", path);
    return;
  }

  in_ssi_tag = len = offset = 0;
  while ((ch = sq_fgetc(filep, offset)) != EOF) {
    if (in_ssi_tag && ch == '>') {
      in_ssi_tag = 0;
      buf[len++] = (char) ch;
      buf[len] = '\0';
      assert(len <= (int) sizeof(buf));
      if (len < 6 || memcmp(buf, "<!--#", 5) != 0) {
        // Not an SSI tag, pass it
        (void) sq_write(conn, buf, (size_t) len);
      } else {
        if (!memcmp(buf + 5, "include", 7)) {
          do_ssi_include(conn, path, buf + 12, include_level);
#if !defined(NO_POPEN)
        } else if (!memcmp(buf + 5, "exec", 4)) {
          do_ssi_exec(conn, buf + 9);
#endif // !NO_POPEN
        } else {
          cry(conn, "%s: unknown SSI " "command: \"%s\"", path, buf);
        }
      }
      len = 0;
    } else if (in_ssi_tag) {
      if (len == 5 && memcmp(buf, "<!--#", 5) != 0) {
        // Not an SSI tag
        in_ssi_tag = 0;
      } else if (len == (int) sizeof(buf) - 2) {
        cry(conn, "%s: SSI tag is too large", path);
        len = 0;
      }
      buf[len++] = ch & 0xff;
    } else if (ch == '<') {
      in_ssi_tag = 1;
      if (len > 0) {
        sq_write(conn, buf, (size_t) len);
      }
      len = 0;
      buf[len++] = ch & 0xff;
    } else {
      buf[len++] = ch & 0xff;
      if (len == (int) sizeof(buf)) {
        sq_write(conn, buf, (size_t) len);
        len = 0;
      }
    }
  }

  // Send the rest of buffered data
  if (len > 0) {
    sq_write(conn, buf, (size_t) len);
  }
}

static void handle_ssi_file_request(struct sq_connection *conn,
                                    const char *path) {
  struct file file = STRUCT_FILE_INITIALIZER;

  if (!sq_fopen(conn, path, "rb", &file)) {
    send_http_error(conn, 500, http_500_error, "fopen(%s): %s", path,
                    strerror(ERRNO));
  } else {
    conn->must_close = 1;
    fclose_on_exec(&file);
    sq_printf(conn, "HTTP/1.1 200 OK\r\n"
              "Content-Type: text/html\r\nConnection: close\r\n\r\n");
    send_ssi_file(conn, path, &file, 0);
    sq_fclose(&file);
  }
}

static void send_options(struct sq_connection *conn) {
  conn->status_code = 200;

  #ifdef ALLOW_UNSAFE_HTTP_METHODS
  sq_printf(conn, "%s", "HTTP/1.1 200 OK\r\n"
            "Allow: GET, POST, HEAD, CONNECT, PUT, DELETE, OPTIONS\r\n"
            "DAV: 1\r\n"
            "Content-Length: 0\r\n\r\n");
  #else
    sq_printf(conn, "%s", "HTTP/1.1 200 OK\r\n"
            "Allow: GET, POST, HEAD, OPTIONS\r\n"
            "Content-Length: 0\r\n\r\n");
  #endif
}

#if defined(USE_WEBSOCKET)

static void base64_encode(const unsigned char *src, int src_len, char *dst) {
  static const char *b64 =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  int i, j, a, b, c;

  for (i = j = 0; i < src_len; i += 3) {
    a = src[i];
    b = i + 1 >= src_len ? 0 : src[i + 1];
    c = i + 2 >= src_len ? 0 : src[i + 2];

    dst[j++] = b64[a >> 2];
    dst[j++] = b64[((a & 3) << 4) | (b >> 4)];
    if (i + 1 < src_len) {
      dst[j++] = b64[(b & 15) << 2 | (c >> 6)];
    }
    if (i + 2 < src_len) {
      dst[j++] = b64[c & 63];
    }
  }
  while (j % 4 != 0) {
    dst[j++] = '=';
  }
  dst[j++] = '\0';
}

static void send_websocket_handshake(struct sq_connection *conn) {
  static const char *magic = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
  char buf[100], sha[20], b64_sha[sizeof(sha) * 2];
  SHA_CTX sha_ctx;

  sq_snprintf(conn, buf, sizeof(buf), "%s%s",
              sq_get_header(conn, "Sec-WebSocket-Key"), magic);
  SHA1_Init(&sha_ctx);
  SHA1_Update(&sha_ctx, (unsigned char *) buf, strlen(buf));
  SHA1_Final((unsigned char *) sha, &sha_ctx);
  base64_encode((unsigned char *) sha, sizeof(sha), b64_sha);
  sq_printf(conn, "%s%s%s",
            "HTTP/1.1 101 Switching Protocols\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            "Sec-WebSocket-Accept: ", b64_sha, "\r\n\r\n");
}

static void read_websocket(struct sq_connection *conn) {
  // Pointer to the beginning of the portion of the incoming websocket message
  // queue. The original websocket upgrade request is never removed,
  // so the queue begins after it.
  unsigned char *buf = (unsigned char *) conn->buf + conn->request_len;
  int bits, n, stop = 0;
  size_t i, len, mask_len, data_len, header_len, body_len;
  // data points to the place where the message is stored when passed to the
  // websocket_data callback. This is either mem on the stack,
  // or a dynamically allocated buffer if it is too large.
  char mem[4 * 1024], mask[4], *data;

  assert(conn->content_len == 0);

  // Loop continuously, reading messages from the socket, invoking the callback,
  // and waiting repeatedly until an error occurs.
  while (!stop) {
    header_len = 0;
    // body_len is the length of the entire queue in bytes
    // len is the length of the current message
    // data_len is the length of the current message's data payload
    // header_len is the length of the current message's header
    if ((body_len = conn->data_len - conn->request_len) >= 2) {
      len = buf[1] & 127;
      mask_len = buf[1] & 128 ? 4 : 0;
      if (len < 126 && body_len >= mask_len) {
        data_len = len;
        header_len = 2 + mask_len;
      } else if (len == 126 && body_len >= 4 + mask_len) {
        header_len = 4 + mask_len;
        data_len = ((((int) buf[2]) << 8) + buf[3]);
      } else if (body_len >= 10 + mask_len) {
        header_len = 10 + mask_len;
        data_len = (((uint64_t) htonl(* (uint32_t *) &buf[2])) << 32) +
          htonl(* (uint32_t *) &buf[6]);
      }
    }

    // Data layout is as follows:
    //  conn->buf               buf
    //     v                     v              frame1           | frame2
    //     |---------------------|----------------|--------------|-------
    //     |                     |<--header_len-->|<--data_len-->|
    //     |<-conn->request_len->|<-----body_len----------->|
    //     |<-------------------conn->data_len------------->|

    if (header_len > 0) {
      // Allocate space to hold websocket payload
      data = mem;
      if (data_len > sizeof(mem) && (data = malloc(data_len)) == NULL) {
        // Allocation failed, exit the loop and then close the connection
        // TODO: notify user about the failure
        break;
      }

      // Save mask and bits, otherwise it may be clobbered by memmove below
      bits = buf[0];
      memcpy(mask, buf + header_len - mask_len, mask_len);

      // Read frame payload into the allocated buffer.
      assert(body_len >= header_len);
      if (data_len + header_len > body_len) {
        len = body_len - header_len;
        memcpy(data, buf + header_len, len);
        // TODO: handle pull error
        pull_all(NULL, conn, data + len, data_len - len);
        conn->data_len = conn->request_len;
      } else {
        len = data_len + header_len;
        memcpy(data, buf + header_len, data_len);
        memmove(buf, buf + len, body_len - len);
        conn->data_len -= len;
      }

      // Apply mask if necessary
      if (mask_len > 0) {
        for (i = 0; i < data_len; i++) {
          data[i] ^= mask[i % 4];
        }
      }

      // Exit the loop if callback signalled to exit,
      // or "connection close" opcode received.
      if ((bits & WEBSOCKET_OPCODE_CONNECTION_CLOSE) ||
          (conn->ctx->callbacks.websocket_data != NULL &&
           !conn->ctx->callbacks.websocket_data(conn, bits, data, data_len))) {
        stop = 1;
      }

      if (data != mem) {
        free(data);
      }
      // Not breaking the loop, process next websocket frame.
    } else {
      // Buffering websocket request
      if ((n = pull(NULL, conn, conn->buf + conn->data_len,
                    conn->buf_size - conn->data_len)) <= 0) {
        break;
      }
      conn->data_len += n;
    }
  }
}

int sq_websocket_write(struct sq_connection* conn, int opcode,
                       const char *data, size_t data_len) {
    unsigned char *copy;
    size_t copy_len = 0;
    int retval = -1;

    if ((copy = (unsigned char *) malloc(data_len + 10)) == NULL) {
      return -1;
    }

    copy[0] = 0x80 + (opcode & 0x0f);

    // Frame format: http://tools.ietf.org/html/rfc6455#section-5.2
    if (data_len < 126) {
      // Inline 7-bit length field
      copy[1] = data_len;
      memcpy(copy + 2, data, data_len);
      copy_len = 2 + data_len;
    } else if (data_len <= 0xFFFF) {
      // 16-bit length field
      copy[1] = 126;
      * (uint16_t *) (copy + 2) = htons(data_len);
      memcpy(copy + 4, data, data_len);
      copy_len = 4 + data_len;
    } else {
      // 64-bit length field
      copy[1] = 127;
      * (uint32_t *) (copy + 2) = htonl((uint64_t) data_len >> 32);
      * (uint32_t *) (copy + 6) = htonl(data_len & 0xffffffff);
      memcpy(copy + 10, data, data_len);
      copy_len = 10 + data_len;
    }

    // Not thread safe
    if (copy_len > 0) {
      retval = sq_write(conn, copy, copy_len);
    }
    free(copy);

    return retval;
}

static void handle_websocket_request(struct sq_connection *conn) {
  const char *version = sq_get_header(conn, "Sec-WebSocket-Version");
  if (version == NULL || strcmp(version, "13") != 0) {
    send_http_error(conn, 426, "Upgrade Required", "%s", "Upgrade Required");
  } else if (conn->ctx->callbacks.websocket_connect != NULL &&
             conn->ctx->callbacks.websocket_connect(conn) != 0) {
    // Callback has returned non-zero, do not proceed with handshake
  } else {
    send_websocket_handshake(conn);
    if (conn->ctx->callbacks.websocket_ready != NULL) {
      conn->ctx->callbacks.websocket_ready(conn);
    }
    read_websocket(conn);
  }
}

static int is_websocket_request(const struct sq_connection *conn) {
  const char *host, *upgrade, *connection, *version, *key;

  host = sq_get_header(conn, "Host");
  upgrade = sq_get_header(conn, "Upgrade");
  connection = sq_get_header(conn, "Connection");
  key = sq_get_header(conn, "Sec-WebSocket-Key");
  version = sq_get_header(conn, "Sec-WebSocket-Version");

  return host != NULL && upgrade != NULL && connection != NULL &&
    key != NULL && version != NULL &&
    sq_strcasestr(upgrade, "websocket") != NULL &&
    sq_strcasestr(connection, "Upgrade") != NULL;
}
#endif // !USE_WEBSOCKET

static int isbyte(int n) {
  return n >= 0 && n <= 255;
}

static int parse_net(const char *spec, uint32_t *net, uint32_t *mask) {
  int n, a, b, c, d, slash = 32, len = 0;

  if ((sscanf(spec, "%d.%d.%d.%d/%d%n", &a, &b, &c, &d, &slash, &n) == 5 ||
      sscanf(spec, "%d.%d.%d.%d%n", &a, &b, &c, &d, &n) == 4) &&
      isbyte(a) && isbyte(b) && isbyte(c) && isbyte(d) &&
      slash >= 0 && slash < 33) {
    len = n;
    *net = ((uint32_t)a << 24) | ((uint32_t)b << 16) | ((uint32_t)c << 8) | d;
    *mask = slash ? 0xffffffffU << (32 - slash) : 0;
  }

  return len;
}

static int set_throttle(const char *spec, uint32_t remote_ip, const char *uri) {
  int throttle = 0;
  struct vec vec, val;
  uint32_t net, mask;
  char mult;
  double v;

  while ((spec = next_option(spec, &vec, &val)) != NULL) {
    mult = ',';
    if (sscanf(val.ptr, "%lf%c", &v, &mult) < 1 || v < 0 ||
        (lowercase(&mult) != 'k' && lowercase(&mult) != 'm' && mult != ',')) {
      continue;
    }
    v *= lowercase(&mult) == 'k' ? 1024 : lowercase(&mult) == 'm' ? 1048576 : 1;
    if (vec.len == 1 && vec.ptr[0] == '*') {
      throttle = (int) v;
    } else if (parse_net(vec.ptr, &net, &mask) > 0) {
      if ((remote_ip & mask) == net) {
        throttle = (int) v;
      }
    } else if (match_prefix(vec.ptr, vec.len, uri) > 0) {
      throttle = (int) v;
    }
  }

  return throttle;
}

static uint32_t get_remote_ip(const struct sq_connection *conn) {
  return ntohl(* (uint32_t *) &conn->client.rsa.sin.sin_addr);
}

#ifdef USE_LUA
#include "mod_lua.c"
#endif // USE_LUA

int sq_upload(struct sq_connection *conn, const char *destination_dir) {
  const char *content_type_header, *boundary_start;
  char buf[SQ_BUF_LEN], path[PATH_MAX], fname[1024], boundary[100], *s;
  FILE *fp;
  int bl, n, i, j, headers_len, boundary_len, eof,
      len = 0, num_uploaded_files = 0;

  // Request looks like this:
  //
  // POST /upload HTTP/1.1
  // Host: 127.0.0.1:8080
  // Content-Length: 244894
  // Content-Type: multipart/form-data; boundary=----WebKitFormBoundaryRVr
  //
  // ------WebKitFormBoundaryRVr
  // Content-Disposition: form-data; name="file"; filename="accum.png"
  // Content-Type: image/png
  //
  //  <89>PNG
  //  <PNG DATA>
  // ------WebKitFormBoundaryRVr

  // Extract boundary string from the Content-Type header
  if ((content_type_header = sq_get_header(conn, "Content-Type")) == NULL ||
      (boundary_start = sq_strcasestr(content_type_header,
                                      "boundary=")) == NULL ||
      (sscanf(boundary_start, "boundary=\"%99[^\"]\"", boundary) == 0 &&
       sscanf(boundary_start, "boundary=%99s", boundary) == 0) ||
      boundary[0] == '\0') {
    return num_uploaded_files;
  }

  boundary_len = strlen(boundary);
  bl = boundary_len + 4;  // \r\n--<boundary>
  for (;;) {
    // Pull in headers
    assert(len >= 0 && len <= (int) sizeof(buf));
    while ((n = sq_read(conn, buf + len, sizeof(buf) - len)) > 0) {
      len += n;
    }
    if ((headers_len = get_request_len(buf, len)) <= 0) {
      break;
    }

    // Fetch file name.
    fname[0] = '\0';
    for (i = j = 0; i < headers_len; i++) {
      if (buf[i] == '\r' && buf[i + 1] == '\n') {
        buf[i] = buf[i + 1] = '\0';
        // TODO(lsm): don't expect filename to be the 3rd field,
        // parse the header properly instead.
        sscanf(&buf[j], "Content-Disposition: %*s %*s filename=\"%1023[^\"]",
               fname);
        j = i + 2;
      }
    }

    // Give up if the headers are not what we expect
    if (fname[0] == '\0') {
      break;
    }

    // Move data to the beginning of the buffer
    assert(len >= headers_len);
    memmove(buf, &buf[headers_len], len - headers_len);
    len -= headers_len;

    // We open the file with exclusive lock held. This guarantee us
    // there is no other thread can save into the same file simultaneously.
    fp = NULL;
    // Construct destination file name. Do not allow paths to have slashes.
    if ((s = strrchr(fname, '/')) == NULL &&
        (s = strrchr(fname, '\\')) == NULL) {
      s = fname;
    }

    // Open file in binary mode. TODO: set an exclusive lock.
    snprintf(path, sizeof(path), "%s/%s", destination_dir, s);
    if ((fp = fopen(path, "wb")) == NULL) {
      break;
    }

    // Read POST data, write into file until boundary is found.
    eof = n = 0;
    do {
      len += n;
      for (i = 0; i < len - bl; i++) {
        if (!memcmp(&buf[i], "\r\n--", 4) &&
            !memcmp(&buf[i + 4], boundary, boundary_len)) {
          // Found boundary, that's the end of file data.
          fwrite(buf, 1, i, fp);
          eof = 1;
          memmove(buf, &buf[i + bl], len - (i + bl));
          len -= i + bl;
          break;
        }
      }
      if (!eof && len > bl) {
        fwrite(buf, 1, len - bl, fp);
        memmove(buf, &buf[len - bl], bl);
        len = bl;
      }
    } while (!eof && (n = sq_read(conn, buf + len, sizeof(buf) - len)) > 0);
    fclose(fp);
    if (eof) {
      num_uploaded_files++;
      if (conn->ctx->callbacks.upload != NULL) {
        conn->ctx->callbacks.upload(conn, path);
      }
    }
  }

  return num_uploaded_files;
}

static int is_put_or_delete_request(const struct sq_connection *conn) {
  const char *s = conn->request_info.request_method;
  return s != NULL && (!strcmp(s, "PUT") ||
                       !strcmp(s, "DELETE"));
}

static int get_first_ssl_listener_index(const struct sq_context *ctx) {
  int i, index = -1;
  for (i = 0; index == -1 && i < ctx->num_listening_sockets; i++) {
    index = ctx->listening_sockets[i].is_ssl ? i : -1;
  }
  return index;
}

static void redirect_to_https_port(struct sq_connection *conn, int ssl_index) {
  char host[1025];
  const char *host_header;

  if ((host_header = sq_get_header(conn, "Host")) == NULL ||
      sscanf(host_header, "%1024[^:]", host) == 0) {
    // Cannot get host from the Host: header. Fallback to our IP address.
    sockaddr_to_string(host, sizeof(host), &conn->client.lsa);
  }

  sq_printf(conn, "HTTP/1.1 302 Found\r\nLocation: https://%s:%d%s\r\n\r\n",
            host, (int) ntohs(conn->ctx->listening_sockets[ssl_index].
                              lsa.sin.sin_port), conn->request_info.uri);
}

// This is the heart of the Squeasel's logic.
// This function is called when the request is read, parsed and validated,
// and Squeasel must decide what action to take: serve a file, or
// a directory, or call embedded function, etcetera.
static void handle_request(struct sq_connection *conn) {
  struct sq_request_info *ri = &conn->request_info;
  char path[PATH_MAX];
  int uri_len, ssl_index;
  struct file file = STRUCT_FILE_INITIALIZER;
  sq_callback_result_t callback_result = SQ_HANDLED_OK;

  if ((conn->request_info.query_string = strchr(ri->uri, '?')) != NULL) {
    * ((char *) conn->request_info.query_string++) = '\0';
  }
  uri_len = (int) strlen(ri->uri);
  sq_url_decode(ri->uri, uri_len, (char *) ri->uri, uri_len + 1, 0);
  remove_double_dots_and_double_slashes((char *) ri->uri);
  convert_uri_to_file_name(conn, path, sizeof(path), &file);
  conn->throttle = set_throttle(conn->ctx->config[THROTTLE],
                                get_remote_ip(conn), ri->uri);

  DEBUG_TRACE(("%s", ri->uri));
  // Perform redirect and auth checks before calling begin_request() handler.
  // Otherwise, begin_request() would need to perform auth checks and redirects.
  if (!conn->client.is_ssl && conn->client.ssl_redir &&
      (ssl_index = get_first_ssl_listener_index(conn->ctx)) > -1) {
    redirect_to_https_port(conn, ssl_index);
  } else if (!is_put_or_delete_request(conn) &&
             !check_authorization(conn, path)) {
    send_authorization_request(conn);
  } else if (conn->ctx->callbacks.begin_request != NULL &&
      ((callback_result = conn->ctx->callbacks.begin_request(conn))
          != SQ_CONTINUE_HANDLING)) {
    // Do nothing, callback has served the request
    conn->must_close = (callback_result == SQ_HANDLED_CLOSE_CONNECTION);
#if defined(USE_WEBSOCKET)
  } else if (is_websocket_request(conn)) {
    handle_websocket_request(conn);
#endif
  } else if (!strcmp(ri->request_method, "OPTIONS")) {
    send_options(conn);
  } else if (conn->ctx->config[DOCUMENT_ROOT] == NULL) {
    send_http_error(conn, 404, "Not Found", "Not Found");
  } else if (is_put_or_delete_request(conn) &&
             (is_authorized_for_put(conn) != 1)) {
    send_authorization_request(conn);
  } else if (!strcmp(ri->request_method, "PUT")) {
    put_file(conn, path);
  } else if (!strcmp(ri->request_method, "DELETE")) {
      struct de de;
      memset(&de.file, 0, sizeof(de.file));
      if(!sq_stat(conn, path, &de.file)) {
          send_http_error(conn, 404, "Not Found", "%s", "File not found");
      } else {
          if(de.file.modification_time) {
              if(de.file.is_directory) {
                  remove_directory(conn, path);
                  send_http_error(conn, 204, "No Content", "%s", "");
              } else if (sq_remove(path) == 0) {
                  send_http_error(conn, 204, "No Content", "%s", "");
              } else {
                  send_http_error(conn, 423, "Locked", "remove(%s): %s", path,
                          strerror(ERRNO));
              }
          }
          else {
              send_http_error(conn, 500, http_500_error, "remove(%s): %s", path,
                    strerror(ERRNO));
          }
      }
  } else if ((file.membuf == NULL && file.modification_time == (time_t) 0) ||
             must_hide_file(conn, path)) {
    send_http_error(conn, 404, "Not Found", "%s", "File not found");
  } else if (file.is_directory && ri->uri[uri_len - 1] != '/') {
    sq_printf(conn, "HTTP/1.1 301 Moved Permanently\r\n"
              "Location: %s/\r\n\r\n", ri->uri);
  } else if (file.is_directory &&
             !substitute_index_file(conn, path, sizeof(path), &file)) {
    if (!sq_strcasecmp(conn->ctx->config[ENABLE_DIRECTORY_LISTING], "yes")) {
      handle_directory_request(conn, path);
    } else {
      send_http_error(conn, 403, "Directory Listing Denied",
          "Directory listing denied");
    }
#ifdef USE_LUA
  } else if (match_prefix("**.lp$", 6, path) > 0) {
    handle_lsp_request(conn, path, &file, NULL);
#endif
#if !defined(NO_CGI)
  } else if (match_prefix(conn->ctx->config[CGI_EXTENSIONS],
                          strlen(conn->ctx->config[CGI_EXTENSIONS]),
                          path) > 0) {
    if (strcmp(ri->request_method, "POST") &&
        strcmp(ri->request_method, "HEAD") &&
        strcmp(ri->request_method, "GET")) {
      send_http_error(conn, 501, "Not Implemented",
                      "Method %s is not implemented", ri->request_method);
    } else {
      handle_cgi_request(conn, path);
    }
#endif // !NO_CGI
  } else if (match_prefix(conn->ctx->config[SSI_EXTENSIONS],
                          strlen(conn->ctx->config[SSI_EXTENSIONS]),
                          path) > 0) {
    handle_ssi_file_request(conn, path);
  } else if (is_not_modified(conn, &file)) {
    send_http_error(conn, 304, "Not Modified", "%s", "");
  } else {
    handle_file_request(conn, path, &file);
  }
}

static void close_all_listening_sockets(struct sq_context *ctx) {
  int i;
  for (i = 0; i < ctx->num_listening_sockets; i++) {
    closesocket(ctx->listening_sockets[i].sock);
  }
  free(ctx->listening_sockets);
}

static int is_valid_port(unsigned int port) {
  return port <= 0xffff;
}

// Valid listening port specification is: [ip_address:]port[s]
// Examples: 80, 443s, 127.0.0.1:3128, 1.2.3.4:8080s
// TODO(lsm): add parsing of the IPv6 address
static int parse_port_string(const struct vec *vec, struct socket *so) {
  unsigned int a, b, c, d, ch, port;
  int len;
#if defined(USE_IPV6)
  char buf[100];
#endif

  // MacOS needs that. If we do not zero it, subsequent bind() will fail.
  // Also, all-zeroes in the socket address means binding to all addresses
  // for both IPv4 and IPv6 (INADDR_ANY and IN6ADDR_ANY_INIT).
  memset(so, 0, sizeof(*so));
  so->lsa.sin.sin_family = AF_INET;

  if (sscanf(vec->ptr, "%u.%u.%u.%u:%u%n", &a, &b, &c, &d, &port, &len) == 5) {
    // Bind to a specific IPv4 address, e.g. 192.168.1.5:8080
    so->lsa.sin.sin_addr.s_addr = htonl((a << 24) | (b << 16) | (c << 8) | d);
    so->lsa.sin.sin_port = htons((uint16_t) port);
#if defined(USE_IPV6)

  } else if (sscanf(vec->ptr, "[%49[^]]]:%d%n", buf, &port, &len) == 2 &&
             inet_pton(AF_INET6, buf, &so->lsa.sin6.sin6_addr)) {
    // IPv6 address, e.g. [3ffe:2a00:100:7031::1]:8080
    so->lsa.sin6.sin6_family = AF_INET6;
    so->lsa.sin6.sin6_port = htons((uint16_t) port);
#endif
  } else if (sscanf(vec->ptr, "%u%n", &port, &len) == 1) {
    // If only port is specified, bind to IPv4, INADDR_ANY
    so->lsa.sin.sin_port = htons((uint16_t) port);
  } else {
    port = len = 0;   // Parsing failure. Make port invalid.
  }

  ch = vec->ptr[len];  // Next character after the port number
  so->is_ssl = ch == 's';
  so->ssl_redir = ch == 'r';

  // Make sure the port is valid and vector ends with 's', 'r' or ','
  return is_valid_port(port) &&
    (ch == '\0' || ch == 's' || ch == 'r' || ch == ',');
}

static int set_ports_option(struct sq_context *ctx) {
  const char *list = ctx->config[LISTENING_PORTS];
  int on = 1, success = 1;
#if defined(USE_IPV6)
  int off = 0;
#endif
  struct vec vec;
  struct socket so, *ptr;

  while (success && (list = next_option(list, &vec, NULL)) != NULL) {
    if (!parse_port_string(&vec, &so)) {
      cry(fc(ctx), "%s: %.*s: invalid port spec. Expecting list of: %s",
          __func__, (int) vec.len, vec.ptr, "[IP_ADDRESS:]PORT[s|r]");
      success = 0;
    } else if (so.is_ssl && ctx->ssl_ctx == NULL) {
      cry(fc(ctx), "Cannot add SSL socket, is -ssl_certificate option set?");
      success = 0;
    } else if ((so.sock = socket(so.lsa.sa.sa_family, SOCK_STREAM, 6)) ==
               INVALID_SOCKET ||
               // On Windows, SO_REUSEADDR is recommended only for
               // broadcast UDP sockets
               setsockopt(so.sock, SOL_SOCKET, SO_REUSEADDR,
                          (void *) &on, sizeof(on)) != 0 ||
#if defined(USE_IPV6)
               (so.lsa.sa.sa_family == AF_INET6 &&
                setsockopt(so.sock, IPPROTO_IPV6, IPV6_V6ONLY, (void *) &off,
                           sizeof(off)) != 0) ||
#endif
               bind(so.sock, &so.lsa.sa, so.lsa.sa.sa_family == AF_INET ?
                    sizeof(so.lsa.sin) : sizeof(so.lsa)) != 0 ||
               listen(so.sock, SOMAXCONN) != 0) {
      cry(fc(ctx), "%s: cannot bind to %.*s: %d (%s)", __func__,
          (int) vec.len, vec.ptr, ERRNO, strerror(errno));
      closesocket(so.sock);
      success = 0;
    } else if ((ptr = (struct socket *) realloc(ctx->listening_sockets,
                              (ctx->num_listening_sockets + 1) *
                              sizeof(ctx->listening_sockets[0]))) == NULL) {
      closesocket(so.sock);
      success = 0;
    } else {
      set_close_on_exec(so.sock);
      ctx->listening_sockets = ptr;
      ctx->listening_sockets[ctx->num_listening_sockets] = so;
      ctx->num_listening_sockets++;
    }
  }

  if (!success) {
    close_all_listening_sockets(ctx);
  }

  return success;
}

static void log_header(const struct sq_connection *conn, const char *header,
                       FILE *fp) {
  const char *header_value;

  if ((header_value = sq_get_header(conn, header)) == NULL) {
    (void) fprintf(fp, "%s", " -");
  } else {
    (void) fprintf(fp, " \"%s\"", header_value);
  }
}

static void log_access(const struct sq_connection *conn) {
  const struct sq_request_info *ri;
  FILE *fp;
  char date[64], src_addr[IP_ADDR_STR_LEN];

  fp = conn->ctx->config[ACCESS_LOG_FILE] == NULL ?  NULL :
    fopen(conn->ctx->config[ACCESS_LOG_FILE], "a+");

  if (fp == NULL)
    return;

  struct tm the_time;
  strftime(date, sizeof(date), "%d/%b/%Y:%H:%M:%S %z",
           localtime_r(&conn->birth_time, &the_time));

  ri = &conn->request_info;
  flockfile(fp);

  sockaddr_to_string(src_addr, sizeof(src_addr), &conn->client.rsa);
  fprintf(fp, "%s - %s [%s] \"%s %s HTTP/%s\" %d %" INT64_FMT,
          src_addr, ri->remote_user == NULL ? "-" : ri->remote_user, date,
          ri->request_method ? ri->request_method : "-",
          ri->uri ? ri->uri : "-", ri->http_version,
          conn->status_code, conn->num_bytes_sent);
  log_header(conn, "Referer", fp);
  log_header(conn, "User-Agent", fp);
  fputc('\n', fp);
  fflush(fp);

  funlockfile(fp);
  fclose(fp);
}

// Verify given socket address against the ACL.
// Return -1 if ACL is malformed, 0 if address is disallowed, 1 if allowed.
static int check_acl(struct sq_context *ctx, uint32_t remote_ip) {
  int allowed, flag;
  uint32_t net, mask;
  struct vec vec;
  const char *list = ctx->config[ACCESS_CONTROL_LIST];

  // If any ACL is set, deny by default
  allowed = list == NULL ? '+' : '-';

  while ((list = next_option(list, &vec, NULL)) != NULL) {
    flag = vec.ptr[0];
    if ((flag != '+' && flag != '-') ||
        parse_net(&vec.ptr[1], &net, &mask) == 0) {
      cry(fc(ctx), "%s: subnet must be [+|-]x.x.x.x[/x]", __func__);
      return -1;
    }

    if (net == (remote_ip & mask)) {
      allowed = flag;
    }
  }

  return allowed == '+';
}

static int set_uid_option(struct sq_context *ctx) {
  struct passwd *pw;
  const char *uid = ctx->config[RUN_AS_USER];
  int success = 0;

  if (uid == NULL) {
    success = 1;
  } else {
    if ((pw = getpwnam(uid)) == NULL) {
      cry(fc(ctx), "%s: unknown user [%s]", __func__, uid);
    } else if (setgid(pw->pw_gid) == -1) {
      cry(fc(ctx), "%s: setgid(%s): %s", __func__, uid, strerror(errno));
    } else if (setuid(pw->pw_uid) == -1) {
      cry(fc(ctx), "%s: setuid(%s): %s", __func__, uid, strerror(errno));
    } else {
      success = 1;
    }
  }

  return success;
}

#if !defined(NO_SSL)

static pthread_mutex_t *ssl_mutexes;

#if OPENSSL_VERSION_NUMBER < 0x10100000L
// IMPALA-11195: disable TLS/SSL renegotiation. In version 1.0.2 and prior it's
// possible to use the undocumented SSL3_FLAGS_NO_RENEGOTIATE_CIPHERS flag.
static void ssl_disable_renegotiation_cb(const SSL *ssl, int where, int ret)
{
    (void)ret;
    if ((where & SSL_CB_HANDSHAKE_DONE) != 0) {
        // disable renegotiation (CVE-2009-3555)
        ssl->s3->flags |= SSL3_FLAGS_NO_RENEGOTIATE_CIPHERS;
    }
}
#endif

static int sslize(struct sq_connection *conn, SSL_CTX *s, int (*func)(SSL *)) {
  return (conn->ssl = SSL_new(s)) != NULL &&
    SSL_set_fd(conn->ssl, conn->client.sock) == 1 &&
    func(conn->ssl) == 1;
}

// Return OpenSSL error message
static const char *ssl_error(void) {
  unsigned long err;
  err = ERR_get_error();
  return err == 0 ? "" : ERR_error_string(err, NULL);
}

static void ssl_locking_callback(int mode, int mutex_num, const char *file,
                                 int line) {
  (void) line;
  (void) file;

  if (mode & 1) {  // 1 is CRYPTO_LOCK
    (void) pthread_mutex_lock(&ssl_mutexes[mutex_num]);
  } else {
    (void) pthread_mutex_unlock(&ssl_mutexes[mutex_num]);
  }
}

static unsigned long ssl_id_callback(void) {
  return (unsigned long) pthread_self();
}

static int ssl_password_callback(char *password, int size, int unused, void *data) {
  struct sq_context *ctx = (struct sq_context*)data;
  strncpy(password, ctx->config[SSL_PRIVATE_KEY_PASSWORD], size);
  // See https://www.openssl.org/docs/manmaster/ssl/SSL_CTX_set_default_passwd_cb.html
  // which strongly hints that password must be NULL terminated.
  password[size - 1] = '\0';
  return strlen(password);
}

// Dynamically load SSL library. Set up ctx->ssl_ctx pointer.
static int set_ssl_option(struct sq_context *ctx) {
  int i, size;
  const char *pem;

  // If PEM file is not specified and the init_ssl callback
  // is not specified, skip SSL initialization.
  if ((pem = ctx->config[SSL_CERTIFICATE]) == NULL &&
      ctx->callbacks.init_ssl == NULL) {
    return 1;
  }

  const char *private_key = ctx->config[SSL_PRIVATE_KEY];
  if (private_key == NULL) private_key = pem;

  // Initialize SSL library, unless the user has disabled this.
  int should_init_ssl = (sq_strcasecmp(ctx->config[SSL_GLOBAL_INIT], "yes") == 0);
  if (should_init_ssl) {
    SSL_library_init();
    SSL_load_error_strings();
    // Initialize locking callbacks, needed for thread safety.
    // http://www.openssl.org/support/faq.html#PROG1
    size = sizeof(pthread_mutex_t) * CRYPTO_num_locks();
    if ((ssl_mutexes = (pthread_mutex_t *) malloc((size_t)size)) == NULL) {
      cry(fc(ctx), "%s: cannot allocate mutexes: %s", __func__, ssl_error());
      return 0;
    }

    for (i = 0; i < CRYPTO_num_locks(); i++) {
      pthread_mutex_init(&ssl_mutexes[i], NULL);
    }

    CRYPTO_set_locking_callback(&ssl_locking_callback);
    CRYPTO_set_id_callback(&ssl_id_callback);
  }

  char* ssl_version = ctx->config[SSL_VERSION];
  int options = SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3;
  if (sq_strcasecmp(ssl_version, "tlsv1") == 0) {
    // No-op - don't exclude any TLS protocols.
  } else if (sq_strcasecmp(ssl_version, "tlsv1.1") == 0) {
    if (SSLeay() < OPENSSL_MIN_VERSION_WITH_TLS_1_1) {
      cry(fc(ctx), "Unsupported TLS version: %s", ssl_version);
      return 0;
    }
    options |= SSL_OP_NO_TLSv1;
  } else if (sq_strcasecmp(ssl_version, "tlsv1.2") == 0) {
    if (SSLeay() < OPENSSL_MIN_VERSION_WITH_TLS_1_1) {
      cry(fc(ctx), "Unsupported TLS version: %s", ssl_version);
      return 0;
    }
    options |= (SSL_OP_NO_TLSv1 | SSL_OP_NO_TLSv1_1);
  } else if (sq_strcasecmp(ssl_version, "tlsv1.3") == 0) {
    if (SSLeay() < OPENSSL_MIN_VERSION_WITH_TLS_1_3) {
      cry(fc(ctx), "Unsupported TLS version: %s", ssl_version);
      return 0;
    }
    options |= (SSL_OP_NO_TLSv1 | SSL_OP_NO_TLSv1_1 | SSL_OP_NO_TLSv1_2);
  } else {
    cry(fc(ctx), "%s: unknown SSL version: %s", __func__, ssl_version);
    return 0;
  }

  ctx->ssl_ctx = SSL_CTX_new(SSLv23_method());
  if (ctx->ssl_ctx == NULL) {
    unsigned long err_code = ERR_peek_error();
    // If it looks like the error is due to SSL not being initialized,
    // provide a better error.
    if (!should_init_ssl &&
        ERR_GET_LIB(err_code) == ERR_LIB_SSL &&
        ERR_GET_REASON(err_code) == SSL_R_LIBRARY_HAS_NO_CIPHERS) {
      cry(fc(ctx), "SSL_CTX_new failed: %s was disabled: OpenSSL must "
                   "be initialized before starting squeasel",
                   config_options[SSL_GLOBAL_INIT * 2]);
    }
    cry(fc(ctx), "SSL_CTX_new (server) error: %s", ssl_error());
    return 0;
  }

#if OPENSSL_VERSION_NUMBER > 0x1010007fL
  // IMPALA-11195: disable TLS/SSL renegotiation.
  // See https://www.openssl.org/docs/man1.1.0/man3/SSL_set_options.html for
  // details. SSL_OP_NO_RENEGOTIATION option was back-ported from 1.1.1-dev to
  // 1.1.0h, so this is a best-effort approach if the binary compiled with
  // newer as per information in the CHANGES file for
  // 'Changes between 1.1.0g and 1.1.0h [27 Mar 2018]':
  //     Note that if an application built against 1.1.0h headers (or above) is
  //     run using an older version of 1.1.0 (prior to 1.1.0h) then the option
  //     will be accepted but nothing will happen, i.e. renegotiation will
  //     not be prevented.
  options |= SSL_OP_NO_RENEGOTIATION;
#elif OPENSSL_VERSION_NUMBER < 0x10100000L
  // IMPALA-11195: disable TLS/SSL renegotiation. In version 1.0.2 and prior it's
  // possible to use the undocumented SSL3_FLAGS_NO_RENEGOTIATE_CIPHERS flag.
  // We need to set the flag in the callback 'ssl_disable_renegotiation_cb' after
  // handshake is done, otherwise the flag would get reset in SSL_accept().
  SSL_CTX_set_info_callback(ctx->ssl_ctx, ssl_disable_renegotiation_cb);
#else
  static_error(false, "Found SSL version that is vulnerable to CVE-2009-3555.");
#endif

  if ((SSL_CTX_set_options(ctx->ssl_ctx, options) & options) != options) {
    cry(fc(ctx), "SSL_CTX_set_options (server) error: could not set options (%d)",
        options);
    return 0;
  }

  if (ctx->config[SSL_PRIVATE_KEY_PASSWORD] != NULL) {
    SSL_CTX_set_default_passwd_cb(ctx->ssl_ctx, ssl_password_callback);
    SSL_CTX_set_default_passwd_cb_userdata(ctx->ssl_ctx, ctx);
  }

  // If user callback returned non-NULL, that means that user callback has
  // set up certificate itself. In this case, skip sertificate setting.
  if ((ctx->callbacks.init_ssl == NULL ||
       !ctx->callbacks.init_ssl(ctx->ssl_ctx, ctx->user_data)) &&
      (SSL_CTX_use_certificate_file(ctx->ssl_ctx, pem, 1) == 0 ||
       SSL_CTX_use_PrivateKey_file(ctx->ssl_ctx, private_key, 1) == 0)) {
    cry(fc(ctx), "%s: cannot open %s: %s", __func__, pem, ssl_error());
    return 0;
  }

  if (pem != NULL) {
    (void) SSL_CTX_use_certificate_chain_file(ctx->ssl_ctx, pem);
  }

  if (ctx->config[TLS_CIPHERSUITES] != NULL) {
#if OPENSSL_VERSION_NUMBER >= 0x10101000L
    // Set TLSv1.3 ciphers.
    if (SSL_CTX_set_ciphersuites(ctx->ssl_ctx, ctx->config[TLS_CIPHERSUITES]) == 0) {
      cry(fc(ctx), "SSL_CTX_set_ciphersuites: error setting ciphersuites (%s): %s",
          ctx->config[TLS_CIPHERSUITES], ssl_error());
      return 0;
    }
#endif
  }

  if (ctx->config[SSL_CIPHERS] != NULL) {
    if (SSL_CTX_set_cipher_list(ctx->ssl_ctx, ctx->config[SSL_CIPHERS]) == 0) {
      cry(fc(ctx), "SSL_CTX_set_cipher_list: error setting ciphers (%s): %s",
          ctx->config[SSL_CIPHERS], ssl_error());
      return 0;
    }
#ifndef OPENSSL_NO_ECDH
#if OPENSSL_VERSION_NUMBER < 0x10002000L
    // OpenSSL 1.0.1 and below only support setting a single ECDH curve at once.
    // We choose prime256v1 because it's the first curve listed in the "modern
    // compatibility" section of the Mozilla Server Side TLS recommendations,
    // accessed Feb. 2017.
    EC_KEY* ecdh = EC_KEY_new_by_curve_name(NID_X9_62_prime256v1);
    if (ecdh == NULL) {
      cry(fc(ctx), "EC_KEY_new_by_curve_name: %s", ssl_error());
    } else {
      int rc = SSL_CTX_set_tmp_ecdh(ctx->ssl_ctx, ecdh);
      if (rc <= 0) {
        cry(fc(ctx), "SSL_CTX_set_tmp_ecdh: %s", ssl_error());
      }
      EC_KEY_free(ecdh);
    }
#elif OPENSSL_VERSION_NUMBER < 0x10100000L
    // OpenSSL 1.0.2 provides the set_ecdh_auto API which internally figures out
    // the best curve to use.
    int rc = SSL_CTX_set_ecdh_auto(ctx->ssl_ctx, 1);
    if (rc <= 0) {
      cry(fc(ctx), "SSL_CTX_set_ecdh_auto: %s", ssl_error());
    }
#endif
#endif

  }

  return 1;
}

static void uninitialize_ssl(struct sq_context *ctx) {
  int i;
  if (ctx->ssl_ctx != NULL &&
      sq_strcasecmp(ctx->config[SSL_GLOBAL_INIT], "yes") == 0) {
    CRYPTO_set_locking_callback(NULL);
    for (i = 0; i < CRYPTO_num_locks(); i++) {
      pthread_mutex_destroy(&ssl_mutexes[i]);
    }
    CRYPTO_set_id_callback(NULL);
  }
}
#endif // !NO_SSL

static int set_gpass_option(struct sq_context *ctx) {
  struct file file = STRUCT_FILE_INITIALIZER;
  const char *path = ctx->config[GLOBAL_PASSWORDS_FILE];
  if (path != NULL && !sq_stat(fc(ctx), path, &file)) {
    cry(fc(ctx), "Cannot open %s: %s", path, strerror(ERRNO));
    return 0;
  }
  return 1;
}

static int set_acl_option(struct sq_context *ctx) {
  return check_acl(ctx, (uint32_t) 0x7f000001UL) != -1;
}

static void reset_per_request_attributes(struct sq_connection *conn) {
  conn->path_info = NULL;
  conn->num_bytes_sent = conn->consumed_content = 0;
  conn->status_code = -1;
  conn->must_close = conn->request_len = conn->throttle = 0;
}

static void close_socket_gracefully(struct sq_connection *conn) {
  struct linger linger;

  // Set linger option to avoid socket hanging out after close. This prevent
  // ephemeral port exhaust problem under high QPS.
  linger.l_onoff = 1;
  linger.l_linger = 1;
  setsockopt(conn->client.sock, SOL_SOCKET, SO_LINGER,
             (char *) &linger, sizeof(linger));

  // Send FIN to the client
  shutdown(conn->client.sock, SHUT_WR);
  set_non_blocking_mode(conn->client.sock);

  // Now we know that our FIN is ACK-ed, safe to close
  closesocket(conn->client.sock);
}

static void close_connection(struct sq_connection *conn) {
  conn->must_close = 1;

#ifndef NO_SSL
  if (conn->ssl != NULL) {
    // Run SSL_shutdown twice to ensure completly close SSL connection
    SSL_shutdown(conn->ssl);
    SSL_free(conn->ssl);
    conn->ssl = NULL;
  }
#endif
  if (conn->client.sock != INVALID_SOCKET) {
    close_socket_gracefully(conn);
    conn->client.sock = INVALID_SOCKET;
  }
}

void sq_close_connection(struct sq_connection *conn) {
#ifndef NO_SSL
  if (conn->client_ssl_ctx != NULL) {
    SSL_CTX_free((SSL_CTX *) conn->client_ssl_ctx);
  }
#endif
  close_connection(conn);
  free(conn);
}

struct sq_connection *sq_connect(const char *host, int port, int use_ssl,
                                 char *ebuf, size_t ebuf_len) {
  static struct sq_context fake_ctx;
  struct sq_connection *conn = NULL;
  SOCKET sock;

  if ((sock = conn2(host, port, use_ssl, ebuf, ebuf_len)) == INVALID_SOCKET) {
  } else if ((conn = (struct sq_connection *)
              calloc(1, sizeof(*conn) + MAX_REQUEST_SIZE)) == NULL) {
    snprintf(ebuf, ebuf_len, "calloc(): %s", strerror(ERRNO));
    closesocket(sock);
#ifndef NO_SSL
  } else if (use_ssl && (conn->client_ssl_ctx =
                         SSL_CTX_new(SSLv23_client_method())) == NULL) {
    snprintf(ebuf, ebuf_len, "SSL_CTX_new error");
    closesocket(sock);
    free(conn);
    conn = NULL;
#endif // NO_SSL
  } else {
    socklen_t len = sizeof(struct sockaddr);
    conn->buf_size = MAX_REQUEST_SIZE;
    conn->buf = (char *) (conn + 1);
    conn->ctx = &fake_ctx;
    conn->client.sock = sock;
    getsockname(sock, &conn->client.rsa.sa, &len);
    conn->client.is_ssl = use_ssl;
#ifndef NO_SSL
    if (use_ssl) {
      // SSL_CTX_set_verify call is needed to switch off server certificate
      // checking, which is off by default in OpenSSL and on in yaSSL.
      SSL_CTX_set_verify(conn->client_ssl_ctx, 0, 0);
      sslize(conn, conn->client_ssl_ctx, SSL_connect);
    }
#endif
  }

  return conn;
}

static int is_valid_uri(const char *uri) {
  // Conform to http://www.w3.org/Protocols/rfc2616/rfc2616-sec5.html#sec5.1.2
  // URI can be an asterisk (*) or should start with slash.
  return uri[0] == '/' || (uri[0] == '*' && uri[1] == '\0');
}


typedef enum {
  GETREQ_OK,
  GETREQ_KEEPALIVE_TIMEOUT,
  GETREQ_ERROR
} GetReqResult;

static GetReqResult getreq(struct sq_connection *conn, char *ebuf, size_t ebuf_len) {
  const char *cl;

  ebuf[0] = '\0';
  reset_per_request_attributes(conn);
  conn->request_len = read_request(NULL, conn, conn->buf, conn->buf_size,
                                   &conn->data_len);
  assert(conn->request_len < 0 || conn->data_len >= conn->request_len);

  if (conn->request_len == 0 && conn->data_len == conn->buf_size) {
    snprintf(ebuf, ebuf_len, "%s", "Request Too Large");
    return GETREQ_ERROR;
  } else if (conn->request_len <= 0) {
    return GETREQ_KEEPALIVE_TIMEOUT;
  } else if (parse_http_message(conn->buf, conn->buf_size,
                                &conn->request_info) <= 0) {
    snprintf(ebuf, ebuf_len, "Bad request: [%.*s]", conn->data_len, conn->buf);
    return GETREQ_ERROR;
  } else {
    // Request is valid
    if ((cl = get_header(&conn->request_info, "Content-Length")) != NULL) {
      conn->content_len = strtoll(cl, NULL, 10);
    } else if (!sq_strcasecmp(conn->request_info.request_method, "POST") ||
               !sq_strcasecmp(conn->request_info.request_method, "PUT")) {
      conn->content_len = -1;
    } else {
      conn->content_len = 0;
    }
    conn->birth_time = time(NULL);
  }
  return GETREQ_OK;
}

struct sq_connection *sq_download(const char *host, int port, int use_ssl,
                                  char *ebuf, size_t ebuf_len,
                                  const char *fmt, ...) {
  struct sq_connection *conn;
  va_list ap;

  va_start(ap, fmt);
  ebuf[0] = '\0';
  if ((conn = sq_connect(host, port, use_ssl, ebuf, ebuf_len)) == NULL) {
  } else if (sq_vprintf(conn, fmt, ap) <= 0) {
    snprintf(ebuf, ebuf_len, "%s", "Error sending request");
  } else {
    getreq(conn, ebuf, ebuf_len);
  }
  if (ebuf[0] != '\0' && conn != NULL) {
    sq_close_connection(conn);
    conn = NULL;
  }
  va_end(ap);

  return conn;
}

static void process_new_connection(struct sq_connection *conn) {
  struct sq_request_info *ri = &conn->request_info;
  int keep_alive_enabled, keep_alive, discard_len;
  char ebuf[100];
  GetReqResult getreq_status;

  keep_alive_enabled = !strcmp(conn->ctx->config[ENABLE_KEEP_ALIVE], "yes");
  keep_alive = 0;

  // Important: on new connection, reset the receiving buffer. Credit goes
  // to crule42.
  conn->data_len = 0;
  do {
    getreq_status = getreq(conn, ebuf, sizeof(ebuf));
    if (getreq_status != GETREQ_OK) {
      if (getreq_status == GETREQ_ERROR) {
        send_http_error(conn, 500, "Server Error", "%s", ebuf);
      }
      conn->must_close = 1;
    } else if (!is_valid_uri(conn->request_info.uri)) {
      char* encoded = (char*) malloc(SQ_BUF_LEN);
      sq_strip_tags(ri->uri, encoded, SQ_BUF_LEN);
      snprintf(ebuf, sizeof(ebuf), "Invalid URI: [%s]", encoded);
      free(encoded);
      send_http_error(conn, 400, "Bad Request", "%s", ebuf);
    } else if (strcmp(ri->http_version, "1.0") &&
               strcmp(ri->http_version, "1.1")) {
      snprintf(ebuf, sizeof(ebuf), "Bad HTTP version: [%s]", ri->http_version);
      send_http_error(conn, 505, "Bad HTTP version", "%s", ebuf);
    }

    if (getreq_status == GETREQ_OK) {
      handle_request(conn);
      if (conn->ctx->callbacks.end_request != NULL) {
        conn->ctx->callbacks.end_request(conn, conn->status_code);
      }
      log_access(conn);
    }
    if (ri->remote_user != NULL) {
      free((void *) ri->remote_user);
      // Important! When having connections with and without auth
      // would cause double free and then crash
      ri->remote_user = NULL;
    }

    // NOTE(lsm): order is important here. should_keep_alive() call
    // is using parsed request, which will be invalid after memmove's below.
    // Therefore, memorize should_keep_alive() result now for later use
    // in loop exit condition.
    keep_alive = conn->ctx->stop_flag == 0 && keep_alive_enabled &&
      conn->content_len >= 0 && should_keep_alive(conn);

    // Discard all buffered data for this request
    discard_len = conn->content_len >= 0 && conn->request_len > 0 &&
      conn->request_len + conn->content_len < (int64_t) conn->data_len ?
      (int) (conn->request_len + conn->content_len) : conn->data_len;
    assert(discard_len >= 0);
    memmove(conn->buf, conn->buf + discard_len, conn->data_len - discard_len);
    conn->data_len -= discard_len;
    assert(conn->data_len >= 0);
    assert(conn->data_len <= conn->buf_size);
  } while (keep_alive);
}

// Worker threads take accepted socket from the queue
static int consume_socket(struct sq_context *ctx, struct socket *sp) {
  (void) pthread_mutex_lock(&ctx->mutex);
  DEBUG_TRACE(("going idle"));

  // If the queue is empty, wait. We're idle at this point.
  // If a request doesn't come within WORKER_THREAD_TIMEOUT_SECS,
  // we'll stop waiting and shut down the thread.
  while (ctx->sq_head == ctx->sq_tail && ctx->stop_flag == 0) {
    struct timespec timeout;
#ifdef __linux__
    if (clock_gettime(CLOCK_MONOTONIC, &timeout) != 0) {
      perror("Unable to get CLOCK_MONOTONIC");
      abort(); // CLOCK_MONOTONIC should always be supported
    }
#elif defined(__MACH__)
    clock_serv_t cclock;
    mach_timespec_t mts;
    host_get_clock_service(mach_host_self(), CALENDAR_CLOCK, &cclock);
    clock_get_time(cclock, &mts);
    mach_port_deallocate(mach_task_self(), cclock);
    timeout.tv_sec = mts.tv_sec;
    timeout.tv_nsec = (long) mts.tv_nsec;
#endif

    ctx->num_free_threads++;
    assert(ctx->num_free_threads <= ctx->num_threads);
    timeout.tv_sec += WORKER_THREAD_TIMEOUT_SECS;
    int err = pthread_cond_timedwait(&ctx->sq_full, &ctx->mutex, &timeout);
    ctx->num_free_threads--;
    if (err == ETIMEDOUT && ctx->sq_head == ctx->sq_tail) {
      DEBUG_TRACE(("worker thread timed out waiting for new connection"));
      // We didn't get signaled, and there's nothing in the queue.
      (void) pthread_mutex_unlock(&ctx->mutex);
      return 0;
    } else if (err != 0 && err != ETIMEDOUT) {
      cry(fc(ctx), "%s: %s", "Failed timedwait", strerror(err));
    }
  }

  // If we're stopping, sq_head may be equal to sq_tail.
  if (ctx->sq_head > ctx->sq_tail) {
    // Copy socket from the queue and increment tail
    *sp = ctx->queue[ctx->sq_tail % ARRAY_SIZE(ctx->queue)];
    ctx->sq_tail++;
    DEBUG_TRACE(("grabbed socket %d, going busy", sp->sock));
    assert(ctx->num_free_threads <= ctx->num_threads);

    // Wrap pointers if needed
    while (ctx->sq_tail > (int) ARRAY_SIZE(ctx->queue)) {
      ctx->sq_tail -= ARRAY_SIZE(ctx->queue);
      ctx->sq_head -= ARRAY_SIZE(ctx->queue);
    }
  }

  (void) pthread_cond_signal(&ctx->sq_empty);
  (void) pthread_mutex_unlock(&ctx->mutex);

  return !ctx->stop_flag;
}

static void *worker_thread(void *thread_func_param) {
#ifdef __linux__
  (void)prctl(PR_SET_NAME, "sq_worker");
#elif defined(__MACH__)
  pthread_setname_np("sq_worker");
#endif

  struct sq_context *ctx = (struct sq_context *) thread_func_param;
  struct sq_connection *conn;

  conn = (struct sq_connection *) calloc(1, sizeof(*conn) + MAX_REQUEST_SIZE);
  if (conn == NULL) {
    cry(fc(ctx), "%s", "Cannot create new connection struct, OOM");
  } else {
    conn->buf_size = MAX_REQUEST_SIZE;
    conn->buf = (char *) (conn + 1);
    conn->ctx = ctx;
    conn->request_info.user_data = ctx->user_data;

    // Call consume_socket() even when ctx->stop_flag > 0, to let it signal
    // sq_empty condvar to wake up the master waiting in produce_socket()
    while (consume_socket(ctx, &conn->client)) {
      conn->birth_time = time(NULL);

      // Fill in IP, port info early so even if SSL setup below fails,
      // error handler would have the corresponding info.
      // Thanks to Johannes Winkelmann for the patch.
      // TODO(lsm): Fix IPv6 case
      conn->request_info.remote_port = ntohs(conn->client.rsa.sin.sin_port);
      memcpy(&conn->request_info.remote_ip,
             &conn->client.rsa.sin.sin_addr.s_addr, 4);
      conn->request_info.remote_ip = ntohl(conn->request_info.remote_ip);
      conn->request_info.is_ssl = conn->client.is_ssl;

      if (!conn->client.is_ssl
#ifndef NO_SSL
          || sslize(conn, conn->ctx->ssl_ctx, SSL_accept)
#endif
         ) {
        process_new_connection(conn);
      }

      close_connection(conn);
    }
    free(conn);
  }

  // Signal master that we're done with connection and exiting
  (void) pthread_mutex_lock(&ctx->mutex);
  ctx->num_threads--;
  (void) pthread_cond_signal(&ctx->cond);
  assert(ctx->num_threads >= 0);
  (void) pthread_mutex_unlock(&ctx->mutex);

  DEBUG_TRACE(("exiting"));
  return NULL;
}

static void try_start_another_worker(struct sq_context *ctx) {
  // REQUIRES: ctx->mutex is locked
  if (ctx->num_threads >= ctx->max_threads) {
    return;
  }

  if (sq_start_thread(worker_thread, ctx) != 0) {
    cry(fc(ctx), "Cannot start worker thread: %ld", (long) ERRNO);
  } else {
    ctx->num_threads++;
  }
}


// Master thread adds accepted socket to a queue
static void produce_socket(struct sq_context *ctx, const struct socket *sp) {
  (void) pthread_mutex_lock(&ctx->mutex);

  // If all of the worker threads are busy, then start another worker thread
  // to handle this request, assuming the limit isn't yet hit.
  if (ctx->num_free_threads == 0) {
    try_start_another_worker(ctx);
  }

  // If the queue is full, wait
  while (ctx->stop_flag == 0 &&
         ctx->sq_head - ctx->sq_tail >= (int) ARRAY_SIZE(ctx->queue)) {

    DEBUG_TRACE(("queue is full - waiting"));
    (void) pthread_cond_wait(&ctx->sq_empty, &ctx->mutex);
  }

  if (ctx->sq_head - ctx->sq_tail < (int) ARRAY_SIZE(ctx->queue)) {
    // Copy socket to the queue and increment head
    ctx->queue[ctx->sq_head % ARRAY_SIZE(ctx->queue)] = *sp;
    ctx->sq_head++;
    DEBUG_TRACE(("queued socket %d", sp->sock));
  }

  (void) pthread_cond_signal(&ctx->sq_full);
  (void) pthread_mutex_unlock(&ctx->mutex);
}

static int set_sock_timeout(SOCKET sock, int milliseconds) {
  struct timeval t;
  t.tv_sec = milliseconds / 1000;
  t.tv_usec = (milliseconds * 1000) % 1000000;
  return setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, (void *) &t, sizeof(t)) ||
    setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, (void *) &t, sizeof(t));
}

static void accept_new_connection(const struct socket *listener,
                                  struct sq_context *ctx) {
  struct socket so;
  char src_addr[IP_ADDR_STR_LEN];
  socklen_t len = sizeof(so.rsa);
  int on = 1;

  if ((so.sock = accept(listener->sock, &so.rsa.sa, &len)) == INVALID_SOCKET) {
  } else if (!check_acl(ctx, ntohl(* (uint32_t *) &so.rsa.sin.sin_addr))) {
    sockaddr_to_string(src_addr, sizeof(src_addr), &so.rsa);
    cry(fc(ctx), "%s: %s is not allowed to connect", __func__, src_addr);
    closesocket(so.sock);
  } else {
    // Put so socket structure into the queue
    DEBUG_TRACE(("Accepted socket %d", (int) so.sock));
    set_close_on_exec(so.sock);
    so.is_ssl = listener->is_ssl;
    so.ssl_redir = listener->ssl_redir;
    getsockname(so.sock, &so.lsa.sa, &len);
    // Set TCP keep-alive. This is needed because if HTTP-level keep-alive
    // is enabled, and client resets the connection, server won't get
    // TCP FIN or RST and will keep the connection open forever. With TCP
    // keep-alive, next keep-alive handshake will figure out that the client
    // is down and will close the server end.
    // Thanks to Igor Klopov who suggested the patch.
    setsockopt(so.sock, SOL_SOCKET, SO_KEEPALIVE, (void *) &on, sizeof(on));
    set_sock_timeout(so.sock, atoi(ctx->config[REQUEST_TIMEOUT]));
    produce_socket(ctx, &so);
  }
}

static void *master_thread(void *thread_func_param) {
#ifdef __linux__
  (void)prctl(PR_SET_NAME, "sq_acceptor");
#elif defined(__MACH__)
  pthread_setname_np("sq_acceptor");
#endif

  struct sq_context *ctx = (struct sq_context *) thread_func_param;
  struct pollfd *pfd;
  int i;

#if defined(ISSUE_317)
  // Increase priority of the master thread
  struct sched_param sched_param;
  sched_param.sched_priority = sched_get_priority_max(SCHED_RR);
  pthread_setschedparam(pthread_self(), SCHED_RR, &sched_param);
#endif

  pfd = (struct pollfd *) calloc(ctx->num_listening_sockets + 1, sizeof(pfd[0]));
  while (pfd != NULL && ctx->stop_flag == 0) {
    for (i = 0; i < ctx->num_listening_sockets; i++) {
      pfd[i].fd = ctx->listening_sockets[i].sock;
      pfd[i].events = POLLIN;
    }
    pfd[ctx->num_listening_sockets].fd = ctx->wakeup_fds[0];
    pfd[ctx->num_listening_sockets].events = POLLIN;

    if (poll(pfd, ctx->num_listening_sockets + 1, 200) > 0) {
      for (i = 0; i < ctx->num_listening_sockets; i++) {
        // NOTE(lsm): on QNX, poll() returns POLLRDNORM after the
        // successfull poll, and POLLIN is defined as (POLLRDNORM | POLLRDBAND)
        // Therefore, we're checking pfd[i].revents & POLLIN, not
        // pfd[i].revents == POLLIN.
        if (ctx->stop_flag == 0 && (pfd[i].revents & POLLIN)) {
          accept_new_connection(&ctx->listening_sockets[i], ctx);
        }
      }
    }
  }
  free(pfd);
  DEBUG_TRACE(("stopping workers"));

  // Stop signal received: somebody called sq_stop. Quit.
  close_all_listening_sockets(ctx);

  // Wakeup workers that are waiting for connections to handle.
  pthread_cond_broadcast(&ctx->sq_full);

  // Wait until all threads finish
  (void) pthread_mutex_lock(&ctx->mutex);
  while (ctx->num_threads > 0) {
    (void) pthread_cond_wait(&ctx->cond, &ctx->mutex);
  }
  (void) pthread_mutex_unlock(&ctx->mutex);

#if !defined(NO_SSL)
  uninitialize_ssl(ctx);
#endif
  DEBUG_TRACE(("exiting"));

  // Signal sq_stop() that we're done.
  (void) pthread_mutex_lock(&ctx->mutex);
  ctx->stop_flag = 2;
  // WARNING: This must be the very last thing this
  // thread does, as ctx may be freed by sq_stop() as soon as the
  // mutex is unlocked.
  (void) pthread_mutex_unlock(&ctx->mutex);
  return NULL;
}

static void free_context(struct sq_context *ctx) {
  int i;

  // Deallocate config parameters
  for (i = 0; i < NUM_OPTIONS; i++) {
    if (ctx->config[i] != NULL)
      free(ctx->config[i]);
  }

#ifndef NO_SSL
  // Deallocate SSL context
  if (ctx->ssl_ctx != NULL) {
    SSL_CTX_free(ctx->ssl_ctx);
  }
  if (ssl_mutexes != NULL) {
    free(ssl_mutexes);
    ssl_mutexes = NULL;
  }
#endif // !NO_SSL

  // If the wakeup fds are open, close them
  if (ctx->wakeup_fds[0] >= 0) {
    close(ctx->wakeup_fds[0]);
    ctx->wakeup_fds[0] = -1;
  }
  if (ctx->wakeup_fds[1] >= 0) {
    close(ctx->wakeup_fds[1]);
    ctx->wakeup_fds[1] = -1;
  }

  // All threads exited, no sync is needed. Destroy mutex and condvars
  (void) pthread_cond_destroy(&ctx->cond);
  (void) pthread_cond_destroy(&ctx->sq_empty);
  (void) pthread_cond_destroy(&ctx->sq_full);
  (void) pthread_mutex_destroy(&ctx->mutex);

  // Deallocate context itself
  free(ctx);
}

void sq_stop(struct sq_context *ctx) {
  int unused;
  char c = 0;

  (void) pthread_mutex_lock(&ctx->mutex);
  ctx->stop_flag = 1;
  (void) pthread_mutex_unlock(&ctx->mutex);

  if (ctx->wakeup_fds[1] != -1) {
    RETRY_ON_EINTR(unused, write(ctx->wakeup_fds[1], &c, 1));
  }

  // Wait until sq_fini() stops
  while (1) {
    (void) pthread_mutex_lock(&ctx->mutex);
    int should_stop = (ctx->stop_flag == 2);
    (void) pthread_mutex_unlock(&ctx->mutex);
    if (should_stop) break;
    (void) sq_sleep(10);
  }

  free_context(ctx);
}

struct sq_context *sq_start(const struct sq_callbacks *callbacks,
                            void *user_data,
                            const char **options) {
  struct sq_context *ctx;
  const char *name, *value, *default_value;
  int i;

  // Allocate context and initialize reasonable general case defaults.
  // TODO(lsm): do proper error handling here.
  if ((ctx = (struct sq_context *) calloc(1, sizeof(*ctx))) == NULL) {
    return NULL;
  }
  ctx->callbacks = *callbacks;
  ctx->user_data = user_data;
  ctx->wakeup_fds[0] = -1;
  ctx->wakeup_fds[1] = -1;

  while (options && (name = *options++) != NULL) {
    if ((i = get_option_index(name)) == -1) {
      cry(fc(ctx), "Invalid option: %s", name);
      free_context(ctx);
      return NULL;
    } else if ((value = *options++) == NULL) {
      cry(fc(ctx), "%s: option value cannot be NULL", name);
      free_context(ctx);
      return NULL;
    }
    if (ctx->config[i] != NULL) {
      cry(fc(ctx), "warning: %s: duplicate option", name);
      free(ctx->config[i]);
    }
    ctx->config[i] = sq_strdup(value);
    DEBUG_TRACE(("[%s] -> [%s]", name, value));
  }

  // Set default value if needed
  for (i = 0; config_options[i * 2] != NULL; i++) {
    default_value = config_options[i * 2 + 1];
    if (ctx->config[i] == NULL && default_value != NULL) {
      ctx->config[i] = sq_strdup(default_value);
    }
  }

  // NOTE(lsm): order is important here. SSL certificates must
  // be initialized before listening ports. UID must be set last.
  if (!set_gpass_option(ctx) ||
#if !defined(NO_SSL)
      !set_ssl_option(ctx) ||
#endif
      !set_ports_option(ctx) ||
      !set_uid_option(ctx) ||
      !set_acl_option(ctx)) {
    free_context(ctx);
    return NULL;
  }

  // Ignore SIGPIPE signal, so if browser cancels the request, it
  // won't kill the whole process.
  (void) signal(SIGPIPE, SIG_IGN);
  // Also ignoring SIGCHLD to let the OS to reap zombies properly.
  (void) signal(SIGCHLD, SIG_IGN);

  (void) pthread_mutex_init(&ctx->mutex, NULL);

  pthread_condattr_t attr;

  pthread_condattr_init(&attr);

#ifdef __linux__
  if (pthread_condattr_setclock(&attr, CLOCK_MONOTONIC) != 0) {
    perror("pthread_condattr_setclock");
    free_context(ctx);
    return NULL;
  }
#endif

  (void) pthread_cond_init(&ctx->cond, &attr);
  (void) pthread_cond_init(&ctx->sq_empty, &attr);
  (void) pthread_cond_init(&ctx->sq_full, &attr);

  // Create a pipe used for sq_stop to wake up the master thread.
  ctx->wakeup_fds[0] = -1;
  ctx->wakeup_fds[1] = -1;
  if (pipe(ctx->wakeup_fds) != 0) {
    cry(fc(ctx), "Cannot create wakeup_fds: %ld", (long) ERRNO);
  } else {
    set_close_on_exec(ctx->wakeup_fds[0]);
    set_close_on_exec(ctx->wakeup_fds[1]);
  }

  ctx->max_threads = atoi(ctx->config[NUM_THREADS]);
  assert(ctx->max_threads > 0);

  // Start master (listening) thread. This thread will spawn worker
  // threads lazily as necessary up to the configured max.
  sq_start_thread(master_thread, ctx);

  return ctx;
}
