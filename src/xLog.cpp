

#include "xLog.h"
#include <stdarg.h>
#include <time.h>


const char* const _errstr[] = { "\033[01;31mERROR\033[0m", "\033[01;33mWARN \033[0m", "\033[01;37mINFO \033[0m", "\033[22;34mDEBUG\033[0m" };
static void (*log_fn)(int level, const char* line) = NULL;
uint32_t _level = LOG_LEVEL_INFO;

void set_log_level(uint32_t level, void (*call_fn)(int level, const char* line))
{
    _level = level;
    log_fn = call_fn;
}

void log_message(uint32_t level, const char* function, int line, const char* fmt, ...)
{
    if (level > _level) {
        return;
    }

    time_t t;
    time(&t);
    struct tm tm;
    ::localtime_r((const time_t*)&t, &tm);

    char data1[DATA_BUF + 4];
    int i = 0;
    va_list args;
    va_start(args, fmt);
    i = vsnprintf(data1, DATA_BUF, fmt, args);
    va_end(args);

    if (i > (int)DATA_BUF - 1) {
        i = DATA_BUF - 5;
        data1[i++] = '.';
        data1[i++] = '.';
        data1[i++] = '.';
        data1[i++] = '\n';
        data1[i] = '\0';
    } else {
        if (i > 0) {
            if (data1[i - 1] != '\n') {
                data1[i++] = '\n';
                data1[i] = '\0';
            }
        }
    }

    while (data1[i - 2] == '\n')
        i--;
    data1[i] = '\0';

    if (log_fn) {
        log_fn(level, data1);
    } else {
        printf("[%04d-%02d-%02d %02d:%02d:%02d] %-5s %s:%d %s",
            tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
            tm.tm_hour, tm.tm_min, tm.tm_sec,
            _errstr[level], function, line, data1);
    }
}

uint64_t atoull(const char* str) { return (NULL == str) ? (0) : (strtoull(str, (char**)NULL, 0)); }
uint32_t atoul(const char* str) { return (NULL == str) ? (0) : ((uint32_t)atol(str)); }
