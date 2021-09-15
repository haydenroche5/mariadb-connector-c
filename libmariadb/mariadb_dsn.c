/************************************************************************************
    Copyright (C) 2021 MariaDB Corporation AB

   This library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Library General Public
   License as published by the Free Software Foundation; either
   version 2 of the License, or (at your option) any later version.

   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Library General Public License for more details.

   You should have received a copy of the GNU Library General Public
   License along with this library; if not see <http://www.gnu.org/licenses>
   or write to the Free Software Foundation, Inc.,
   51 Franklin St., Fifth Floor, Boston, MA 02110, USA

*************************************************************************************/
#include <mysql.h>
#include <ma_global.h>

#include <ma_sys.h>
#include <ma_string.h>
#include <mariadb_ctype.h>
#include <ma_common.h>
#include <string.h>
#include <stdio.h>
#include <errmsg.h>

extern my_bool _mariadb_set_conf_option(MYSQL *mysql, const char *config_option, const char *config_value);

/**
 * @brief: simple DSN parser
 *
 * key/value pairs (or key only) are separated by semicolons.
 * If a semicolon is part of a value, it must be enclosed in
 * curly braces.
 *
 * The use of dsn keyword is permitted to prevent endless
 * recursion
 * 
 * Unknown keys will be ignored.
 */
int ma_parse_dsn(MYSQL *mysql, const char *dsn, ssize_t len)
{
  char *dsn_save;
  char *end, *pos, *key= NULL, *val= NULL;
  my_bool in_curly_brace= 0;

  if (len == -1)
    len= strlen(dsn);

  /* don't modify original dsn */
  dsn_save= (char *)alloca(len + 1);
  memcpy(dsn_save, dsn, len);
  dsn_save[len]= 0;

  /* start and end */
  pos= dsn_save;
  end= dsn_save + len;

  while (pos <= end)
  {
    /* ignore white space, unless it is between curly braces */
    if (isspace(*pos) && !in_curly_brace)
    {
      pos++;
      continue;
    }

    switch (*pos) {
      case '{':
        if (!key)
          goto error;
        if (!in_curly_brace)
        {
          in_curly_brace= 1;
          if (pos < end)
          {
            pos++;
            val= pos;
          }
        }
        break;
      case '}':
        if (in_curly_brace)
        {
          if (!key)
            goto error;
          if (pos < end && *(pos + 1) == '}')
          {
            memmove(pos, pos + 1, end - pos - 1);
            end--;
            pos += 2;
            continue;
          }
          if (in_curly_brace)
            in_curly_brace= 0;
          else
            goto error;
          *pos++= 0;
          continue;
        }
        break;
      case '=':
        if (in_curly_brace)
        {
          pos++;
          continue;
        }
        if (!key)
          goto error;
        *pos++= 0;
        if (pos < end)
          val= pos;
        continue;
        break;
      case ';':
        if (in_curly_brace)
        {
           pos++;
           continue;
        }
        if (!key)
          goto error;
        *pos++= 0;
        if (key && strcasecmp(key, "dsn") != 0)
          _mariadb_set_conf_option(mysql, key, val);
        key= val= NULL;
        continue;
        break;
    }
    if (!key && *pos)
      key= pos;
    pos++;
  }
  if (key && strcasecmp(key, "dsn") != 0)
    _mariadb_set_conf_option(mysql, key, val);
  return 0;

error:
  my_set_error(mysql, CR_DSN_PARSE_ERROR, SQLSTATE_UNKNOWN, 0, pos - dsn_save);
  return 1;
}

/**
 * @brief: connect to a database server via data source name (DSN)
 *
 * @param: mysql - a handle which was previously allocated or inited
 *         by mysql_init() function
 * @param: dsn - data source name, containing connection details like
 *         hostname, user, etc.
 * @param: len - length of dsn string, or -1 if dsn is zero terminated.
 * 
 * @return: A mysql connected to specified database server or NULL on error.
 *
 * Supported keywords are all keywords which are supported in the client
 * section of my.cnf configuration files.
 * Unsupported keywords will be ignored without raising an error.
 **/
MYSQL * STDCALL
mariadb_dsn_connect(MYSQL *mysql, const char *dsn, ssize_t len)
{
  if (!mysql)
    return NULL;

  if (!ma_parse_dsn(mysql, dsn, len))
    return mysql_real_connect(mysql, NULL, NULL, NULL, NULL, 0, NULL, 0);
  return NULL;
}
