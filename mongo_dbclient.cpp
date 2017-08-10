#include <client/dbclient.h>
#include <string>
#include <list>
#include "utils.h"
#include "common.h"

using namespace mongo;

extern int cursor_create(lua_State *L, DBClientBase *connection, const char *ns,
                         const Query &query, int nToReturn, int nToSkip,
                         const BSONObj *fieldsToReturn, int queryOptions, int batchSize);

extern void lua_to_bson(lua_State *L, int stackpos, BSONObj &obj);
extern void bson_to_lua(lua_State *L, const BSONObj &obj);
extern void lua_push_value(lua_State *L, const BSONElement &elem);

extern bool lua_to_bson_ordered(lua_State *L, int index, BSONObj &object);
extern bool lua_to_bson_ordered_query(lua_State *L, int index, Query &query);
extern bool lua_to_bson_batched(lua_State *L, int index, std::vector<BSONObj> &objects);


DBClientBase* userdata_to_dbclient(lua_State *L, int stackpos)
{
  // adapted from http://www.lua.org/source/5.1/lauxlib.c.html#luaL_checkudata
  void *ud = lua_touserdata(L, stackpos);
  if (ud == NULL)
    luaL_typeerror(L, stackpos, "userdata");

  // try Connection
  lua_getfield(L, LUA_REGISTRYINDEX, LUAMONGO_CONNECTION);
  if (lua_getmetatable(L, stackpos))
    {
      if (lua_rawequal(L, -1, -2))
        {
          DBClientConnection *connection = *((DBClientConnection **)ud);
          lua_pop(L, 2);
          return connection;
        }
      lua_pop(L, 2);
    }
  else
    lua_pop(L, 1);

  // try ReplicaSet
  lua_getfield(L, LUA_REGISTRYINDEX, LUAMONGO_REPLICASET);  // get correct metatable
  if (lua_getmetatable(L, stackpos))
    {
      if (lua_rawequal(L, -1, -2))
        {
          DBClientReplicaSet *replicaset = *((DBClientReplicaSet **)ud);
          lua_pop(L, 2); // remove both metatables
          return replicaset;
        }
      lua_pop(L, 2);
    }
  else
    lua_pop(L, 1);

  luaL_typeerror(L, stackpos, LUAMONGO_DBCLIENT);
  return NULL; // should never get here
}


/***********************************************************************/
// The following methods are common to all DBClients
// (DBClientConnection and DBClientReplicaSet)
/***********************************************************************/

/*
 * created = db:create_index(ns, json_str/lua_table/array of lua table(ordered)[, options lua_table or json_str])
 */
static int dbclient_create_index(lua_State *L) {
  DBClientBase *dbclient = userdata_to_dbclient(L, 1); 
  try {
     const char *ns = luaL_checkstring(L, 2);
     BSONObj fields;
     IndexSpec options;
 
     if (!lua_to_bson_ordered(L, 3, fields)) {
        throw (LUAMONGO_REQUIRES_JSON_OR_TABLE);
     }
     options.addKeys(fields);
	      
     BSONObj more_options_bson;
     if (lua_type(L, 4) != LUA_TNIL && !lua_to_bson_ordered(L, 4, more_options_bson)) {
        throw(LUAMONGO_REQUIRES_JSON_OR_TABLE);
     }
     options.addOptions(more_options_bson);
     
     dbclient->createIndex(ns, options);
     lua_pushboolean(L, true);
     return 1;
  } catch (std::exception &e) {
    lua_pushnil(L);
    lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "create_index", e.what());
    return 2;
  } catch (const char *err) {
    lua_pushnil(L);
    lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "create_index", err);
    return 2;
  }
  
}

/*
 * ok,err = db:auth({})
 *    accepts a table of parameters:
 *       dbname           database to authenticate (required)
 *       username         username to authenticate against (required)
 *       password         password to authenticate against (required)
 *       digestPassword   set to true if password is pre-digested (default = true)
 *
 */
static int dbclient_auth(lua_State *L) {
  DBClientBase *dbclient = userdata_to_dbclient(L, 1);
  try {
     luaL_checktype(L, 2, LUA_TTABLE);
     lua_getfield(L, 2, "dbname");
     const char *dbname = luaL_checkstring(L, -1);
     lua_getfield(L, 2, "username");
     const char *username = luaL_checkstring(L, -1);
     lua_getfield(L, 2, "password");
     const char *password = luaL_checkstring(L, -1);
     lua_getfield(L, 2, "digestPassword");
     bool digestPassword = lua_isnil(L, -1) ? true : lua_toboolean(L, -1);
       
     std::string errmsg;
     bool success = dbclient->auth(dbname, username, password, errmsg, digestPassword);
     lua_pop(L, 4);
     if (!success) {
       lua_pushnil(L);
       lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "auth", errmsg.c_str());
       return 2;
     }
     lua_pushboolean(L, 1);
     return 1;
  } catch (std::exception &e) {
     lua_pushnil(L);
     lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "auth", e.what());
     return 2;
  } catch (const char *err) {
     lua_pushnil(L);
     lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "auth", err);
     return 2;
  }
}

/*
 * is_failed = db:is_failed()
 */
static int dbclient_is_failed(lua_State *L) {
  DBClientBase *dbclient = userdata_to_dbclient(L, 1);
  try {
    bool is_failed = dbclient->isFailed();
    lua_pushboolean(L, is_failed);
    return 1;
  } catch (std::exception &e) {
    lua_pushnil(L);
    lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "is_failed", e.what());
    return 2;
  } catch (const char *err) {
    lua_pushnil(L);
    lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "is_failed", err);
    return 2;
  }
}

/*
 * addr = db:get_server_address()
 */
static int dbclient_get_server_address(lua_State *L) {
  DBClientBase *dbclient = userdata_to_dbclient(L, 1);
  try {
     std::string address = dbclient->getServerAddress();
     lua_pushstring(L, address.c_str());
     return 1;
  }catch (std::exception &e) {
    lua_pushnil(L);
    lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "get_server_address", e.what());
    return 2;
  } catch (const char *err) {
    lua_pushnil(L);
    lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "get_server_address", err);
    return 2;
  }
}

/*
 * count,err = db:count(ns, json_str/lua_table/array of lua table(ordered))
 */
static int dbclient_count(lua_State *L) {
  DBClientBase *dbclient = userdata_to_dbclient(L, 1);
  try {
    const char *ns = luaL_checkstring(L, 2);
    BSONObj query;
    if (!lua_to_bson_ordered(L, 3, query)) {
      throw (LUAMONGO_REQUIRES_JSON_OR_TABLE);
    }
    int count = dbclient->count(ns, query);
    lua_pushinteger(L, count);
    return 1;
  } catch (std::exception &e) {
    lua_pushnil(L);
    lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "count", e.what());
    return 2;
  } catch (const char *err) {
    lua_pushnil(L);
    lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "count", err);
    return 2;
  }
}

/*
 * ok,err = db:insert(ns, json_str/lua_table/array of lua table(ordered))
 */
static int dbclient_insert(lua_State *L) {
  DBClientBase *dbclient = userdata_to_dbclient(L, 1);
  try {
    const char *ns = luaL_checkstring(L, 2);
    BSONObj data;
    if (!lua_to_bson_ordered(L, 3, data)) {
      throw (LUAMONGO_REQUIRES_JSON_OR_TABLE);
    }
    dbclient->insert(ns, data);
    lua_pushboolean(L, 1);
    return 1;
  } catch (std::exception &e) {
    lua_pushnil(L);
    lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "insert", e.what());
    return 2;
  } catch (const char *err) {
    lua_pushnil(L);
    lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "insert", err);
    return 2;
  } 
}

/*
 * ok,err = db:insert_batch(ns, json_str/lua_table/array of lua table(ordered))
 */
static int dbclient_insert_batch(lua_State *L) {
  DBClientBase *dbclient = userdata_to_dbclient(L, 1);
  try {
    const char *ns = luaL_checkstring(L, 2);
    std::vector<BSONObj> vdata;
    if (!lua_to_bson_batched(L, 3, vdata)) {
      throw (LUAMONGO_REQUIRES_JSON_OR_TABLE);
    }    
    dbclient->insert(ns, vdata);
    lua_pushboolean(L, 1);
    return 1;
  } catch (std::exception &e) {
    lua_pushnil(L);
    lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "insert_batch", e.what());
    return 2;
  } catch (const char *err) {
    lua_pushnil(L);
    lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "insert_batch", err);
    return 2;
  }
}

/*
 * cursor,err = db:query(ns, json_str/lua_table/query_obj/array of lua table(ordered), limit, skip, json_str/lua_table/array of lua table(ordered), options, batchsize)
 */
static int dbclient_query(lua_State *L) {
  DBClientBase *dbclient = userdata_to_dbclient(L, 1);
  try {
    const char *ns = luaL_checkstring(L, 2);
    Query query;
    if (!lua_isnoneornil(L, 3)) {
      if (!lua_to_bson_ordered_query(L, 3, query)) {
        throw (LUAMONGO_REQUIRES_QUERY);
      }
    }

    int nToReturn = luaL_optint(L, 4, 0);
    int nToSkip = luaL_optint(L, 5, 0);

    const BSONObj *fieldsToReturn = NULL;
    if (!lua_isnoneornil(L, 6)) {
      BSONObj object;
      if (lua_to_bson_ordered(L, 6, object)) {
        fieldsToReturn = new BSONObj(object);
      } else {
        throw (LUAMONGO_REQUIRES_JSON_OR_TABLE);
      }
    }

    int queryOptions = luaL_optint(L, 7, 0);
    int batchSize = luaL_optint(L, 8, 0);

    //wont throw as handles it internally
    int res = cursor_create(L, dbclient, ns, query, nToReturn, nToSkip,
            fieldsToReturn, queryOptions, batchSize);

    if (fieldsToReturn) {
      delete fieldsToReturn;
    }

    return res;
  } catch (std::exception &e) {
    lua_pushnil(L);
    lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "query", e.what());
    return 2;
  } catch (const char *err) {
    lua_pushnil(L);
    lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "query", err);
    return 2;
  }
}

/**
 * lua_table,err = db:find_one(ns, json_str/lua_table/query_obj/array of lua table(ordered), json_str/lua_table/array of lua table(ordered), options)
 */
static int dbclient_find_one(lua_State *L) {
  DBClientBase *dbclient = userdata_to_dbclient(L, 1);
  try {
    const char *ns = luaL_checkstring(L, 2);
    Query query;
    if (!lua_isnoneornil(L, 3)) {
      if (!lua_to_bson_ordered_query(L, 3, query)) {
        throw (LUAMONGO_REQUIRES_QUERY);
      }
    }

    const BSONObj *fieldsToReturn = NULL;
    if (!lua_isnoneornil(L, 4)) {
      BSONObj object;
      if (lua_to_bson_ordered(L, 4, object)) {
        fieldsToReturn = new BSONObj(object);
      } else {
        throw (LUAMONGO_REQUIRES_JSON_OR_TABLE);
      }
    }

    int queryOptions = luaL_optint(L, 5, 0);
    BSONObj ret = dbclient->findOne(ns, query, fieldsToReturn, queryOptions);
    bson_to_lua(L, ret);
    if (fieldsToReturn) {
      delete fieldsToReturn;
    }
    return 1;
  } catch (std::exception &e) {
    lua_pushnil(L);
    lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "find_one", e.what());
    return 2;
  } catch (const char *err) {
    lua_pushnil(L);
    lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "find_one", err);
    return 2;
  }
}

/*
 * ok,err = db:remove(ns, json_str/lua_table/query_obj/array of lua table(ordered))
 */
static int dbclient_remove(lua_State *L) {
  DBClientBase *dbclient = userdata_to_dbclient(L, 1);
  try {
    const char *ns = luaL_checkstring(L, 2);
    Query query;
    if (!lua_to_bson_ordered_query(L, 3, query)) {
      throw (LUAMONGO_REQUIRES_QUERY);
    }
    bool justOne = lua_toboolean(L, 4);
    dbclient->remove(ns, query, justOne);
    lua_pushboolean(L, 1);
    return 1;
  } catch (std::exception &e) {
    lua_pushnil(L);
    lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "remove", e.what());
    return 2;
  } catch (const char *err) {
    lua_pushnil(L);
    lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "remove", err);
    return 2;
  }
}


/*
 * ok,err = db:update(ns, json_str/lua_table/query_obj/array of lua table(ordered), json_str/lua_table/array of lua table(ordered), upsert, multi)
 */
static int dbclient_update(lua_State *L) {
  DBClientBase *dbclient = userdata_to_dbclient(L, 1);
  try {
    const char *ns = luaL_checkstring(L, 2);
    Query query;
    if (!lua_to_bson_ordered_query(L, 3, query)) {
      throw (LUAMONGO_REQUIRES_QUERY);
    }
    BSONObj obj;

    if (!lua_to_bson_ordered(L, 4, obj)) {
      throw (LUAMONGO_REQUIRES_JSON_OR_TABLE);
    }
    bool upsert = lua_toboolean(L, 5);
    bool multi = lua_toboolean(L, 6);

    dbclient->update(ns, query, obj, upsert, multi);
    lua_pushboolean(L, 1);
    return 1;
  } catch (std::exception &e) {
    lua_pushnil(L);
    lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "update", e.what());
    return 2;
  } catch (const char *err) {
    lua_pushnil(L);
    lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "update", err);
    return 2;
  }
}

/*
 * ok,err = db:drop_collection(ns)
 */
static int dbclient_drop_collection(lua_State *L) {
  DBClientBase *dbclient = userdata_to_dbclient(L, 1);
  try {
    const char *ns = luaL_checkstring(L, 2);
    dbclient->dropCollection(ns);
    lua_pushboolean(L, 1);
    return 1;
  } catch (std::exception &e) {
    lua_pushnil(L);
    lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "drop_collection", e.what());
    return 2;
  } catch (const char *err) {
    lua_pushnil(L);
    lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "drop_collection", err);
    return 2;
  }
}

/*
 * ok,err = db:drop_index_by_fields(ns, json_str/lua_table/array of lua table(ordered))
 */
static int dbclient_drop_index_by_fields(lua_State *L) {
  DBClientBase *dbclient = userdata_to_dbclient(L, 1);
  try {
    const char *ns = luaL_checkstring(L, 2);
    BSONObj keys;
    if (!lua_to_bson_ordered(L, 3, keys)) {
      throw (LUAMONGO_REQUIRES_JSON_OR_TABLE);
    }
    dbclient->dropIndex(ns, keys);
    lua_pushboolean(L, 1);
    return 1;
  } catch (std::exception &e) {
    lua_pushnil(L);
    lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "drop_index_by_fields", e.what());
    return 2;
  } catch (const char *err) {
    lua_pushnil(L);
    lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "drop_index_by_fields", err);
    return 2;
  }
}

/*
 * ok,err = db:drop_index_by_name(ns, index_name)
 */
static int dbclient_drop_index_by_name(lua_State *L) {
  DBClientBase *dbclient = userdata_to_dbclient(L, 1);
  try {
    const char *ns = luaL_checkstring(L, 2);
    dbclient->dropIndex(ns, luaL_checkstring(L, 3));
    lua_pushboolean(L, 1);
    return 1;
  } catch (std::exception &e) {
    lua_pushnil(L);
    lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "drop_index_by_name", e.what());
    return 2;
  } catch (const char *err) {
    lua_pushnil(L);
    lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "drop_index_by_name", err);
    return 2;
  }
}

/*
 * ok,err = db:drop_indexes(ns)
 */
static int dbclient_drop_indexes(lua_State *L) {
    DBClientBase *dbclient = userdata_to_dbclient(L, 1);
    try {
      const char *ns = luaL_checkstring(L, 2);
      dbclient->dropIndexes(ns);
      lua_pushboolean(L, 1);
      return 1;
    } catch (std::exception &e) {
      lua_pushnil(L);
      lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "drop_indexes", e.what());
      return 2;
    } catch (const char *err) {
      lua_pushnil(L);
      lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "drop_indexes", err);
      return 2;
    }
}
/*
 * res,err = (dbname, jscode[, args_table])
 */
static int dbclient_eval(lua_State *L) {
  DBClientBase *dbclient = userdata_to_dbclient(L, 1);
  try {

    const char *dbname = luaL_checkstring(L, 2);
    const char *jscode = luaL_checkstring(L, 3);
    BSONObj *args = NULL;
    if (!lua_isnoneornil(L, 4)) {
      BSONObj tmpObj;
      if (lua_to_bson_ordered(L, 4, tmpObj)) {
        args = new BSONObj(tmpObj);
      } else {
        throw (LUAMONGO_REQUIRES_JSON_OR_TABLE);
      }
    }

    BSONObj info;
    BSONElement retval;
    bool res = dbclient->eval(dbname, jscode, info, retval, args);
    if (args) {
      delete args;
    }
    if (!res) {
      lua_pushboolean(L, 0);
      lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "eval", info["errmsg"].str().c_str());
      return 2;
    }
    lua_push_value(L, retval);
    return 1;
  } catch (std::exception &e) {
    lua_pushnil(L);
    lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "eval", e.what());
    return 2;
  } catch (const char *err) {
    lua_pushnil(L);
    lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "eval", err);
    return 2;
  }
}
/*
 * bool = db:exists(ns)
 */
static int dbclient_exists(lua_State *L) {
  DBClientBase *dbclient = userdata_to_dbclient(L, 1);
  try {
    const char *ns = luaL_checkstring(L, 2);
    bool res = dbclient->exists(ns);
    lua_pushboolean(L, res);
    return 1;
  } catch (std::exception &e) {
    lua_pushnil(L);
    lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "exists", e.what());
    return 2;
  } catch (const char *err) {
    lua_pushnil(L);
    lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "exists", err);
    return 2;
  }
}

/*
 * name = db:gen_index_name(json_str/lua_table/array of lua table(ordered))
 */
static int dbclient_gen_index_name(lua_State *L) {
  DBClientBase *dbclient = userdata_to_dbclient(L, 1);
  try {
    std::string name = "";
    BSONObj tmpObj;
    if (!lua_to_bson_ordered(L, 2, tmpObj)) {
      throw (LUAMONGO_REQUIRES_JSON_OR_TABLE);
    }
    name = dbclient->genIndexName(tmpObj);
    lua_pushstring(L, name.c_str());
    return 1;
  } catch (std::exception &e) {
    lua_pushnil(L);
    lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "gen_index_name", e.what());
    return 2;
  } catch (const char *err) {
    lua_pushnil(L);
    lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "gen_index_name", err);
    return 2;
  }
}

/*
 * cursor,err = db:enumerate_indexes(ns)
 */
static int dbclient_enumerate_indexes(lua_State *L) {
  DBClientBase *dbclient = userdata_to_dbclient(L, 1);

  try {
     const char *ns = luaL_checkstring(L, 2);
   
     std::auto_ptr<DBClientCursor> autocursor = dbclient->enumerateIndexes(ns);
     
     if (!autocursor.get()) {
       lua_pushnil(L);
       lua_pushstring(L, LUAMONGO_ERR_CONNECTION_LOST);
       return 2;
     }
   
     DBClientCursor **cursor =
       (DBClientCursor **)lua_newuserdata(L, sizeof(DBClientCursor *));
     *cursor = autocursor.get();
     autocursor.release();
   
     luaL_getmetatable(L, LUAMONGO_CURSOR);
     lua_setmetatable(L, -2);
   
     return 1;     
  } catch (std::exception &e) {
    lua_pushnil(L);
    lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "enumerate_indexes", e.what());
    return 2;
  } catch (const char *err) {
    lua_pushnil(L);
    lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "enumerate_indexes", err);
    return 2;
  }  
}

/*
 * res,err = db:mapreduce(ns, jsmapfunc, jsreducefunc[, query[, output]])
 */
static int dbclient_mapreduce(lua_State *L) {
  DBClientBase *dbclient = userdata_to_dbclient(L, 1);
  try {
    const char *ns = luaL_checkstring(L, 2);
    const char *jsmapfunc = luaL_checkstring(L, 3);
    const char *jsreducefunc = luaL_checkstring(L, 4);
 
    BSONObj query;
    if (!lua_isnoneornil(L, 5)) {
      if (!lua_to_bson_ordered(L, 5, query)) {
        throw (LUAMONGO_REQUIRES_JSON_OR_TABLE);
      }
    }

    BSONObj res;
    const char *output = NULL;
    if (!lua_isnoneornil(L, 6)) output = luaL_checkstring(L, 6);
    
    if (output == NULL)
       res = dbclient->mapreduce(ns, jsmapfunc, jsreducefunc, query);
    else
       res = dbclient->mapreduce(ns, jsmapfunc, jsreducefunc, query, output);
    bson_to_lua(L, res);
    return 1;
  } catch (std::exception &e) {
    lua_pushnil(L);
    lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "mapreduce", e.what());
    return 2;
  } catch (const char *err) {
    lua_pushnil(L);
    lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "mapreduce", err);
    return 2;
  }
}

/*
 * ok,err = db:reindex(ns);
 */
static int dbclient_reindex(lua_State *L) {
  DBClientBase *dbclient = userdata_to_dbclient(L, 1);
  const char *ns = luaL_checkstring(L, 2);
  try {
    dbclient->reIndex(ns);
    lua_pushboolean(L, 1);
    return 1;
  } catch (std::exception &e) {
    lua_pushnil(L);
    lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "reindex", e.what());
    return 2;
  } catch (const char *err) {
    lua_pushnil(L);
    lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "reindex", err);
    return 2;
  }
}


/*
 * db:reset_index_cache()
 */
/*
static int dbclient_reset_index_cache(lua_State *L) {
  DBClientBase *dbclient = userdata_to_dbclient(L, 1);
  try {
    dbclient->resetIndexCache();
    return 0;
  } catch (std::exception &e) {
    lua_pushnil(L);
    lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "reset_index_cache", e.what());
    return 2;
  } catch (const char *err) {
    lua_pushnil(L);
    lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "reset_index_cache", err);
    return 2;
  }
}
*/

/*
 * db:get_last_error()
 */
static int dbclient_get_last_error(lua_State *L) {
  DBClientBase *dbclient = userdata_to_dbclient(L, 1);
  try {
    std::string result = dbclient->getLastError();
    lua_pushlstring(L, result.c_str(), result.size());
    return 1;
  } catch (std::exception &e) {
    lua_pushnil(L);
    lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "get_last_error", e.what());
    return 2;
  } catch (const char *err) {
    lua_pushnil(L);
    lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "get_last_error", err);
    return 2;
  }
}

/*
 * db:get_last_error_detailed()
 */
static int dbclient_get_last_error_detailed(lua_State *L) {
  DBClientBase *dbclient = userdata_to_dbclient(L, 1);
  try {
    BSONObj res = dbclient->getLastErrorDetailed();
    bson_to_lua(L, res);
    return 1;
  } catch (std::exception &e) {
    lua_pushnil(L);
    lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "get_last_error_detailed", e.what());
    return 2;
  } catch (const char *err) {
    lua_pushnil(L);
    lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "get_last_error_detailed", err);
    return 2;
  }
}
/*
 * res,err = db:run_command(dbname, json_str/lua_table/array of lua table(ordered), options)
 */
static int dbclient_run_command(lua_State *L) {
  DBClientBase *dbclient = userdata_to_dbclient(L, 1);
  try {
    const char *ns = luaL_checkstring(L, 2);
    int options = lua_tointeger(L, 4); // if it is invalid it returns 0


    BSONObj retval;
    bool success;

    BSONObj command; // arg 3
    if (!lua_to_bson_ordered(L, 3, command)) {
       throw (LUAMONGO_REQUIRES_JSON_OR_TABLE);
    }
    if (!command.hasElement("cmd")) {
      success = dbclient->runCommand(ns, command, retval, options);
    } else {
      const char *cmd_key = command.getStringField("cmd");
      BSONElement cmd = command[cmd_key];
      command.removeField("cmd");
      // TODO: it is necessary => command.removeField(cmd_key);
      BSONObjBuilder b;
      b.append(cmd);
      b.appendElementsUnique(command);
      BSONObj command_new = b.obj();
      success = dbclient->runCommand(ns, command_new, retval, options);
    }

    if (!success)
      throw retval["errmsg"].str().c_str();

    bson_to_lua(L, retval);
    return 1;
  } catch (std::exception &e) {
    lua_pushnil(L);
    lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "run_command", e.what());
    return 2;
  } catch (const char *err) {
    lua_pushnil(L);
    lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "run_command", err);
    return 2;
  }
}

/*
 * res,err = db:get_dbnames()
 */
static int dbclient_get_dbnames(lua_State *L) {
  DBClientBase *dbclient = userdata_to_dbclient(L, 1);
  try {
    std::list<std::string> dbs = dbclient->getDatabaseNames();
    lua_newtable(L);
    int i=1;
    for (std::list<std::string>::iterator it=dbs.begin(); it!=dbs.end(); ++it, ++i) {
      lua_pushnumber(L,i);
      lua_pushstring(L,it->c_str());
      lua_settable(L,-3);
    }
    return 1;
  } catch (std::exception &e) {
    lua_pushnil(L);
    lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "get_dbnames", e.what());
    return 2;
  } catch (const char *err) {
    lua_pushnil(L);
    lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "get_dbnames", err);
    return 2;
  }
}

/*
 * res,err = db:get_collections()
 */
static int dbclient_get_collections(lua_State *L) {
  DBClientBase *dbclient = userdata_to_dbclient(L, 1);
  const char *ns = luaL_checkstring(L, 2);
  try {
    std::list<std::string> dbs = dbclient->getCollectionNames(ns);
    lua_newtable(L);
    int i=1;
    for (std::list<std::string>::iterator it=dbs.begin(); it!=dbs.end(); ++it, ++i) {
      lua_pushnumber(L,i);
      lua_pushstring(L,it->c_str());
      lua_settable(L,-3);
    }
    return 1;
  } catch (std::exception &e) {
    lua_pushnil(L);
    lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "get_collections", e.what());
    return 2;
  } catch (const char *err) {
    lua_pushnil(L);
    lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "get_collections", err);
    return 2;
  }
}

// Method registration table for DBClients
extern const luaL_Reg dbclient_methods[] = {
  {"auth", dbclient_auth},
  {"count", dbclient_count},
  {"drop_collection", dbclient_drop_collection},
  {"drop_index_by_fields", dbclient_drop_index_by_fields},
  {"drop_index_by_name", dbclient_drop_index_by_name},
  {"drop_indexes", dbclient_drop_indexes},
  {"create_index", dbclient_create_index},
  {"eval", dbclient_eval},
  {"exists", dbclient_exists},
  {"find_one", dbclient_find_one},
  {"gen_index_name", dbclient_gen_index_name},
  {"enumerate_indexes", dbclient_enumerate_indexes},
  {"get_last_error", dbclient_get_last_error},
  {"get_last_error_detailed", dbclient_get_last_error_detailed},
  {"get_server_address", dbclient_get_server_address},
  {"insert", dbclient_insert},
  {"insert_batch", dbclient_insert_batch},
  {"is_failed", dbclient_is_failed},
  {"mapreduce", dbclient_mapreduce},
  {"query", dbclient_query},
  {"reindex", dbclient_reindex},
  {"remove", dbclient_remove},
  // {"reset_index_cache", dbclient_reset_index_cache},
  {"run_command", dbclient_run_command},
  {"update", dbclient_update},
  {"get_dbnames", dbclient_get_dbnames},
  {"get_collections", dbclient_get_collections},
  {NULL, NULL}
};


