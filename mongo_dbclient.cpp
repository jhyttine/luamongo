#include <client/dbclient.h>

extern "C" {
#include <lua.h>
#include <lauxlib.h>

#if !defined(LUA_VERSION_NUM) || (LUA_VERSION_NUM < 501)
#include <compat-5.1.h>
#endif
};

#include "utils.h"
#include "common.h"

using namespace mongo;

extern int cursor_create(lua_State *L, DBClientBase *connection, const char *ns,
                         const Query &query, int nToReturn, int nToSkip,
                         const BSONObj *fieldsToReturn, int queryOptions, int batchSize);

extern void lua_to_bson(lua_State *L, int stackpos, BSONObj &obj);
extern void bson_to_lua(lua_State *L, const BSONObj &obj);
extern void lua_push_value(lua_State *L, const BSONElement &elem);


DBClientBase* userdata_to_dbclient(lua_State *L, int stackpos)
{
  // adapted from http://www.lua.org/source/5.1/lauxlib.c.html#luaL_checkudata
  void *ud = lua_touserdata(L, stackpos);
  if (ud == NULL)
    luaL_typerror(L, stackpos, "userdata");

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

  luaL_typerror(L, stackpos, LUAMONGO_DBCLIENT);
  return NULL; // should never get here
}

/***********************************************************************/
// The following methods are helpers to parse lua tables parameter
/***********************************************************************/

/**
 * Store helper
 */
template<typename T1>
struct bson_store_to_target {
};

template<>
struct bson_store_to_target<std::vector<BSONObj> > {

    static inline bool store(BSONObj &obj, std::vector<BSONObj> &objects) {
        objects.push_back(obj);
        return true;
    }

};

template<>
struct bson_store_to_target<BSONObjBuilder > {

    static inline bool store(BSONObj &obj, BSONObjBuilder &builder) {
        builder.appendElementsUnique(obj);
        return true;
    }

};

/**
 * Template to generate BSONObj with function T2 and store it to T1
 */

template<typename T1, bool (*T2)(lua_State *, int, BSONObj &)>
struct lua_to_bson_generate_and_store {

    static inline bool action(lua_State *L, int index, T1 &target) {
        BSONObj obj;
        if (T2(L, index, obj)) {
            bson_store_to_target<T1>::store(obj, target);
            return true;
        }
        return false;
    }
};

/**
 * To generate content to target from next source:
 *    "array of lua tables" (ordered)
 *       sub table object are generated with T2 function
 *    "else"
 *       object is generated with T3 function
 */
template<typename T1, bool (*T2)(lua_State *, int , BSONObj &), bool (*T3)(lua_State *, int, BSONObj &)>
bool lua_to_bson_auto_array(lua_State *L, int index, T1& target) {
    bool res = false;
    int type = lua_type(L, index);
    size_t tlen = lua_objlen(L, index);
    if (type == LUA_TTABLE && tlen) {
        for (size_t i = 1; i <= tlen; ++i) {
            lua_rawgeti(L, index, i);
            res = lua_to_bson_generate_and_store<T1, T2>::action(L, index + 1, target);
            lua_pop(L, 1);
            if (!res) {
                break;
            }
        }
    } else {
        res = lua_to_bson_generate_and_store<T1, T3>::action(L, index, target);
    }
    return res;
}

/**
 * To generate BSONObject from next source:
 *    "json string"
 *    "lua table"
 */
bool lua_to_bson_select(lua_State *L, int index, BSONObj &obj) {
    int type = lua_type(L, index);
    if (type == LUA_TSTRING) {
        const char *jsonstr = luaL_checkstring(L, index);
        obj = fromjson(jsonstr);
        return true;
    } else if (type == LUA_TTABLE) {
        lua_to_bson(L, index, obj);
        return true;
    }
    return false;
}

/**
 * To generate BSONObject from next source:
 *    "json string"
 *    "lua table"
 *    "array of lua tables" (ordered)
 */
bool lua_to_bson_ordered(lua_State *L, int index, BSONObj &object) {
    if (1) {
        BSONObjBuilder builder;
        if (lua_to_bson_auto_array<BSONObjBuilder, lua_to_bson_select, lua_to_bson_select>(L, index, builder)) {
            object = builder.obj();
            return true;
        }
    } else {
        //totally without ordered support
        //as before
        if (lua_to_bson_select(L, index, object)) {
            return true;
        }
    }
    return false;
}

/**
 * To generate Query from next source:
 *    "Query object"
 *    "json string"
 *    "lua table"
 *    "array of lua tables" (ordered)
 */
bool lua_to_bson_ordered_query(lua_State *L, int index, Query &query) {
    if (LUA_TUSERDATA == lua_type(L, index)) {
        void *uq = 0;
        uq = luaL_checkudata(L, index, LUAMONGO_QUERY);
        query = *(*((Query **) uq));
        return true;
    } else {
        BSONObj obj;
        if (lua_to_bson_ordered(L, index, obj)) {
            query = obj;
            return true;
        }
    }
    return false;
}

/**
 * To generate vector of BSONObj from next source:
 *    "json string"
 *    "lua table"
 *    "array of" (batch)
 *       "json string"
 *       "lua table"
 *       "array of lua tables" (ordered)
 */
bool lua_to_bson_batched(lua_State *L, int index, std::vector<BSONObj> &objects) {
    return lua_to_bson_auto_array<std::vector<BSONObj>, lua_to_bson_ordered, lua_to_bson_select>(L, index, objects);
}


/***********************************************************************/
// The following methods are common to all DBClients
// (DBClientConnection and DBClientReplicaSet)
/***********************************************************************/

/*
 * created = db:ensure_index(ns, json_str or lua_table OR "ARRAY OF LUA TABLE" (ORDERED) [, unique[, name]])
 */
static int dbclient_ensure_index(lua_State *L) {
  DBClientBase *dbclient = userdata_to_dbclient(L, 1);
  try {
    const char *ns = luaL_checkstring(L, 2);
    BSONObj fields;
    if (!lua_to_bson_ordered(L, 3, fields)) {
        throw (LUAMONGO_REQUIRES_JSON_OR_TABLE);
    }
    bool unique = lua_toboolean(L, 4);
    const char *name = luaL_optstring(L, 5, "");
    bool cache = true;
    bool backgroud = false;
    int v_value = -1;
    int ttl = luaL_optinteger(L, 6, 0);

    bool res = dbclient->ensureIndex(ns, fields, unique, name, cache, backgroud, v_value, ttl);

    lua_pushboolean(L, res);
    return 1;
  } catch (std::exception &e) {
    lua_pushboolean(L, 0);
    lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION,
                    "ensure_index", e.what());
    return 2;
  } catch (const char *err) {
    lua_pushboolean(L, 0);
    lua_pushstring(L, err);
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
    lua_pop(L, 4);
    std::string errmsg;
    bool success = dbclient->auth(dbname, username, password, errmsg, digestPassword);
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
    lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "is_failed(", e.what());
    return 2;
  } catch (const char *err) {
    lua_pushnil(L);
    lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "is_failed(", err);
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
  } catch (std::exception &e) {
    lua_pushnil(L);
    lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "get_server_address(", e.what());
    return 2;
  } catch (const char *err) {
    lua_pushnil(L);
    lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "get_server_address(", err);
    return 2;
  }
}

/*
 * count,err = db:count(ns, lua_table or json_str OR "ARRAY OF LUA TABLE" (ORDERED))
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
 * ok,err = db:insert(ns, lua_table or json_str OR "ARRAY OF LUA TABLE" (ORDERED))
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
 * ok,err = db:insert_batch(ns, lua_array_of_tables/json_str/"ARRAY OF LUA TABLE" (ORDERED))
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
 * cursor,err = db:query(ns, lua_table or json_str or query_obj OR "ARRAY OF LUA TABLE" (ORDERED), limit, skip, lua_table or json_str OR "ARRAY OF LUA TABLE" (ORDERED), options, batchsize)
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
 * lua_table,err = db:find_one(ns, lua_table or json_str or query_obj OR "ARRAY OF LUA TABLE" (ORDERED), lua_table or json_str OR "ARRAY OF LUA TABLE" (ORDERED), options)
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
 * ok,err = db:remove(ns, lua_table or json_str or query_obj OR "ARRAY OF LUA TABLE" (ORDERED))
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
 * ok,err = db:update(ns, lua_table or json_str or query_obj OR "ARRAY OF LUA TABLE" (ORDERED), lua_table or json_str OR "ARRAY OF LUA TABLE" (ORDERED), upsert, multi)
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
 * ok,err = db:drop_index_by_fields(ns, json_str or lua_table OR "ARRAY OF LUA TABLE" (ORDERED))
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
 * name = db:gen_index_name(json_str or lua_table OR "ARRAY OF LUA TABLE" (ORDERED))
 */
static int dbclient_gen_index_name(lua_State *L) {
  DBClientBase *dbclient = userdata_to_dbclient(L, 1);
  try {
    string name = "";
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
 * cursor,err = db:get_indexes(ns)
 */
static int dbclient_get_indexes(lua_State *L) {
  DBClientBase *dbclient = userdata_to_dbclient(L, 1);

  try {
    const char *ns = luaL_checkstring(L, 2);

    auto_ptr<DBClientCursor> autocursor = dbclient->getIndexes(ns);

    if (!autocursor.get()) {
      lua_pushnil(L);
      lua_pushstring(L, LUAMONGO_ERR_CONNECTION_LOST);
      return 2;
    }

    DBClientCursor **cursor = (DBClientCursor **) lua_newuserdata(L, sizeof (DBClientCursor *));
    *cursor = autocursor.get();
    autocursor.release();

    luaL_getmetatable(L, LUAMONGO_CURSOR);
    lua_setmetatable(L, -2);

    return 1;
  } catch (std::exception &e) {
    lua_pushnil(L);
    lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "get_indexes", e.what());
    return 2;
  } catch (const char *err) {
    lua_pushnil(L);
    lua_pushfstring(L, LUAMONGO_ERR_CALLING, LUAMONGO_CONNECTION, "get_indexes", err);
    return 2;
  }
}

/*
 * res,err = db:mapreduce(jsmapfunc, jsreducefunc[, query[, output]])
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

    const char *output = luaL_optstring(L, 6, "");
    BSONObj res = dbclient->mapreduce(ns, jsmapfunc, jsreducefunc, query, output);
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

/*
 * db:get_last_error()
 */
static int dbclient_get_last_error(lua_State *L) {
  DBClientBase *dbclient = userdata_to_dbclient(L, 1);
  try {
    string result = dbclient->getLastError();
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
 * res,err = db:run_command(dbname, lua_table or json_str OR "ARRAY OF LUA TABLE" (ORDERED), options)
 */
static int dbclient_run_command(lua_State *L) {
  DBClientBase *dbclient = userdata_to_dbclient(L, 1);

  try {
    const char *ns = luaL_checkstring(L, 2);
    int options = lua_tointeger(L, 4); // if it is invalid it returns 0

    BSONObj command; // arg 3
    if (!lua_to_bson_ordered(L, 3, command)) {
      throw (LUAMONGO_REQUIRES_JSON_OR_TABLE);
    }
    BSONObj retval;
    bool success = dbclient->runCommand(ns, command, retval, options);
    if (!success)
      throw "run_command failed";

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



// Method registration table for DBClients
extern const luaL_Reg dbclient_methods[] = {
  {"auth", dbclient_auth},
  {"count", dbclient_count},
  {"drop_collection", dbclient_drop_collection},
  {"drop_index_by_fields", dbclient_drop_index_by_fields},
  {"drop_index_by_name", dbclient_drop_index_by_name},
  {"drop_indexes", dbclient_drop_indexes},
  {"ensure_index", dbclient_ensure_index},
  {"eval", dbclient_eval},
  {"exists", dbclient_exists},
  {"find_one", dbclient_find_one},
  {"gen_index_name", dbclient_gen_index_name},
  {"get_indexes", dbclient_get_indexes},
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
  {"reset_index_cache", dbclient_reset_index_cache},
  {"run_command", dbclient_run_command},
  {"update", dbclient_update},
  {NULL, NULL}
};


