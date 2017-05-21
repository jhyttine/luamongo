#include <iostream>
#include <client/dbclient.h>
#include "utils.h"
#include "common.h"
#include <limits.h>
#include <sstream>

using namespace mongo;

extern void push_bsontype_table(lua_State* L, mongo::BSONType bsontype);
void lua_push_value(lua_State *L, const BSONElement &elem);
const char *bson_name(int type);

static void bson_to_array(lua_State *L, const BSONObj &obj) {
    BSONObjIterator it = BSONObjIterator(obj);

    lua_newtable(L);

    int n = 1;
    while (it.more()) {
        BSONElement elem = it.next();

        lua_push_value(L, elem);
        lua_rawseti(L, -2, n++);
    }
}

static void bson_to_table(lua_State *L, const BSONObj &obj) {
    BSONObjIterator it = BSONObjIterator(obj);

    lua_newtable(L);

    while (it.more()) {
        BSONElement elem = it.next();
        const char *key = elem.fieldName();

        lua_pushstring(L, key);
        lua_push_value(L, elem);
        lua_rawset(L, -3);
    }
}

void lua_push_value(lua_State *L, const BSONElement &elem) {
    lua_checkstack(L, 2);
    int type = elem.type();

    switch(type) {
    case mongo::Undefined:
        lua_pushnil(L);
        break;
    case mongo::NumberInt:
        lua_pushinteger(L, elem.numberInt());
        break;
    case mongo::NumberLong:
    case mongo::NumberDouble:
        lua_pushnumber(L, elem.number());
        break;
    case mongo::Bool:
        lua_pushboolean(L, elem.boolean());
        break;
    case mongo::String:
        lua_pushstring(L, elem.valuestr());
        break;
    case mongo::Array:
        bson_to_array(L, elem.embeddedObject());
        break;
    case mongo::Object:
        bson_to_table(L, elem.embeddedObject());
        break;
    case mongo::Date:
        push_bsontype_table(L, mongo::Date);
        lua_pushnumber(L, elem.date());
        lua_rawseti(L, -2, 1);
        break;
    case mongo::Timestamp:
	{
	    push_bsontype_table(L, mongo::Date);
	    Timestamp_t t = elem.Timestamp();
	    lua_pushnumber(L, t.seconds() + t.increment());
	    lua_rawseti(L, -2, 1);
	}
        break;
    case mongo::Symbol:
        push_bsontype_table(L, mongo::Symbol);
        lua_pushstring(L, elem.valuestr());
        lua_rawseti(L, -2, 1);
        break;
    case mongo::BinData: {
        push_bsontype_table(L, mongo::BinData);
        int l;
        const char* c = elem.binData(l);
        lua_pushlstring(L, c, l);
        lua_rawseti(L, -2, 1);
        break;
    }
    case mongo::RegEx:
        push_bsontype_table(L, mongo::RegEx);
        lua_pushstring(L, elem.regex());
        lua_rawseti(L, -2, 1);
        lua_pushstring(L, elem.regexFlags());
        lua_rawseti(L, -2, 2);
        break;
    case mongo::jstOID:
        push_bsontype_table(L, mongo::jstOID);
        lua_pushstring(L, elem.__oid().toString().c_str());
        lua_rawseti(L, -2, 1);
        break;
    case mongo::jstNULL:
        push_bsontype_table(L, mongo::jstNULL);
        break;
    case mongo::EOO:
        break;
    /*default:
        luaL_error(L, LUAMONGO_UNSUPPORTED_BSON_TYPE, bson_name(type));*/
    }
}

static void lua_append_bson(lua_State *L, const char *key, int stackpos, BSONObjBuilder *builder, int ref) {
    int type = lua_type(L, stackpos);

    if (type == LUA_TTABLE) {
        if (stackpos < 0) stackpos = lua_gettop(L) + stackpos + 1;
        lua_checkstack(L, 3);

        int bsontype_found = luaL_getmetafield(L, stackpos, "__bsontype");
        if (!bsontype_found) {
            // not a special bsontype
            // handle as a regular table, iterating keys

            lua_rawgeti(L, LUA_REGISTRYINDEX, ref);
            lua_pushvalue(L, stackpos);
            lua_rawget(L, -2);
            if (lua_toboolean(L, -1)) { // do nothing if the same table encountered
                lua_pop(L, 2);
            } else {
                lua_pop(L, 1);
                lua_pushvalue(L, stackpos);
                lua_pushboolean(L, 1);
                lua_rawset(L, -3);
                lua_pop(L, 1);

                BSONObjBuilder b;

                bool dense = true;
                int len = 0;
                for (lua_pushnil(L); lua_next(L, stackpos); lua_pop(L, 1)) {
                    ++len;
                    if ((lua_type(L, -2) != LUA_TNUMBER) || (lua_tointeger(L, -2) != len)) {
                        lua_pop(L, 2);
                        dense = false;
                        break;
                    }
                }

                if (dense) {
                    for (int i = 0; i < len; i++) {
                        lua_rawgeti(L, stackpos, i+1);
                        std::stringstream ss;
                        ss << i;

                        lua_append_bson(L, ss.str().c_str(), -1, &b, ref);
                        lua_pop(L, 1);
                    }

                    builder->appendArray(key, b.obj());
                } else {
                    for (lua_pushnil(L); lua_next(L, stackpos); lua_pop(L, 1)) {
                        switch (lua_type(L, -2)) { // key type
                            case LUA_TNUMBER: {
                                std::stringstream ss;
                                ss << lua_tonumber(L, -2);
                                lua_append_bson(L, ss.str().c_str(), -1, &b, ref);
                                break;
                            }
                            case LUA_TSTRING: {
                                lua_append_bson(L, lua_tostring(L, -2), -1, &b, ref);
                                break;
                            }
                        }
                    }

                    builder->append(key, b.obj());
                }
            }
        } else {
            int bson_type = lua_tointeger(L, -1);
            lua_pop(L, 1);
            lua_rawgeti(L, -1, 1);
            switch (bson_type) {
            case mongo::Date:
                builder->appendDate(key, lua_tonumber(L, -1));
                break;
            case mongo::Timestamp:
                builder->appendTimestamp(key);
                break;
            case mongo::RegEx: {
                const char* regex = lua_tostring(L, -1);
                lua_rawgeti(L, -2, 2); // options
                const char* options = lua_tostring(L, -1);
                lua_pop(L, 1);
                if (regex && options) builder->appendRegex(key, regex, options);
                break;
            }
            case mongo::NumberInt:
                builder->append(key, static_cast<int32_t>(lua_tointeger(L, -1)));
                break;
            case mongo::NumberLong:
                builder->append(key, static_cast<long long int>(lua_tonumber(L, -1)));
                break;
            case mongo::Symbol: {
                const char* c = lua_tostring(L, -1);
                if (c) builder->appendSymbol(key, c);
                break;
            }
            case mongo::BinData: {
                size_t l;
                const char* c = lua_tolstring(L, -1, &l);
                if (c) builder->appendBinData(key, l, mongo::BinDataGeneral, c);
                break;
            }
            case mongo::jstOID: {
                OID oid;
                const char* c = lua_tostring(L, -1);
                if (c) {
                    oid.init(c);
                    builder->appendOID(key, &oid);
                }
                break;
            }
            case mongo::jstNULL:
                builder->appendNull(key);
                break;

            /*default:
                luaL_error(L, LUAMONGO_UNSUPPORTED_BSON_TYPE, luaL_typename(L, stackpos));*/
            }
            lua_pop(L, 1);
        }
    } else if (type == LUA_TNIL) {
        builder->appendNull(key);
    } else if (type == LUA_TNUMBER) {
        double numval = lua_tonumber(L, stackpos);
        if ((numval == floor(numval)) && fabs(numval)< INT_MAX ) {
            // The numeric value looks like an integer, treat it as such.
            // This is closer to how JSON datatypes behave.
            int intval = lua_tointeger(L, stackpos);
            builder->append(key, static_cast<int32_t>(intval));
        } else {
            builder->append(key, numval);
        }
    } else if (type == LUA_TBOOLEAN) {
        builder->appendBool(key, lua_toboolean(L, stackpos));
    } else if (type == LUA_TSTRING) {
        builder->append(key, lua_tostring(L, stackpos));
    }/* else {
        luaL_error(L, LUAMONGO_UNSUPPORTED_LUA_TYPE, luaL_typename(L, stackpos));
    }*/
}

void bson_to_lua(lua_State *L, const BSONObj &obj) {
    if (obj.isEmpty()) {
        lua_pushnil(L);
    } else {
        bson_to_table(L, obj);
    }
}

// stackpos must be relative to the bottom, i.e., not negative
void lua_to_bson(lua_State *L, int stackpos, BSONObj &obj) {
    BSONObjBuilder builder;

    lua_newtable(L);
    int ref = luaL_ref(L, LUA_REGISTRYINDEX);

    for (lua_pushnil(L); lua_next(L, stackpos); lua_pop(L, 1)) {
        switch (lua_type(L, -2)) { // key type
            case LUA_TNUMBER: {
                std::ostringstream ss;
                ss << lua_tonumber(L, -2);
                lua_append_bson(L, ss.str().c_str(), -1, &builder, ref);
                break;
            }
            case LUA_TSTRING: {
                lua_append_bson(L, lua_tostring(L, -2), -1, &builder, ref);
                break;
            }
        }
    }
    luaL_unref(L, LUA_REGISTRYINDEX, ref);

    obj = builder.obj();
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
    static inline bool store(BSONObj& obj, std::vector<BSONObj>& objects) {
        objects.push_back(obj);
        return true;
    }
};

template<>
struct bson_store_to_target<BSONObjBuilder > {
    static inline bool store(BSONObj& obj, BSONObjBuilder& builder) {
        builder.appendElementsUnique(obj);
        return true;
    }
};

/**
 * Template to generate BSONObj with function T2 and store it to T1
 */

template<typename T1, bool (*T2)(lua_State*, int, BSONObj&)>
struct lua_to_bson_generate_and_store {
    static inline bool action(lua_State* L, int index, T1& target) {
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
template<typename T1, bool (*T2)(lua_State*, int , BSONObj&), bool (*T3)(lua_State*, int, BSONObj&)>
bool lua_to_bson_auto_array(lua_State* L, int index, T1& target) {
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
bool lua_to_bson_select(lua_State* L, int index, BSONObj& obj) {
    switch (lua_type(L, index)) {
        case LUA_TSTRING:
        {
            const char *jsonstr = luaL_checkstring(L, index);
            obj = fromjson(jsonstr);
            return true;
            break;
        }
        case LUA_TTABLE:
        {
            lua_to_bson(L, index, obj);
            return true;
            break;
        }
        default:
        {
            return false;
        }
    };
    return false;
}

/**
 * To generate BSONObject from next source:
 *    "json string"
 *    "lua table"
 *    "array of lua tables" (ordered)
 */
bool lua_to_bson_ordered(lua_State* L, int index, BSONObj& object) {
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
bool lua_to_bson_batched(lua_State* L, int index, std::vector<BSONObj>& objects) {
    return lua_to_bson_auto_array<std::vector<BSONObj>, lua_to_bson_ordered, lua_to_bson_select>(L, index, objects);
}

/***********************************************************************/
//
/***********************************************************************/

/**
 * @param type
 * @return 
 */
const char *bson_name(int type) {
    const char *name;

    switch(type) {
        case mongo::EOO:
            name = "EndOfObject";
            break;
        case mongo::NumberDouble:
            name = "NumberDouble";
            break;
        case mongo::String:
            name = "String";
            break;
        case mongo::Object:
            name = "Object";
            break;
        case mongo::Array:
            name = "Array";
            break;
        case mongo::BinData:
            name = "BinData";
            break;
        case mongo::Undefined:
            name = "Undefined";
            break;
        case mongo::jstOID:
            name = "ObjectID";
            break;
        case mongo::Bool:
            name = "Bool";
            break;
        case mongo::Date:
            name = "Date";
            break;
        case mongo::jstNULL:
            name = "NULL";
            break;
        case mongo::RegEx:
            name = "RegEx";
            break;
        case mongo::DBRef:
            name = "DBRef";
            break;
        case mongo::Code:
            name = "Code";
            break;
        case mongo::Symbol:
            name = "Symbol";
            break;
        case mongo::CodeWScope:
            name = "CodeWScope";
            break;
        case mongo::NumberInt:
            name = "NumberInt";
            break;
        case mongo::Timestamp:
            name = "Timestamp";
            break;
        case mongo::NumberLong:
            name = "NumberLong";
            break;
        default:
            name = "UnknownType";
    }

    return name;
}

/* this was removed in Lua 5.2 */
LUALIB_API int luaL_typeerror (lua_State *L, int narg, const char *tname) {
  const char *msg;
  msg = lua_pushfstring(L, "%s expected, got %s",
                        tname, lua_typename(L, narg));
  return luaL_argerror(L, narg, msg);
}

#if LUA_VERSION_NUM < 502
LUALIB_API void luaL_setfuncs(lua_State *L, const luaL_Reg *l, int nup) {
  luaL_checkstack(L, nup+1, "too many upvalues");
  for (; l->name != NULL; l++) {  /* fill the table with given functions */
    int i;
    lua_pushstring(L, l->name);
    for (i = 0; i < nup; i++)  /* copy upvalues to the top */
      lua_pushvalue(L, -(nup+1));
    lua_pushcclosure(L, l->func, nup);  /* closure with those upvalues */
    lua_settable(L, -(nup + 3));
  }
  lua_pop(L, nup);  /* remove upvalues */
}
#endif
