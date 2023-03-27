use std::{collections::HashMap, error::Error, sync::Arc};

use bson::{oid::ObjectId, Bson, Document};
use rlua::{Context, FromLua, Lua, ToLua, Value};

#[derive(Clone)]
pub(crate) struct LuaEngine {
    pub(crate) state: Arc<Lua>,
}

#[derive(Debug)]
pub(crate) struct LuaBsonRepr(Bson);

impl From<Bson> for LuaBsonRepr {
    fn from(bson: Bson) -> Self {
        Self(bson)
    }
}

impl ToLua<'_> for LuaBsonRepr {
    fn to_lua(self, lua: rlua::Context) -> rlua::Result<Value> {
        Ok(match self.0 {
            Bson::String(s) => s.to_lua(lua)?,
            Bson::Boolean(b) => b.to_lua(lua)?,
            Bson::JavaScriptCode(c) => c.to_lua(lua)?,
            Bson::Int32(i) => i.to_lua(lua)?,
            Bson::Int64(i) => i.to_lua(lua)?,
            Bson::Binary(t) => t.bytes.to_lua(lua)?,
            Bson::DateTime(d) => d.timestamp_millis().to_lua(lua)?,
            Bson::ObjectId(o) => LuaObjectIdRepr(o).to_lua(lua)?,
            Bson::Symbol(s) => s.to_lua(lua)?,
            Bson::Document(d) => d
                .into_iter()
                .map(|(k, v)| (k, Self(v)))
                .collect::<HashMap<_, _>>()
                .to_lua(lua)?,
            Bson::Array(a) => a.into_iter().map(Self).collect::<Vec<_>>().to_lua(lua)?,
            Bson::RegularExpression(r) => format!("{:?}", r).to_lua(lua)?,
            Bson::Double(d) => d.to_lua(lua)?,
            Bson::Decimal128(d) => format!("{:?}", d).to_lua(lua)?,
            Bson::Timestamp(t) => format!("{:?}", t).to_lua(lua)?,
            Bson::MaxKey => "MaxKey".to_lua(lua)?,
            Bson::MinKey => "MinKey".to_lua(lua)?,
            _ => Value::Nil,
        })
    }
}

impl<'lua> FromLua<'lua> for LuaBsonRepr {
    fn from_lua(lua_value: Value<'lua>, lua: Context<'lua>) -> rlua::Result<Self> {
        if let Value::Table(table) = &lua_value {
            let obj_type = table.get::<_, String>("__type");
            if let Ok(obj_type) = obj_type {
                if obj_type == "ObjectId" {
                    return Ok(LuaObjectIdRepr::from_lua(lua_value, lua)?.into());
                }
            }
        }

        Ok(match lua_value {
            Value::String(s) => Self(s.to_str()?.into()),
            Value::Boolean(b) => Self(b.into()),
            Value::Integer(i) => Self(i.into()),
            Value::Number(n) => Self(n.into()),
            Value::Table(t) => Self(
                Document::from_iter(
                    t.pairs()
                        .map(|r| {
                            let (k, v) = r?;
                            Ok((String::from_lua(k, lua)?, Self::from_lua(v, lua)?.0))
                        })
                        .collect::<rlua::Result<HashMap<_, _>>>()?,
                )
                .into(),
            ),
            Value::Nil => Self(Bson::Null),
            _ => Self(Bson::Null),
        })
    }
}

#[derive(Debug)]
pub(crate) struct LuaObjectIdRepr(bson::oid::ObjectId);

impl From<bson::oid::ObjectId> for LuaObjectIdRepr {
    fn from(value: bson::oid::ObjectId) -> Self {
        Self(value)
    }
}

impl From<LuaObjectIdRepr> for LuaBsonRepr {
    fn from(value: LuaObjectIdRepr) -> Self {
        Self(Bson::ObjectId(value.0))
    }
}

impl ToLua<'_> for LuaObjectIdRepr {
    fn to_lua(self, lua: rlua::Context) -> rlua::Result<Value> {
        let obj = lua.create_table()?;
        obj.set("__type", "ObjectId")?;
        obj.set("__value", self.0.bytes().to_lua(lua)?)?;
        obj.set("string_repr", self.0.to_string().to_lua(lua)?)?;
        obj.to_lua(lua)
    }
}

impl FromLua<'_> for LuaObjectIdRepr {
    fn from_lua(val: Value<'_>, _: Context<'_>) -> rlua::Result<Self> {
        match &val {
            Value::Table(t) => {
                let obj_type = t.get::<_, String>("__type")?;
                if obj_type != "ObjectId" {
                    return Err(rlua::Error::FromLuaConversionError {
                        from: val.type_name(),
                        to: "ObjectId",
                        message: Some("Not an ObjectId".to_string()),
                    });
                }

                let obj_value = t.get::<_, Vec<u8>>("__value")?;
                if obj_value.len() != 12 {
                    return Err(rlua::Error::FromLuaConversionError {
                        from: val.type_name(),
                        to: "ObjectId",
                        message: Some("Invalid ObjectId".to_string()),
                    });
                }
                let mut val = [0; 12];
                val.copy_from_slice(&obj_value);
                Ok(Self(ObjectId::from_bytes(val)))
            }
            _ => Err(rlua::Error::FromLuaConversionError {
                from: val.type_name(),
                to: "ObjectId",
                message: Some("Invalid ObjectId".to_string()),
            }),
        }
    }
}

impl LuaEngine {
    pub fn new() -> Result<Self, Box<dyn Error>> {
        let state = Lua::new();

        state.context(|ctx| {
            ctx.globals()
                .set(
                    "print",
                    ctx.create_function(|_, s: String| {
                        println!("{}", s);
                        Ok(())
                    })
                    .unwrap(),
                )
                .unwrap();

            ctx.globals()
                .set(
                    "println",
                    ctx.create_function(|_, s: String| {
                        println!("{}", s);
                        Ok(())
                    })
                    .unwrap(),
                )
                .unwrap();

            ctx.globals()
                .set(
                    "dumpTable",
                    ctx.create_function(|_, t: LuaBsonRepr| {
                        println!("{:#?}", t);
                        Ok(())
                    })
                    .unwrap(),
                )
                .unwrap();

            ctx.globals()
                .set(
                    "newObjectId",
                    ctx.create_function(|_, ()| {
                        let oid = ObjectId::new();
                        Ok(LuaObjectIdRepr(oid))
                    })
                    .unwrap(),
                )
                .unwrap();

            ctx.globals()
                .set(
                    "seaHash",
                    ctx.create_function(|_, v: String| {
                        let hash = seahash::hash(v.as_bytes());
                        let hash = format!("{:x}", hash);
                        Ok(hash)
                    })
                    .unwrap(),
                )
                .unwrap();
        });

        Ok(Self {
            state: Arc::new(state),
        })
    }

    pub fn load_script(&self, script: &str) -> Result<(), rlua::Error> {
        self.state.context(|ctx| ctx.load(script).exec())
    }

    pub fn load_document(&self, val: Document) -> Result<(), rlua::Error> {
        self.state.context(|ctx| {
            let globals = ctx.globals();
            let doc = ctx.create_table()?;
            for (k, v) in val {
                doc.set(k, LuaBsonRepr(v))?;
            }
            globals.set("doc", doc)?;
            Ok(())
        })
    }

    pub fn get_document(&self) -> Result<Document, rlua::Error> {
        self.state.context(|ctx| {
            let globals = ctx.globals();
            let doc = globals.get::<_, LuaBsonRepr>("doc")?;
            Ok(doc.0.as_document().unwrap().clone())
        })
    }
}
