use sled::{Db, IVec};
use std::{convert::TryInto, path::Path, str};

use crate::{KvError, Kvpair, Storage, StorageIter, Value};

#[derive(Debug)]
pub struct SledDb(Db);

impl SledDb {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self(sled::open(path).unwrap())
    }

    // 在sleddb里, 因为它可以scan_prefix, 我们用prefix
    // 来模拟一个table, 当然还可以用其他方案
    fn get_full_key(table: &str, key: &str) -> String {
        format!("{}:{}", table, key)
    }

    // 遍历table的key时, 我们直接把prefix: 当成table
    fn get_table_prefix(table: &str) -> String {
        format!("{}:", table)
    }
}

/// 把Option<Result<T, E>> flip 成 Result<Option<T>, E>
/// 从这个函数里,你可以看到函数式编程的优雅
fn flip<T, E>(x: Option<Result<T, E>>) -> Result<Option<T>, E> {
    x.map_or(Ok(None), |v| v.map(Some))
}

impl Storage for SledDb {
    fn get(&self, table: &str, key: &str) -> Result<Option<Value>, KvError> {
        let name = SledDb::get_full_key(table, key);
        let result = self.0.get(name.as_bytes())?.map(|v|v.as_ref().try_into());
        flip(result)
    }

    fn set(
        &self,
        table: &str,
        key: impl Into<String>,
        value:impl Into<Value>,
    ) -> Result<Option<Value>, KvError> {
        let key = key.into();
        let name = SledDb::get_full_key(table, &key);
        let data: Vec<u8> = value.into().try_into()?;

        let result = self.0.insert(name, data)?.map(|v|v.as_ref().try_into());
        flip(result)
    }

    fn contains(&self, table: &str, key: &str) -> Result<bool, KvError> {
        todo!()
    }

    fn del(&self, table: &str, key: &str) -> Result<Option<Value>, KvError> {
        todo!()
    }

    fn get_all(&self, table: &str) -> Result<Vec<Kvpair>, KvError> {
        todo!()
    }

    fn get_iter(&self, table: &str) -> Result<Box<dyn Iterator<Item=Kvpair>>, KvError> {
        todo!()
    }
}