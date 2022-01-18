use crate::{KvError, Kvpair, Storage, StorageIter, Value};
use dashmap::{mapref::one::Ref, DashMap};

/// 使用DashMap构建的MemTable, 实现了Storage trait
#[derive(Clone, Debug, Default)]
pub struct MemTable {
    tables: DashMap<String, DashMap<String, Value>>,
}

impl MemTable {
    /// 创建一个缺省的MemTable
    pub fn new() -> Self {
        Self::default()
    }

    /// 如果名为name的hash table 不存在,则创建,否则返回
    fn get_or_create_table(&self, name: &str) -> Ref<String, DashMap<String, Value>> {
        match self.tables.get(name) {
            Some(table) => table,
            None => {
                let entry = self.tables.entry(name.into()).or_default();
                entry.downgrade()
            }
        }
    }
}

impl Storage for MemTable {
    fn get(&self, table: &str, key: &str) -> Result<Option<Value>, KvError> {
        let table = self.get_or_create_table(table);
        Ok(table.get(key).map(|v| v.value().clone()))
    }

    fn set(&self, table: &str, key: String, value: Value) -> Result<Option<Value>, KvError> {
        let table = self.get_or_create_table(table);
        Ok(table.insert(key, value))
    }

    fn contains(&self, table: &str, key: &str) -> Result<bool, KvError> {
        let table = self.get_or_create_table(table);
        Ok(table.contains_key(key))
    }

    fn del(&self, table: &str, key: &str) -> Result<Option<Value>, KvError> {
        let table = self.get_or_create_table(table);
        Ok(table.remove(key).map(|(_k, v)| v))
    }

    fn get_all(&self, table: &str) -> Result<Vec<Kvpair>, KvError> {
        let table = self.get_or_create_table(table);
        Ok(table
            .iter()
            .map(|v| Kvpair::new(v.key(), v.value().clone()))
            .collect())
    }

    fn get_iter(&self, table: &str) -> Result<Box<dyn Iterator<Item = Kvpair>>, KvError> {
        // 使用clone()来获取table的snapshot
        let table = self.get_or_create_table(table).clone();
        let iter = StorageIter::new(table.into_iter()); // 这行改掉了
        Ok(Box::new(iter))
    }
}

// 从 DashMap 中 iterate 出来的值 (String, Value) 需要转换成 Kvpair，
// 我们依旧用 into() 来完成这件事。为此，需要为 Kvpair 实现这个简单的 Fromtrait：
impl From<(String, Value)> for Kvpair {
    fn from(data: (String, Value)) -> Self {
        Kvpair::new(data.0, data.1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_or_create_table_should_work() {
        let store = MemTable::new();
        assert!(!store.tables.contains_key("t1"));
        store.get_or_create_table("t1");
        assert!(store.tables.contains_key("t1"));
    }
}

// fn get_iter(&self, table: &str) -> Result<Box<dyn Iterator<Item = Kvpair>>, KvError> {
//     // 使用clone()来获取table的snapshot
//     let table = self.get_or_create_table(table).clone();
//     // 版本一:
//     // let iter = table
//     //     .iter()
//     //     .map(|v|Kvpair::new(v.key(), v.value().clone()));
//     // Ok(Box::new(iter)) // <-- 编译出错
//     //  很不幸的，编译器提示我们 Box::new(iter) 不行，“cannot return value referencing local variable table” 。
//     // 这让人很不爽，究其原因，table.iter() 使用了 table 的引用，我们返回 iter，
//     // 但 iter 引用了作为局部变量的 table，所以无法编译通过。
//
//     // 版本二:
//     // 这里又遇到了数据转换，从 DashMap 中 iterate 出来的值 (String, Value) 需要转换成 Kvpair，
//     // 我们依旧用 into() 来完成这件事。为此，需要为 Kvpair 实现这个简单的 Fromtrait：
//     // let iter = table.into_iter().map(|data| data.into());
//     // Ok(Box::new(iter))
//
//     // 版本三:
//     let iter = StorageIter::new(table.into_iter()); // 这行改掉了
//     Ok(Box::new(iter))
// }
