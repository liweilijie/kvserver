mod memory;
mod sleddb;

use crate::{KvError, Kvpair, Value};
pub use memory::MemTable;
pub use sleddb::SledDb;

/// 对存储的抽象,我们不关心数据在哪儿,但需要定义外界如何和存储打交道
pub trait Storage {
    /// 从一个HashTable里获取一个key的value
    fn get(&self, table: &str, key: &str) -> Result<Option<Value>, KvError>;
    /// 从一个HashTable里设置一个key的value, 返回旧的value
    fn set(
        &self,
        table: &str,
        key: impl Into<String>,
        value: impl Into<Value>,
    ) -> Result<Option<Value>, KvError>;
    /// 查看HashTable中是否有key
    fn contains(&self, table: &str, key: &str) -> Result<bool, KvError>;
    /// 从HashTable中删除一个key
    fn del(&self, table: &str, key: &str) -> Result<Option<Value>, KvError>;
    /// 遍历HashTable, 返回所有的kv pair (这个接口不好)
    fn get_all(&self, table: &str) -> Result<Vec<Kvpair>, KvError>;
    /// 遍历HashTable, 返回kv pair的Iterator
    fn get_iter(&self, table: &str) -> Result<Box<dyn Iterator<Item = Kvpair>>, KvError>;
}

/// 提供 Storage iterator, 这样trait的实现者只需要
/// 把它们的iterator提供给StorageIter, 然后它们保证
/// next()传出的类型实现了Into<Kvpair>即可
pub struct StorageIter<T> {
    data: T,
}

impl<T> StorageIter<T> {
    pub fn new(data: T) -> Self {
        Self { data }
    }
}

impl<T> Iterator for StorageIter<T>
where
    T: Iterator,
    T::Item: Into<Kvpair>,
{
    type Item = Kvpair;

    fn next(&mut self) -> Option<Self::Item> {
        self.data.next().map(|v| v.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn memtable_basic_interface_should_work() {
        let store = MemTable::new();
        test_basic_interface(store);
    }

    #[test]
    fn memtable_get_all_should_work() {
        let store = MemTable::new();
        test_get_all(store);
    }

    #[test]
    fn memtable_iter_should_work() {
        let store = MemTable::new();
        test_get_iter(store);
    }

    #[test]
    fn sleddb_basic_interface_should_work() {
        let dir = tempdir().unwrap();
        let store = SledDb::new(dir);
        test_basic_interface(store);
    }

    #[test]
    fn sleddb_get_all_should_work() {
        let dir = tempdir().unwrap();
        let store = SledDb::new(dir);
        test_get_all(store);
    }

    #[test]
    fn sleddb_iter_should_work() {
        let dir = tempdir().unwrap();
        let store = SledDb::new(dir);
        test_get_iter(store);
    }

    fn test_basic_interface(store: impl Storage) {
        // 第一次set 会创建table, 插入key 并返回None(之前没值)
        let v = store.set("t1", "hello", "world");
        assert!(v.unwrap().is_none());
        // 再次set同样的key会更新,并返回之前的值
        let v1 = store.set("t1", "hello", "world1");
        assert_eq!(v1.unwrap(), Some("world".into()));

        // get 存在的key会得到最新的值
        let v = store.get("t1", "hello");
        assert_eq!(v.unwrap(), Some("world1".into()));

        // get 不存在的key或者table会返回None
        assert_eq!(None, store.get("t1", "hello1").unwrap());
        assert!(store.get("t2", "hello1").unwrap().is_none());

        // contains存在的key会返回true,否则false
        assert!(store.contains("t1", "hello").unwrap());
        assert!(!store.contains("t1", "hello1").unwrap());
        assert!(!store.contains("t2", "hello").unwrap());

        // del存在的key返回之前的值
        let v = store.del("t1", "hello");
        assert_eq!(v.unwrap(), Some("world1".into()));

        // del 不存在的key或者table返回None
        assert_eq!(None, store.del("t1", "hello1").unwrap());
        assert_eq!(None, store.del("t2", "hello").unwrap());
    }

    fn test_get_all(store: impl Storage) {
        store.set("t2", "k1", "v1").unwrap();
        store.set("t2", "k2", "v2").unwrap();
        let mut data = store.get_all("t2").unwrap();
        data.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_eq!(
            data,
            vec![
                Kvpair::new("k1", "v1".into()),
                Kvpair::new("k2", "v2".into()),
            ]
        )
    }

    fn test_get_iter(store: impl Storage) {
        store.set("t2", "k1", "v1").unwrap();
        store.set("t2", "k2", "v2").unwrap();
        let mut data: Vec<_> = store.get_iter("t2").unwrap().collect();
        data.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_eq!(
            data,
            vec![
                Kvpair::new("k1", "v1".into()),
                Kvpair::new("k2", "v2".into()),
            ]
        )
    }
}
