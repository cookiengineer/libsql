use crate::Result;
use parking_lot::Mutex;

use super::{MakeNamespace, Namespace, NamespaceName, RestoreOption};

pub struct MetaStore<T> {
    inner: Mutex<MetaStoreInner<T>>,
}

struct MetaStoreInner<T> {
    store: Namespace<T>,
}

impl<T> MetaStore<T> {
    pub async fn new<M>(make_namespace: &M) -> Result<Self>
    where
        M: MakeNamespace<Database = T>,
    {
        let store = make_namespace
            .create(
                NamespaceName("internal".into()),
                RestoreOption::Latest,
                true,
                Box::new(|_| ()),
            )
            .await?;

        Ok(Self {
            inner: Mutex::new(MetaStoreInner { store }),
        })
    }
}
