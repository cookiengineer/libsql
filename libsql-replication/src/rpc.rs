pub mod proxy {
    #![allow(clippy::all)]

    use sqld_libsql_bindings::rusqlite::types::ValueRef;

    tonic::include_proto!("proxy");

    impl From<ValueRef<'_>> for RowValue {
        fn from(value: ValueRef<'_>) -> Self {
            use row_value::Value;

            let value = Some(match value {
                ValueRef::Null => Value::Null(true),
                ValueRef::Integer(i) => Value::Integer(i),
                ValueRef::Real(x) => Value::Real(x),
                ValueRef::Text(s) => Value::Text(String::from_utf8(s.to_vec()).unwrap()),
                ValueRef::Blob(b) => Value::Blob(b.to_vec()),
            });

            RowValue { value }
        }
    }
}

pub mod replication {
    #![allow(clippy::all)]

    use uuid::Uuid;
    tonic::include_proto!("wal_log");

    pub const NO_HELLO_ERROR_MSG: &str = "NO_HELLO";
    pub const NEED_SNAPSHOT_ERROR_MSG: &str = "NEED_SNAPSHOT";

    pub const SESSION_TOKEN_KEY: &str = "x-session-token";
    pub const NAMESPACE_METADATA_KEY: &str = "x-namespace-bin";

    // Verify that the session token is valid
    pub fn verify_session_token(
        token: &[u8],
    ) -> Result<(), Box<dyn std::error::Error + Sync + Send + 'static>> {
        let s = std::str::from_utf8(token)?;
        s.parse::<Uuid>()?;

        Ok(())
    }
}
