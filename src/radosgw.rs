extern crate failure;

pub mod admin {
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;

    #[derive(Debug, Deserialize, Serialize, Clone)]
    pub struct BucketStats {
        pub bucket: String,
        pub pool: String,
        pub index_pool: String,
        pub id: String,
        pub marker: String,
        pub owner: String,
        pub ver: String,
        pub master_ver: String,
        pub mtime: String,
        pub max_marker: String,
        pub usage: HashMap<String, BucketStatsUsage>,
        pub bucket_quota: BucketStatsBucketQuota,
    }

    #[derive(Debug, Deserialize, Serialize, Clone)]
    pub struct BucketStatsUsage {
        pub size_kb: u64,
        pub size_kb_actual: u64,
        pub num_objects: u64,
    }

    #[derive(Debug, Deserialize, Serialize, Clone)]
    pub struct BucketStatsBucketQuota {
        pub enabled: bool,
        pub max_size_kb: i64,
        pub max_objects: i64,
    }

    pub fn bucket_stats() -> Result<Vec<BucketStats>, failure::Error> {
        let output = subprocess::Exec::cmd("radosgw-admin")
            .args(&["bucket", "stats"])
            .capture()?
            .stdout_str();
        trace!("{}", output);
        std::fs::write("bucket_stats.json", &output).unwrap();
        let statses: Vec<BucketStats> = serde_json::from_str(&output)?;
        Ok(statses)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use chrono; // 0.4.6
    use chrono::{DateTime, Utc};
    use serde; // 1.0.88
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Deserialize, Serialize, Clone)]
    pub struct Foo {
        pub mtime: DateTime<Utc>,
    }

    #[test]
    fn read_bucket_infos() {
        let _infos = admin::bucket_stats().unwrap();
    }
}
