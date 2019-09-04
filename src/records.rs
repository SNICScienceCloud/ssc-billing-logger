use chrono::offset::TimeZone;
use chrono::{DateTime, Duration, Utc};
use rust_decimal::Decimal;
use std::io::Write;
use std::str::FromStr;
use xml::writer::{EventWriter, XmlEvent};

trait EventWriterExt {
    fn write_simple_element(&mut self, name: &str, value: &str) -> Result<(), failure::Error>;
}

impl<W: Write> EventWriterExt for EventWriter<W> {
    fn write_simple_element(&mut self, name: &str, value: &str) -> Result<(), failure::Error> {
        self.write(XmlEvent::start_element(name))?;
        self.write(XmlEvent::characters(value))?;
        self.write(XmlEvent::end_element())?;

        Ok(())
    }
}

pub trait WriteToXML {
    fn write_to<W: Write>(&self, w: &mut EventWriter<W>) -> Result<(), failure::Error>;
}

pub mod v2 {
    use super::*;

    #[derive(Debug)]
    pub struct CloudRecordCommon {
        pub create_time: DateTime<Utc>,
        pub site: String,
        pub project: String,
        pub user: String,
        pub id: String,
        pub start_time: DateTime<Utc>,
        pub end_time: DateTime<Utc>,
        pub duration: Duration,
        pub region: String,
        pub resource: String,
        pub zone: String,
        pub cost: Decimal,
    }

    #[derive(Debug)]
    pub struct CloudComputeRecord {
        pub common: CloudRecordCommon,
    }
}

pub mod v1 {
    use super::*;

    #[derive(Debug)]
    pub struct CloudRecordCommon {
        // <cr:RecordIdentity cr:createTime="2019-02-13T12:15:54.417093+00:00" cr:recordId="ssc/HPC2N/cr/1161cbd4-4c31-4052-8154-0c98881a1a69/1550059200" />
        pub create_time: DateTime<Utc>,

        // <cr:Site>HPC2N</cr:Site>
        pub site: String,

        // <cr:Project>SNIC 2018/10-30</cr:Project>
        pub project: String,

        // <cr:User>s11778</cr:User>
        pub user: String,

        // <cr:InstanceId>1161cbd4-4c31-4052-8154-0c98881a1a69</cr:InstanceId>
        pub instance_id: String,

        // <cr:StartTime>2019-02-13T11:00:00+00:00</cr:StartTime>
        pub start_time: DateTime<Utc>,

        // <cr:EndTime>2019-02-13T12:00:00+00:00</cr:EndTime>
        pub end_time: DateTime<Utc>,

        // <cr:Duration>PT3600S</cr:Duration>
        pub duration: Duration,

        // <cr:Region>HPC2N</cr:Region>
        pub region: String,

        // <cr:Resource>SE-SNIC-SSC</cr:Resource>
        pub resource: String,

        // <cr:Zone>nova</cr:Zone>
        pub zone: String,

        // <cr:Cost>0.125</cr:Cost>
        pub cost: Decimal,

        // <cr:AllocatedDisk>0</cr:AllocatedDisk>
        pub allocated_disk: u64,
    }

    #[derive(Debug)]
    pub struct CloudComputeRecord {
        pub common: CloudRecordCommon,

        // <cr:Flavour>ssc.small</cr:Flavour>
        pub flavour: String,

        // <cr:AllocatedCPU>1.0</cr:AllocatedCPU>
        pub allocated_cpu: Decimal,

        // <cr:AllocatedMemory>2048</cr:AllocatedMemory>
        pub allocated_memory: u64,

        pub used_cpu: Option<Decimal>,
        pub used_memory: Option<u64>,
        pub used_network_up: Option<u64>,
        pub used_network_down: Option<u64>,
        pub iops: Option<u64>,
    }

    impl CloudComputeRecord {
        pub fn example() -> Self {
            let create_time = Utc::now();
            // ssc/HPC2N/cr/1161cbd4-4c31-4052-8154-0c98881a1a69/1550059200
            let common = CloudRecordCommon {
                create_time,
                site: "HPC2N".to_owned(),
                project: "SNIC 2018/10-30".to_owned(),
                user: "s11778".to_owned(),
                instance_id: "1161cbd4-4c31-4052-8154-0c98881a1a69".to_owned(),
                start_time: Utc.timestamp(1550055600i64, 0),
                end_time: Utc.timestamp(1550059200, 0),
                duration: Duration::seconds(3600),
                region: "HPC2N".to_owned(),
                resource: "SE-SNIC-SSC".to_owned(),
                zone: "nova".to_owned(),
                cost: Decimal::from_str("0.125").unwrap(),
                allocated_disk: 0,
            };

            CloudComputeRecord {
                common,
                flavour: "ssc.small".to_owned(),
                allocated_cpu: Decimal::from_str("1.0").unwrap(),
                allocated_memory: 2048,
                used_cpu: None,
                used_memory: None,
                used_network_up: None,
                used_network_down: None,
                iops: None,
            }
        }
    }

    impl WriteToXML for CloudComputeRecord {
        fn write_to<W: Write>(&self, w: &mut EventWriter<W>) -> Result<(), failure::Error> {
            let common = &self.common;
            w.write(XmlEvent::start_element("cr:CloudComputeRecord"))?;

            w.write(
                XmlEvent::start_element("cr:RecordIdentity")
                    .attr("cr:createTime", &common.create_time.to_rfc3339())
                    .attr(
                        "cr:recordId",
                        &format!(
                            "ssc/{}/cr/{}/{}",
                            common.site,
                            common.instance_id,
                            common.end_time.timestamp()
                        ),
                    ),
            )?;
            w.write(XmlEvent::end_element())?;

            w.write_simple_element("cr:Site", &common.site)?;
            w.write_simple_element("cr:Project", &common.project)?;
            w.write_simple_element("cr:User", &common.user)?;
            w.write_simple_element("cr:InstanceId", &common.instance_id)?;
            w.write_simple_element("cr:StartTime", &common.start_time.to_rfc3339())?;
            w.write_simple_element("cr:EndTime", &common.end_time.to_rfc3339())?;
            w.write_simple_element("cr:Duration", &common.duration.to_string())?;
            w.write_simple_element("cr:Region", &common.region)?;
            w.write_simple_element("cr:Resource", &common.resource)?;
            w.write_simple_element("cr:Zone", &common.zone)?;
            w.write_simple_element("cr:Flavour", &self.flavour)?;
            w.write_simple_element("cr:Cost", &common.cost.to_string())?;
            w.write_simple_element("cr:AllocatedCPU", &self.allocated_cpu.to_string())?;
            w.write_simple_element("cr:AllocatedDisk", &common.allocated_disk.to_string())?;
            w.write_simple_element("cr:AllocatedMemory", &self.allocated_memory.to_string())?;

            if let Some(v) = self.used_cpu {
                w.write_simple_element("cr:UsedCPU", &v.to_string())?;
            }
            if let Some(v) = self.used_memory {
                w.write_simple_element("cr:UsedMemory", &v.to_string())?;
            }
            if let Some(v) = self.used_network_up {
                w.write_simple_element("cr:UsedNetworkUp", &v.to_string())?;
            }
            if let Some(v) = self.used_network_down {
                w.write_simple_element("cr:UsedNetworkDown", &v.to_string())?;
            }
            if let Some(v) = self.iops {
                w.write_simple_element("cr:IOPS", &v.to_string())?;
            }

            w.write(XmlEvent::end_element())?;

            Ok(())
        }
    }

    #[derive(Debug)]
    pub struct CloudStorageRecord {
        pub common: CloudRecordCommon,

        // <cr:StorageType>Block</cr:StorageType>
        pub storage_type: String,

        // <cr:FileCount>0</cr:FileCount>
        pub file_count: u64,
    }

    impl CloudStorageRecord {
        pub fn example() -> Self {
            let create_time = Utc::now();
            let common = CloudRecordCommon {
                create_time,
                site: "HPC2N".to_owned(),
                project: "SNIC 2018/10-20".to_owned(),
                user: "s3245".to_owned(),
                instance_id: "41d169a8-e2e8-4e81-a8d0-6fda07316251".to_owned(),
                start_time: Utc.timestamp(1550055600i64, 0),
                end_time: Utc.timestamp(1550059200, 0),
                duration: Duration::seconds(3600),
                region: "HPC2N".to_owned(),
                resource: "SE-SNIC-SSC".to_owned(),
                zone: "nova".to_owned(),
                cost: Decimal::from_str("0.001").unwrap(),
                allocated_disk: 10737418240u64,
            };
            CloudStorageRecord {
                common,
                storage_type: "Block".to_owned(),
                file_count: 0u64,
            }
        }
    }

    impl WriteToXML for CloudStorageRecord {
        fn write_to<W: Write>(&self, w: &mut EventWriter<W>) -> Result<(), failure::Error> {
            let common = &self.common;
            w.write(XmlEvent::start_element("cr:CloudStorageRecord"))?;

            w.write(
                XmlEvent::start_element("cr:RecordIdentity")
                    .attr("cr:createTime", &common.create_time.to_rfc3339())
                    .attr(
                        "cr:recordId",
                        &format!(
                            "ssc/{}/cr/{}/{}",
                            common.site,
                            common.instance_id,
                            common.end_time.timestamp()
                        ),
                    ),
            )?;
            w.write(XmlEvent::end_element())?;

            w.write_simple_element("cr:Site", &common.site)?;
            w.write_simple_element("cr:Project", &common.project)?;
            w.write_simple_element("cr:User", &common.user)?;
            w.write_simple_element("cr:InstanceId", &common.instance_id)?;
            w.write_simple_element("cr:StorageType", &self.storage_type)?;
            w.write_simple_element("cr:StartTime", &common.start_time.to_rfc3339())?;
            w.write_simple_element("cr:EndTime", &common.end_time.to_rfc3339())?;
            w.write_simple_element("cr:Duration", &common.duration.to_string())?;
            w.write_simple_element("cr:Region", &common.region)?;
            w.write_simple_element("cr:Resource", &common.resource)?;
            w.write_simple_element("cr:Zone", &common.zone)?;
            w.write_simple_element("cr:Cost", &common.cost.to_string())?;
            w.write_simple_element("cr:AllocatedDisk", &common.allocated_disk.to_string())?;
            w.write_simple_element("cr:FileCount", &self.file_count.to_string())?;

            w.write(XmlEvent::end_element())?;

            Ok(())
        }
    }

    pub fn write_xml_to<'a, W, ComputeIter, StorageIter>(
        writer: W,
        computes: ComputeIter,
        storages: StorageIter,
    ) -> Result<(), failure::Error>
    where
        W: Write,
        ComputeIter: IntoIterator<Item = &'a CloudComputeRecord>,
        StorageIter: IntoIterator<Item = &'a CloudStorageRecord>,
    {
        use xml::writer::EmitterConfig;
        let mut w = EmitterConfig::new()
            .perform_indent(true)
            .create_writer(writer);

        w.write(
            XmlEvent::start_element("cr:CloudRecords")
                .ns("cr", "http://sams.snic.se/namespaces/2016/04/cloudrecords"),
        )?;
        for cr in computes {
            cr.write_to(&mut w)?;
        }
        for sr in storages {
            sr.write_to(&mut w)?;
        }
        w.write(XmlEvent::end_element())?;
        Ok(())
    }
}
