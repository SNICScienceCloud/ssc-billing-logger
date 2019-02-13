use chrono::offset::TimeZone;
use chrono::{DateTime, Duration, Utc};
use rust_decimal::Decimal;
use std::io::Write;
use std::str::FromStr;
use xml::writer::{EmitterConfig, EventWriter, XmlEvent};

#[derive(Debug)]
struct RecordIdentity {
    create_time: DateTime<Utc>,
    record_id: String,
}

#[derive(Debug)]
struct CloudComputeRecord {
    // <cr:RecordIdentity cr:createTime="2019-02-13T12:15:54.417093+00:00" cr:recordId="ssc/HPC2N/cr/1161cbd4-4c31-4052-8154-0c98881a1a69/1550059200" />
    record_identity: RecordIdentity,

    // <cr:Site>HPC2N</cr:Site>
    site: String,

    // <cr:Project>SNIC 2018/10-30</cr:Project>
    project: String,

    // <cr:User>s11778</cr:User>
    user: String,

    // <cr:InstanceId>1161cbd4-4c31-4052-8154-0c98881a1a69</cr:InstanceId>
    instance_id: String,

    // <cr:StartTime>2019-02-13T11:00:00+00:00</cr:StartTime>
    start_time: DateTime<Utc>,

    // <cr:EndTime>2019-02-13T12:00:00+00:00</cr:EndTime>
    end_time: DateTime<Utc>,

    // <cr:Duration>PT3600S</cr:Duration>
    duration: Duration,

    // <cr:Region>HPC2N</cr:Region>
    region: String,

    // <cr:Resource>SE-SNIC-SSC</cr:Resource>
    resource: String,

    // <cr:Zone>nova</cr:Zone>
    zone: String,

    // <cr:Flavour>ssc.small</cr:Flavour>
    flavour: String,

    // <cr:Cost>0.125</cr:Cost>
    cost: Decimal,

    // <cr:AllocatedCPU>1.0</cr:AllocatedCPU>
    allocated_cpu: Decimal,

    // <cr:AllocatedDisk>0</cr:AllocatedDisk>
    allocated_disk: u64,

    // <cr:AllocatedMemory>2048</cr:AllocatedMemory>
    allocated_memory: u64,

    used_cpu: Option<Decimal>,
    used_memory: Option<u64>,
    used_network_up: Option<u64>,
    used_network_down: Option<u64>,
    iops: Option<u64>,
}

impl CloudComputeRecord {
    fn new() -> Self {
        let create_time = Utc::now();
        // ssc/HPC2N/cr/1161cbd4-4c31-4052-8154-0c98881a1a69/1550059200
        let site = "HPC2N";
        let instance_id = "1161cbd4-4c31-4052-8154-0c98881a1a69";
        let start_time = 1550055600i64;
        let record_identity = RecordIdentity {
            create_time,
            record_id: format!("ssc/{}/cr/{}/{}", site, instance_id, start_time),
        };
        CloudComputeRecord {
            record_identity,
            site: site.to_owned(),
            project: "SNIC 2018/10-30".to_owned(),
            user: "s11778".to_owned(),
            instance_id: instance_id.to_owned(),
            start_time: Utc.timestamp(start_time, 0),
            end_time: Utc.timestamp(1550059200, 0),
            duration: Duration::seconds(3600),
            region: "HPC2N".to_owned(),
            resource: "SE-SNIC-SSC".to_owned(),
            zone: "nova".to_owned(),
            flavour: "ssc.small".to_owned(),
            cost: Decimal::from_str("0.125").unwrap(),
            allocated_cpu: Decimal::from_str("1.0").unwrap(),
            allocated_disk: 0,
            allocated_memory: 2048,
            used_cpu: None,
            used_memory: None,
            used_network_up: None,
            used_network_down: None,
            iops: None,
        }
    }

    fn write_to<W: Write>(&self, w: &mut EventWriter<W>) -> Result<(), failure::Error> {
        w.write(XmlEvent::start_element("cr:CloudComputeRecord"))?;

        w.write(
            XmlEvent::start_element("cr:RecordIdentity")
                .attr(
                    "cr:createTime",
                    &self.record_identity.create_time.to_rfc3339(),
                )
                .attr("cr:recordId", &self.record_identity.record_id),
        )?;
        w.write(XmlEvent::end_element())?;

        w.write_simple_element("cr:Site", &self.site)?;
        w.write_simple_element("cr:Project", &self.project)?;
        w.write_simple_element("cr:User", &self.user)?;
        w.write_simple_element("cr:InstanceId", &self.instance_id)?;
        w.write_simple_element("cr:StartTime", &self.start_time.to_rfc3339())?;
        w.write_simple_element("cr:EndTime", &self.end_time.to_rfc3339())?;
        w.write_simple_element("cr:Duration", &self.duration.to_string())?;
        w.write_simple_element("cr:Region", &self.region)?;
        w.write_simple_element("cr:Resource", &self.resource)?;
        w.write_simple_element("cr:Zone", &self.zone)?;
        w.write_simple_element("cr:Flavour", &self.flavour)?;
        w.write_simple_element("cr:Cost", &self.cost.to_string())?;
        w.write_simple_element("cr:AllocatedCPU", &self.allocated_cpu.to_string())?;
        w.write_simple_element("cr:AllocatedDisk", &self.allocated_disk.to_string())?;
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

#[derive(Debug)]
struct CloudStorageRecord {
    // <cr:RecordIdentity cr:createTime="2019-02-13T12:15:54.417216+00:00" cr:recordId="ssc/HPC2N/sr/41d169a8-e2e8-4e81-a8d0-6fda07316251/1550059200" />

    // <cr:Site>HPC2N</cr:Site>
    site: String,

    // <cr:Project>SNIC 2018/10-20</cr:Project>
    project: String,

    // <cr:User>s3245</cr:User>
    user: String,

    // <cr:InstanceId>41d169a8-e2e8-4e81-a8d0-6fda07316251</cr:InstanceId>
    instance_id: String,

    // <cr:StorageType>Block</cr:StorageType>
    storage_type: String,

    // <cr:StartTime>2019-02-13T11:00:00+00:00</cr:StartTime>
    start_time: DateTime<Utc>,

    // <cr:EndTime>2019-02-13T12:00:00+00:00</cr:EndTime>
    end_time: DateTime<Utc>,

    // <cr:Duration>PT3600S</cr:Duration>
    duration: Duration,

    // <cr:Region>HPC2N</cr:Region>
    region: String,

    // <cr:Resource>SE-SNIC-SSC</cr:Resource>
    resource: String,

    // <cr:Zone>nova</cr:Zone>
    zone: String,

    // <cr:Cost>0.001</cr:Cost>
    cost: Decimal,

    // <cr:AllocatedDisk>10737418240</cr:AllocatedDisk>
    allocated_disk: u64,

    // <cr:FileCount>0</cr:FileCount>
    file_count: u64,
}

fn main() -> Result<(), failure::Error> {
    let mut w = EmitterConfig::new()
        .perform_indent(true)
        .create_writer(std::io::stdout());

    w.write(
        XmlEvent::start_element("cr:CloudRecords")
            .ns("cr", "http://sams.snic.se/namespaces/2016/04/cloudrecords"),
    )?;
    CloudComputeRecord::new().write_to(&mut w)?;
    w.write(XmlEvent::end_element())?;

    Ok(())
}
