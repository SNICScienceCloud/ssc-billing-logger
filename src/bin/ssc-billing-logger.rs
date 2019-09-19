use ::ssc_billing_logger::openstack;
use ::ssc_billing_logger::radosgw;
use ::ssc_billing_logger::records;

#[macro_use] extern crate failure;
#[macro_use] extern crate log;

use chrono::{DateTime, Timelike, Utc};
use num::{ToPrimitive, Zero};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;
// use std::str::FromStr;
use structopt::StructOpt;
use url::Url;

#[derive(Debug, StructOpt)]
#[structopt(rename_all = "kebab_case")]
struct Opt {
    #[structopt(short, long, parse(from_os_str))]
    config: PathBuf,

    #[structopt(long)]
    rewrite_host: bool,

    #[structopt(long, parse(from_os_str))]
    save_snapshot: Option<PathBuf>,

    #[structopt(long, parse(from_os_str))]
    load_snapshot: Option<PathBuf>,

    #[structopt(long)]
    dry_run: bool,

    #[structopt(long)]
    force: bool,
}

#[derive(Debug, Deserialize)]
pub struct Config {
    username: String,
    password: String,
    domain: String,
    project: String,
    keystone_url: Url,

    site: String,
    resource: String,
    region: String,
    datadir: String,
}

#[derive(Debug, Deserialize)]
pub struct Costs {
    regions: HashMap<String, HashMap<String, Decimal>>,
}

#[derive(Debug, Default)]
pub struct ProjectBreakdown<'a> {
    active: Vec<(Decimal, &'a openstack::nova::Server)>,
    inert: Vec<(Decimal, &'a openstack::nova::Server)>,
    volumes: Vec<(Decimal, &'a openstack::cinder::Volume)>,
    images: Vec<(Decimal, &'a openstack::glance::Image)>,
}

#[derive(Debug, Eq, PartialEq, Hash)]
enum BillingCategory {
    Active,
    Inactive,
    Unbilled,
}

impl BillingCategory {
    fn from_status(s: &str) -> BillingCategory {
        match s {
            "PAUSED" | "SUSPENDED" | "SOFT_SUSPENDED" | "SOFT_DELETED" | "SHUTOFF" => {
                BillingCategory::Inactive
            }
            "DELETED" | "SHELVED" | "SHELVED_OFFLOADED" => BillingCategory::Unbilled,
            _ => BillingCategory::Active,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Default)]
struct PersistentState {
    last_timepoint: Option<DateTime<Utc>>,
}

#[derive(Debug)]
struct PersistentStateFile {
    filename: PathBuf,
    state: PersistentState,
}

impl PersistentStateFile {
    fn open<P: Into<PathBuf>>(datadir: P) -> Result<PersistentStateFile, failure::Error> {
        let filename = datadir.into().join("logger-state/state.json");
        let fh = File::open(&filename);
        let state = fh
            .ok()
            .and_then(|fh| serde_json::from_reader(fh).ok())
            .unwrap_or_default();
        Ok(PersistentStateFile { filename, state })
    }

    fn write(&self) -> Result<(), failure::Error> {
        let contents = serde_json::to_vec_pretty(&self.state)?;
        std::fs::write(&self.filename, &contents)?;
        Ok(())
    }
}

const DEFAULT_USER: &str = "default";
const DEFAULT_PROJECT: &str = "default";
const DEFAULT_ZONE: &str = "default";

#[derive(Debug, Serialize, Deserialize)]
struct Snapshot {
    version: usize,
    datetime: DateTime<Utc>,
    servers: Vec<openstack::nova::Server>,
    flavors: openstack::Flavors,
    images: Vec<openstack::glance::Image>,
    volumes: Vec<openstack::cinder::Volume>,
    object_bucket_stats: Option<Vec<radosgw::admin::BucketStats>>,
    users: openstack::NameMapping,
    projects: openstack::NameMapping,
}

struct PerProjectInfo<'a> {
    categorized_server_costs_by_project: HashMap<
        BillingCategory,
        HashMap<String, Vec<(Decimal, &'a openstack::nova::Server)>>,
    >,
    volume_costs_by_project: HashMap<String, Vec<(Decimal, &'a openstack::cinder::Volume)>>,
    image_costs_by_project: HashMap<String, Vec<(Decimal, &'a openstack::glance::Image)>>,
}

impl<'a> PerProjectInfo<'a> {
    fn new() -> Self {
        Self {
            categorized_server_costs_by_project: HashMap::new(),
            volume_costs_by_project: HashMap::new(),
            image_costs_by_project: HashMap::new(),
        }
    }
}

fn collate_breakdowns(ppi: &PerProjectInfo) {
    // Group by instance status

    let mut project_breakdowns: HashMap<String, ProjectBreakdown> = HashMap::new();

    if let Some(category) = ppi.categorized_server_costs_by_project.get(&BillingCategory::Active) {
        for (proj, server_costs) in category.iter() {
            let costs = server_costs.iter().map(|c| c.0).collect::<Vec<Decimal>>();
            let total_cost = costs.iter().fold(Decimal::zero(), |sum, cost| sum + cost);
            debug!(
                "proj: {}; active server costs: {:.5} = sum({:.5?})",
                proj, total_cost, costs
            );
            project_breakdowns
                .entry(proj.clone())
                .or_default()
                .active
                .extend(server_costs.iter());
        }
    }

    if let Some(category) = ppi.categorized_server_costs_by_project.get(&BillingCategory::Inactive) {
        for (proj, server_costs) in category.iter() {
            let costs = server_costs.iter().map(|c| c.0).collect::<Vec<Decimal>>();
            let total_cost = costs.iter().fold(Decimal::zero(), |sum, cost| sum + cost);
            debug!(
                "proj: {}; inactive server costs: {:.5} = sum({:.5?})",
                proj, total_cost, costs
            );
            project_breakdowns
                .entry(proj.clone())
                .or_default()
                .inert
                .extend(server_costs.iter());
        }
    }

    if let Some(category) = ppi.categorized_server_costs_by_project.get(&BillingCategory::Unbilled) {
        for (proj, server_costs) in category.iter() {
            let costs = server_costs.iter().map(|c| c.0).collect::<Vec<Decimal>>();
            let total_cost = costs.iter().fold(Decimal::zero(), |sum, cost| sum + cost);
            debug!(
                "proj: {}; unbilled server costs: {:.5} = sum({:.5?})",
                proj, total_cost, costs
            );
            project_breakdowns
                .entry(proj.clone())
                .or_default()
                .inert
                .extend(server_costs.iter());
        }
    }

    for (proj, volume_costs) in ppi.volume_costs_by_project.iter() {
        let costs = volume_costs.iter().map(|c| c.0).collect::<Vec<Decimal>>();
        let total_cost = costs.iter().fold(Decimal::zero(), |sum, cost| sum + cost);
        debug!(
            "proj: {}; volume costs: {:.5} = sum({:.5?})",
            proj, total_cost, costs
        );
        project_breakdowns
            .entry(proj.clone())
            .or_default()
            .volumes
            .extend(volume_costs.iter());
    }

    for (proj, image_costs) in ppi.image_costs_by_project.iter() {
        let costs = image_costs.iter().map(|c| c.0).collect::<Vec<Decimal>>();
        let total_cost = costs.iter().fold(Decimal::zero(), |sum, cost| sum + cost);
        debug!(
            "proj: {}; image costs: {:.5} = sum({:.5?})",
            proj, total_cost, costs
        );
        project_breakdowns
            .entry(proj.clone())
            .or_default()
            .images
            .extend(image_costs.iter());
    }
}

fn main() -> Result<(), failure::Error> {
    env_logger::init();

    let opt = Opt::from_args();
    info!("Loading configuration from {:?}", &opt.config);
    let cfg: Config = serde_json::from_reader(File::open(&opt.config)?)?;
    let datadir = PathBuf::from(&cfg.datadir);
    info!("Opening persistent state file in {}", &cfg.datadir);
    let mut persistent_state = PersistentStateFile::open(&cfg.datadir)?;

    let costs_path = datadir.join("logger-state/costs.json");
    info!("Reading costs from {:?}", &costs_path);
    let costs: Costs =
        serde_json::from_reader(File::open(&costs_path)?)?;

    let region_costs = costs
        .regions
        .get(&cfg.region)
        .ok_or(format_err!("Region {} not found in costs.json", cfg.region))?;

    let now = Utc::now();
    let this_run_datetime = now.date().and_hms(now.hour(), 0, 0);
    if !opt.force {
        if let Some(last_run) = persistent_state.state.last_timepoint {
            if last_run == this_run_datetime {
                return Ok(());
            }
        }
    }

    let snap = if let Some(snap_path) = opt.load_snapshot {
        let snap: Snapshot =
            serde_json::from_str(&std::fs::read_to_string(snap_path).unwrap()).unwrap();
        snap
    } else {
        let credentials = openstack::Credentials {
            username: cfg.username,
            password: cfg.password,
            domain: cfg.domain,
            project: cfg.project,
        };

        let session = openstack::Session::new(
            &credentials,
            &cfg.keystone_url,
            &cfg.region,
            opt.rewrite_host,
        )?;

        let servers = session.servers()?;
        let flavors = session.flavors()?;
        let images = session.images()?;
        let volumes = session.volumes()?;
        let object_bucket_stats = radosgw::admin::bucket_stats();

        let users = session.user_mappings()?;
        let projects = session.project_mappings()?;

        let snap = Snapshot {
            version: 1,
            datetime: this_run_datetime,
            servers,
            flavors,
            images,
            volumes,
            object_bucket_stats: object_bucket_stats.ok(),
            users,
            projects,
        };

        if let Some(snap_path) = opt.save_snapshot {
            std::fs::write(snap_path, &serde_json::to_string_pretty(&snap).unwrap()).unwrap();
        }

        snap
    };

    let mut object_bucket_costs = HashMap::new();
    if let Some(stats) = &snap.object_bucket_stats {
        let gig_rate = region_costs
            .get("storage.object")
            .cloned()
            .unwrap_or(0u32.into());
        let kb_to_gb = Decimal::from(1u32) / Decimal::from(1024u32.pow(2));
        for s in stats {
            if !s.usage.is_empty() {
                let gb_sum = s.usage.iter().fold(Decimal::from(0u32), |sum, u| {
                    sum + Decimal::from(u.1.size_kb) * kb_to_gb
                });
                let cost = gig_rate * gb_sum;
                object_bucket_costs.insert(s.id.clone(), (cost, s, gb_sum));
            }
        }
    }
    debug!("{:?}", object_bucket_costs);

    let start_time = Utc::now()
        .with_minute(0)
        .unwrap()
        .with_second(0)
        .unwrap()
        .with_nanosecond(0)
        .unwrap();
    let duration = chrono::Duration::hours(1);
    let end_time = start_time + duration;

    // Operator test project - "SNIC 2018/10-1"
    let _op_servers = snap
        .servers
        .iter()
        .filter(|srv| srv.tenant_id == "7d4b838241d9486e972bf1b371cc8718");

    let mut used_os_volume_discount: HashMap<String, u64> = HashMap::new();

    let mut ppi = PerProjectInfo::new();

    let mut v1_compute_records: Vec<records::v1::CloudComputeRecord> = Vec::new();
    let mut v1_storage_records: Vec<records::v1::CloudStorageRecord> = Vec::new();

    info!("Processing servers");
    for server in &snap.servers {
        use openstack::nova;

        let user = snap.users.get(&server.user_id);
        let project = snap.projects.get(&server.tenant_id);
        let flavor = snap.flavors.get(&server.flavor.id);

        let image_backed = match &server.image {
            nova::Image::StringRep(x) => x != "",
            nova::Image::ObjectRep { id } => id != "",
        };
        let volume_backed = !image_backed && !server.attached_volumes.is_empty();

        // debug!(
        //     "user: {:?}, project: {:?}, flavour: {:?}",
        //     user, project, flavor
        // );
        // debug!("{:?}", server);

        if let (Some(user), Some(project), Some(flavor)) = (user, project, flavor) {
            let cost = region_costs
                .get(&flavor.name)
                .cloned()
                .unwrap_or(0u32.into());

            let billing_category = BillingCategory::from_status(server.status.as_ref());

            ppi.categorized_server_costs_by_project
                .entry(billing_category)
                .or_default()
                .entry(server.tenant_id.clone())
                .or_default()
                .push((cost, server));

            if volume_backed {
                used_os_volume_discount.insert(server.attached_volumes[0].id.clone(), flavor.disk);
            }

            let create_time = Utc::now();

            {
                let allocated_disk = flavor.disk * 1024u64.pow(3);
                let allocated_cpu: Decimal = flavor.vcpus.into();
                let allocated_memory = flavor.ram;

                use records::v1::{CloudComputeRecord, CloudRecordCommon};

                let cr = CloudComputeRecord {
                    common: CloudRecordCommon {
                        create_time: create_time,
                        site: cfg.site.clone(),
                        project,
                        user,
                        instance_id: server.id.clone(),
                        start_time,
                        end_time,
                        duration,
                        region: cfg.region.clone(),
                        resource: cfg.resource.clone(),
                        zone: server.zone.clone(),
                        cost,
                        allocated_disk,
                    },
                    flavour: flavor.name.clone(),
                    allocated_cpu,
                    allocated_memory,
                    used_cpu: None,
                    used_memory: None,
                    used_network_up: None,
                    used_network_down: None,
                    iops: None,
                };
                v1_compute_records.push(cr);
            }
        }
    }

    info!("Processing volumes");
    for volume in &snap.volumes {
        let gig_rate = region_costs
            .get("storage.block")
            .cloned()
            .unwrap_or(0u32.into());
        let discount = used_os_volume_discount.get(&volume.id).unwrap_or(&0);
        let actual_gigs = volume.size;
        let discount_gigs = volume.size.saturating_sub(*discount);
        let cost = Decimal::from(discount_gigs) * gig_rate;
        ppi.volume_costs_by_project
            .entry(volume.tenant_id.clone())
            .or_default()
            .push((cost, volume));

        let user = snap.users.get(&volume.user_id);
        let project = snap.projects.get(&volume.tenant_id);

        let create_time = Utc::now();
        let allocated_disk = actual_gigs * 1024u64.pow(3);

        if let (Some(user), Some(project)) = (user, project) {
            use records::v1::{CloudRecordCommon, CloudStorageRecord};
            let sr = CloudStorageRecord {
                common: CloudRecordCommon {
                    create_time: create_time,
                    site: cfg.site.clone(),
                    project,
                    user,
                    instance_id: volume.id.clone(),
                    start_time,
                    end_time,
                    duration,
                    region: cfg.region.clone(),
                    resource: cfg.resource.clone(),
                    zone: volume.availability_zone.clone(),
                    cost,
                    allocated_disk,
                },
                file_count: 0,
                storage_type: "Block".to_owned(),
            };
            v1_storage_records.push(sr);
        }
    }

    info!("Processing images");
    for image in &snap.images {
        let gig_rate = region_costs
            .get("storage.block")
            .cloned()
            .unwrap_or(0u32.into());
        if let (Some(bytes), Some(owner)) = (image.size, &image.owner) {
            let cost = Decimal::from(bytes) / Decimal::from(1024u64.pow(3)) * gig_rate;
            ppi.image_costs_by_project
                .entry(owner.clone())
                .or_default()
                .push((cost, image));

            let user = snap
                .users
                .get(&image.user_id.as_ref().unwrap_or(&DEFAULT_USER.to_owned()));
            let project = snap.projects.get(
                &image
                    .owner_id
                    .as_ref()
                    .or(image.owner.as_ref())
                    .unwrap_or(&DEFAULT_PROJECT.to_owned()),
            );

            let create_time = Utc::now();
            let allocated_disk = bytes;

            if let (Some(user), Some(project)) = (user, project) {
                use records::v1::{CloudRecordCommon, CloudStorageRecord};
                let sr = CloudStorageRecord {
                    common: CloudRecordCommon {
                        create_time: create_time,
                        site: cfg.site.clone(),
                        project,
                        user,
                        instance_id: image.id.clone(),
                        start_time,
                        end_time,
                        duration,
                        region: cfg.region.clone(),
                        resource: cfg.resource.clone(),
                        zone: DEFAULT_ZONE.to_owned(),
                        cost,
                        allocated_disk,
                    },
                    file_count: 0,
                    storage_type: "Block".to_owned(),
                };
                v1_storage_records.push(sr);
            }
        }
    }

    info!("Processing object buckets");
    for (_, (cost, stat, gigs)) in &object_bucket_costs {
        if let Some(project) = snap.projects.get(&stat.owner) {
            let create_time = Utc::now();
            let gb_to_b: Decimal = 1024u64.pow(3).into();
            let bytes = gigs * gb_to_b;

            use records::v1::{CloudRecordCommon, CloudStorageRecord};
            let sr = CloudStorageRecord {
                common: CloudRecordCommon {
                    create_time: create_time,
                    site: cfg.site.clone(),
                    project,
                    user: DEFAULT_USER.to_owned(),
                    instance_id: stat.id.clone(),
                    start_time,
                    end_time,
                    duration,
                    region: cfg.region.clone(),
                    resource: cfg.resource.clone(),
                    zone: DEFAULT_ZONE.to_owned(),
                    cost: *cost,
                    allocated_disk: bytes.to_u64().unwrap(),
                },
                file_count: 0,
                storage_type: "Block".to_owned(),
            };
            v1_storage_records.push(sr);
        }
    }

    debug!("total images: {}", snap.images.len());
    debug!("total volumes: {}", snap.volumes.len());
    debug!("used OS volumes: {}", used_os_volume_discount.len());

    collate_breakdowns(&ppi);

    if !opt.dry_run {
        let xml_dir = PathBuf::from(cfg.datadir).join("records");
        info!("Writing records to {:?}", &xml_dir);
        std::fs::create_dir_all(&xml_dir)?;
        let xml_leaf_name = format!("{}.xml", this_run_datetime.format("%FT%TZ"));
        let xml_filename = xml_dir.join(xml_leaf_name);
        let fh = std::fs::File::create(xml_filename)?;
        records::v1::write_xml_to(fh, v1_compute_records.iter(), v1_storage_records.iter())?;

        info!("Persisting state");
        persistent_state.state.last_timepoint = Some(this_run_datetime);
        persistent_state.write()?;
    }

    info!("All done!");
    Ok(())
}
