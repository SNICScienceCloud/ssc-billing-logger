use ::ssc_billing_logger::openstack;
use ::ssc_billing_logger::radosgw;
use ::ssc_billing_logger::records;

#[macro_use]
extern crate failure;
#[macro_use]
extern crate log;

use chrono::{DateTime, Timelike, Utc};
use num::ToPrimitive;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::File;
use std::path::PathBuf;
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
    resources: BTreeMap<String, String>,
    region: String,
    datadir: String,
}

type ResourceCosts = BTreeMap<String, Decimal>;

#[derive(Debug, Deserialize)]
pub struct RegionCosts {
    #[serde(flatten)]
    resources: BTreeMap<String, ResourceCosts>,
}

#[derive(Debug, Deserialize)]
pub struct CostsFile {
    regions: BTreeMap<String, RegionCosts>,
}

#[derive(Debug, Default)]
pub struct ProjectBreakdown<'a> {
    active: Vec<(Option<Decimal>, &'a openstack::nova::Server)>,
    inert: Vec<(Option<Decimal>, &'a openstack::nova::Server)>,
    volumes: Vec<(Option<Decimal>, &'a openstack::cinder::Volume)>,
    images: Vec<(Option<Decimal>, &'a openstack::glance::Image)>,
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
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

struct CostLookup<'a> {
    config: &'a Config,
    domains: BTreeMap<String, String>,
    region_costs: &'a RegionCosts,
    projects: &'a openstack::NameMapping,
}

impl<'a> CostLookup<'a> {
    fn new(
        config: &'a Config,
        costs: &'a CostsFile,
        domains: &'a openstack::keystone::Domains,
        projects: &'a openstack::NameMapping,
    ) -> Option<Self> {
        let region_costs = costs.regions.get(&config.region)?;
        let domains = domains
            .domains
            .iter()
            .map(|d| (d.id.clone(), d.name.clone()))
            .collect();
        Some(Self {
            config,
            domains,
            projects,
            region_costs,
        })
    }

    fn project_costs_by_id(&'a self, proj_id: &str) -> Option<ProjectCost> {
        let proj = self.projects.get(proj_id)?;
        let domain_name = self.domains.get(&proj.domain_id)?;
        let resource = self.config.resources.get(domain_name)?;
        let costs = self.region_costs.resources.get(resource)?;
        Some(ProjectCost { resource, costs })
    }
}

struct ProjectCost<'a> {
    pub resource: &'a String,
    pub costs: &'a ResourceCosts,
}

impl<'a> ProjectCost<'a> {
    fn get(&self, kind: &str) -> Option<Decimal> {
        self.costs.get(kind).cloned()
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
    domains: openstack::keystone::Domains,
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
    let costs: CostsFile = serde_json::from_reader(File::open(&costs_path)?)?;

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
        if snap.version < 3 {
            bail!("Snapshot version predates domains, exiting.");
        }
        snap
    } else {
        let credentials = openstack::Credentials {
            username: cfg.username.clone(),
            password: cfg.password.clone(),
            domain: cfg.domain.clone(),
            project: cfg.project.clone(),
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
        let domains = session.domains()?;

        let snap = Snapshot {
            version: 3,
            datetime: this_run_datetime,
            servers,
            flavors,
            images,
            volumes,
            object_bucket_stats: object_bucket_stats.ok(),
            users,
            projects,
            domains,
        };

        if let Some(snap_path) = opt.save_snapshot {
            std::fs::write(snap_path, &serde_json::to_string_pretty(&snap).unwrap()).unwrap();
        }

        snap
    };
    let this_run_datetime = snap.datetime;

    let cost_lookup = CostLookup::new(&cfg, &costs, &snap.domains, &snap.projects)
        .ok_or(format_err!("Could not construct costs lookup."))?;

    let mut object_bucket_sizes = BTreeMap::new();
    if let Some(stats) = &snap.object_bucket_stats {
        let kb_to_gb = Decimal::from(1u32) / Decimal::from(1024u32.pow(2));
        for s in stats {
            if !s.usage.is_empty() {
                let gb_sum = s.usage.iter().fold(Decimal::from(0u32), |sum, u| {
                    sum + Decimal::from(u.1.size_kb) * kb_to_gb
                });
                object_bucket_sizes.insert(s.id.clone(), (s, gb_sum));
            }
        }
    }
    debug!("{:?}", object_bucket_sizes);

    let start_time = this_run_datetime
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

    let mut used_os_volume_discount: BTreeMap<String, u64> = BTreeMap::new();

    let mut v1_compute_records: Vec<records::v1::CloudComputeRecord> = Vec::new();
    let mut v1_storage_records: Vec<records::v1::CloudStorageRecord> = Vec::new();

    info!("Processing servers");
    'server_loop: for server in &snap.servers {
        use openstack::nova;

        if server.zone.is_none() {
            warn!("Skipping server instance {} due to no zone", server.id);
            continue 'server_loop;
        }

        if server.zone.as_ref().unwrap().is_empty() {
            warn!("Skipping server instance {} due to empty zone", server.id);
            continue 'server_loop;
        }

        let user = snap.users.get(&server.user_id);
        let project = snap.projects.get(&server.tenant_id);
        let flavor = snap.flavors.get(&server.flavor.id);
        let proj_costs = cost_lookup.project_costs_by_id(&server.tenant_id);

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

        if let (Some(user), Some(project), Some(flavor), Some(proj_costs)) =
            (user, project, flavor, proj_costs)
        {
            let cost = proj_costs.get(&flavor.name);

            let _billing_category = BillingCategory::from_status(server.status.as_ref());

            if volume_backed {
                used_os_volume_discount.insert(server.attached_volumes[0].id.clone(), flavor.disk);
            }

            let create_time = Utc::now();

            if let Some(cost) = cost {
                if !cost.is_zero() {
                    let allocated_disk = flavor.disk * 1024u64.pow(3);
                    let allocated_cpu: Decimal = flavor.vcpus.into();
                    let allocated_memory = flavor.ram;

                    use records::v1::{CloudComputeRecord, CloudRecordCommon};

                    let cr = CloudComputeRecord {
                        common: CloudRecordCommon {
                            create_time: create_time,
                            site: cfg.site.clone(),
                            project: project.name,
                            user: user.name,
                            instance_id: server.id.clone(),
                            start_time,
                            end_time,
                            duration,
                            region: cfg.region.clone(),
                            resource: proj_costs.resource.clone(),
                            zone: server.zone.clone().unwrap(),
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
    }

    info!("Processing volumes");
    for volume in &snap.volumes {
        use records::v1::{CloudRecordCommon, CloudStorageRecord};
        let mut process_volume = || -> Option<CloudStorageRecord> {
            let proj_costs = cost_lookup.project_costs_by_id(&volume.tenant_id)?;
            let gig_rate = proj_costs.get("storage.block");
            let discount = *used_os_volume_discount.get(&volume.id).unwrap_or(&0);
            let actual_gigs = volume.size;
            let discount_gigs = volume.size.saturating_sub(discount);
            {
                let dv = used_os_volume_discount.get_mut(&volume.id)?;
                *dv = dv.saturating_sub(actual_gigs);
            }
            let cost = gig_rate.map(|r| Decimal::from(discount_gigs) * r);
            let user = snap.users.get(&volume.user_id)?;
            let project = snap.projects.get(&volume.tenant_id)?;

            let create_time = Utc::now();
            let allocated_disk = actual_gigs * 1024u64.pow(3);

            let cost = cost?;
            if !cost.is_zero() {
                let sr = CloudStorageRecord {
                    common: CloudRecordCommon {
                        create_time: create_time,
                        site: cfg.site.clone(),
                        project: project.name,
                        user: user.name,
                        instance_id: volume.id.clone(),
                        start_time,
                        end_time,
                        duration,
                        region: cfg.region.clone(),
                        resource: proj_costs.resource.clone(),
                        zone: volume.availability_zone.clone(),
                        cost,
                        allocated_disk,
                    },
                    file_count: 0,
                    storage_type: "Block".to_owned(),
                };
                Some(sr)
            } else {
                None
            }
        };
        process_volume().map(|sr| v1_storage_records.push(sr));
    }

    info!("Processing images");
    for image in &snap.images {
        use records::v1::{CloudRecordCommon, CloudStorageRecord};
        let process_image = || -> Option<CloudStorageRecord> {
            let bytes = image.size?;
            let owner = image.owner.as_ref()?;
            let proj_costs = cost_lookup.project_costs_by_id(owner)?;
            let gig_rate = proj_costs.get("storage.block");
            let cost = gig_rate.map(|r| Decimal::from(bytes) / Decimal::from(1024u64.pow(3)) * r);
            let project = snap.projects.get(owner)?;

            // Not all images have an user name associated with them, only an owning project.
            let user_name: &str = image
                .owner_user_name
                .as_ref()
                .and_then(|user_name| {
                    if snap.users.has_name_in_domain(user_name, &project.domain_id) {
                        Some(user_name.as_ref())
                    } else {
                        None
                    }
                })
                .unwrap_or(DEFAULT_USER);

            let create_time = Utc::now();
            let allocated_disk = bytes;

            if let Some(cost) = cost {
                if !cost.is_zero() {
                    let sr = CloudStorageRecord {
                        common: CloudRecordCommon {
                            create_time: create_time,
                            site: cfg.site.clone(),
                            project: project.name,
                            user: user_name.to_owned(),
                            instance_id: image.id.clone(),
                            start_time,
                            end_time,
                            duration,
                            region: cfg.region.clone(),
                            resource: proj_costs.resource.clone(),
                            zone: DEFAULT_ZONE.to_owned(),
                            cost,
                            allocated_disk,
                        },
                        file_count: 0,
                        storage_type: "Block".to_owned(),
                    };
                    return Some(sr);
                }
            }
            None
        };
        process_image().map(|sr| v1_storage_records.push(sr));
    }

    info!("Processing object buckets");
    for (_, (stat, gigs)) in &object_bucket_sizes {
        use records::v1::{CloudRecordCommon, CloudStorageRecord};
        let process_object_bucket = || -> Option<CloudStorageRecord> {
            let project = snap.projects.get(&stat.owner)?;
            let proj_costs = cost_lookup.project_costs_by_id(&stat.owner)?;
            let gig_rate = proj_costs.get("storage.object")?;
            let cost = gig_rate * gigs;
            if cost.is_zero() {
                return None;
            }
            let create_time = Utc::now();
            let gb_to_b: Decimal = 1024u64.pow(3).into();
            let bytes = gigs * gb_to_b;

            let sr = CloudStorageRecord {
                common: CloudRecordCommon {
                    create_time: create_time,
                    site: cfg.site.clone(),
                    project: project.name,
                    user: DEFAULT_USER.to_owned(),
                    instance_id: stat.id.clone(),
                    start_time,
                    end_time,
                    duration,
                    region: cfg.region.clone(),
                    resource: proj_costs.resource.clone(),
                    zone: DEFAULT_ZONE.to_owned(),
                    cost,
                    allocated_disk: bytes.to_u64().unwrap(),
                },
                file_count: 0,
                storage_type: "Block".to_owned(),
            };
            Some(sr)
        };
        process_object_bucket().map(|sr| v1_storage_records.push(sr));
    }

    debug!("total images: {}", snap.images.len());
    debug!("total volumes: {}", snap.volumes.len());
    debug!("used OS volumes: {}", used_os_volume_discount.len());

    if !opt.dry_run {
        let xml_dir = PathBuf::from(cfg.datadir).join("records");
        info!("Writing records to {:?}", &xml_dir);
        std::fs::create_dir_all(&xml_dir)?;
        let xml_leaf_name = format!("{}.xml", this_run_datetime.format("%Y%m%dT%H%MZ"));
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
