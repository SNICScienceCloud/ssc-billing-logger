use ::ssc_billing_logger::openstack;
use ::ssc_billing_logger::records;
use ::ssc_billing_logger::radosgw;
use chrono::Timelike;

#[macro_use]
extern crate failure;

use chrono::Utc;
use num::Zero;
use rust_decimal::Decimal;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
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
}

#[derive(Debug, Deserialize)]
pub struct Config {
    username: String,
    password: String,
    domain: String,
    project: String,

    #[serde(with = "url_serde")]
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
            "PAUSED" | "SUSPENDED" | "SOFT_SUSPENDED" | "SOFT_DELETED" | "SHUTOFF" => BillingCategory::Inactive,
            "DELETED" | "SHELVED" | "SHELVED_OFFLOADED" => BillingCategory::Unbilled,
            _ => BillingCategory::Active,
        }
    }
}

fn main() -> Result<(), failure::Error> {
    let opt = Opt::from_args();
    let cfg: Config = serde_json::from_reader(File::open(&opt.config)?)?;
    let datadir: PathBuf = cfg.datadir.into();
    let costs: Costs =
        serde_json::from_reader(File::open(&datadir.join("logger-state/costs.json"))?)?;

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
    eprintln!("Session: {:#?}", session);
    // eprintln!("Identity URL: {:?}", session.keystone_url);
    // eprintln!("Compute URL: {:?}", session.nova_url);
    // eprintln!("Image URL: {:?}", session.glance_url);
    // eprintln!("Volume URL: {:?}", session.cinder_url);

    let servers = session.servers()?;
    let flavors = session.flavors()?;
    let images = session.images()?;
    let volumes = session.volumes()?;
    // TODO: Implement object storage stuff
    // let object_buckets = session.buckets()?;

    let users = session.user_mappings()?;
    let projects = session.project_mappings()?;

    eprintln!("{:?}", servers.len());
    eprintln!("{:#?}", &servers[0]);

    let start_time = Utc::now()
        .with_minute(0)
        .unwrap()
        .with_second(0)
        .unwrap()
        .with_nanosecond(0)
        .unwrap();
    let duration = chrono::Duration::hours(1);
    let end_time = start_time + duration;

    let region_costs = costs
        .regions
        .get(&cfg.region)
        .ok_or(format_err!("Region {} not found in costs.json", cfg.region))?;

    // Operator test project - "SNIC 2018/10-1"
    let op_servers = servers
        .iter()
        .filter(|srv| srv.tenant_id == "7d4b838241d9486e972bf1b371cc8718");

    let mut used_os_volume_discount: HashMap<String, u64> = HashMap::new();

    let mut categorized_server_costs_by_project: HashMap<BillingCategory, HashMap<String, Vec<(Decimal, &openstack::nova::Server)>>> = HashMap::new();

    for server in &servers {
        use openstack::nova;

        let user = users.get(&server.user_id);
        let project = projects.get(&server.tenant_id);
        let flavor = flavors.get(&server.flavor.id);

        let image_backed = match &server.image {
            nova::Image::StringRep(x) => x != "",
            nova::Image::ObjectRep { id } => id != "",
        };
        let volume_backed = !image_backed && !server.attached_volumes.is_empty();

        // eprintln!(
        //     "user: {:?}, project: {:?}, flavour: {:?}",
        //     user, project, flavor
        // );
        // eprintln!("{:?}", server);

        if let (Some(user), Some(project), Some(flavor)) = (user, project, flavor) {
            let cost = region_costs
                .get(&flavor.name)
                .cloned()
                .unwrap_or(0u32.into());

            let billing_category = BillingCategory::from_status(server.status.as_ref());

            categorized_server_costs_by_project.entry(billing_category).or_default()
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

                use records::v1::{CloudComputeRecord, CloudRecordCommon, CloudStorageRecord};

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
                // let cr = records::CloudComputeRecord::example();
                // eprintln!("cr: {:?}", cr);
            }
        }
    }

    let mut volume_costs_by_project: HashMap<String, Vec<(Decimal, &openstack::cinder::Volume)>> =
        HashMap::new();

    for volume in &volumes {
        let gig_rate = region_costs
            .get("storage.block")
            .cloned()
            .unwrap_or(0u32.into());
        let discount = used_os_volume_discount.get(&volume.id).unwrap_or(&0);
        let gigs = volume.size.saturating_sub(*discount);
        let cost = Decimal::from(gigs) * gig_rate;
        volume_costs_by_project
            .entry(volume.tenant_id.clone())
            .or_default()
            .push((cost, volume));
    }

    let mut image_costs_by_project: HashMap<String, Vec<(Decimal, &openstack::glance::Image)>> =
        HashMap::new();

    for image in &images {
        let gig_rate = region_costs
            .get("storage.block")
            .cloned()
            .unwrap_or(0u32.into());
        if let (Some(bytes), Some(owner)) = (image.size, &image.owner) {
            let cost = Decimal::from(bytes) / Decimal::from(1024u64.pow(3)) * gig_rate;
            image_costs_by_project
                .entry(owner.clone())
                .or_default()
                .push((cost, image));
        }
    }

    eprintln!("total images: {}", images.len());
    eprintln!("total volumes: {}", volumes.len());
    eprintln!("used OS volumes: {}", used_os_volume_discount.len());

    // Group by instance status

    let mut project_breakdowns: HashMap<String, ProjectBreakdown> = HashMap::new();

    if let Some(category) = categorized_server_costs_by_project.get(&BillingCategory::Active) {
        for (proj, server_costs) in category.iter() {
            let costs = server_costs.iter().map(|c| c.0).collect::<Vec<Decimal>>();
            let total_cost = costs.iter().fold(Decimal::zero(), |sum, cost| sum + cost);
            eprintln!("proj: {}; active server costs: {:.5} = sum({:.5?})", proj, total_cost, costs);
            project_breakdowns
                .entry(proj.clone())
                .or_default()
                .active
                .extend(server_costs.iter());
        }
    }

    if let Some(category) = categorized_server_costs_by_project.get(&BillingCategory::Inactive) {
        for (proj, server_costs) in category.iter() {
            let costs = server_costs.iter().map(|c| c.0).collect::<Vec<Decimal>>();
            let total_cost = costs.iter().fold(Decimal::zero(), |sum, cost| sum + cost);
            eprintln!("proj: {}; inactive server costs: {:.5} = sum({:.5?})", proj, total_cost, costs);
            project_breakdowns
                .entry(proj.clone())
                .or_default()
                .inert
                .extend(server_costs.iter());
        }
    }

    if let Some(category) = categorized_server_costs_by_project.get(&BillingCategory::Unbilled) {
        for (proj, server_costs) in category.iter() {
            let costs = server_costs.iter().map(|c| c.0).collect::<Vec<Decimal>>();
            let total_cost = costs.iter().fold(Decimal::zero(), |sum, cost| sum + cost);
            eprintln!("proj: {}; unbilled server costs: {:.5} = sum({:.5?})", proj, total_cost, costs);
            project_breakdowns
                .entry(proj.clone())
                .or_default()
                .inert
                .extend(server_costs.iter());
        }
    }

    for (proj, volume_costs) in volume_costs_by_project.iter() {
        let costs = volume_costs.iter().map(|c| c.0).collect::<Vec<Decimal>>();
        let total_cost = costs.iter().fold(Decimal::zero(), |sum, cost| sum + cost);
        eprintln!("proj: {}; volume costs: {:.5} = sum({:.5?})", proj, total_cost, costs);
        project_breakdowns
            .entry(proj.clone())
            .or_default()
            .volumes
            .extend(volume_costs.iter());
    }

    for (proj, image_costs) in image_costs_by_project.iter() {
        let costs = image_costs.iter().map(|c| c.0).collect::<Vec<Decimal>>();
        let total_cost = costs.iter().fold(Decimal::zero(), |sum, cost| sum + cost);
        eprintln!("proj: {}; image costs: {:.5} = sum({:.5?})", proj, total_cost, costs);
        project_breakdowns
            .entry(proj.clone())
            .or_default()
            .images
            .extend(image_costs.iter());
    }

    Ok(())
}
