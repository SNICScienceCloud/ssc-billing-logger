extern crate failure;
extern crate serde_json;

use reqwest::header::CONTENT_TYPE;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use url::Url;

#[derive(Debug)]
pub struct Session {
    auth_token: String,
    keystone_url: Url,
    nova_url: Url,
    cinder_url: Url,
    glance_url: Url,
    swift_url: Option<Url>,
}

pub mod keystone {
    use serde::{Deserialize, Serialize};
    #[derive(Debug, Deserialize, Serialize, Clone)]
    pub struct TokenInfo {
        pub token: Token,
    }

    #[derive(Debug, Deserialize, Serialize, Clone)]
    pub struct Token {
        pub catalog: Vec<Service>,
    }

    #[derive(Debug, Deserialize, Serialize, Clone)]
    pub struct Service {
        pub name: String,

        #[serde(rename = "type")]
        pub typ: String,
        pub endpoints: Vec<Endpoint>,
    }

    #[derive(Debug, Deserialize, Serialize, Clone)]
    pub struct Endpoint {
        pub region: String,
        pub interface: String,
        pub url: String,
    }

    #[derive(Debug, Deserialize, Serialize, Clone)]
    pub struct Users {
        pub users: Vec<User>,
    }

    #[derive(Debug, Deserialize, Serialize, Clone)]
    pub struct User {
        pub id: String,
        pub name: String,
    }

    #[derive(Debug, Deserialize, Serialize, Clone)]
    pub struct Projects {
        pub projects: Vec<Project>,
    }

    #[derive(Debug, Deserialize, Serialize, Clone)]
    pub struct Project {
        pub id: String,
        pub name: String,
    }
}

pub struct Credentials {
    pub username: String,
    pub password: String,
    pub domain: String,
    pub project: String,
}

impl Session {
    fn auth_scoped_payload(creds: &Credentials) -> String {
        json!({"auth": {
            "identity": {
                "methods": ["password"],
                "password": {
                    "user": {
                        "name": creds.username,
                        "password": creds.password,
                        "domain": {"id": creds.domain},
                    }
                }
            },
            "scope": {
                "project": {
                    "domain": {"id": creds.domain},
                    "name": creds.project
                }
            }
        }})
        .to_string()
    }

    pub fn new(
        creds: &Credentials,
        keystone_url: &Url,
        region: &str,
        rewrite_host: bool,
    ) -> Result<Session, failure::Error> {
        let keystone_url = {
            let mut url = keystone_url.clone();
            url.path_segments_mut().unwrap().pop_if_empty().push(""); // ensure that the URL ends in a slash
            url
        };
        let client = reqwest::Client::new();
        let mut res = client
            .post(keystone_url.join("auth/tokens/")?.as_str())
            .header(CONTENT_TYPE, "application/json")
            .body(Session::auth_scoped_payload(&creds))
            .send()?;
        trace!("{:?}", res);
        let admin_scoped_token: String = res
            .headers()
            .get("X-Subject-Token")
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();
        let body = res.text()?;
        let token_info: keystone::TokenInfo = serde_json::from_str(&body)?;
        trace!("{:#?}", token_info);
        trace!("Admin scoped token: {}", admin_scoped_token);

        let region_endpoints = token_info
            .token
            .catalog
            .iter()
            .filter_map(|svc| {
                svc.endpoints
                    .iter()
                    .find(|ep| ep.region == region && ep.interface == "admin")
                    .map(|ep| {
                        let mut url = Url::parse(&ep.url).unwrap();
                        url.path_segments_mut().unwrap().pop_if_empty().push("");
                        ((svc.name.as_str(), svc.typ.as_str()), url)
                    })
            })
            .collect::<HashMap<_, _>>();

        let mut nova_url = region_endpoints
            .get(&("nova", "compute"))
            .ok_or(format_err!("Could not find Nova endpoint"))?
            .clone();
        let mut cinder_url = region_endpoints
            .get(&("cinderv3", "volumev3"))
            .ok_or(format_err!("Could not find Cinder endpoint"))?
            .clone();
        let mut glance_url = region_endpoints
            .get(&("glance", "image"))
            .ok_or(format_err!("Could not find Glance endpoint"))?
            .clone();
        let mut swift_url = region_endpoints.get(&("swiftv1", "object-store")).cloned();

        if rewrite_host {
            for url in [&mut nova_url, &mut cinder_url, &mut glance_url].iter_mut() {
                url.set_host(Some("localhost"))?;
            }
            swift_url
                .as_mut()
                .map(|url| url.set_host(Some("localhost")));
        }

        Ok(Session {
            auth_token: admin_scoped_token,
            keystone_url: keystone_url,
            nova_url,
            cinder_url,
            glance_url,
            swift_url,
        })
    }
}

pub mod cinder {
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Deserialize)]
    pub struct Volumes {
        pub volumes: Vec<Volume>,

        #[serde(rename = "volumes_links", default)]
        pub links: Vec<Link>,
    }

    #[derive(Debug, Deserialize, Serialize, Clone)]
    pub struct Volume {
        pub id: String,
        pub size: u64,
        pub user_id: String,

        #[serde(rename = "os-vol-tenant-attr:tenant_id")]
        pub tenant_id: String,

        pub availability_zone: String,
    }

    #[derive(Debug, Deserialize, Serialize, Clone)]
    pub struct Link {
        pub rel: String,
        pub href: url::Url,
    }
}

impl Session {
    fn fetch_volume_set(
        &self,
        client: &reqwest::Client,
        url: &url::Url,
    ) -> Result<cinder::Volumes, failure::Error> {
        let mut res = client
            .get(url.as_str())
            .header("X-Auth-Token", self.auth_token.as_str())
            .send()?;

        if !res.status().is_success() {
            bail!("Could not retrieve volumes from Glance");
        }

        let text = res.text()?;
        // std::fs::write("volumes.json", &text)?;
        let volumes: cinder::Volumes = serde_json::from_str(&text)?;
        Ok(volumes)
    }

    pub fn volumes(&self) -> Result<Vec<cinder::Volume>, failure::Error> {
        let client = reqwest::Client::new();
        let mut url = self.cinder_url.join("volumes/detail?all_tenants=1")?;

        let mut ret = Vec::new();
        loop {
            let mut volumes = self.fetch_volume_set(&client, &url)?;
            ret.append(&mut volumes.volumes);
            trace!("{:#?}", volumes.links);
            if let Some(next) = volumes.links.iter().find(|lnk| lnk.rel == "next") {
                trace!("next: {}", next.href);
                url = next.href.clone();
            } else {
                break;
            }
        }

        Ok(ret)
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct NameMapping {
    id_to_name: HashMap<String, String>,
}

impl NameMapping {
    pub fn get<'a, S: AsRef<str>>(&'a self, id: S) -> Option<String> {
        self.id_to_name.get(id.as_ref()).cloned()
    }
}

pub type Flavors = HashMap<String, nova::Flavor>;

impl Session {
    fn users(&self) -> Result<keystone::Users, failure::Error> {
        let client = reqwest::Client::new();
        let mut res = client
            .get(self.keystone_url.join("users/")?.as_str())
            .header("X-Auth-Token", self.auth_token.as_str())
            .send()?;

        if !res.status().is_success() {
            bail!("Could not retrieve users from Keystone");
        }

        let text = res.text()?;
        // std::fs::write("users.json", &text)?;
        let users: keystone::Users = serde_json::from_str(&text)?;
        Ok(users)
    }

    pub fn user_mappings(&self) -> Result<NameMapping, failure::Error> {
        let users = self.users()?;

        let mut id_to_name = HashMap::new();
        for user in users.users {
            id_to_name.insert(user.id, user.name);
        }

        Ok(NameMapping { id_to_name })
    }

    pub fn project_mappings(&self) -> Result<NameMapping, failure::Error> {
        let client = reqwest::Client::new();
        let mut res = client
            .get(self.keystone_url.join("projects/")?.as_str())
            .header("X-Auth-Token", self.auth_token.as_str())
            .send()?;

        if !res.status().is_success() {
            bail!("Could not retrieve projects from Keystone");
        }

        let text = res.text()?;
        // std::fs::write("projects.json", &text)?;
        let projects: keystone::Projects = serde_json::from_str(&text)?;

        let mut id_to_name = HashMap::new();
        for proj in projects.projects {
            id_to_name.insert(proj.id, proj.name);
        }

        Ok(NameMapping { id_to_name })
    }

    pub fn flavors(&self) -> Result<Flavors, failure::Error> {
        let client = reqwest::Client::new();
        let url = self.nova_url.join("flavors/detail?is_public=None")?;
        trace!("flavor url: {:?}", url);
        let mut res = client
            .get(url.as_str())
            .header("X-Auth-Token", self.auth_token.as_str())
            .send()?;

        if !res.status().is_success() {
            bail!("Could not retrieve flavors from Nova");
        }

        let text = res.text()?;
        // std::fs::write("flavors.json", &text)?;
        let flavors: nova::Flavors = serde_json::from_str(&text)?;

        let mut ret = HashMap::new();
        for flavor in flavors.flavors {
            ret.insert(flavor.id.clone(), flavor);
        }

        Ok(ret)
    }
}

pub mod glance {
    use chrono::{DateTime, Utc};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Deserialize, Serialize, Clone)]
    pub struct Images {
        pub images: Vec<Image>,

        pub next: Option<String>,
    }

    #[derive(Debug, Deserialize, Serialize, Clone)]
    pub struct Image {
        pub container_format: Option<String>,
        pub created_at: DateTime<Utc>,
        pub disk_format: Option<String>,
        pub id: String,
        pub min_disk: Option<u64>,
        pub min_ram: Option<u64>,
        pub name: Option<String>,
        pub os_hash_algo: Option<String>,
        pub os_hash_value: Option<String>,
        pub os_hidden: Option<bool>,
        pub owner: Option<String>,
        pub size: Option<u64>,
        pub status: String,
        pub tags: Vec<String>,
        pub updated_at: Option<DateTime<Utc>>,
        pub virtual_size: Option<u64>,
        pub visibility: String,
        pub direct_url: Option<String>,
        pub locations: Vec<serde_json::Value>,
        pub owner_id: Option<String>,
        pub user_id: Option<String>,
    }
}

impl Session {
    fn fetch_image_set(
        &self,
        client: &reqwest::Client,
        url: &url::Url,
    ) -> Result<glance::Images, failure::Error> {
        let mut res = client
            .get(url.as_str())
            .header("X-Auth-Token", self.auth_token.as_str())
            .send()?;

        if !res.status().is_success() {
            bail!("Could not retrieve images from Glance");
        }

        let text = res.text()?;
        // std::fs::write("images.json", &text)?;
        let images: glance::Images = serde_json::from_str(&text)?;
        Ok(images)
    }

    pub fn images(&self) -> Result<Vec<glance::Image>, failure::Error> {
        let client = reqwest::Client::new();
        let base_url = self.glance_url.join("v2/images")?;
        let mut url = base_url.clone();

        let mut ret = Vec::new();
        loop {
            let mut images = self.fetch_image_set(&client, &url)?;
            ret.append(&mut images.images);
            if let Some(next) = images.next {
                url = base_url.join(&next)?;
            } else {
                break;
            }
        }

        Ok(ret)
    }
}

pub mod nova {
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Deserialize, Serialize, Clone)]
    pub struct Servers {
        pub servers: Vec<Server>,
    }

    #[derive(Debug, Deserialize, Serialize, Clone)]
    pub struct Server {
        pub id: String,
        pub user_id: String,
        pub tenant_id: String,
        pub flavor: ServerFlavor,
        pub image: Image,
        pub status: String,

        #[serde(rename = "OS-EXT-AZ:availability_zone")]
        pub zone: Option<String>,

        #[serde(rename = "os-extended-volumes:volumes_attached")]
        pub attached_volumes: Vec<AttachedVolume>,
    }

    #[derive(Debug, Deserialize, Serialize, Clone)]
    #[serde(untagged)]
    pub enum Image {
        StringRep(String),
        ObjectRep { id: String },
    }

    #[derive(Debug, Deserialize, Serialize, Clone)]
    pub struct AttachedVolume {
        pub id: String,
    }

    #[derive(Debug, Deserialize, Serialize, Clone)]
    pub struct ServerFlavor {
        pub id: String,
    }

    #[derive(Debug, Deserialize, Serialize, Clone)]
    pub struct Flavors {
        pub flavors: Vec<Flavor>,
    }

    #[derive(Debug, Deserialize, Serialize, Clone)]
    pub struct Flavor {
        pub id: String,
        pub name: String,
        pub vcpus: u64,
        pub ram: u64,
        pub disk: u64,
    }
}

impl Session {
    /// Obtain a list of servers from the API.
    pub fn servers(&self) -> Result<Vec<nova::Server>, failure::Error> {
        let client = reqwest::Client::new();
        let mut req_url = self.nova_url.join("servers/detail")?;
        req_url.query_pairs_mut().append_pair("all_tenants", "True");

        let mut res = client
            .get(req_url.as_str())
            .header("X-Auth-Token", self.auth_token.as_str())
            .send()?;

        trace!("{:?}", &res);
        if !res.status().is_success() {
            bail!("Could not retrieve instances from Keystone");
        }

        let text = res.text()?;
        // std::fs::write("servers.json", &text)?;
        let servers: nova::Servers = serde_json::from_str(&text)?;

        Ok(servers.servers)
    }
}

pub mod swift {
    use chrono::{DateTime, Utc};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Deserialize, Serialize, Clone)]
    pub struct Container {
        pub count: u64,
        pub bytes: u64,
        pub name: String,
        pub last_modified: Option<DateTime<Utc>>,
    }
}

impl Session {
    fn fetch_container_set(
        &self,
        client: &reqwest::Client,
        url: &url::Url,
    ) -> Result<Vec<swift::Container>, failure::Error> {
        let mut res = client
            .get(url.as_str())
            .header("X-Auth-Token", self.auth_token.as_str())
            .send()?;

        if !res.status().is_success() {
            bail!("Could not retrieve images from Glance");
        }

        let text = res.text()?;
        // std::fs::write("containers.json", &text)?;
        let containers: Vec<swift::Container> = serde_json::from_str(&text)?;
        Ok(containers)
    }

    #[allow(unreachable_code, unused_variables)]
    pub fn containers(&self, project: &str) -> Result<Vec<swift::Container>, failure::Error> {
        return Ok(vec![]);

        if let Some(swift_url) = self.swift_url {
            let client = reqwest::Client::new();
            let base_url = swift_url.join(project)?;
            let marker: Option<String> = None;

            let mut ret = Vec::new();
            loop {
                let mut url = base_url.clone();
                let qp = url.query_pairs_mut().append_pair("limit", "10");
                if let Some(marker) = marker {
                    qp.append_pair("marker", &marker);
                }
                drop(qp);
                let mut containers = self.fetch_container_set(&client, &url)?;
                let done = containers.len() == 0;
                ret.append(&mut containers);
                if done {
                    break;
                }
                marker = Some(containers.last().unwrap().name.clone());
            }

            Ok(ret)
        } else {
            Ok(vec![])
        }
    }
}
