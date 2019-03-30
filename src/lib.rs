#[macro_use]
extern crate failure;

#[macro_use]
extern crate serde_json;

pub mod openstack;
pub mod radosgw;
pub mod records;

// use xml::writer::{EmitterConfig, XmlEvent};
// use records::WriteToXML;

// fn main() -> Result<(), failure::Error> {
//     let mut computes = Vec::new();
//     computes.push(records::CloudComputeRecord::example());

//     let mut storages = Vec::new();
//     storages.push(records::CloudStorageRecord::example());

//     let instance_id_rx = openstack::instances();
//     while let Ok(id) = instance_id_rx.recv() {
//         eprintln!("{:?}", id);
//     }

//     let mut w = EmitterConfig::new()
//         .perform_indent(true)
//         .create_writer(std::io::stdout());

//     w.write(
//         XmlEvent::start_element("cr:CloudRecords")
//             .ns("cr", "http://sams.snic.se/namespaces/2016/04/cloudrecords"),
//     )?;
//     for cr in computes {
//         cr.write_to(&mut w)?;
//     }
//     for sr in storages {
//         sr.write_to(&mut w)?;
//     }
//     w.write(XmlEvent::end_element())?;

//     Ok(())
// }
