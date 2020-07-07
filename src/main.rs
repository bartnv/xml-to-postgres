extern crate quick_xml;
extern crate yaml_rust;

use std::io::Read;
use std::fs::File;
use std::env;
use quick_xml::Reader;
use quick_xml::events::Event;
use yaml_rust::YamlLoader;

#[derive(Debug)]
struct Column<'a> {
  name: String,
  path: String,
  value: String,
  convert: Option<&'a str>,
  search: Option<&'a str>,
  replace: Option<&'a str>,
}

struct Geometry {
  gtype: u8,
  srid: u32,
  num: u32,
  pos: Vec<f64>
}

fn geom_to_ewkb(geom: Geometry) -> Vec<u8> {
  let mut ewkb = Vec::new();
  ewkb.push(1);
  ewkb.push(geom.gtype);
  ewkb.push(0);
  ewkb.push(0);
  ewkb.push(32);
  ewkb.extend_from_slice(&geom.srid.to_le_bytes());
  for pos in geom.pos.iter() {
    ewkb.extend_from_slice(&pos.to_le_bytes());
  }
  return ewkb;
}

fn main() {
  let args: Vec<_> = env::args().collect();
  if args.len() != 3 {
    eprintln!("usage: {} <configfile> <xmlfile>", args[0]);
    return;
  }

  let mut config_str = String::new();
  File::open(&args[1]).unwrap().read_to_string(&mut config_str).unwrap();
  let config = &YamlLoader::load_from_str(&config_str).unwrap()[0];

  let mut reader;
  reader = Reader::from_file(&args[2]).unwrap();
  reader.trim_text(true);

  let mut path = String::new();
  let mut buf = Vec::new();

  let mut count = 0;

  let rowpath = config["rowpath"].as_str().expect("No valid 'rowpath' entry in configuration file");
  let colspec = config["columns"].as_vec().expect("No valid 'columns' array in configuration file");
  let mut columns = Vec::new();

  for col in colspec {
    let name = col["name"].as_str().expect("Column has no 'name' entry in configuration file");
    let colpath = col["path"].as_str().expect("Column has no 'path' entry in configuration file");
    let convert = col["convert"].as_str();
    let search = col["search"].as_str();
    let replace = col["replace"].as_str();
    let mut path = String::from(rowpath);
    path.push_str(colpath);
    columns.push(Column { name: name.to_string(), path, value: String::new(), convert, search, replace });
  }

  let mut xmltotext = false;
  let mut text = String::new();
  let mut gmltoewkb = false;
  let mut gmlsrid: u32 = 4326;
  let mut gmlrings: u32 = 0;
  let mut gmldims: u8 = 2;
  let mut gmlpos = false;
  let mut gmlposlist: Vec<f64> = Vec::new();
  let mut ewkb: Vec<u8> = vec![1];
  loop {
    match reader.read_event(&mut buf) {
      Ok(Event::Start(ref e)) => {
        path.push('/');
        path.push_str(reader.decode(e.name()).unwrap());
        if xmltotext {
          text.push_str(&format!("<{}>", &e.unescape_and_decode(&reader).unwrap()));
          continue;
        }
        else if gmltoewkb {
          for res in e.attributes() {
            match res {
              Err(_) => (),
              Ok(attr) => {
                let key = reader.decode(attr.key);
                match key {
                  Ok("srsName") => {
                    let mut value = String::from(reader.decode(&attr.value).unwrap());
                    if let Some(i) = value.rfind("::") {
                      value = value.split_off(i+2);
                    }
                    match value.parse::<u32>() {
                      Ok(int) => gmlsrid = int,
                      Err(_) => eprintln!("Invalid srsName {} in GML", value)
                    }
                  },
                  Ok("srsDimension") => {
                    let value = reader.decode(&attr.value).unwrap();
                    match value.parse::<u8>() {
                      Ok(int) => gmldims = int,
                      Err(_) => eprintln!("Invalid srsDimension {} in GML", value)
                    }
                  }
                  _ => ()
                }
              }
            }
          }
          match reader.decode(e.name()) {
            Err(_) => (),
            Ok(tag) => match tag {
              "gml:Point" => ewkb.push(1),
              "gml:LineString" => ewkb.push(2),
              "gml:Polygon" => ewkb.push(3),
              "gml:exterior" => (),
              "gml:LinearRing" => gmlrings += 1,
              "gml:posList" => {
                gmlpos = true;
                ewkb.push(0);
                ewkb.push(0);
                let code = match gmldims {
                  2 => 32,
                  3 => 32 | 128,
                  _ => {
                    eprintln!("GML number of dimensions {} not supported", gmldims);
                    32
                  }
                };
                ewkb.push(code);
                ewkb.extend_from_slice(&gmlsrid.to_le_bytes());
              }
              _ => eprintln!("GML type {} not supported", tag)
            }
          }
          continue;
        }
        else if path == rowpath {
          count += 1;
        }
        else if path.len() > rowpath.len() {
          for i in 0..columns.len() {
            if path == columns[i].path {
              match columns[i].convert {
                None => (),
                Some("xml-to-text") => xmltotext = true,
                Some("gml-to-ewkb") => gmltoewkb = true,
                Some(_) => (),
              }
              break;
            }
          }
        }
      },
      Ok(Event::Text(ref e)) => {
        if xmltotext {
          text.push_str(&e.unescape_and_decode(&reader).unwrap());
          continue;
        }
        else if gmltoewkb {
          if gmlpos {
            let value = String::from(&e.unescape_and_decode(&reader).unwrap());
            for pos in value.split(' ') {
              gmlposlist.push(pos.parse().unwrap());
            }
          }
          continue;
        }
        for i in 0..columns.len() {
          if path == columns[i].path {
            columns[i].value.push_str(&e.unescape_and_decode(&reader).unwrap().replace("\\", "\\\\"));
          }
        }
      },
      Ok(Event::End(_)) => {
        if path == rowpath {
          for i in 0..columns.len() {
            if i > 0 { print!("\t"); }
            if columns[i].value.is_empty() { print!("\\N"); }
            else {
              if let (Some(s), Some(r)) = (columns[i].search, columns[i].replace) {
                columns[i].value = columns[i].value.replace(s, r);
              }
              print!("{}", columns[i].value);
              columns[i].value.clear();
            }
          }
          println!("");
        }
        let i = path.rfind('/').unwrap();
        let tag = path.split_off(i);
        if xmltotext {
          text.push_str(&format!("<{}>", tag));
          for i in 0..columns.len() {
            if path == columns[i].path {
              xmltotext = false;
              if let (Some(s), Some(r)) = (columns[i].search, columns[i].replace) {
                text = text.replace(s, r);
              }
              columns[i].value.push_str(&text);
              text.clear();
              break;
            }
          }
        }
        else if gmltoewkb {
          for i in 0..columns.len() {
            if path == columns[i].path {
              gmltoewkb = false;
              ewkb.extend_from_slice(&gmlrings.to_le_bytes());
              ewkb.extend_from_slice(&((gmlposlist.len() as u32)/gmldims as u32).to_le_bytes());
              for pos in gmlposlist.iter() {
                ewkb.extend_from_slice(&pos.to_le_bytes());
              }
              for byte in ewkb.iter() {
                columns[i].value.push_str(&format!("{:02X}", byte));
              }
              ewkb.clear();
              gmlposlist.clear();
              break;
            }
          }
        }
      },
      Ok(Event::Eof) => break,
      Err(e) => panic!("Error at position {}: {:?}", reader.buffer_position(), e),
      _ => ()
    }
    buf.clear();
  }
  eprintln!("{} rows processed", count);
}
