extern crate quick_xml;
extern crate yaml_rust;

use std::io::{Read, Write, stdout};
use std::fs::File;
use std::path::Path;
use std::env;
use std::time::Instant;
use quick_xml::Reader;
use quick_xml::events::Event;
use yaml_rust::YamlLoader;
use yaml_rust::yaml::Yaml;

struct Table<'a> {
  path: String,
  file: Box<dyn Write>,
  columns: Vec<Column<'a>>
}
impl<'a> Table<'a> {
  fn new<'b>(path: &str, file: Option<&str>) -> Table<'b> {
    Table {
      path: String::from(path),
      file: match file {
        None => Box::new(stdout()),
        Some(ref file) => Box::new(File::create(&Path::new(file)).unwrap())
      },
      columns: Vec::new()
    }
  }
}

struct Column<'a> {
  name: String,
  path: String,
  value: String,
  convert: Option<&'a str>,
  search: Option<&'a str>,
  replace: Option<&'a str>,
  consol: Option<&'a str>
}

struct Geometry {
  gtype: u8,
  dims: u8,
  srid: u32,
  rings: Vec<Vec<f64>>
}
impl Geometry {
  fn new() -> Geometry {
    Geometry { gtype: 0, dims: 2, srid: 4326, rings: Vec::new() }
  }
  fn reset(&mut self) {
    self.gtype = 0;
    self.dims = 2;
    self.srid = 4326;
    self.rings.clear();
  }
}

fn gml_to_ewkb(value: &mut String, geom: &Geometry) {
  let mut ewkb = vec![1, geom.gtype, 0, 0];
  let code = match geom.dims {
    2 => 32,
    3 => 32 | 128,
    _ => {
      eprintln!("GML number of dimensions {} not supported", geom.dims);
      32
    }
  };
  ewkb.push(code);
  ewkb.extend_from_slice(&geom.srid.to_le_bytes());
  ewkb.extend_from_slice(&(geom.rings.len() as u32).to_le_bytes());
  for ring in geom.rings.iter() {
    ewkb.extend_from_slice(&((ring.len() as u32)/geom.dims as u32).to_le_bytes());
    for pos in ring.iter() {
      ewkb.extend_from_slice(&pos.to_le_bytes());
    }
  }
  for byte in ewkb.iter() {
    value.push_str(&format!("{:02X}", byte));
  }
}

fn add_table<'a>(tables: &mut Vec<Table<'a>>, rowpath: &str, outfile: Option<&str>, colspec: &'a Vec<Yaml>) {
  tables.push(Table::new(rowpath, outfile));
  let table = tables.last_mut().unwrap();
  for col in colspec {
    let name = col["name"].as_str().expect("Column has no 'name' entry in configuration file");
    let colpath = col["path"].as_str().expect("Column has no 'path' entry in configuration file");
    let convert = col["convert"].as_str();
    let search = col["search"].as_str();
    let replace = col["replace"].as_str();
    let consol = col["consol"].as_str();
    let mut path = String::from(rowpath);
    path.push_str(colpath);
    table.columns.push(Column { name: name.to_string(), path, value: String::new(), convert, search, replace, consol });
  }
}

fn main() -> std::io::Result<()> {
  let args: Vec<_> = env::args().collect();
  if args.len() != 3 {
    eprintln!("usage: {} <configfile> <xmlfile>", args[0]);
    return Ok(());
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

  let rowpath = config["path"].as_str().expect("No valid 'rowpath' entry in configuration file");
  let colspec = config["cols"].as_vec().expect("No valid 'columns' array in configuration file");
  let outfile = config["file"].as_str();
  let mut tables: Vec<Table> = Vec::new();
  add_table(&mut tables, rowpath, outfile, colspec);
  let table = tables.first_mut().unwrap();

  let mut xmltotext = false;
  let mut text = String::new();
  let mut gmltoewkb = false;
  let mut gmlpos = false;
  let mut gmlgeom = Geometry::new();
  let start = Instant::now();
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
                      Ok(int) => gmlgeom.srid = int,
                      Err(_) => eprintln!("Invalid srsName {} in GML", value)
                    }
                  },
                  Ok("srsDimension") => {
                    let value = reader.decode(&attr.value).unwrap();
                    match value.parse::<u8>() {
                      Ok(int) => gmlgeom.dims = int,
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
              "gml:Point" => gmlgeom.gtype = 1,
              "gml:LineString" => gmlgeom.gtype = 2,
              "gml:Polygon" => gmlgeom.gtype = 3,
              "gml:exterior" => (),
              "gml:interior" => (),
              "gml:LinearRing" => gmlgeom.rings.push(Vec::new()),
              "gml:posList" => gmlpos = true,
              _ => eprintln!("GML type {} not supported", tag)
            }
          }
          continue;
        }
        else if path == table.path {
          count += 1;
        }
        else if path.len() > table.path.len() {
          for i in 0..table.columns.len() {
            if path == table.columns[i].path {
              match table.columns[i].convert {
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
              gmlgeom.rings.last_mut().unwrap().push(pos.parse().unwrap());
            }
          }
          continue;
        }
        for i in 0..table.columns.len() {
          if path == table.columns[i].path {
            match table.columns[i].consol {
              None => {
                if !table.columns[i].value.is_empty() {
                  eprintln!("Column '{}' has multiple occurrences without a consolidation method; using 'first'", table.columns[i].name);
                  break;
                }
              },
              Some("append") => {
                if !table.columns[i].value.is_empty() { table.columns[i].value.push(','); }
              },
              Some(s) => {
                eprintln!("Column '{}' has invalid consolidation method {}", table.columns[i].name, s);
                break;
              }
            }
            table.columns[i].value.push_str(&e.unescape_and_decode(&reader).unwrap().replace("\\", "\\\\"));
            break;
          }
        }
      },
      Ok(Event::End(_)) => {
        if path == table.path {
          for i in 0..table.columns.len() {
            if i > 0 { write!(table.file, "\t")?; }
            if table.columns[i].value.is_empty() { write!(table.file, "\\N")?; }
            else {
              if let (Some(s), Some(r)) = (table.columns[i].search, table.columns[i].replace) {
                table.columns[i].value = table.columns[i].value.replace(s, r);
              }
              write!(table.file, "{}", table.columns[i].value)?;
              table.columns[i].value.clear();
            }
          }
          writeln!(table.file)?;
        }
        let i = path.rfind('/').unwrap();
        let tag = path.split_off(i);
        if xmltotext {
          text.push_str(&format!("<{}>", tag));
          for i in 0..table.columns.len() {
            if path == table.columns[i].path {
              xmltotext = false;
              if let (Some(s), Some(r)) = (table.columns[i].search, table.columns[i].replace) {
                text = text.replace(s, r);
              }
              table.columns[i].value.push_str(&text);
              text.clear();
              break;
            }
          }
        }
        else if gmltoewkb {
          for i in 0..table.columns.len() {
            if path == table.columns[i].path {
              gmltoewkb = false;
              gml_to_ewkb(&mut table.columns[i].value, &gmlgeom);
              gmlgeom.reset();
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
  eprintln!("{} rows processed in {} seconds", count, start.elapsed().as_secs());
  Ok(())
}
