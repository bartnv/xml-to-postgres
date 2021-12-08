use std::io::{Read, Write, BufReader, BufRead, stdin, stdout};
use std::fs::{File, OpenOptions};
use std::path::Path;
use std::env;
use std::cell::RefCell;
use std::time::Instant;
use quick_xml::Reader;
use quick_xml::events::Event;
use yaml_rust::YamlLoader;
use yaml_rust::yaml::Yaml;
use regex::Regex;

struct Table<'a> {
  path: String,
  file: RefCell<Box<dyn Write>>,
  skip: String,
  columns: Vec<Column<'a>>
}
impl<'a> Table<'a> {
  fn new(path: &str, file: Option<&str>, filemode: &str, skip: Option<&'a str>) -> Table<'a> {
    Table {
      path: String::from(path),
      file: match file {
        None => RefCell::new(Box::new(stdout())),
        Some(ref file) => RefCell::new(Box::new(
          match filemode {
            "truncate" => File::create(&Path::new(file)).unwrap(),
            "append" => OpenOptions::new().append(true).create(true).open(&Path::new(file)).unwrap(),
            mode => panic!("Invalid 'mode' setting in configuration file: {}", mode)
          }
        ))
      },
      columns: Vec::new(),
      skip: match skip { Some(s) => format!("{}{}", path, s), None => String::new() }
    }
  }
  fn write(&self, text: &str) {
    self.file.borrow_mut().write_all(text.as_bytes()).expect("Write error encountered; exiting...");
  }
  fn clear_columns(&self) {
    for col in &self.columns {
      col.value.borrow_mut().clear();
    }
  }
}

struct Column<'a> {
  name: String,
  path: String,
  value: RefCell<String>,
  attr: Option<&'a str>,
  filter: Option<Regex>,
  convert: Option<&'a str>,
  find: Option<&'a str>,
  replace: Option<&'a str>,
  consol: Option<&'a str>,
  subtable: Option<Table<'a>>
}
impl std::fmt::Debug for Column<'_> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("Column")
      .field("name", &self.name)
      .field("path", &self.path)
      .field("attr", &self.attr)
      .finish()
  }
}

struct Geometry {
  gtype: u8,
  dims: u8,
  srid: u32,
  rings: Vec<Vec<f64>>
}
impl Geometry {
  fn new(gtype: u8) -> Geometry {
    Geometry { gtype, dims: 2, srid: 4326, rings: Vec::new() }
  }
}

fn gml_to_ewkb(cell: &RefCell<String>, coll: &[Geometry]) {
  let mut ewkb: Vec<u8> = vec![];

  if coll.len() > 1 {
    let multitype = coll.first().unwrap().gtype+3;
    ewkb.extend_from_slice(&[1, multitype, 0, 0, 0]);
    ewkb.extend_from_slice(&(coll.len() as u32).to_le_bytes());
  }

  for geom in coll {
    let code = match geom.dims {
      2 => 32, // Indicate EWKB where the srid follows this byte
      3 => 32 | 128, // Add bit to indicate the presense of Z values
      _ => {
        eprintln!("GML number of dimensions {} not supported", geom.dims);
        32
      }
    };
    ewkb.extend_from_slice(&[1, geom.gtype, 0, 0, code]);
    ewkb.extend_from_slice(&geom.srid.to_le_bytes());
    if geom.gtype == 3 { ewkb.extend_from_slice(&(geom.rings.len() as u32).to_le_bytes()); } // Only polygons have multiple rings
    for ring in geom.rings.iter() {
      if geom.gtype != 1 { ewkb.extend_from_slice(&((ring.len() as u32)/geom.dims as u32).to_le_bytes()); } // Points don't have multiple vertices
      for pos in ring.iter() {
        ewkb.extend_from_slice(&pos.to_le_bytes());
      }
    }
  }

  let mut value = cell.borrow_mut();
  for byte in ewkb.iter() {
    value.push_str(&format!("{:02X}", byte));
  }
}

fn add_table<'a>(rowpath: &str, outfile: Option<&str>, filemode: &str, skip: Option<&'a str>, colspec: &'a [Yaml]) -> Table<'a> {
  let mut table = Table::new(rowpath, outfile, filemode, skip);
  for col in colspec {
    let name = col["name"].as_str().expect("Column has no 'name' entry in configuration file");
    let colpath = col["path"].as_str().expect("Column has no 'path' entry in configuration file");
    let mut path = String::from(rowpath);
    path.push_str(colpath);
    let subtable: Option<Table> = match col["cols"].is_badvalue() {
      true => None,
      false => {
        let file = col["file"].as_str().expect("Subtable has no 'file' entry");
        Some(add_table(&path, Some(file), filemode, skip, col["cols"].as_vec().expect("Subtable 'cols' entry is not an array")))
      }
    };
    let filter: Option<Regex> = col["filt"].as_str().map(|str| Regex::new(str).expect("Invalid regex in 'filt' entry in configuration file"));
    let attr = col["attr"].as_str();
    let convert = col["conv"].as_str();
    let find = col["find"].as_str();
    let replace = col["repl"].as_str();
    let consol = col["cons"].as_str();

    if convert.is_some() && !vec!("xml-to-text", "gml-to-ewkb").contains(&convert.unwrap()) {
      panic!("Option 'convert' contains invalid value {}", convert.unwrap());
    }
    if filter.is_some() {
      if convert.is_some() {
        panic!("Option 'filt' and 'conv' cannot be used together on a single column");
      }
      if find.is_some() {
        eprintln!("Notice: when using a filter and find/replace on a single column, the filter is applied before replacements");
      }
      if consol.is_some() {
        eprintln!("Notice: when using a filter and consolidation on a single column, the filter is applied to each phase of consolidation separately");
      }
    }

    let column = Column { name: name.to_string(), path, value: RefCell::new(String::new()), attr, filter, convert, find, replace, consol, subtable };
    table.columns.push(column);
  }
  table
}

fn main() -> std::io::Result<()> {
  let args: Vec<_> = env::args().collect();
  let bufread: Box<dyn BufRead>;
  if args.len() == 2 {
    bufread = Box::new(BufReader::new(stdin()));
  }
  else if args.len() == 3 {
    bufread = Box::new(BufReader::new(File::open(&args[2])?));
  }
  else {
    eprintln!("usage: {} <configfile> <xmlfile>", args[0]);
    return Ok(());
  }

  let config = {
    let mut config_str = String::new();
    File::open(&args[1]).unwrap().read_to_string(&mut config_str).unwrap();
    &YamlLoader::load_from_str(&config_str).unwrap()[0]
  };

  let mut reader;
  reader = Reader::from_reader(bufread);
  reader.trim_text(true)
        .expand_empty_elements(true);

  let mut path = String::new();
  let mut buf = Vec::new();
  let mut fullcount = 0;
  let mut filtercount = 0;
  let mut skipcount = 0;

  let rowpath = config["path"].as_str().expect("No valid 'path' entry in configuration file");
  let colspec = config["cols"].as_vec().expect("No valid 'cols' array in configuration file");
  let outfile = config["file"].as_str();
  let filemode = match config["mode"].is_badvalue() {
    true => "truncate",
    false => config["mode"].as_str().expect("Invalid 'mode' entry in configuration file")
  };
  let skip = config["skip"].as_str();
  let maintable = add_table(rowpath, outfile, filemode, skip, colspec);
  let mut tables: Vec<&Table> = Vec::new();
  let mut table = &maintable;

  let mut filtered = false;
  let mut skipped = false;
  let mut xmltotext = false;
  let mut text = String::new();
  let mut gmltoewkb = false;
  let mut gmlpos = false;
  let mut gmlcoll: Vec<Geometry> = vec![];
  let start = Instant::now();
  loop {
    match reader.read_event(&mut buf) {
      Ok(Event::Start(ref e)) => {
        path.push('/');
        path.push_str(reader.decode(e.name()).unwrap());
        if filtered || skipped { continue; }
        if path == table.skip {
          skipped = true;
          continue;
        }
        else if xmltotext {
          text.push_str(&format!("<{}>", &e.unescape_and_decode(&reader).unwrap()));
          continue;
        }
        else if gmltoewkb {
          match reader.decode(e.name()) {
            Err(_) => (),
            Ok(tag) => match tag {
              "gml:Point" => {
                gmlcoll.push(Geometry::new(1));
                gmlcoll.last_mut().unwrap().rings.push(Vec::new());
              },
              "gml:LineString" => gmlcoll.push(Geometry::new(2)),
              "gml:Polygon" => gmlcoll.push(Geometry::new(3)),
              "gml:MultiPolygon" => (),
              "gml:polygonMember" => (),
              "gml:exterior" => (),
              "gml:interior" => (),
              "gml:LinearRing" => gmlcoll.last_mut().unwrap().rings.push(Vec::new()),
              "gml:posList" => gmlpos = true,
              "gml:pos" => gmlpos = true,
              _ => eprintln!("GML type {} not supported", tag)
            }
          }
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
                      Ok(int) => {
                        if let Some(geom) = gmlcoll.last_mut() { geom.srid = int };
                      },
                      Err(_) => eprintln!("Invalid srsName {} in GML", value)
                    }
                  },
                  Ok("srsDimension") => {
                    let value = reader.decode(&attr.value).unwrap();
                    match value.parse::<u8>() {
                      Ok(int) => {
                        if let Some(geom) = gmlcoll.last_mut() { geom.dims = int };
                      },
                      Err(_) => eprintln!("Invalid srsDimension {} in GML", value)
                    }
                  }
                  _ => ()
                }
              }
            }
          }
          continue;
        }
        else if path == table.path {
          fullcount += 1;
        }
        else if path.len() > table.path.len() {
          for i in 0..table.columns.len() {
            if path == table.columns[i].path { // This start tag matches one of the defined columns

              // Handle 'subtable' case (the 'cols' entry has 'cols' of its own)
              if table.columns[i].subtable.is_some() {
                tables.push(table);
                table = table.columns[i].subtable.as_ref().unwrap();
                break;
              }

              // Handle the 'attr' case where the content is read from an attribute of this tag
              if let Some(request) = table.columns[i].attr {
                for res in e.attributes() {
                  if let Ok(attr) = res {
                    if let Ok(key) = reader.decode(attr.key) {
                      if key == request {
                        if let Ok(value) = reader.decode(&attr.value) {
                          table.columns[i].value.borrow_mut().push_str(value)
                        }
                        else { eprintln!("Failed to decode attribute {} for column {}", request, table.columns[i].name); }
                        break;
                      }
                    }
                    else { eprintln!("Failed to decode an attribute for column {}", table.columns[i].name); }
                  }
                  else { eprintln!("Error reading attributes for column {}", table.columns[i].name); }
                }
                if table.columns[i].value.borrow().is_empty() {
                  eprintln!("Column {} requested attribute {} not found", table.columns[i].name, request);
                }
                if let Some(re) = &table.columns[i].filter {
                  if !re.is_match(&table.columns[i].value.borrow()) {
                    filtered = true;
                    table.clear_columns();
                  }
                }
              }

              // Set the appropriate convert flag for the following data in case the 'conv' option is present
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
        if filtered || skipped { continue; }
        if xmltotext {
          text.push_str(&e.unescape_and_decode(&reader).unwrap());
          continue;
        }
        else if gmltoewkb {
          if gmlpos {
            let value = String::from(&e.unescape_and_decode(&reader).unwrap());
            for pos in value.split(' ') {
              gmlcoll.last_mut().unwrap().rings.last_mut().unwrap().push(pos.parse().unwrap());
            }
          }
          continue;
        }
        for i in 0..table.columns.len() {
          if path == table.columns[i].path {
            if table.columns[i].attr.is_some() { break; }
            match table.columns[i].consol {
              None => {
                if !table.columns[i].value.borrow().is_empty() {
                  eprintln!("Column '{}' has multiple occurrences without a consolidation method; using 'first'", table.columns[i].name);
                  break;
                }
              },
              Some("first") => {
                break;
              },
              Some("append") => {
                if !table.columns[i].value.borrow().is_empty() { table.columns[i].value.borrow_mut().push(','); }
              },
              Some(s) => {
                eprintln!("Column '{}' has invalid consolidation method {}", table.columns[i].name, s);
                break;
              }
            }
            table.columns[i].value.borrow_mut().push_str(&e.unescape_and_decode(&reader).unwrap().replace("\\", "\\\\"));
            if let Some(re) = &table.columns[i].filter {
              if !re.is_match(&table.columns[i].value.borrow()) {
                filtered = true;
                table.clear_columns();
              }
            }
            break;
          }
        }
      },
      Ok(Event::End(_)) => {
        if path == table.path { // This is an end tag of the row path
          if filtered {
            filtered = false;
            filtercount += 1;
          }
          else {

            if !tables.is_empty() { // This is a subtable; write the first column value of the parent table as the first column of the subtable (for use as a foreign key)
              table.write(&tables.last().unwrap().columns[0].value.borrow());
              table.write("\t");
            }

            // Now write out the other column values
            for i in 0..table.columns.len() {
              if table.columns[i].subtable.is_some() { continue; }
              if i > 0 { table.write("\t"); }
              if table.columns[i].value.borrow().is_empty() { table.write("\\N"); }
              else {
                if let (Some(s), Some(r)) = (table.columns[i].find, table.columns[i].replace) {
                  let mut value = table.columns[i].value.borrow_mut();
                  *value = value.replace(s, r);
                }
                table.write(&table.columns[i].value.borrow());
                table.columns[i].value.borrow_mut().clear();
              }
            }
            table.write("\n");
            if !tables.is_empty() { table = tables.pop().unwrap(); }
          }
        }
        else if path == table.skip {
          skipped = false;
          skipcount += 1;
        }
        let i = path.rfind('/').unwrap();
        let tag = path.split_off(i);
        if xmltotext {
          text.push_str(&format!("<{}>", tag));
          for i in 0..table.columns.len() {
            if path == table.columns[i].path {
              xmltotext = false;
              if let (Some(s), Some(r)) = (table.columns[i].find, table.columns[i].replace) {
                text = text.replace(s, r);
              }
              table.columns[i].value.borrow_mut().push_str(&text);
              text.clear();
              break;
            }
          }
        }
        else if gmltoewkb {
          if gmlpos && ((tag == "/gml:pos") || (tag == "/gml:posList")) { gmlpos = false; }
          for i in 0..table.columns.len() {
            if path == table.columns[i].path {
              gmltoewkb = false;
              gml_to_ewkb(&table.columns[i].value, &gmlcoll);
              gmlcoll.clear();
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
  eprintln!("{} rows processed in {} seconds{}{}",
    fullcount-filtercount-skipcount,
    start.elapsed().as_secs(),
    match filtercount { 0 => "".to_owned(), n => format!(" ({} filtered)", n) },
    match skipcount { 0 => "".to_owned(), n => format!(" ({} skipped)", n) }
  );
  Ok(())
}
