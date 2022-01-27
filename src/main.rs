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
use lazy_static::lazy_static;
use cow_utils::CowUtils;

macro_rules! fatalerr {
    () => ({
      eprintln!();
      std::process::exit(1);
    });
    ($($arg:tt)*) => ({
        eprintln!($($arg)*);
        std::process::exit(1);
    });
}

struct Table<'a> {
  path: String,
  file: RefCell<Box<dyn Write>>,
  skip: String,
  columns: Vec<Column<'a>>
}
impl<'a> Table<'a> {
  fn new(path: &str, file: Option<&str>, filemode: &str, skip: Option<&'a str>) -> Table<'a> {
    let mut ownpath = String::from(path);
    if !ownpath.is_empty() && !ownpath.starts_with('/') { ownpath.insert(0, '/'); }
    if ownpath.ends_with('/') { ownpath.pop(); }
    Table {
      path: ownpath,
      file: match file {
        None => RefCell::new(Box::new(stdout())),
        Some(ref file) => RefCell::new(Box::new(
          match filemode {
            "truncate" => File::create(&Path::new(file)).unwrap_or_else(|err| fatalerr!("Error: failed to create output file '{}': {}", file, err)),
            "append" => OpenOptions::new().append(true).create(true).open(&Path::new(file)).unwrap_or_else(|err| fatalerr!("Error: failed to open output file '{}': {}", file, err)),
            mode => fatalerr!("Error: invalid 'mode' setting in configuration file: {}", mode)
          }
        ))
      },
      columns: Vec::new(),
      skip: match skip { Some(s) => format!("{}{}", path, s), None => String::new() }
    }
  }
  fn write(&self, text: &str) {
    self.file.borrow_mut().write_all(text.as_bytes()).unwrap_or_else(|err| fatalerr!("Error: IO error encountered while writing table: {}", err));
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
  hide: bool,
  include: Option<Regex>,
  exclude: Option<Regex>,
  convert: Option<&'a str>,
  find: Option<&'a str>,
  replace: Option<&'a str>,
  consol: Option<&'a str>,
  subtable: Option<Table<'a>>,
  bbox: Option<BBox>,
  multitype: bool
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

#[derive(Debug)]
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

struct BBox {
  minx: f64,
  miny: f64,
  maxx: f64,
  maxy: f64
}
impl BBox {
  fn from(str: &str) -> Option<BBox> {
    lazy_static! {
      static ref RE: Regex = Regex::new(r"^([0-9.]+),([0-9.]+) ([0-9.]+),([0-9.]+)$").unwrap();
    }
    RE.captures(str).map(|caps|
      BBox { minx: caps[1].parse().unwrap(), miny: caps[2].parse().unwrap(), maxx: caps[3].parse().unwrap(), maxy: caps[4].parse().unwrap() }
    )
  }
}

fn gml_to_ewkb(cell: &RefCell<String>, coll: &[Geometry], bbox: Option<&BBox>, multitype: bool) -> bool {
  let mut ewkb: Vec<u8> = vec![];

  if multitype || coll.len() > 1 {
    let multitype = coll.first().unwrap().gtype+3;
    ewkb.extend_from_slice(&[1, multitype, 0, 0, 0]);
    ewkb.extend_from_slice(&(coll.len() as u32).to_le_bytes());
  }

  for geom in coll {
    // println!("{:?}", geom);
    let code = match geom.dims {
      2 => 32, // Indicate EWKB where the srid follows this byte
      3 => 32 | 128, // Add bit to indicate the presence of Z values
      _ => {
        eprintln!("Warning: GML number of dimensions {} not supported", geom.dims);
        32
      }
    };
    ewkb.extend_from_slice(&[1, geom.gtype, 0, 0, code]);
    ewkb.extend_from_slice(&geom.srid.to_le_bytes());
    if geom.gtype == 3 { ewkb.extend_from_slice(&(geom.rings.len() as u32).to_le_bytes()); } // Only polygons can have multiple rings
    if let Some(bbox) = bbox {
      let mut overlap = false;
      let mut overlapx = false;
      for ring in geom.rings.iter() {
        if geom.gtype != 1 { ewkb.extend_from_slice(&((ring.len() as u32)/geom.dims as u32).to_le_bytes()); } // Points don't have multiple vertices
        for (i, pos) in ring.iter().enumerate() {
          if overlap { }
          else if geom.dims == 2 {
            if i%2 == 0 {
              overlapx = false;
              if *pos >= bbox.minx && *pos <= bbox.maxx { overlapx = true; }
            }
            else if overlapx && *pos < bbox.miny && *pos > bbox.maxy { overlap = true; }
          }
          else { // geom.dims == 3
            if i%3 == 0 {
              overlapx = false;
              if *pos >= bbox.minx && *pos <= bbox.maxx { overlapx = true; }
            }
            else if overlapx && i%3 == 1 && (*pos >= bbox.miny && *pos <= bbox.maxy) { overlap = true; }
          }
          ewkb.extend_from_slice(&pos.to_le_bytes());
        }
      }
      if !overlap { return false; }
    }
    else {
      for ring in geom.rings.iter() {
        if geom.gtype != 1 { ewkb.extend_from_slice(&((ring.len() as u32)/geom.dims as u32).to_le_bytes()); } // Points don't have multiple vertices
        for pos in ring.iter() {
          ewkb.extend_from_slice(&pos.to_le_bytes());
        }
      }
    }
  }

  let mut value = cell.borrow_mut();
  for byte in ewkb.iter() {
    value.push_str(&format!("{:02X}", byte));
  }
  true
}

fn add_table<'a>(rowpath: &str, outfile: Option<&str>, filemode: &str, skip: Option<&'a str>, colspec: &'a [Yaml]) -> Table<'a> {
  let mut table = Table::new(rowpath, outfile, filemode, skip);
  for col in colspec {
    let name = col["name"].as_str().unwrap_or_else(|| fatalerr!("Error: column has no 'name' entry in configuration file"));
    let colpath = col["path"].as_str().unwrap_or_else(|| fatalerr!("Error: column has no 'path' entry in configuration file"));
    let mut path = String::from(&table.path);
    if !colpath.is_empty() && !colpath.starts_with('/') { path.push('/'); }
    path.push_str(colpath);
    if path.ends_with('/') { path.pop(); }
    let subtable: Option<Table> = match col["cols"].is_badvalue() {
      true => None,
      false => {
        let file = col["file"].as_str().unwrap_or_else(|| fatalerr!("Error: subtable has no 'file' entry"));
        Some(add_table(&path, Some(file), filemode, skip, col["cols"].as_vec().unwrap_or_else(|| fatalerr!("Error: subtable 'cols' entry is not an array"))))
      }
    };
    let hide = col["hide"].as_bool().unwrap_or(false);
    let include: Option<Regex> = col["incl"].as_str().map(|str| Regex::new(str).unwrap_or_else(|err| fatalerr!("Error: invalid regex in 'incl' entry in configuration file: {}", err)));
    let exclude: Option<Regex> = col["excl"].as_str().map(|str| Regex::new(str).unwrap_or_else(|err| fatalerr!("Error: invalid regex in 'excl' entry in configuration file: {}", err)));
    let attr = col["attr"].as_str();
    let convert = col["conv"].as_str();
    let find = col["find"].as_str();
    let replace = col["repl"].as_str();
    let consol = col["cons"].as_str();
    let bbox = col["bbox"].as_str().and_then(BBox::from);
    let multitype = col["mult"].as_bool().unwrap_or(false);

    if let Some(val) = convert {
      if !vec!("xml-to-text", "gml-to-ewkb").contains(&val) {
        fatalerr!("Error: option 'convert' contains invalid value: {}", val);
      }
      if val == "gml-to-ewkb" {
        eprintln!("Warning: gml-to-ewkb conversion is experimental and in no way complete or standards compliant; use at your own risk");
      }
    }
    if include.is_some() || exclude.is_some() {
      if convert.is_some() {
        fatalerr!("Error: filtering (incl/excl) and 'conv' cannot be used together on a single column");
      }
      if find.is_some() {
        eprintln!("Notice: when using filtering (incl/excl) and find/replace on a single column, the filter is checked before replacements");
      }
      if consol.is_some() {
        eprintln!("Notice: when using filtering (incl/excl) and consolidation on a single column, the filter is checked at each phase of consolidation separately");
      }
    }
    if bbox.is_some() && (convert.is_none() || convert.unwrap() != "gml-to-ewkb") {
      eprintln!("Warning: the bbox option has no function without conversion type 'gml-to-ekwb'");
    }

    let column = Column { name: name.to_string(), path, value: RefCell::new(String::new()), attr, hide, include, exclude, convert, find, replace, consol, subtable, bbox, multitype };
    table.columns.push(column);
  }
  table
}

fn main() {
  let args: Vec<_> = env::args().collect();
  let bufread: Box<dyn BufRead>;
  if args.len() == 2 {
    bufread = Box::new(BufReader::new(stdin()));
  }
  else if args.len() == 3 {
    bufread = Box::new(BufReader::new(File::open(&args[2]).unwrap_or_else(|err| fatalerr!("Error: failed to open input file '{}': {}", args[2], err))));
  }
  else { fatalerr!("Usage: {} <configfile> [xmlfile]", args[0]); }

  let config = {
    let mut config_str = String::new();
    let mut file = File::open(&args[1]).unwrap_or_else(|err| fatalerr!("Error: failed to open configuration file '{}': {}", args[1], err));
    file.read_to_string(&mut config_str).unwrap_or_else(|err| fatalerr!("Error: failed to read configuration file '{}': {}", args[1], err));
    &YamlLoader::load_from_str(&config_str).unwrap_or_else(|err| fatalerr!("Error: invalid syntax in configuration file: {}", err))[0]
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

  let rowpath = config["path"].as_str().unwrap_or_else(|| fatalerr!("Error: no valid 'path' entry in configuration file"));
  let colspec = config["cols"].as_vec().unwrap_or_else(|| fatalerr!("Error: no valid 'cols' array in configuration file"));
  let outfile = config["file"].as_str();
  let filemode = match config["mode"].is_badvalue() {
    true => "truncate",
    false => config["mode"].as_str().unwrap_or_else(|| fatalerr!("Error: invalid 'mode' entry in configuration file"))
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
        path.push_str(reader.decode(e.name()).unwrap_or_else(|err| fatalerr!("Error: failed to decode XML tag '{}': {}", String::from_utf8_lossy(e.name()), err)));
        if filtered || skipped { continue; }
        if path == table.skip {
          skipped = true;
          continue;
        }
        else if xmltotext {
          text.push_str(&format!("<{}>", &e.unescape_and_decode(&reader).unwrap_or_else(|err| fatalerr!("Error: failed to decode XML tag '{}': {}", String::from_utf8_lossy(e.name()), err))));
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
                    let mut value = String::from(reader.decode(&attr.value).unwrap_or_else(|err| fatalerr!("Error: failed to decode XML attribute '{}': {}", String::from_utf8_lossy(&attr.value), err)));
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
                    let value = reader.decode(&attr.value).unwrap_or_else(|err| fatalerr!("Error: failed to decode XML attribute '{}': {}", String::from_utf8_lossy(&attr.value), err));
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
        else if path.len() >= table.path.len() {
          if path == table.path { fullcount += 1; }

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
                if let Some(re) = &table.columns[i].include {
                  if !re.is_match(&table.columns[i].value.borrow()) {
                    filtered = true;
                    table.clear_columns();
                  }
                }
                if let Some(re) = &table.columns[i].exclude {
                  if re.is_match(&table.columns[i].value.borrow()) {
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
          text.push_str(&e.unescape_and_decode(&reader).unwrap_or_else(|err| fatalerr!("Error: failed to decode XML text node '{}': {}", String::from_utf8_lossy(e), err)));
          continue;
        }
        else if gmltoewkb {
          if gmlpos {
            let value = String::from(&e.unescape_and_decode(&reader).unwrap_or_else(|err| fatalerr!("Error: failed to decode XML gmlpos '{}': {}", String::from_utf8_lossy(e), err)));
            for pos in value.split(' ') {
              gmlcoll.last_mut().unwrap().rings.last_mut().unwrap().push(pos.parse().unwrap_or_else(|err| fatalerr!("Error: failed to parse GML pos '{}' into float: {}", pos, err)));
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
            let unescaped = e.unescaped().unwrap_or_else(|err| fatalerr!("Error: failed to unescape XML text node '{}': {}", String::from_utf8_lossy(e), err));
            let decoded = reader.decode(&unescaped).unwrap_or_else(|err| fatalerr!("Error: failed to decode XML text node '{}': {}", String::from_utf8_lossy(e), err));
            table.columns[i].value.borrow_mut().push_str(&decoded.cow_replace("\\", "\\\\").cow_replace("\r", "\\r").cow_replace("\n", "\\n").cow_replace("\t", "\\t"));
            if let Some(re) = &table.columns[i].include {
              if !re.is_match(&table.columns[i].value.borrow()) {
                filtered = true;
                table.clear_columns();
              }
            }
            if let Some(re) = &table.columns[i].exclude {
              if re.is_match(&table.columns[i].value.borrow()) {
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
              if table.columns[i].hide {
                table.columns[i].value.borrow_mut().clear();
                continue;
              }
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
              if !gml_to_ewkb(&table.columns[i].value, &gmlcoll, table.columns[i].bbox.as_ref(), table.columns[i].multitype) {
                filtered = true;
                table.clear_columns();
              }
              gmlcoll.clear();
              break;
            }
          }
        }
      },
      Ok(Event::Eof) => break,
      Err(e) => fatalerr!("Error: failed to parse XML at position {}: {}", reader.buffer_position(), e),
      _ => ()
    }
    buf.clear();
  }
  eprintln!("{} rows processed in {} seconds{}{}",
    fullcount-filtercount-skipcount,
    start.elapsed().as_secs(),
    match filtercount { 0 => "".to_owned(), n => format!(" ({} excluded)", n) },
    match skipcount { 0 => "".to_owned(), n => format!(" ({} skipped)", n) }
  );
}
