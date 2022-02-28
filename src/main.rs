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

struct Settings {
  filemode: String,
  skip: String,
  emit_copyfrom: bool,
  emit_createtable: bool,
  emit_starttransaction: bool,
  emit_truncate: bool,
  emit_droptable: bool,
  hush_info: bool,
  hush_notice: bool,
  hush_warning: bool
}

struct Table<'a> {
  name: String,
  path: String,
  file: RefCell<Box<dyn Write>>,
  columns: Vec<Column<'a>>,
  emit_copyfrom: bool,
  emit_starttransaction: bool
}
impl<'a> Table<'a> {
  fn new(name: &str, path: &str, file: Option<&str>, settings: &Settings) -> Table<'a> {
    let mut ownpath = String::from(path);
    if !ownpath.is_empty() && !ownpath.starts_with('/') { ownpath.insert(0, '/'); }
    if ownpath.ends_with('/') { ownpath.pop(); }
    Table {
      name: name.to_owned(),
      path: ownpath,
      file: match file {
        None => RefCell::new(Box::new(stdout())),
        Some(ref file) => RefCell::new(Box::new(
          match settings.filemode.as_ref() {
            "truncate" => File::create(&Path::new(file)).unwrap_or_else(|err| fatalerr!("Error: failed to create output file '{}': {}", file, err)),
            "append" => OpenOptions::new().append(true).create(true).open(&Path::new(file)).unwrap_or_else(|err| fatalerr!("Error: failed to open output file '{}': {}", file, err)),
            mode => fatalerr!("Error: invalid 'mode' setting in configuration file: {}", mode)
          }
        ))
      },
      columns: Vec::new(),
      emit_copyfrom: settings.emit_copyfrom,
      emit_starttransaction: settings.emit_starttransaction
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
impl<'a> Drop for Table<'a> {
  fn drop(&mut self) {
    if self.emit_copyfrom { self.write("\\.\n"); }
    if self.emit_starttransaction { self.write("COMMIT;\n"); }
  }
}

struct Column<'a> {
  name: String,
  path: String,
  datatype: String,
  value: RefCell<String>,
  attr: Option<&'a str>,
  hide: bool,
  include: Option<Regex>,
  exclude: Option<Regex>,
  trim: bool,
  convert: Option<&'a str>,
  find: Option<&'a str>,
  replace: Option<&'a str>,
  aggr: Option<&'a str>,
  subtable: Option<Table<'a>>,
  bbox: Option<BBox>,
  multitype: bool
}
impl std::borrow::Borrow<str> for Column<'_> {
  fn borrow(&self) -> &str {
    &*self.name
  }
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

fn gml_to_ewkb(cell: &RefCell<String>, coll: &[Geometry], bbox: Option<&BBox>, multitype: bool, settings: &Settings) -> bool {
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
        if !settings.hush_warning { eprintln!("Warning: GML number of dimensions {} not supported", geom.dims); }
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

  static CHARS: &'static [u8] = b"0123456789ABCDEF";
  let mut value = cell.borrow_mut();
  value.reserve(ewkb.len()*2);
  for byte in ewkb.iter() {
    value.push(CHARS[(byte >>  4) as usize].into());
    value.push(CHARS[(byte & 0xf) as usize].into());
  }
  true
}

fn add_table<'a>(name: &str, rowpath: &str, outfile: Option<&str>, settings: &Settings, colspec: &'a [Yaml], fkey: Option<String>) -> Table<'a> {
  let mut table = Table::new(name, rowpath, outfile, settings);
  for col in colspec {
    let colname = col["name"].as_str().unwrap_or_else(|| fatalerr!("Error: column has no 'name' entry in configuration file"));
    let colpath = col["path"].as_str().unwrap_or_else(|| fatalerr!("Error: column has no 'path' entry in configuration file"));
    let mut path = String::from(&table.path);
    if !colpath.is_empty() && !colpath.starts_with('/') { path.push('/'); }
    path.push_str(colpath);
    if path.ends_with('/') { path.pop(); }
    let datatype = col["type"].as_str().unwrap_or("text").to_string();
    let subtable: Option<Table> = match col["cols"].is_badvalue() {
      true => None,
      false => {
        let file = col["file"].as_str().unwrap_or_else(|| fatalerr!("Error: subtable has no 'file' entry"));
        Some(add_table(colname, &path, Some(file), settings, col["cols"].as_vec().unwrap_or_else(|| fatalerr!("Error: subtable 'cols' entry is not an array")), Some(format!("{} {}", name, table.columns[0].datatype))))
      }
    };
    let hide = col["hide"].as_bool().unwrap_or(false);
    let include: Option<Regex> = col["incl"].as_str().map(|str| Regex::new(str).unwrap_or_else(|err| fatalerr!("Error: invalid regex in 'incl' entry in configuration file: {}", err)));
    let exclude: Option<Regex> = col["excl"].as_str().map(|str| Regex::new(str).unwrap_or_else(|err| fatalerr!("Error: invalid regex in 'excl' entry in configuration file: {}", err)));
    let trim = col["trim"].as_bool().unwrap_or(false);
    let attr = col["attr"].as_str();
    let convert = col["conv"].as_str();
    let find = col["find"].as_str();
    let replace = col["repl"].as_str();
    let aggr = col["aggr"].as_str();
    let bbox = col["bbox"].as_str().and_then(BBox::from);
    let multitype = col["mult"].as_bool().unwrap_or(false);

    if let Some(val) = convert {
      if !vec!("xml-to-text", "gml-to-ewkb").contains(&val) {
        fatalerr!("Error: option 'convert' contains invalid value: {}", val);
      }
      if val == "gml-to-ewkb" && !settings.hush_notice {
        eprintln!("Notice: gml-to-ewkb conversion is experimental and in no way complete or standards compliant; use at your own risk");
      }
    }
    if include.is_some() || exclude.is_some() {
      if convert.is_some() {
        fatalerr!("Error: filtering (incl/excl) and 'conv' cannot be used together on a single column");
      }
      if find.is_some() && !settings.hush_notice {
        eprintln!("Notice: when using filtering (incl/excl) and find/replace on a single column, the filter is checked before replacements");
      }
      if aggr.is_some() && !settings.hush_notice {
        eprintln!("Notice: when using filtering (incl/excl) and aggregation on a single column, the filter is checked at each step of aggregation separately");
      }
    }
    if bbox.is_some() && (convert.is_none() || convert.unwrap() != "gml-to-ewkb") && !settings.hush_warning {
      eprintln!("Warning: the bbox option has no function without conversion type 'gml-to-ekwb'");
    }

    let column = Column { name: colname.to_string(), path, datatype, value: RefCell::new(String::new()), attr, hide, include, exclude, trim, convert, find, replace, aggr, subtable, bbox, multitype };
    table.columns.push(column);
  }

  emit_preamble(&table, settings, fkey);
  table
}
fn emit_preamble(table: &Table, settings: &Settings, fkey: Option<String>) {
  if settings.emit_starttransaction {
    table.write("START TRANSACTION;\n");
  }
  if settings.emit_droptable {
    table.write(&format!("DROP TABLE IF EXISTS {};\n", table.name));
  }
  if settings.emit_createtable {
    let mut cols = table.columns.iter().filter_map(|c| {
      if c.subtable.is_some() { return None; }
      let mut spec = String::from(&c.name);
      spec.push(' ');
      spec.push_str(&c.datatype);
      Some(spec)
    }).collect::<Vec<String>>().join(", ");
    if fkey.is_some() { cols.insert_str(0, &format!("{}, ", fkey.as_ref().unwrap())); }
    table.write(&format!("CREATE TABLE IF NOT EXISTS {} ({});\n", table.name, cols));
  }
  if settings.emit_truncate {
    table.write(&format!("TRUNCATE {};\n", table.name));
  }
  if settings.emit_copyfrom {
    let cols = table.columns.iter().filter_map(|c| match c.subtable { None=> Some(String::from(&c.name)), Some(_) => None }).collect::<Vec<String>>().join(", ");
    if fkey.is_some() {
      table.write(&format!("COPY {} ({}, {}) FROM stdin;\n", table.name, fkey.unwrap().split(' ').next().unwrap(), cols));
    }
    else { table.write(&format!("COPY {} ({}) FROM stdin;\n", table.name, cols)); }
  }
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

  let name = config["name"].as_str().unwrap_or_else(|| fatalerr!("Error: no valid 'name' entry in configuration file"));
  let rowpath = config["path"].as_str().unwrap_or_else(|| fatalerr!("Error: no valid 'path' entry in configuration file"));
  let colspec = config["cols"].as_vec().unwrap_or_else(|| fatalerr!("Error: no valid 'cols' array in configuration file"));
  let outfile = config["file"].as_str();
  let emit = config["emit"].as_str().unwrap_or("");
  let hush = config["hush"].as_str().unwrap_or("");
  let mut settings = Settings {
    filemode: config["mode"].as_str().unwrap_or("truncate").to_owned(),
    skip: config["skip"].as_str().unwrap_or("").to_owned(),
    emit_copyfrom: emit.contains("copy_from") || emit.contains("create_table") || emit.contains("start_trans") || emit.contains("truncate") || emit.contains("drop_table"),
    emit_createtable: emit.contains("create_table"),
    emit_starttransaction: emit.contains("start_trans"),
    emit_truncate: emit.contains("truncate"),
    emit_droptable: emit.contains("drop_table"),
    hush_info: hush.contains("info"),
    hush_notice: hush.contains("notice"),
    hush_warning: hush.contains("warning")
  };
  let maintable = add_table(name, rowpath, outfile, &settings, colspec, None);
  if !settings.skip.is_empty() {
    if !settings.skip.starts_with('/') { settings.skip.insert(0, '/'); }
    settings.skip.insert_str(0, &maintable.path); // Maintable path is normalized in add_table()
  }
  let mut tables: Vec<&Table> = Vec::new();
  let mut table = &maintable;

  let mut filtered = false;
  let mut skipped = false;
  let mut xmltotext = false;
  let mut text = String::new();
  let mut gmltoewkb = false;
  let mut gmlpos = false;
  let mut gmlcoll: Vec<Geometry> = vec![];
  let trimre = Regex::new("[ \n\r\t]*\n[ \n\r\t]*").unwrap();

  let start = Instant::now();
  let mut loops;
  'main: loop { // Main loop over the XML nodes
    let event = reader.read_event(&mut buf);
    loops = 0;
    'restart: loop { // Restart loop to be able to process a node twice
      loops += 1;
      match event {
        Ok(Event::Start(ref e)) => {
          if loops == 1 {
            path.push('/');
            path.push_str(reader.decode(e.name()).unwrap_or_else(|err| fatalerr!("Error: failed to decode XML tag '{}': {}", String::from_utf8_lossy(e.name()), err)));
          }
          if filtered || skipped { break; }
          if path == settings.skip {
            skipped = true;
            break;
          }
          else if xmltotext {
            text.push_str(&format!("<{}>", &e.unescape_and_decode(&reader).unwrap_or_else(|err| fatalerr!("Error: failed to decode XML tag '{}': {}", String::from_utf8_lossy(e.name()), err))));
            break;
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
                _ => if !settings.hush_warning { eprintln!("Warning: GML type {} not supported", tag); }
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
                        Err(_) => if !settings.hush_warning { eprintln!("Warning: invalid srsName {} in GML", value); }
                      }
                    },
                    Ok("srsDimension") => {
                      let value = reader.decode(&attr.value).unwrap_or_else(|err| fatalerr!("Error: failed to decode XML attribute '{}': {}", String::from_utf8_lossy(&attr.value), err));
                      match value.parse::<u8>() {
                        Ok(int) => {
                          if let Some(geom) = gmlcoll.last_mut() { geom.dims = int };
                        },
                        Err(_) => if !settings.hush_warning { eprintln!("Warning: invalid srsDimension {} in GML", value); }
                      }
                    }
                    _ => ()
                  }
                }
              }
            }
            break;
          }
          else if path.len() >= table.path.len() {
            if path == maintable.path { fullcount += 1; }

            for i in 0..table.columns.len() {
              if path == table.columns[i].path { // This start tag matches one of the defined columns

                // Handle 'subtable' case (the 'cols' entry has 'cols' of its own)
                if table.columns[i].subtable.is_some() {
                  tables.push(table);
                  table = table.columns[i].subtable.as_ref().unwrap();
                  continue 'restart; // Continue the restart loop because a subtable column may also match the current path
                }

                // Handle the 'attr' case where the content is read from an attribute of this tag
                if let Some(request) = table.columns[i].attr {
                  for res in e.attributes() {
                    if let Ok(attr) = res {
                      if let Ok(key) = reader.decode(attr.key) {
                        if key == request {
                          if let Ok(value) = reader.decode(&attr.value) {
                            if !table.columns[i].value.borrow().is_empty() && !allow_iteration(&table.columns[i], &settings) { break; }
                            table.columns[i].value.borrow_mut().push_str(value)
                          }
                          else if !settings.hush_warning { eprintln!("Warning: failed to decode attribute {} for column {}", request, table.columns[i].name); }
                          break;
                        }
                      }
                      else if !settings.hush_warning { eprintln!("Warning: failed to decode an attribute for column {}", table.columns[i].name); }
                    }
                    else if !settings.hush_warning { eprintln!("Warning: failed to read attributes for column {}", table.columns[i].name); }
                  }
                  if table.columns[i].value.borrow().is_empty() && !settings.hush_warning {
                    eprintln!("Warning: column {} requested attribute {} not found", table.columns[i].name, request);
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
          if filtered || skipped { break; }
          if xmltotext {
            text.push_str(&e.unescape_and_decode(&reader).unwrap_or_else(|err| fatalerr!("Error: failed to decode XML text node '{}': {}", String::from_utf8_lossy(e), err)));
            break;
          }
          else if gmltoewkb {
            if gmlpos {
              let value = String::from(&e.unescape_and_decode(&reader).unwrap_or_else(|err| fatalerr!("Error: failed to decode XML gmlpos '{}': {}", String::from_utf8_lossy(e), err)));
              for pos in value.split(' ') {
                gmlcoll.last_mut().unwrap().rings.last_mut().unwrap().push(fast_float::parse(pos).unwrap_or_else(|err| fatalerr!("Error: failed to parse GML pos '{}' into float: {}", pos, err)));
              }
            }
            break;
          }
          for i in 0..table.columns.len() {
            if path == table.columns[i].path {
              if table.columns[i].attr.is_some() { break; }
              if !table.columns[i].value.borrow().is_empty() && !allow_iteration(&table.columns[i], &settings) { break; }

              let unescaped = e.unescaped().unwrap_or_else(|err| fatalerr!("Error: failed to unescape XML text node '{}': {}", String::from_utf8_lossy(e), err));
              let decoded = reader.decode(&unescaped).unwrap_or_else(|err| fatalerr!("Error: failed to decode XML text node '{}': {}", String::from_utf8_lossy(e), err));
              if table.columns[i].trim {
                let trimmed = trimre.replace_all(decoded, " ");
                table.columns[i].value.borrow_mut().push_str(&trimmed.cow_replace("\\", "\\\\").cow_replace("\t", "\\t"));
              }
              else {
                table.columns[i].value.borrow_mut().push_str(&decoded.cow_replace("\\", "\\\\").cow_replace("\r", "\\r").cow_replace("\n", "\\n").cow_replace("\t", "\\t"));
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
              if !tables.is_empty() {
                table = tables.pop().unwrap();
                continue 'restart;
              }
            }
          }
          else if skipped && path == settings.skip {
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
                if !gml_to_ewkb(&table.columns[i].value, &gmlcoll, table.columns[i].bbox.as_ref(), table.columns[i].multitype, &settings) {
                  filtered = true;
                  table.clear_columns();
                }
                gmlcoll.clear();
                break;
              }
            }
          }
        },
        Ok(Event::Eof) => break 'main,
        Err(e) => fatalerr!("Error: failed to parse XML at position {}: {}", reader.buffer_position(), e),
        _ => ()
      }
      break; // By default we break out of the restart loop
    }
    buf.clear();
  }
  if !settings.hush_info {
    eprintln!("Info: [{}] {} rows processed in {} seconds{}{}",
      maintable.name,
      fullcount-filtercount-skipcount,
      start.elapsed().as_secs(),
      match filtercount { 0 => "".to_owned(), n => format!(" ({} excluded)", n) },
      match skipcount { 0 => "".to_owned(), n => format!(" ({} skipped)", n) }
    );
  }
}

fn allow_iteration(column: &Column, settings: &Settings) -> bool {
  match column.aggr {
    None if settings.hush_warning => false,
    None => {
      eprintln!("Warning: column '{}' has multiple occurrences without an aggregation method; using 'first'", column.name);
      false
    },
    Some("first") => false,
    Some("append") => {
      if !column.value.borrow().is_empty() { column.value.borrow_mut().push(','); }
      true
    },
    _ => true
  }
}
