use std::io::{stdin, stdout, BufRead, BufReader, IsTerminal as _, Read, Write};
use std::fs::{File, OpenOptions};
use std::mem;
use std::fmt::Write as _;
use std::path::Path;
use std::env;
use std::cell::{ Cell, RefCell };
use std::time::Instant;
use std::sync::mpsc;
use std::thread;
use std::default::Default;
use std::collections::HashMap;
use quick_xml::Reader;
use quick_xml::events::Event;
use yaml_rust2::YamlLoader;
use yaml_rust2::yaml::Yaml;
use regex::Regex;
use lazy_static::lazy_static;
use cow_utils::CowUtils;
use git_version::git_version;
use glob_match::glob_match;

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
  hush_version: bool,
  hush_info: bool,
  hush_notice: bool,
  hush_warning: bool,
  show_progress: bool
}

#[derive(Copy, Clone, PartialEq, Debug)]
enum Cardinality {
  Default,
  OneToMany,
  ManyToOne,
  ManyToMany,
  None
}

struct Table<'a> {
  name: String,
  path: String,
  buf: RefCell<String>,
  writer_channel: mpsc::SyncSender<String>,
  writer_thread: Option<thread::JoinHandle<()>>,
  columns: Vec<Column<'a>>,
  lastid: RefCell<String>,
  domain: Box<Option<RefCell<Domain<'a>>>>,
  cardinality: Cardinality,
  emit_copyfrom: bool,
  emit_starttransaction: bool
}
impl<'a> Table<'a> {
  fn new(name: &str, path: &str, file: Option<&str>, settings: &Settings, cardinality: Cardinality) -> Table<'a> {
    //println!("Table {} path {} file {:?} cardinality {:?}", name, path, file, cardinality);
    let out: RefCell<Box<dyn Write + Send>> = match file {
      None => RefCell::new(Box::new(stdout())),
      Some(ref file) => RefCell::new(Box::new(
        match settings.filemode.as_ref() {
          "truncate" => File::create(Path::new(file)).unwrap_or_else(|err| fatalerr!("Error: failed to create output file '{}': {}", file, err)),
          "append" => OpenOptions::new().append(true).create(true).open(Path::new(file)).unwrap_or_else(|err| fatalerr!("Error: failed to open output file '{}': {}", file, err)),
          mode => fatalerr!("Error: invalid 'mode' setting in configuration file: {}", mode)
        }
      ))
    };
    let (writer_channel, rx) = mpsc::sync_channel(100);
    let writer_thread = thread::Builder::new().name(format!("write {}", name)).spawn(move || write_output(out, rx)).unwrap_or_else(|err| fatalerr!("Error: failed to create writer thread: {}", err));
    let mut ownpath = String::from(path);
    if !ownpath.is_empty() && !ownpath.starts_with('/') { ownpath.insert(0, '/'); }
    if ownpath.ends_with('/') { ownpath.pop(); }
    Table {
      name: name.to_owned(),
      path: ownpath,
      buf: RefCell::new(String::new()),
      writer_channel,
      writer_thread: Some(writer_thread),
      columns: Vec::new(),
      lastid: RefCell::new(String::new()),
      domain: Box::new(None),
      cardinality,
      emit_copyfrom: if cardinality != Cardinality::None { settings.emit_copyfrom } else { false },
      emit_starttransaction: if cardinality != Cardinality::None { settings.emit_starttransaction } else { false }
    }
  }
  fn flush(&self) {
    if self.buf.borrow().len() > 0 { self.writer_channel.send(std::mem::take(&mut self.buf.borrow_mut())).unwrap(); }
  }
  fn clear_columns(&self) {
    for col in &self.columns {
      col.value.borrow_mut().clear();
    }
  }
}
impl<'a> Drop for Table<'a> {
  fn drop(&mut self) {
    if self.emit_copyfrom { write!(self.buf.borrow_mut(), "\\.\n").unwrap(); }
    if self.emit_starttransaction { write!(self.buf.borrow_mut(), "COMMIT;\n").unwrap(); }
    self.flush();
    self.writer_channel.send(String::new()).unwrap(); // Terminates the writer thread
    let thread = std::mem::take(&mut self.writer_thread);
    thread.unwrap().join().unwrap_or_else(|_| eprintln!("Table writer thread for [{}] crashed", self.name));
  }
}

struct Domain<'a> {
  lastid: u32,
  map: HashMap<String, u32>,
  table: Table<'a>
}
impl<'a> Domain<'a> {
  fn new(tabname: &str, filename: Option<&str>, settings: &Settings) -> Domain<'a> {
    Domain {
      lastid: 0,
      map: HashMap::new(),
      table: Table::new(tabname, "_domain_", filename, settings, match filename { Some(_) => Cardinality::ManyToOne, None => Cardinality::None })
    }
  }
}

#[derive(Default)]
struct Column<'a> {
  name: String,
  path: String,
  serial: Option<Cell<u64>>,
  datatype: String,
  value: RefCell<String>,
  attr: Option<&'a str>,
  hide: bool,
  include: Option<Regex>,
  exclude: Option<Regex>,
  find: Option<Regex>,
  replace: Option<&'a str>,
  trim: bool,
  convert: Option<&'a str>,
  aggr: Option<&'a str>,
  subtable: Option<Table<'a>>,
  domain: Option<RefCell<Domain<'a>>>,
  bbox: Option<BBox>,
  multitype: bool,
  used: RefCell<bool>
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

#[derive(PartialEq, Debug)]
enum Step {
  Next,
  Repeat,
  Defer,
  Apply,
  Done
}
struct State<'a, 'b> {
  settings: Settings,
  reader: Reader<Box<dyn BufRead>>,
  tables: Vec<&'b Table<'a>>,
  table: &'b Table<'a>,
  rowpath: String,
  path: String,
  parentcol: Option<&'b Column<'a>>,
  deferred: Option<String>,
  filtered: bool,
  skipped: bool,
  fullcount: u64,
  filtercount: u64,
  skipcount: u64,
  concattext: bool,
  xmltotext: bool,
  text: String,
  gmltoewkb: bool,
  gmlpos: bool,
  gmlcoll: Vec<Geometry>,
  trimre: Regex,
  step: Step
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

  static CHARS: &[u8] = b"0123456789ABCDEF";
  let mut value = cell.borrow_mut();
  value.reserve(ewkb.len()*2);
  for byte in ewkb.iter() {
    value.push(CHARS[(byte >>  4) as usize].into());
    value.push(CHARS[(byte & 0xf) as usize].into());
  }
  true
}

fn add_table<'a>(name: &str, rowpath: &str, outfile: Option<&str>, settings: &Settings, colspec: &'a [Yaml], cardinality: Cardinality) -> Table<'a> {
  let mut table = Table::new(name, rowpath, outfile, settings, cardinality);
  for col in colspec {
    let colname = col["name"].as_str().unwrap_or_else(|| fatalerr!("Error: column has no 'name' entry in configuration file"));
    let colpath = match col["seri"].as_bool() {
      Some(true) => "/",
      _ => col["path"].as_str().unwrap_or_else(|| fatalerr!("Error: table '{}' column '{}' has no 'path' entry in configuration file", name, colname))
    };
    let mut path = String::from(&table.path);
    if !colpath.is_empty() && !colpath.starts_with('/') { path.push('/'); }
    path.push_str(colpath);
    if path.ends_with('/') { path.pop(); }
    let serial = match col["seri"].as_bool() {
      Some(true) => {
        if *col != colspec[0] && !settings.hush_warning { eprintln!("Warning: a 'seri' column usually needs to be the first column; {} in table {} is not", colname, table.name); }
        Some(Cell::new(0))
      },
      _ => None
    };
    let mut datatype = col["type"].as_str().unwrap_or("text").to_string();
    let mut include: Option<Regex> = col["incl"].as_str().map(|str| Regex::new(str).unwrap_or_else(|err| fatalerr!("Error: invalid regex in 'incl' entry in configuration file: {}", err)));
    let mut exclude: Option<Regex> = col["excl"].as_str().map(|str| Regex::new(str).unwrap_or_else(|err| fatalerr!("Error: invalid regex in 'excl' entry in configuration file: {}", err)));
    let norm = col["norm"].as_str();
    let file = col["file"].as_str();
    let cardinality = match (file, norm) { // The combination of 'file' and 'norm' options determine relation to the subtable (if any)
      (None, None) => Cardinality::Default,
      (Some(_), None) => Cardinality::OneToMany,
      (None, Some(_)) => Cardinality::ManyToOne,
      (Some(_), Some(_)) => Cardinality::ManyToMany
    };
    let mut subtable: Option<Table> = match col["cols"].is_badvalue() {
      true => match cardinality { // No 'cols' setting; the current column is the only one in the subtable
        Cardinality::OneToMany => {
          let filename = col["file"].as_str().unwrap();
          if table.columns.is_empty() { fatalerr!("Error: table '{}' cannot have a subtable as first column", name); }
          let mut subtable = add_table(colname, &path, Some(filename), settings, &[], cardinality);
          subtable.columns.push(Column { name: colname.to_string(), path: path.clone(), datatype: datatype.to_string(), include: mem::take(&mut include), exclude: mem::take(&mut exclude), ..Default::default() });
          emit_preamble(&subtable, settings, Some(format!("{} {}", name, table.columns[0].datatype)));
          Some(subtable)
        },
        Cardinality::ManyToMany => {
          let filename = col["file"].as_str().unwrap();
          if table.columns.is_empty() { fatalerr!("Error: table '{}' cannot have a subtable as first column", name); }
          let mut subtable = add_table(colname, &path, Some(filename), settings, &[], cardinality);
//          subtable.columns.push(Column { name: String::from("id"), path: String::new(), datatype: String::from("integer"), ..Default::default() });
          subtable.columns.push(Column { name: colname.to_string(), path: path.clone(), datatype: "integer".to_string(), include: mem::take(&mut include), exclude: mem::take(&mut exclude), ..Default::default() });
          emit_preamble(&subtable, settings, Some(format!("{} {}", name, table.columns[0].datatype)));
          Some(subtable)
        },
        _ => None
      },
      false => match cardinality {
        Cardinality::ManyToOne => { // Many-to-one relation (subtable with fkey in parent table)
          let subtable = add_table(colname, &path, norm, settings, col["cols"].as_vec().unwrap_or_else(|| fatalerr!("Error: subtable 'cols' entry is not an array")), cardinality);
          emit_preamble(&subtable, settings, None);
          Some(subtable)
        },
        Cardinality::ManyToMany => { // Many-to-many relation (this file will contain the crosslink table)
          let filename = col["file"].as_str().unwrap_or_else(|| fatalerr!("Error: subtable {} has no 'file' entry", colname));
          if table.columns.is_empty() { fatalerr!("Error: table '{}' cannot have a subtable as first column", name); }
          let subtable = add_table(colname, &path, Some(filename), settings, col["cols"].as_vec().unwrap_or_else(|| fatalerr!("Error: subtable 'cols' entry is not an array")), cardinality);
          emit_preamble(&subtable, settings, Some(format!("{} {}", name, table.columns[0].datatype)));
          Some(subtable)
        },
        _ => { // One-to-many relation (this file will contain the subtable with the parent table fkey)
          let filename = col["file"].as_str().unwrap_or_else(|| fatalerr!("Error: subtable {} has no 'file' entry", colname));
          if table.columns.is_empty() { fatalerr!("Error: table '{}' cannot have a subtable as first column", name); }
          let subtable = add_table(colname, &path, Some(filename), settings, col["cols"].as_vec().unwrap_or_else(|| fatalerr!("Error: subtable 'cols' entry is not an array")), cardinality);
          emit_preamble(&subtable, settings, Some(format!("{} {}", name, table.columns[0].datatype)));
          Some(subtable)
        }
      }
    };
    let hide = col["hide"].as_bool().unwrap_or(false);
    let trim = col["trim"].as_bool().unwrap_or(false);
    let attr = col["attr"].as_str();
    let convert = col["conv"].as_str();
    let find = col["find"].as_str().map(|str| Regex::new(str).unwrap_or_else(|err| fatalerr!("Error: invalid regex in 'find' entry in configuration file: {}", err)));
    let replace = col["repl"].as_str();
    let aggr = col["aggr"].as_str();
    let domain = match norm {
      Some(filename) => {
        if filename == "true" { fatalerr!("Error: 'norm' option now takes a file path instead of a boolean"); }
        let file = match subtable {
          Some(_) if col["file"].is_badvalue() => None, // Many-to-one relation (subtable with fkey in parent table); rows go into subtable file
          Some(_) => Some(filename),                    // Many-to-many relation (subtable with crosslink table); rows go into this file
          None => Some(filename)                        // Many-to-one relation (single column) with auto serial; rows go into this file
        };
        let mut domain = Domain::new(colname, file, settings);
        if file.is_some() {
          if subtable.is_some() && !col["cols"].is_badvalue() {
            for col in col["cols"].as_vec().unwrap() {
              let colname = col["name"].as_str().unwrap_or_else(|| fatalerr!("Error: column has no 'name' entry in configuration file"));
              let datatype = col["type"].as_str().unwrap_or("text");
              domain.table.columns.push(Column { name: colname.to_string(), path: String::new(), datatype: datatype.to_string(), ..Default::default() });
            }
          }
          else {
            domain.table.columns.push(Column { name: String::from("id"), path: String::new(), datatype: String::from("integer"), ..Default::default() });
            domain.table.columns.push(Column { name: colname.to_string(), path: String::new(), datatype, ..Default::default() });
          }
          emit_preamble(&domain.table, settings, None);
        }
        datatype = String::from("integer");
        if let Some(ref mut table) = subtable { // Push the domain down to the subtable
          table.domain = Box::new(Some(RefCell::new(domain)));
          None
        }
        else { Some(RefCell::new(domain)) }
      },
      None => None
    };
    let bbox = col["bbox"].as_str().and_then(BBox::from);
    let multitype = col["mult"].as_bool().unwrap_or(false);

    if let Some(val) = convert {
      if !vec!("xml-to-text", "gml-to-ewkb", "concat-text").contains(&val) {
        fatalerr!("Error: table '{}' option 'conv' contains invalid value: {}", name, val);
      }
      if val == "gml-to-ewkb" && !settings.hush_notice {
        eprintln!("Notice: gml-to-ewkb conversion is experimental and in no way complete or standards compliant; use at your own risk");
      }
      if col["type"].is_badvalue() { // Set datatype unless overridden
        if val == "gml-to-ewkb" { datatype = String::from("geometry"); }
      }
    }
    if let Some(val) = aggr {
      if !vec!("first", "last", "append").contains(&val) {
        fatalerr!("Error: table '{}' option 'aggr' contains invalid value: {}", name, val);
      }
    }
    if include.is_some() || exclude.is_some() {
      if convert.is_some() {
        fatalerr!("Error: filtering (incl/excl) and 'conv' cannot be used together on a single column");
      }
      if find.is_some() && !settings.hush_notice {
        eprintln!("Notice: when using filtering (incl/excl) and find/replace on a single column, the filter is checked after replacements");
      }
      if aggr.is_some() && !settings.hush_notice {
        eprintln!("Notice: when using filtering (incl/excl) and aggregation on a single column, the filter is checked after aggregation");
      }
    }
    if bbox.is_some() && (convert.is_none() || convert.unwrap() != "gml-to-ewkb") && !settings.hush_warning {
      eprintln!("Warning: the bbox option has no function without conversion type 'gml-to-ekwb'");
    }

    let column = Column { name: colname.to_string(), path, serial, datatype, attr, hide, include, exclude, trim, convert, find, replace, aggr, subtable, domain, bbox, multitype, ..Default::default() };
    table.columns.push(column);
  }

  table
}
fn emit_preamble(table: &Table, settings: &Settings, fkey: Option<String>) {
  if settings.emit_starttransaction {
    write!(table.buf.borrow_mut(), "START TRANSACTION;\n").unwrap();
  }
  if settings.emit_droptable {
    write!(table.buf.borrow_mut(), "DROP TABLE IF EXISTS {};\n", table.name).unwrap();
  }
  if settings.emit_createtable {
    if table.cardinality == Cardinality::ManyToMany {
      let fkey = fkey.as_ref().unwrap();
      write!(table.buf.borrow_mut(), "CREATE TABLE IF NOT EXISTS {}_{} ({}, {} {});\n", fkey.split_once(' ').unwrap().0, table.name, fkey, table.name, if table.columns.is_empty() { "integer" } else { &table.columns[0].datatype }).unwrap();
    }
    else {
      let mut cols = table.columns.iter().filter_map(|c| {
        if c.hide || (c.subtable.is_some() && c.subtable.as_ref().unwrap().cardinality != Cardinality::ManyToOne) { return None; }
        let mut spec = String::from(&c.name);
        spec.push(' ');
        spec.push_str(&c.datatype);
        Some(spec)
      }).collect::<Vec<String>>().join(", ");
      if fkey.is_some() { cols.insert_str(0, &format!("{}, ", fkey.as_ref().unwrap())); }
      write!(table.buf.borrow_mut(), "CREATE TABLE IF NOT EXISTS {} ({});\n", table.name, cols).unwrap();
    }
  }
  if settings.emit_truncate {
    write!(table.buf.borrow_mut(), "TRUNCATE {};\n", table.name).unwrap();
  }
  if settings.emit_copyfrom {
    if table.cardinality == Cardinality::ManyToMany {
      let parent = fkey.as_ref().unwrap().split_once(' ').unwrap().0;
      write!(table.buf.borrow_mut(), "COPY {}_{} ({}, {}) FROM stdin;\n", parent, table.name, parent, table.name).unwrap();
    }
    else {
      let cols = table.columns.iter().filter_map(|c| {
        if c.hide || (c.subtable.is_some() && c.subtable.as_ref().unwrap().cardinality != Cardinality::ManyToOne) { return None; }
        Some(String::from(&c.name))
      }).collect::<Vec<String>>().join(", ");
      if fkey.is_some() {
        write!(table.buf.borrow_mut(), "COPY {} ({}, {}) FROM stdin;\n", table.name, fkey.unwrap().split(' ').next().unwrap(), cols).unwrap();
      }
      else { write!(table.buf.borrow_mut(), "COPY {} ({}) FROM stdin;\n", table.name, cols).unwrap(); }
    }
  }
  table.flush();
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
  else {
    eprintln!("xml-to-postgres {}", git_version!(args = ["--always", "--tags", "--dirty=-modified"]));
    fatalerr!("Usage: {} <configfile> [xmlfile]", args[0]);
  }

  let config = {
    let mut config_str = String::new();
    let mut file = File::open(&args[1]).unwrap_or_else(|err| fatalerr!("Error: failed to open configuration file '{}': {}", args[1], err));
    file.read_to_string(&mut config_str).unwrap_or_else(|err| fatalerr!("Error: failed to read configuration file '{}': {}", args[1], err));
    &YamlLoader::load_from_str(&config_str).unwrap_or_else(|err| fatalerr!("Error: invalid syntax in configuration file: {}", err))[0]
  };

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
    hush_version: hush.contains("version"),
    hush_info: hush.contains("info"),
    hush_notice: hush.contains("notice"),
    hush_warning: hush.contains("warn"),
    show_progress: config["prog"].as_bool().unwrap_or_else(|| std::io::stdout().is_terminal())
  };

  let maintable = add_table(name, rowpath, outfile, &settings, colspec, Cardinality::Default);
  emit_preamble(&maintable, &settings, None);
  if !settings.skip.is_empty() {
    if !settings.skip.starts_with('/') { settings.skip.insert(0, '/'); }
    settings.skip.insert_str(0, &maintable.path); // Maintable path is normalized in add_table()
  }

  let mut reader;
  reader = Reader::from_reader(bufread);
  reader.config_mut().trim_text(true);
  reader.config_mut().expand_empty_elements = true;
  let mut state = State {
    settings,
    reader,
    tables: Vec::new(),
    table: &maintable,
    rowpath: rowpath.to_string(),
    path: String::new(),
    parentcol: None,
    deferred: None,
    filtered: false,
    skipped: false,
    fullcount: 0,
    filtercount: 0,
    skipcount: 0,
    concattext: false,
    xmltotext: false,
    text: String::new(),
    gmltoewkb: false,
    gmlpos: false,
    gmlcoll: vec![],
    step: Step::Next,
    trimre: Regex::new("[ \n\r\t]*\n[ \n\r\t]*").unwrap()
  };

  let mut buf = Vec::new();
  let mut deferred = Vec::new();
  let start = Instant::now();
  'main: loop { // Main loop over the XML nodes
    let event = state.reader.read_event_into(&mut buf).unwrap_or_else(|e| fatalerr!("Error: failed to parse XML at position {}: {}", state.reader.buffer_position(), e));
    loop { // Repeat loop to be able to process a node twice
      state.step = process_event(&event, &mut state);
      match state.step {
        Step::Next => break,
        Step::Repeat => {
            // if !deferred.is_empty() { deferred.clear(); }
            continue
        },
        Step::Defer => {
          // println!("Defer {:?}", event);
          deferred.push(event.into_owned());
          break;
        },
        Step::Apply => {
          if state.table.lastid.borrow().is_empty() {
            fatalerr!("Subtable defer failed to yield a key for parent table");
          }
          // println!("Applying {} deferred events", deferred.len());
          state.step = Step::Repeat;
          state.path = state.deferred.unwrap();
          state.deferred = None;
          deferred.reverse();
          let mut event = deferred.pop().expect("deferred array should never be empty at this stage");
          loop {
            // println!("Event: {:?}", event);
            state.step = process_event(&event, &mut state);
            match state.step {
              Step::Repeat => continue,
              Step::Defer => fatalerr!("Error: you have nested subtables that need non-linear processing; this is not currently supported"),
              Step::Done => break 'main,
              _ => ()
            }
            let result = deferred.pop();
            if result.is_none() { break; }
            event = result.unwrap();
          }
          state.path.clear();
          let i = state.table.path.rfind('/').unwrap();
          state.path.push_str(&state.table.path[0..i]);
          break;
        },
        Step::Done => break 'main
      }
    }
    buf.clear();
  }
  if !state.settings.hush_warning { check_columns_used(&maintable); }
  if !state.settings.hush_info {
    let elapsed = start.elapsed().as_secs_f32();
    eprintln!("{}Info: [{}] {} rows processed in {:.*} seconds{}{}",
      match state.settings.show_progress { true => "\r", false => "" },
      maintable.name,
      state.fullcount-state.filtercount-state.skipcount,
      if elapsed > 9.9 { 0 } else if elapsed > 0.99 { 1 } else if elapsed > 0.099 { 2 } else { 3 },
      elapsed,
      match state.filtercount { 0 => "".to_owned(), n => format!(" ({} excluded)", n) },
      match state.skipcount { 0 => "".to_owned(), n => format!(" ({} skipped)", n) }
    );
  }
}

fn check_columns_used(table: &Table) {
  for col in &table.columns {
    if col.subtable.is_some() {
      let sub = col.subtable.as_ref().unwrap();
      check_columns_used(sub);
    }
    else if !*col.used.borrow() {
      eprintln!("Warning: table {} column {} was never found", table.name, col.name);
    }
  }
}

fn process_event(event: &Event, state: &mut State) -> Step {
  let table = &state.table;
  match event {
    Event::Decl(ref e) => {
      if !state.settings.hush_version && !state.settings.hush_info {
        eprintln!("Info: reading XML version {} with encoding {}",
          str::from_utf8(&e.version().unwrap_or_else(|_| fatalerr!("Error: missing or invalid XML version attribute: {:#?}", e.as_ref()))).unwrap(),
          str::from_utf8(match e.encoding() {
            Some(Ok(Cow::Borrowed(encoding))) => encoding,
            _ => b"unknown"
          }).unwrap()
        );
      }
    },
    Event::Start(ref e) => {
      if state.step != Step::Repeat {
        state.path.push('/');
        state.path.push_str(&state.reader.decoder().decode(e.name().as_ref()).unwrap_or_else(|err| fatalerr!("Error: failed to decode XML tag '{}': {}", String::from_utf8_lossy(e.name().as_ref()), err)));
      }
      if let Some(path) = &state.deferred {
        if state.path.starts_with(path) { return Step::Defer; }
      }
      if state.filtered || state.skipped { return Step::Next; }
      if !state.tables.is_empty() && path_match(&state.path, &table.path) { // Start of a subtable
        if table.cardinality != Cardinality::ManyToOne { // Subtable needs a foreign key from parent
          if state.tables.last().unwrap().lastid.borrow().is_empty() {
            if state.deferred.is_some() { fatalerr!("Error: you have multiple subtables that precede the parent table id column; this is not currently supported"); }
            // println!("Defer subtable {}", table.name);
            state.deferred = Some(state.path.clone());
            return Step::Defer;
          }
        }
      }
      if path_match(&state.path, &state.settings.skip) {
        state.skipped = true;
        return Step::Next;
      }
      else if state.concattext {
        return Step::Next;
      }
      else if state.xmltotext {
        state.text.push_str(&format!("<{}>", state.reader.decoder().decode(e.name().as_ref()).unwrap_or_else(|err| fatalerr!("Error: failed to decode XML tag '{}': {}", String::from_utf8_lossy(e.name().as_ref()), err))));
        return Step::Next;
      }
      else if state.gmltoewkb {
        match state.reader.decoder().decode(e.name().as_ref()) {
          Err(_) => (),
          Ok(tag) => match tag.as_ref() {
            "gml:Point" => {
              state.gmlcoll.push(Geometry::new(1));
              state.gmlcoll.last_mut().unwrap().rings.push(Vec::new());
            },
            "gml:LineString" => {
              state.gmlcoll.push(Geometry::new(2));
              state.gmlcoll.last_mut().unwrap().rings.push(Vec::new());
            },
            "gml:Polygon" => state.gmlcoll.push(Geometry::new(3)),
            "gml:MultiPolygon" => (),
            "gml:polygonMember" => (),
            "gml:exterior" => (),
            "gml:interior" => (),
            "gml:LinearRing" => state.gmlcoll.last_mut().unwrap().rings.push(Vec::new()),
            "gml:posList" => state.gmlpos = true,
            "gml:pos" => state.gmlpos = true,
            _ => if !state.settings.hush_warning { eprintln!("Warning: GML type {} not supported", tag); }
          }
        }
        for res in e.attributes() {
          match res {
            Err(_) => (),
            Ok(attr) => {
              let key = state.reader.decoder().decode(attr.key.as_ref());
              if key.is_err() { continue; }
              match key.unwrap().as_ref() {
                "srsName" => {
                  let mut value = String::from(state.reader.decoder().decode(&attr.value).unwrap_or_else(|err| fatalerr!("Error: failed to decode XML attribute '{}': {}", String::from_utf8_lossy(&attr.value), err)));
                  if let Some(i) = value.rfind("::") {
                    value = value.split_off(i+2);
                  }
                  match value.parse::<u32>() {
                    Ok(int) => {
                      if let Some(geom) = state.gmlcoll.last_mut() { geom.srid = int };
                    },
                    Err(_) => if !state.settings.hush_warning { eprintln!("Warning: invalid srsName {} in GML", value); }
                  }
                },
                "srsDimension" => {
                  let value = state.reader.decoder().decode(&attr.value).unwrap_or_else(|err| fatalerr!("Error: failed to decode XML attribute '{}': {}", String::from_utf8_lossy(&attr.value), err));
                  match value.parse::<u8>() {
                    Ok(int) => {
                      if let Some(geom) = state.gmlcoll.last_mut() { geom.dims = int };
                    },
                    Err(_) => if !state.settings.hush_warning { eprintln!("Warning: invalid srsDimension {} in GML", value); }
                  }
                }
                _ => ()
              }
            }
          }
        }
        return Step::Next;
      }
      else if state.path.len() >= table.path.len() { // This optimization may need to go to properly support globbing everywhere
        if path_match(&state.path, &table.path) { state.table.lastid.borrow_mut().clear(); }
        if path_match(&state.path, &state.rowpath) {
          state.fullcount += 1;
          if state.settings.show_progress && !state.settings.hush_info && state.fullcount%100000 == 0 {
            eprint!("\rInfo: [{}] {} rows processed{}{}",
              table.name,
              state.fullcount-state.filtercount-state.skipcount,
              match state.filtercount { 0 => "".to_owned(), n => format!(" ({} excluded)", n) },
              match state.skipcount { 0 => "".to_owned(), n => format!(" ({} skipped)", n) }
            );
          }
        }
        let mut subtable = None;

        for i in 0..table.columns.len() {
          if path_match(&state.path, &table.columns[i].path) { // This start tag matches one of the defined columns
            // Handle the 'seri' case where this column is a virtual auto-incrementing serial
            if let Some(ref serial) = table.columns[i].serial {
              // if table.cardinality == Cardinality::ManyToOne { continue; }
              if table.columns[i].value.borrow().is_empty() {
                let id = serial.get()+1;
                let idstr = id.to_string();
                table.columns[i].value.borrow_mut().push_str(&idstr);
                table.lastid.borrow_mut().push_str(&idstr);
                serial.set(id);
                continue;
              }
            }
            // Handle 'subtable' case (the 'cols' entry has 'cols' of its own)
            if table.columns[i].subtable.is_some() {
                if subtable.is_some() { fatalerr!("Error: multiple subtables starting from the same element is not supported"); }
                subtable = Some(i);
            }
            // Handle the 'attr' case where the content is read from an attribute of this tag
            if let Some(request) = table.columns[i].attr {
              for res in e.attributes() {
                if let Ok(attr) = res {
                  if let Ok(key) = state.reader.decoder().decode(attr.key.as_ref()) {
                    if key == request {
                      if let Ok(value) = state.reader.decoder().decode(&attr.value) {
                        if !table.columns[i].value.borrow().is_empty() {
                          if !allow_iteration(&table.columns[i], &state.settings) { break; }
                          if let Some("last") = table.columns[i].aggr { table.columns[i].value.borrow_mut().clear(); }
                        }
                        if i == 0 { table.lastid.borrow_mut().push_str(&value); }
                        if let (Some(regex), Some(replacer)) = (table.columns[i].find.as_ref(), table.columns[i].replace) {
                          table.columns[i].value.borrow_mut().push_str(&regex.replace_all(&value, replacer));
                        }
                        else { table.columns[i].value.borrow_mut().push_str(&value); }
                      }
                      else if !state.settings.hush_warning { eprintln!("Warning: failed to decode attribute {} for column {}", request, table.columns[i].name); }
                    }
                  }
                  else if !state.settings.hush_warning { eprintln!("Warning: failed to decode an attribute for column {}", table.columns[i].name); }
                }
                else if !state.settings.hush_warning { eprintln!("Warning: failed to read attributes for column {}", table.columns[i].name); }
              }
              if table.columns[i].value.borrow().is_empty() && !state.settings.hush_warning {
                eprintln!("Warning: column {} requested attribute {} not found", table.columns[i].name, request);
              }
              continue;
            }
            // Set the appropriate convert flag for the following data in case the 'conv' option is present
            match table.columns[i].convert {
              None => (),
              Some("xml-to-text") => state.xmltotext = true,
              Some("gml-to-ewkb") => state.gmltoewkb = true,
              Some("concat-text") => state.concattext = true,
              Some(_) => (),
            }
          }
        }
        if let Some(i) = subtable {
            state.tables.push(table);
            state.parentcol = Some(&table.columns[i]);
            state.table = table.columns[i].subtable.as_ref().unwrap();
            return Step::Repeat; // Continue the repeat loop because a subtable column may also match the current path
        }
      }
    },
    Event::Text(ref e) => {
      if let Some(path) = &state.deferred {
        if state.path.starts_with(path) { return Step::Defer; }
      }
      if state.filtered || state.skipped { return Step::Next; }
      if state.concattext {
        if !state.text.is_empty() { state.text.push(' '); }
        state.text.push_str(&e.unescape().unwrap_or_else(|err| fatalerr!("Error: failed to decode XML text node '{}': {}", String::from_utf8_lossy(e), err)));
        return Step::Next;
      }
      else if state.xmltotext {
        state.text.push_str(&e.unescape().unwrap_or_else(|err| fatalerr!("Error: failed to decode XML text node '{}': {}", String::from_utf8_lossy(e), err)));
        return Step::Next;
      }
      else if state.gmltoewkb {
        if state.gmlpos {
          let value = String::from(e.unescape().unwrap_or_else(|err| fatalerr!("Error: failed to decode XML gmlpos '{}': {}", String::from_utf8_lossy(e), err)));
          for pos in value.split(' ') {
            state.gmlcoll.last_mut().unwrap().rings.last_mut().unwrap().push(pos.parse::<f64>().unwrap_or_else(|err| fatalerr!("Error: failed to parse GML pos '{}' into float: {}", pos, err)));
          }
        }
        return Step::Next;
      }
      for i in 0..table.columns.len() {
        if path_match(&state.path, &table.columns[i].path) {
          if table.columns[i].attr.is_some() || table.columns[i].serial.is_some() { continue; }
          if !table.columns[i].value.borrow().is_empty() {
            if !allow_iteration(&table.columns[i], &state.settings) { return Step::Next; }
            if let Some("last") = table.columns[i].aggr { table.columns[i].value.borrow_mut().clear(); }
          }
          let decoded = e.unescape().unwrap_or_else(|err| fatalerr!("Error: failed to decode XML text node '{}': {}", String::from_utf8_lossy(e), err));
          if table.columns[i].trim {
            let trimmed = state.trimre.replace_all(&decoded, " ");
            table.columns[i].value.borrow_mut().push_str(&trimmed.cow_replace("\\", "\\\\").cow_replace("\t", "\\t"));
          }
          else {
            table.columns[i].value.borrow_mut().push_str(&decoded.cow_replace("\\", "\\\\").cow_replace("\r", "\\r").cow_replace("\n", "\\n").cow_replace("\t", "\\t"));
          }
          if let (Some(regex), Some(replacer)) = (table.columns[i].find.as_ref(), table.columns[i].replace) {
            let mut value = table.columns[i].value.borrow_mut();
            *value = regex.replace_all(&value, replacer).to_string();
          }
          // println!("Table {} column {} value {}", table.name, table.columns[i].name, &table.columns[i].value.borrow());
          if i == 0 {
              table.lastid.borrow_mut().push_str(&table.columns[0].value.borrow());
          }
          return Step::Next;
        }
      }
    },
    Event::End(_) => {
      if let Some(path) = &state.deferred {
        if state.path.starts_with(path) {
          if path_match(&state.path, &table.path) && !state.tables.is_empty() {
            state.table = state.tables.pop().unwrap();
          }
          let i = state.path.rfind('/').unwrap();
          state.path.truncate(i);
          return Step::Defer;
        }
      }

      if state.concattext {
        for i in 0..table.columns.len() {
          if path_match(&state.path, &table.columns[i].path) {
            state.concattext = false;
            table.columns[i].value.borrow_mut().push_str(&state.text);
            state.text.clear();
          }
        }
      }

      if path_match(&state.path, &table.path) { // This is an end tag of the row path
        for i in 0..table.columns.len() {
          if !*table.columns[i].used.borrow() && !table.columns[i].value.borrow().is_empty() {
              *state.table.columns[i].used.borrow_mut() = true;
          }
          if let Some(re) = &table.columns[i].include {
            if !re.is_match(&table.columns[i].value.borrow()) {
              state.filtered = true;
            }
          }
          if let Some(re) = &table.columns[i].exclude {
            if re.is_match(&table.columns[i].value.borrow()) {
              state.filtered = true;
            }
          }
        }
        if state.filtered {
          state.filtered = false;
          table.clear_columns();
          if state.tables.is_empty() { state.filtercount += 1; } // Only count filtered for the main table
          else { // Subtable; nothing more to do in this case
            state.table = state.tables.pop().unwrap();
            return Step::Repeat;
          }
        }
        else {
          if !state.tables.is_empty() { // This is a subtable
            if table.cardinality != Cardinality::ManyToOne { // Write the first column value of the parent table as the first column of the subtable (for use as a foreign key)
              let key = state.tables.last().unwrap().lastid.borrow();
              if key.is_empty() && !state.settings.hush_warning { println!("Warning: subtable {} has no foreign key for parent (you may need to add a 'seri' column)", table.name); }
              write!(table.buf.borrow_mut(), "{}\t", key).unwrap();
              let rowid;
              if let Some(domain) = table.domain.as_ref() {
                let mut domain = domain.borrow_mut();
                let key = match table.columns[0].serial {
                    Some(_) => table.columns[1..].iter().map(|c| c.value.borrow().to_string()).collect::<String>(),
                    None => table.lastid.borrow().to_string()
                };
                if !domain.map.contains_key(&key) {
                  domain.lastid += 1;
                  rowid = domain.lastid;
                  domain.map.insert(key, rowid);
                  if table.columns.len() == 1 {
                    write!(domain.table.buf.borrow_mut(), "{}\t", rowid).unwrap();
                  }
                  for i in 0..table.columns.len() {
                    if table.columns[i].subtable.is_some() { continue; }
                    if table.columns[i].hide { continue; }
                    if i > 0 { write!(domain.table.buf.borrow_mut(), "\t").unwrap(); }
                    if table.columns[i].value.borrow().is_empty() { write!(domain.table.buf.borrow_mut(), "\\N").unwrap(); }
                    else if let Some(domain) = table.columns[i].domain.as_ref() {
                      let mut domain = domain.borrow_mut();
                      let id = match domain.map.get(&table.columns[i].value.borrow().to_string()) {
                        Some(id) => *id,
                        None => {
                          domain.lastid += 1;
                          let id = domain.lastid;
                          domain.map.insert(table.columns[i].value.borrow().to_string(), id);
                          write!(domain.table.buf.borrow_mut(), "{}\t{}\n", id, *table.columns[i].value.borrow()).unwrap();
                          domain.table.flush();
                          id
                        }
                      };
                      write!(domain.table.buf.borrow_mut(), "{}", id).unwrap();
                    }
                    else {
                      write!(domain.table.buf.borrow_mut(), "{}", &table.columns[i].value.borrow()).unwrap();
                    }
                  }
                  write!(domain.table.buf.borrow_mut(), "\n").unwrap();
                  domain.table.flush();
                }
                else { rowid = *domain.map.get(&key).unwrap(); }
                if table.columns.len() == 1 { // Single column many-to-many subtable; needs the id from the domain map
                  write!(table.buf.borrow_mut(), "{}" , rowid).unwrap();
                }
                else {
                  if table.lastid.borrow().is_empty() && !state.settings.hush_warning { println!("Warning: subtable {} has no primary key to normalize on", table.name); }
                  write!(table.buf.borrow_mut(), "{}" , table.lastid.borrow()).unwrap(); // This is a many-to-many relation; write the two keys into the link table
                }
                write!(table.buf.borrow_mut(), "\n").unwrap();
                table.flush();
                table.clear_columns();
                state.table = state.tables.pop().unwrap();
                return Step::Repeat;
              }
            }
            else { // Many-to-one relation; write the id of this subtable into the parent table
              if let Some(domain) = table.domain.as_ref() {
                let mut domain = domain.borrow_mut();
                let key = match table.columns[0].serial {
                    Some(_) => table.columns[1..].iter().map(|c| c.value.borrow().to_string()).collect::<String>(),
                    None => table.lastid.borrow().to_string()
                };
                if domain.map.contains_key(&key) {
                  if table.columns[0].serial.is_some() {
                    state.parentcol.unwrap().value.borrow_mut().push_str(&format!("{}", *domain.map.get(&key).unwrap()));
                  }
                  else { state.parentcol.unwrap().value.borrow_mut().push_str(&table.lastid.borrow()); }
                  table.clear_columns();
                  state.table = state.tables.pop().unwrap();
                  return Step::Repeat;
                }
                domain.lastid += 1;
                let id = domain.lastid;
                domain.map.insert(key, id);
                // The for loop below will now write out the new row
              }
              if state.parentcol.unwrap().value.borrow().is_empty() {
                state.parentcol.unwrap().value.borrow_mut().push_str(&table.lastid.borrow());
              }
              else if allow_iteration(state.parentcol.unwrap(), &state.settings) {
                // TODO: make it do something...
              }
            }
          }
          // Now write out the other column values
          for i in 0..table.columns.len() {
            if table.columns[i].subtable.is_some() && table.columns[i].subtable.as_ref().unwrap().cardinality != Cardinality::ManyToOne { continue; }
            if table.columns[i].hide {
              table.columns[i].value.borrow_mut().clear();
              continue;
            }
            if i > 0 { write!(table.buf.borrow_mut(), "\t").unwrap(); }
            if table.columns[i].value.borrow().is_empty() { write!(table.buf.borrow_mut(), "\\N").unwrap(); }
            else if let Some(domain) = table.columns[i].domain.as_ref() {
              let mut domain = domain.borrow_mut();
              let id = match domain.map.get(&table.columns[i].value.borrow().to_string()) {
                Some(id) => *id,
                None => {
                  domain.lastid += 1;
                  let id = domain.lastid;
                  domain.map.insert(table.columns[i].value.borrow().to_string(), id);
                  write!(domain.table.buf.borrow_mut(), "{}\t{}\n", id, *table.columns[i].value.borrow()).unwrap();
                  domain.table.flush();
                  id
                }
              };
              write!(table.buf.borrow_mut(), "{}", id).unwrap();
              table.columns[i].value.borrow_mut().clear();
            }
            else {
              write!(table.buf.borrow_mut(), "{}", &table.columns[i].value.borrow()).unwrap();
              table.columns[i].value.borrow_mut().clear();
            }
          }
          write!(table.buf.borrow_mut(), "\n").unwrap();
          table.flush();
        }
        if !state.tables.is_empty() {
            state.table = state.tables.pop().unwrap();
            return Step::Repeat;
        }
      }
      else if state.skipped && path_match(&state.path, &state.settings.skip) {
        state.skipped = false;
        state.skipcount += 1;
      }

      if let Some(path) = &state.deferred {
        if path_match(&state.path, &table.path) && state.path.len() < path.len() { // We've just processed the deferred subtable's parent; apply the deferred events
          return Step::Apply;
        }
      }

      let i = state.path.rfind('/').expect("no slash in path; shouldn't happen");
      let tag = state.path.split_off(i);

      if state.xmltotext {
        state.text.push_str(&format!("<{}>", tag));
        for i in 0..table.columns.len() {
          if path_match(&state.path, &table.columns[i].path) {
            state.xmltotext = false;
            if let (Some(regex), Some(replacer)) = (table.columns[i].find.as_ref(), table.columns[i].replace) {
              state.text = regex.replace_all(&state.text, replacer).to_string();
            }
            table.columns[i].value.borrow_mut().push_str(&state.text);
            state.text.clear();
            return Step::Next;
          }
        }
      }
      else if state.gmltoewkb {
        if state.gmlpos && ((tag == "/gml:pos") || (tag == "/gml:posList")) { state.gmlpos = false; }
        for i in 0..table.columns.len() {
          if path_match(&state.path, &table.columns[i].path) {
            state.gmltoewkb = false;
            if !gml_to_ewkb(&table.columns[i].value, &state.gmlcoll, table.columns[i].bbox.as_ref(), table.columns[i].multitype, &state.settings) {
              state.filtered = true;
            }
            state.gmlcoll.clear();
            return Step::Next;
          }
        }
      }
    },
    Event::Eof => return Step::Done,
    _ => ()
  }

  Step::Next
}

fn path_match(path: &String, mask: &String) -> bool {
  if !mask.contains("*") && !mask.contains("{") { return path == mask; }
  glob_match(mask, path)
}

fn allow_iteration(column: &Column, settings: &Settings) -> bool {
  match column.aggr {
    None if settings.hush_warning => false,
    None => {
      eprintln!("Warning: column '{}' has multiple occurrences without an aggregation method; using 'first'", column.name);
      false
    },
    Some("first") => false,
    Some("last") => true,
    Some("append") => {
      if !column.value.borrow().is_empty() { column.value.borrow_mut().push(','); }
      true
    },
    _ => true
  }
}

fn write_output(file: RefCell<Box<dyn Write>>, rx: mpsc::Receiver<String>) {
  while let Ok(buf) = rx.recv() {
    if buf.len() == 0 { break; }
    file.borrow_mut().write_all(buf.as_bytes()).unwrap_or_else(|err| fatalerr!("Error: IO error encountered while writing table: {}", err))
  }
}
