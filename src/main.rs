extern crate quick_xml;
extern crate yaml_rust;

use std::io::Read;
use std::fs::File;
use std::env;
use quick_xml::Reader;
use quick_xml::events::Event;
use yaml_rust::{Yaml, YamlLoader};

#[derive(Debug)]
struct Column {
  name: String,
  path: String,
  value: String
}

fn main() {
  let args: Vec<_> = env::args().collect();
  if args.len() != 3 {
    println!("usage: {} <configfile> <xmlfile>", args[0]);
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
  let namefield = &Yaml::from_str("name");
  let pathfield = &Yaml::from_str("path");
  let mut columns = Vec::new();

  for col in colspec {
    let hash = col.as_hash().expect("Column entry is not a valid hash");
    let name = hash[namefield].as_str().unwrap();
    let colpath = hash[pathfield].as_str().unwrap();
    println!("Column name {} path {}", name, colpath);
    let mut path = String::from(rowpath);
    path.push_str(colpath);
    columns.push(Column { name: name.to_string(), path: path, value: String::new() });
  }

  loop {
    match reader.read_event(&mut buf) {
      Ok(Event::Start(ref e)) => {
        path.push('/');
        path.push_str(reader.decode(e.name()).unwrap());
        if path == rowpath {
          count += 1;
        }
      },
      Ok(Event::Text(ref e)) => {
        for i in 0..columns.len() {
          if path == columns[i].path {
            columns[i].value.push_str(&e.unescape_and_decode(&reader).unwrap());
          }
        }
      },
      Ok(Event::End(_)) => {
        if path == rowpath {
          println!("Insert row with id {}", columns[0].value);
          for i in 0..columns.len() { columns[i].value.clear(); }
        }
        let i = path.rfind('/').unwrap();
        let _ = path.split_off(i);
      },
      Ok(Event::Eof) => break,
      Err(e) => panic!("Error at position {}: {:?}", reader.buffer_position(), e),
      _ => ()
    }
    buf.clear();
  }
  println!("{} rows processed", count);
}
