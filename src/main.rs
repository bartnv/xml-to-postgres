extern crate quick_xml;
extern crate yaml_rust;

use std::io::Read;
use std::fs::File;
use std::env;
use quick_xml::Reader;
use quick_xml::events::Event;
use yaml_rust::YamlLoader;

#[derive(Debug)]
struct Column {
  name: String,
  path: String,
  value: String,
  raw: bool
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
    let name = col["name"].as_str().unwrap();
    let mut raw = false;
    if !col["raw"].is_badvalue() { raw = col["raw"].as_bool().unwrap() }
    let colpath = col["path"].as_str().unwrap();
//    println!("Column name {} path {}", name, colpath);
    let mut path = String::from(rowpath);
    path.push_str(colpath);
    columns.push(Column { name: name.to_string(), path: path, value: String::new(), raw: raw });
  }

  let mut raw = false;
  let mut rawstr = String::new();
  loop {
    match reader.read_event(&mut buf) {
      Ok(Event::Start(ref e)) => {
        path.push('/');
        path.push_str(reader.decode(e.name()).unwrap());
        if raw {
          rawstr.push_str(&format!("<{}>", &e.unescape_and_decode(&reader).unwrap()));
          continue;
        }
        else if path == rowpath {
          count += 1;
        }
        else if path.len() > rowpath.len() {
          for i in 0..columns.len() {
            if path == columns[i].path {
              if columns[i].raw { raw = true; }
              break;
            }
          }
        }
      },
      Ok(Event::Text(ref e)) => {
        if raw {
          rawstr.push_str(&e.unescape_and_decode(&reader).unwrap());
          continue;
        }
        for i in 0..columns.len() {
          if path == columns[i].path {
            columns[i].value.push_str(&e.unescape_and_decode(&reader).unwrap());
          }
        }
      },
      Ok(Event::End(_)) => {
        if path == rowpath {
          for i in 0..columns.len() {
            if i > 0 { print!("\t"); }
            if columns[i].value.is_empty() { print!("\\N"); }
            else {
              print!("{}", columns[i].value);
              columns[i].value.clear();
            }
          }
          println!("");
        }
        let i = path.rfind('/').unwrap();
        let tag = path.split_off(i);
        if raw {
          rawstr.push_str(&format!("<{}>", tag));
          for i in 0..columns.len() {
            if path == columns[i].path {
              raw = false;
              columns[i].value.push_str(&rawstr);
              rawstr.clear();
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
