mod fs;
mod path;
mod security;
mod thread_pool;
mod utils;
mod windows;

use std::{fs::File, io::BufReader, sync::Arc};

use clap::{builder::FalseyValueParser, Arg, ArgMatches, Command};
use dokan::{init, shutdown, unmount, FileSystemMounter, MountFlags, MountOptions};

use fs::{
    entry::{DirEntry, Entry, EntryName},
    handler::MemFsHandler,
    metadata::Stat,
};
use ignore::gitignore::{Gitignore, GitignoreBuilder};
use log::debug;
use security::SecurityDescriptor;
use thread_pool::ThreadPool;
use url::Url;
use widestring::{U16CString, U16String};

fn command() -> Command {
    Command::new("Http FileSystem bridge")
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .help_template(
"\
{name} {version}
{about-with-newline}
USAGE:
    {usage}

OPTIONS:
{options}
"
        )
        .arg(
            Arg::new("mount_point")
                .short('m')
                .long("mount-point")
                .num_args(1)
                .value_name("MOUNT_POINT")
                .required(true)
                .help("Mount point path."),
        )
        .arg(
            Arg::new("url")
                .short('u')
                .long("url")
                .num_args(1)
                .value_name("URL")
                .required(true)
                .help("http url."),
        )
        .arg(
            Arg::new("dir")
                .short('j')
                .long("dir_tree")
                .num_args(1)
                .value_name("DIR_TREE")
                .required(true)
                .help("dir tree in json format."),
        )
        .arg(
            Arg::new("fs_ignore")
                .short('i')
                .long("fs-ignore")
                .value_name("BOOL")
                .value_parser(FalseyValueParser::new())
                .num_args(0..=1)
                .require_equals(true)
                .default_value("false")
                .default_missing_value("true")
                .help("ignore files using .fsignore .ignore or .gitignore."),
        )
        .arg(
            Arg::new("single_thread")
                .short('t')
                .long("single-thread")
                .action(clap::ArgAction::SetTrue)
                .default_missing_value("true")
                .value_parser(FalseyValueParser::new())
                .help("Force a single thread. Otherwise Dokan will allocate the number of threads regarding the workload."),
        )
        .arg(
            Arg::new("dokan_debug")
                .short('d')
                .long("dokan-debug")
                .num_args(0)
                .action(clap::ArgAction::SetTrue)
                .default_missing_value("true")
                .value_parser(FalseyValueParser::new())
                .help("Enable Dokan's debug output."),
        )
        .arg(
            Arg::new("removable")
                .short('r')
                .long("removable")
                .num_args(0)
                .action(clap::ArgAction::SetTrue)
                .default_missing_value("true")
                .value_parser(FalseyValueParser::new())
                .help("Mount as a removable drive."),
        )
}
fn arg_parser() -> ArgMatches {
    command().get_matches()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_arg_parser_optional_flags() {
        env_logger::init();
        let matches = command().try_get_matches_from(vec![
            "test_binary",
            "--mount-point",
            "C:\\mount",
            "--url",
            "http://example.com",
            "--dir_tree",
            "dir_tree.json",
            "--fs-ignore",
            "--single-thread",
            "--dokan-debug",
            "--removable",
        ]);

        assert!(matches.is_ok());
        let matches = matches.unwrap();
        let b = matches.get_one::<bool>("fs_ignore");
        assert!(b.is_some());
        debug!("fs_ignore = {:?}", b);
        assert!(b.unwrap());
        assert!(matches.get_flag("fs_ignore"));
        assert!(matches.get_flag("single_thread"));
        assert!(matches.get_flag("dokan_debug"));
        assert!(matches.get_flag("removable"));
    }

    #[test]
    fn test_arg_parser_no_optional_flags() {
        let matches = command().try_get_matches_from(vec![
            "test_binary",
            "--mount-point",
            "C:\\mount",
            "--url",
            "http://example.com",
            "--dir_tree",
            "dir_tree.json",
        ]);

        assert!(matches.is_ok());
        let matches = matches.unwrap();

        assert!(!matches.get_flag("fs_ignore"));
        assert!(!matches.get_flag("single_thread"));
        assert!(!matches.get_flag("dokan_debug"));
        assert!(!matches.get_flag("removable"));
    }
}

fn opt_ignore(enable: bool) -> Option<Gitignore> {
    match enable {
        true => {
            let mut gitignore_builder = GitignoreBuilder::new(".");
            let ignores = [
                gitignore_builder.add(".gitignore"),
                gitignore_builder.add(".ignore"),
                gitignore_builder.add(".fsignore"),
            ];
            if ignores.iter().all(|res| res.is_some()) {
                panic!("Failed to add ignore files");
            }
            Some(gitignore_builder.build().unwrap())
        }
        false => {
            debug!("fs ignore is disabled");
            None
        }
    }
}

#[test]
fn test_opt_ignore_enabled() {
    env_logger::init();
    let gitignore = opt_ignore(true);
    assert!(gitignore.is_some());
    let gitignore = gitignore.unwrap();
    let files_to_check = vec![
        ("\\.git", true, true), //
        ("\\refs", false, true),
        ("\\_locales", false, false),
    ];
    let results: Vec<_> = files_to_check
        .iter()
        .map(|(file, is_dir, should_match)| {
            let matched = gitignore.matched(file.trim_start_matches('\\'), *is_dir);
            debug!(
                "File: {}, Should match: {}, Matched result: {:?}",
                file, should_match, matched
            );
            (
                file.trim_start_matches('\\'),
                should_match,
                matched.is_ignore() == *should_match,
            )
        })
        .collect();

    for (file, should_match, is_correct) in results {
        assert!(
            is_correct,
            "File: {}, Expected: {}, Got: {:?}",
            file, should_match, !should_match
        );
    }
    // Assuming the .gitignore, .ignore, or .fsignore files exist and have rules
    // Add assertions based on expected behavior of the ignore rules
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::builder().format_timestamp_millis().init();
    let matches = arg_parser();

    let mount_point = U16CString::from_str(matches.get_one::<String>("mount_point").unwrap())?;

    let url = Url::parse(matches.get_one::<String>("url").unwrap()).unwrap();

    let dir_tree_path = matches.get_one::<String>("dir").unwrap();
    let dir_tree_string = BufReader::new(File::open(dir_tree_path)?);
    let dir_tree: utils::DirTree = serde_json::from_reader(dir_tree_string)?;

    let ignore = opt_ignore(matches.get_flag("fs_ignore"));

    let mut flags = MountFlags::ALT_STREAM;
    if matches.get_flag("dokan_debug") {
        flags |= MountFlags::DEBUG | MountFlags::STDERR;
    }
    if matches.get_flag("removable") {
        flags |= MountFlags::REMOVABLE;
    }

    let options = MountOptions {
        single_thread: matches.get_flag("single_thread"),
        flags,
        ..Default::default()
    };

    let thread_pool = Arc::new(ThreadPool::new(20));
    let _thread_pool = Arc::clone(&thread_pool);
    let handler = MemFsHandler::new(url, thread_pool, ignore);

    build_tree(&handler, dir_tree);
    init();

    let mut mounter = FileSystemMounter::new(&handler, &mount_point, &options);

    println!("File system will mount...");

    let file_system = mounter.mount()?;

    // Another thread can unmount the file system.
    let mount_point = mount_point.clone();
    ctrlc::set_handler(move || {
        if unmount(&mount_point) {
            println!("File system will unmount...")
        } else {
            let blocking_num = _thread_pool.working_num();
            eprintln!(
                "Failed to unmount file system. blocking thread pool:{:}",
                blocking_num
            );
        }
    })
    .expect("failed to set Ctrl-C handler");

    println!("File system is mounted, press Ctrl-C to unmount.");

    drop(file_system);

    println!("File system is unmounted.");

    shutdown();

    Ok(())
}

fn build_tree(handler: &MemFsHandler, dir_tree: utils::DirTree) {
    let root = &handler.root;
    let mut stack = vec![(Arc::clone(&root), dir_tree)];
    while let Some((parent, dir_tree)) = stack.pop() {
        for child in dir_tree.children {
            let child_stat = Stat::new(
                handler.next_id(),
                0,
                SecurityDescriptor::new_default().unwrap(),
                Arc::downgrade(&parent),
            );
            let child_entry = match child.is_folder() {
                true => {
                    let dir_entry = Arc::new(DirEntry::new(child_stat));
                    stack.push((Arc::clone(&dir_entry), child.clone()));
                    Ok(Entry::Directory(dir_entry))
                }
                // false => Entry::HttpFile(Arc::new(HttpFileEntry::new(
                //     child_stat,
                //     url.join(), // FIXME:
                // ))),
                false => Err("TODO: not supported file yet"),
            };
            parent.children.write().unwrap().insert(
                EntryName(U16String::from_str(&child.name.replace("/", ""))),
                Arc::new(child_entry.unwrap()),
            );
        }
    }
    fn print_tree(entry: &Arc<DirEntry>, prefix: String) {
        let children = entry.children.read().unwrap();
        for (name, child) in children.iter() {
            let name_str = name.0.to_string_lossy();
            match child.as_ref() {
                Entry::Directory(dir) => {
                    debug!("{}[Dir] {}", prefix, name_str);
                    print_tree(dir, format!("{}  ", prefix));
                }
                Entry::File(_) => {
                    debug!("{}[File] {}", prefix, name_str);
                }
                Entry::HttpFile(_) => {
                    debug!("{}[HttpFile] {}", prefix, name_str);
                }
            }
        }
    }

    print_tree(&root, String::new());
    // root
}
