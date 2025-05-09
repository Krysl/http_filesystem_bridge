mod fs;
mod path;
mod security;

use clap::{builder::FalseyValueParser, Arg, Command};
use dokan::{init, shutdown, unmount, FileSystemMounter, MountFlags, MountOptions};

use fs::handler::MemFsHandler;
use widestring::U16CString;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let matches = Command::new("dokan-rust memfs example")
		.author(env!("CARGO_PKG_AUTHORS"))
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
			Arg::new("single_thread")
				.short('t')
				.long("single-thread")
                .action(clap::ArgAction::SetTrue)
                .default_missing_value("false")
                .value_parser(FalseyValueParser::new())
				.help("Force a single thread. Otherwise Dokan will allocate the number of threads regarding the workload."),
		)
		.arg(
			Arg::new("dokan_debug")
				.short('d')
				.long("dokan-debug")
                .num_args(0)
                .action(clap::ArgAction::SetTrue)
                .default_missing_value("false")
                .value_parser(FalseyValueParser::new())
				.help("Enable Dokan's debug output."),
        )
		.arg(
			Arg::new("removable")
				.short('r')
				.long("removable")
                .num_args(0)
                .action(clap::ArgAction::SetTrue)
                .default_missing_value("false")
                .value_parser(FalseyValueParser::new())
				.help("Mount as a removable drive."),
		)
		.get_matches();

    let mount_point = U16CString::from_str(matches.get_one::<String>("mount_point").unwrap())?;

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

    let handler = MemFsHandler::new();

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
            eprintln!("Failed to unmount file system.");
        }
    })
    .expect("failed to set Ctrl-C handler");

    println!("File system is mounted, press Ctrl-C to unmount.");

    drop(file_system);

    println!("File system is unmounted.");

    shutdown();

    Ok(())
}
