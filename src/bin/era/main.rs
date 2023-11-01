use clap::Parser;
use std::process;

mod console;
mod daemon;

#[derive(Parser)]
#[clap(name = "Era")]
#[clap(bin_name = "era")]
#[clap(author, version, about, long_about = None)]
enum Era {
    Daemon(daemon::Args),
}

fn main() {
    let args = Era::parse();

    let result = match args {
        Era::Daemon(x) => daemon::run(&x),
    };

    if let Err(err) = &result {
        eprintln!("ERROR: {:#?}", err);
        process::exit(1);
    }

    process::exit(0);
}
