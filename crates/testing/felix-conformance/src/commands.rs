//! The subcommands that work on the scenario catalogue rather than a broker.

use anyhow::{Result, anyhow};
use felix_conformance::kit;

/// Check a client's results against the catalogue.
pub(crate) fn run_verify(args: &[String]) -> Result<()> {
    let path = args
        .first()
        .ok_or_else(|| anyhow!("usage: felix-conformance verify <results.json>"))?;
    let catalogue = kit::catalogue()?;
    let results = kit::read_results(std::path::Path::new(path))?;
    let report = kit::verify(&catalogue, &results)?;
    report.print(&results.client);
    if report.conformant() {
        Ok(())
    } else {
        Err(anyhow!("{} is not conformant", results.client))
    }
}

/// Print the catalogue, so a client author can see what to implement.
pub(crate) fn run_scenarios() -> Result<()> {
    let catalogue = kit::catalogue()?;
    println!(
        "Felix client conformance scenarios (v{})\n",
        catalogue.version
    );
    for scenario in &catalogue.scenarios {
        let tag = if scenario.required {
            "required"
        } else {
            "optional"
        };
        println!("{} [{tag}]\n  {}", scenario.id, scenario.title);
        for line in scenario.detail.trim().lines() {
            println!("  {line}");
        }
        println!();
    }
    Ok(())
}
