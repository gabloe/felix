fn agent_banner() -> &'static str {
    "Hello, world!"
}

fn main() {
    // Placeholder agent entry point; real logic will live here later.
    println!("{}", agent_banner());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn banner_is_expected() {
        assert_eq!(agent_banner(), "Hello, world!");
    }

    #[test]
    fn main_runs() {
        main();
    }
}
