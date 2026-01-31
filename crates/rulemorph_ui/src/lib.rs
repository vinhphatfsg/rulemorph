use std::path::PathBuf;

pub fn default_ui_dir() -> PathBuf {
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let release_path = cwd.join("ui").join("dist");
    if release_path.exists() {
        return release_path;
    }
    cwd.join("crates/rulemorph_ui/ui/dist")
}
