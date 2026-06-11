use std::fs;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

pub(super) fn serialize_json_output(value: &serde_json::Value) -> Result<String, ()> {
    match serde_json::to_string(value) {
        Ok(text) => Ok(text),
        Err(err) => {
            eprintln!("failed to serialize output JSON: {}", err);
            Err(())
        }
    }
}

pub(super) fn emit_text_output(text: &str, output: Option<&PathBuf>) -> Result<(), ()> {
    if let Some(path) = output {
        ensure_parent_dir(path)?;
        if let Err(err) = fs::write(path, text.as_bytes()) {
            eprintln!("failed to write output: {}", err);
            return Err(());
        }
    } else {
        println!("{}", text);
    }

    Ok(())
}

pub(super) fn create_output_writer(
    output: Option<&PathBuf>,
) -> Result<BufWriter<Box<dyn Write>>, ()> {
    let writer: Box<dyn Write> = match output {
        Some(path) => {
            ensure_parent_dir(path)?;
            match fs::File::create(path) {
                Ok(file) => Box::new(file),
                Err(err) => {
                    eprintln!("failed to write output: {}", err);
                    return Err(());
                }
            }
        }
        None => Box::new(io::stdout()),
    };

    Ok(BufWriter::new(writer))
}

pub(super) fn write_json_line(
    writer: &mut BufWriter<Box<dyn Write>>,
    value: &serde_json::Value,
) -> Result<(), ()> {
    let output_text = match serde_json::to_string(value) {
        Ok(text) => text,
        Err(err) => {
            eprintln!("failed to serialize output JSON: {}", err);
            return Err(());
        }
    };

    if let Err(err) = writeln!(writer, "{}", output_text) {
        eprintln!("failed to write output: {}", err);
        return Err(());
    }

    Ok(())
}

fn ensure_parent_dir(path: &Path) -> Result<(), ()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
        && let Err(err) = fs::create_dir_all(parent)
    {
        eprintln!("failed to create output directory: {}", err);
        return Err(());
    }

    Ok(())
}
