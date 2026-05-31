#![allow(dead_code, unused_imports)]

use std::io::{Cursor, Write};

use zip::{CompressionMethod, ZipWriter, write::FileOptions};

mod dynamodb;
mod fixture;

pub(crate) use dynamodb::build_dynamodb_users_xlsx;
pub(crate) use fixture::{XlsxFixtureOptions, build_test_xlsx};

include!("xlsx/string_table.rs");

pub(super) fn write_zip_file(
    zip: &mut ZipWriter<Cursor<Vec<u8>>>,
    name: &str,
    contents: &str,
    options: FileOptions,
) {
    zip.start_file(name, options).expect("start xlsx part");
    zip.write_all(contents.as_bytes()).expect("write xlsx part");
}
