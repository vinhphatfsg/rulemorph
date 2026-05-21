#[test]
fn transform_accepts_text_input_normalization_formats() {
    let cases = [
        (
            "yaml",
            r#"
version: 2
input:
  format: yaml
  yaml:
    records_path: users
mappings:
  - target: "id"
    source: "id"
  - target: "name"
    source: "name"
"#,
            "users:\n  - id: \"1\"\n    name: Alice\n",
        ),
        (
            "toml",
            r#"
version: 2
input:
  format: toml
  toml:
    records_path: users
mappings:
  - target: "id"
    source: "id"
  - target: "name"
    source: "name"
"#,
            "[[users]]\nid = \"1\"\nname = \"Alice\"\n",
        ),
        (
            "xml",
            r##"
version: 2
input:
  format: xml
  xml:
    records_path: users.user
    attr_prefix: "@"
    text_key: "#text"
mappings:
  - target: "id"
    source: 'input.["@id"]'
  - target: "name"
    source: 'input.name[0]["#text"]'
"##,
            r#"<users><user id="1"><name>Alice</name></user></users>"#,
        ),
        (
            "html",
            r#"
version: 2
input:
  format: html
  html:
    records_selector: "table#users tbody tr"
    fields:
      id:
        selector: "td:nth-child(1)"
        value: text
      name:
        selector: "td:nth-child(2)"
        value: text
mappings:
  - target: "id"
    source: "id"
  - target: "name"
    source: "name"
"#,
            r#"<table id="users"><tbody><tr><td>1</td><td>Alice</td></tr></tbody></table>"#,
        ),
    ];

    for (index, (format, rules_text, input_text)) in cases.into_iter().enumerate() {
        let mut server = McpServer::start();
        initialize(&mut server);
        let request = json!({
            "jsonrpc": "2.0",
            "id": 100 + index,
            "method": "tools/call",
            "params": {
                "name": "transform",
                "arguments": {
                    "rules_text": rules_text,
                    "input_text": input_text,
                    "format": format,
                    "return_output_json": true
                }
            }
        });
        let response = server.send(&request);
        assert!(response.get("error").is_none(), "response: {response}");
        assert_eq!(
            response["result"]["meta"]["output"],
            json!([{ "id": "1", "name": "Alice" }])
        );
        server.shutdown();
    }
}
