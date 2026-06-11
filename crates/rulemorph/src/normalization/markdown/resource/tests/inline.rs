use super::super::*;

#[test]
fn preflight_counts_gfm_autolinks_before_parsing() {
    let input = "https://example.com support@example.com ".repeat(3);
    let options = NormalizationOptions {
        max_markdown_nodes: 8,
        ..NormalizationOptions::default()
    };

    let err = enforce_markdown_structural_preflight(&input, true, &options)
        .expect_err("GFM autolinks should exceed the preflight node estimate");

    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("max_markdown_nodes"));
}

#[test]
fn preflight_ignores_gfm_autolinks_when_extensions_are_disabled() {
    let input = "https://example.com support@example.com ".repeat(3);
    let options = NormalizationOptions {
        max_markdown_nodes: 8,
        ..NormalizationOptions::default()
    };

    enforce_markdown_structural_preflight(&input, false, &options)
        .expect("CommonMark mode should not count GFM bare URL autolinks");
}

#[test]
fn preflight_does_not_double_count_angle_bracket_autolinks_as_gfm_bare_links() {
    let input = "<https://example.com> <user@example.com>";
    let options = NormalizationOptions {
        max_markdown_nodes: 5,
        ..NormalizationOptions::default()
    };

    enforce_markdown_structural_preflight(input, true, &options)
        .expect("angle-bracket autolinks should not also count as GFM bare autolinks");
}

#[test]
fn preflight_counts_single_marker_emphasis_before_parsing() {
    let input = "*x* _y_ ".repeat(4);
    let options = NormalizationOptions {
        max_markdown_nodes: 8,
        ..NormalizationOptions::default()
    };

    let err = enforce_markdown_structural_preflight(&input, true, &options)
        .expect_err("single-marker emphasis should exceed the preflight node estimate");

    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("max_markdown_nodes"));
}

#[test]
fn preflight_counts_gfm_strikethrough_before_parsing() {
    let input = "~~x~~ ".repeat(6);
    let options = NormalizationOptions {
        max_markdown_nodes: 8,
        ..NormalizationOptions::default()
    };

    let err = enforce_markdown_structural_preflight(&input, true, &options)
        .expect_err("GFM strikethrough should exceed the preflight node estimate");

    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("max_markdown_nodes"));
}

#[test]
fn preflight_counts_closing_inline_html_before_parsing() {
    let input = "x </span> ".repeat(4);
    let options = NormalizationOptions {
        max_markdown_nodes: 5,
        ..NormalizationOptions::default()
    };

    let err = enforce_markdown_structural_preflight(&input, true, &options)
        .expect_err("closing inline HTML should exceed the preflight node estimate");

    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("max_markdown_nodes"));
}

#[test]
fn preflight_counts_reference_links_before_parsing() {
    let input = format!("{}\n\n[ref]: https://example.com", "[x][ref] ".repeat(5));
    let options = NormalizationOptions {
        max_markdown_nodes: 8,
        ..NormalizationOptions::default()
    };

    let err = enforce_markdown_structural_preflight(&input, true, &options)
        .expect_err("reference links should exceed the preflight node estimate");

    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("max_markdown_nodes"));
}

#[test]
fn preflight_counts_shortcut_reference_links_before_parsing() {
    let input = format!("{}\n\n[x]: https://example.com", "[x] ".repeat(5));
    let options = NormalizationOptions {
        max_markdown_nodes: 8,
        ..NormalizationOptions::default()
    };

    let err = enforce_markdown_structural_preflight(&input, true, &options)
        .expect_err("shortcut reference links should exceed the preflight node estimate");

    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("max_markdown_nodes"));
}

#[test]
fn preflight_counts_collapsed_reference_links_before_parsing() {
    let input = format!("{}\n\n[x]: https://example.com", "[x][] ".repeat(5));
    let options = NormalizationOptions {
        max_markdown_nodes: 8,
        ..NormalizationOptions::default()
    };

    let err = enforce_markdown_structural_preflight(&input, true, &options)
        .expect_err("collapsed reference links should exceed the preflight node estimate");

    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("max_markdown_nodes"));
}

#[test]
fn preflight_counts_reference_images_before_parsing() {
    for input in [
        format!(
            "{}\n\n[img]: https://example.com/image.png",
            "![alt][img] ".repeat(4)
        ),
        format!(
            "{}\n\n[alt]: https://example.com/image.png",
            "![alt][] ".repeat(4)
        ),
    ] {
        let options = NormalizationOptions {
            max_markdown_nodes: 10,
            ..NormalizationOptions::default()
        };

        let err = enforce_markdown_structural_preflight(&input, true, &options)
            .expect_err("reference images should exceed the preflight node estimate");

        assert_eq!(err.kind, TransformErrorKind::InvalidInput);
        assert!(err.message.contains("max_markdown_nodes"));
    }
}

#[test]
fn preflight_ignores_undefined_reference_labels() {
    let input = "A [not-a-link]\n[real]: not-url";
    let options = NormalizationOptions {
        max_markdown_nodes: 5,
        ..NormalizationOptions::default()
    };

    enforce_markdown_structural_preflight(input, true, &options)
        .expect("undefined reference labels should not count as reference link nodes");
}

#[test]
fn preflight_ignores_reference_text_inside_code_span() {
    let input = format!("{}\n\n[x]: https://example.com", "`[x]` ".repeat(3));
    let options = NormalizationOptions {
        max_markdown_nodes: 8,
        ..NormalizationOptions::default()
    };

    enforce_markdown_structural_preflight(&input, true, &options)
        .expect("bracket text inside code spans should not count as reference link nodes");
}

#[test]
fn preflight_ignores_explicit_link_markers_inside_code_spans() {
    let input = "`a](b)` ".repeat(4);
    let options = NormalizationOptions {
        max_markdown_nodes: 8,
        ..NormalizationOptions::default()
    };

    enforce_markdown_structural_preflight(&input, true, &options)
        .expect("explicit link-like text inside code spans should not count as link nodes");
}

#[test]
fn preflight_ignores_image_markers_inside_code_spans() {
    let input = "`![alt](image.png)` ".repeat(4);
    let options = NormalizationOptions {
        max_markdown_nodes: 8,
        ..NormalizationOptions::default()
    };

    enforce_markdown_structural_preflight(&input, true, &options)
        .expect("image-like text inside code spans should not count as image nodes");
}

#[test]
fn preflight_does_not_count_explicit_images_as_links() {
    let input = "![alt](image.png) ".repeat(4);
    let options = NormalizationOptions {
        max_markdown_nodes: 10,
        ..NormalizationOptions::default()
    };

    enforce_markdown_structural_preflight(&input, true, &options)
        .expect("explicit images should not also count as explicit links");
}

#[test]
fn preflight_counts_escaped_bang_explicit_links_as_links() {
    let input = "\\![alt](image.png) ".repeat(4);
    let options = NormalizationOptions {
        max_markdown_nodes: 10,
        ..NormalizationOptions::default()
    };

    let err = enforce_markdown_structural_preflight(&input, true, &options)
        .expect_err("escaped bang should leave an explicit link to count");

    assert_eq!(err.kind, TransformErrorKind::InvalidInput);
    assert!(err.message.contains("max_markdown_nodes"));
}
