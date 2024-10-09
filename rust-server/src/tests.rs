use crate::iter_json_stream;

#[tokio::test]
async fn test_chunked_json_stream() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let inputs = [
        r#""#,
        r#"{"msg"#,
        "\": \"hi\", \"context\": 3}\n",
        r#"
{"type": "span", "id": "a"}
{"type": "span", "id": "b", "parentSpan":"#,
        r#""#,
        r#" "a"}"#,
        r#"   "#,
    ];
    let mut outputs = vec![];
    iter_json_stream(
        futures::stream::iter(inputs.map(|str| Ok(str.into()))),
        |payload| {
            outputs.push(payload.to_owned());
            async move { Ok(()) }
        },
    )
    .await
    .unwrap();
    let output_strings = outputs
        .into_iter()
        .map(|bytes| std::str::from_utf8(&bytes).unwrap().to_string())
        .collect::<Vec<_>>();
    let expected_eq = vec![
        r#"{"msg": "hi", "context": 3}"#,
        r#"

{"type": "span", "id": "a"}"#,
        r#"
{"type": "span", "id": "b", "parentSpan": "a"}"#,
    ];
    assert_eq!(output_strings.len(), expected_eq.len());
    assert_eq!(output_strings, expected_eq);
    Ok(())
}

#[tokio::test]
/// Ensure that starting a valid JSON value doesn't result in success.
async fn test_chunked_json_stream_trailing_garbage() -> anyhow::Result<()> {
    let _ = env_logger::try_init();
    let inputs = [
        r#""#,
        r#"{"msg"#,
        "\": \"hi\", \"context\": 3}\n",
        r#"
{"type": "span", "id": "a"}
{"type": "span", "id": "b", "parentSpan":"#,
        r#""#,
        r#" "a"}"#,
        r#" {  "#,
    ];
    let mut outputs = vec![];
    let result = iter_json_stream(
        futures::stream::iter(inputs.map(|str| Ok(str.into()))),
        |payload| {
            outputs.push(payload.to_owned());
            async { Ok(()) }
        },
    )
    .await;
    result
        .as_ref()
        .expect_err("should error on trailing json value");
    dbg!(&result);
    let output_strings = outputs
        .into_iter()
        .map(|bytes| std::str::from_utf8(&bytes).unwrap().to_string())
        .collect::<Vec<_>>();
    let expected_eq = vec![
        r#"{"msg": "hi", "context": 3}"#,
        r#"

{"type": "span", "id": "a"}"#,
        r#"
{"type": "span", "id": "b", "parentSpan": "a"}"#,
    ];
    assert_eq!(output_strings.len(), expected_eq.len());
    assert_eq!(output_strings, expected_eq);
    Ok(())
}
