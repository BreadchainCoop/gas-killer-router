//! The OpenAPI document for the ingress HTTP API, generated from the handlers themselves.
//!
//! [`ApiDoc`] is the whole contract: every route [`build_app`](crate::ingress::build_app) serves
//! is listed here, and each one is described by a `#[utoipa::path]` attribute sitting on the
//! handler that implements it. Keeping the description next to the handler is the point of
//! generating rather than hand-writing the document, since a route cannot change shape without
//! the annotation being right there in the diff.
//!
//! Two schemas cannot be inferred from their Rust types and are built by hand below:
//! [`CallData`], whose serde representation is a JSON array of byte integers rather than a hex
//! string, and [`TransitionIndex`], which accepts three different JSON types. The Ethereum
//! primitives have the same problem and live in [`gas_killer_common::openapi`].
//!
//! The document is rendered to disk in two forms, both committed and both checked by the tests
//! below. `router/docs/openapi.json` is the integrator-facing API and is what the docs site renders;
//! `router/docs/openapi.internal.json` additionally carries the operator surface. See [`PRIVATE_TAGS`]
//! for what separates them and why. The `openapi` binary writes both.

use crate::error::{ApiErrorBody, ApiErrorEnvelope, ErrorCode};
use crate::ingress::{
    AvsMetadata, AvsOperatorSetMetadata, AvsOperatorSetSoftware, CreateApiKeyRequest,
    GasKillerTaskRequest, GasKillerTaskRequestBody, TaskAcceptedResponse, TaskView,
};
use crate::store::{ApiKeyMetadata, CreatedApiKey, TaskStatus};
use gas_killer_common::avs_contracts::AvsContracts;
use gas_killer_common::openapi::{Address, HexBytes, HexUint256};
use gas_killer_common::payload::PayloadView;
use gas_killer_common::task_data::MAX_EVM_TX_CALLDATA_SIZE;
use std::collections::BTreeSet;
use utoipa::openapi::RefOr;
use utoipa::openapi::path::Operation;
use utoipa::openapi::schema::{
    AnyOfBuilder, ArrayBuilder, KnownFormat, ObjectBuilder, Schema, SchemaFormat, SchemaType, Type,
};
use utoipa::openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme};
use utoipa::{Modify, OpenApi, PartialSchema, ToSchema};

/// Version of the HTTP API contract, reported as `info.version`.
///
/// Tracks the wire contract rather than the crate version: bump it when a route, a field, or an
/// error code changes shape, not on every release.
pub const API_VERSION: &str = "0.3.0";

/// Shortest accepted `call_data`, the four bytes of an ABI function selector.
const SELECTOR_LEN: usize = 4;

/// ABI-encoded calldata, serialized as a JSON array of byte values rather than a hex string.
///
/// The unusual representation is serde's default for `Vec<u8>` and is part of the published
/// contract, so it is described as it is rather than corrected here.
pub struct CallData;

impl PartialSchema for CallData {
    fn schema() -> RefOr<Schema> {
        ArrayBuilder::new()
            .items(
                ObjectBuilder::new()
                    .schema_type(SchemaType::new(Type::Integer))
                    .minimum(Some(0))
                    .maximum(Some(255)),
            )
            .min_items(Some(SELECTOR_LEN))
            .max_items(Some(MAX_EVM_TX_CALLDATA_SIZE))
            .description(Some(
                "ABI-encoded calldata as an array of byte values (0 to 255). The first four \
                 bytes are the function selector, so at least 4 bytes are required; the maximum \
                 accepted size is 128 KiB.",
            ))
            .examples([serde_json::json!([171, 205, 239, 1])])
            .into()
    }
}

impl ToSchema for CallData {}

/// The state-transition slot a submission occupies: an integer to claim a specific slot, or the
/// string `"auto"` (equivalently `null`, or the field omitted) to let the router resolve the next
/// free slot when the task is dequeued.
///
/// A three-way union, because `"auto"` has to be distinguishable from slot 0 while an absent
/// field has to keep meaning the same thing it did before the string form existed.
pub struct TransitionIndex;

impl PartialSchema for TransitionIndex {
    fn schema() -> RefOr<Schema> {
        AnyOfBuilder::new()
            .item(
                ObjectBuilder::new()
                    .schema_type(SchemaType::new(Type::Integer))
                    .format(Some(SchemaFormat::KnownFormat(KnownFormat::Int64)))
                    .minimum(Some(0)),
            )
            .item(
                ObjectBuilder::new()
                    .schema_type(SchemaType::new(Type::String))
                    .enum_values(Some(["auto"])),
            )
            .item(ObjectBuilder::new().schema_type(SchemaType::new(Type::Null)))
            .description(Some(
                "The state-transition slot to occupy. Send an integer to target a specific slot, \
                 or \"auto\" (or null, or omit the field) to let the router resolve the next \
                 available slot at dequeue time, which is what makes safe parallel submissions \
                 possible.",
            ))
            .default(Some(serde_json::json!("auto")))
            .into()
    }
}

impl ToSchema for TransitionIndex {}

/// Registers the two bearer schemes the routes reference.
///
/// Both are `Authorization: Bearer <token>`, but they are separate schemes because they are
/// separate credentials with different blast radii: an API key scopes a caller to its own tasks,
/// while `ADMIN_KEY` mints and revokes those keys.
struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let components = openapi.components.get_or_insert_with(Default::default);
        components.add_security_scheme(
            "ApiKeyAuth",
            SecurityScheme::Http(
                HttpBuilder::new()
                    .scheme(HttpAuthScheme::Bearer)
                    .description(Some(
                        "An API key for task submission and polling, sent as \
                         `Authorization: Bearer gk_...`.",
                    ))
                    .build(),
            ),
        );
        components.add_security_scheme(
            "AdminAuth",
            SecurityScheme::Http(
                HttpBuilder::new()
                    .scheme(HttpAuthScheme::Bearer)
                    .description(Some(
                        "The operator's `ADMIN_KEY` shared secret, sent as \
                         `Authorization: Bearer <ADMIN_KEY>`. Mints and revokes API keys, so it \
                         is not an integrator credential.",
                    ))
                    .build(),
            ),
        );
    }
}

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Gas Killer Router API",
        version = API_VERSION,
        description = "HTTP API for the Gas Killer router (the AVS aggregator).

Clients submit compute tasks to `POST /tasks`; the router validates them, fans them out to \
restaked operator nodes, and aggregates the BLS signatures into a completed round. Rather than \
broadcasting on-chain itself, the router returns a ready-to-sign transaction, the *payload*. The \
typical flow is:

1. `POST /tasks` to submit a task and receive a `task_id`.
2. `GET /tasks/{task_id}` to poll until `status` is `ready`, at which point the response carries \
a `payload` object.
3. Sign and submit that `payload` from your own wallet before its `valid_until_block`.

Task submission and polling are authenticated with an API key.",
        contact(
            name = "Gas Killer",
            url = "https://gaskiller.xyz",
            email = "contact@gaskiller.xyz"
        ),
        license(name = "PPL")
    ),
    servers(
        (
            url = "{baseUrl}",
            description = "Gas Killer router ingress",
            variables(
                ("baseUrl" = (
                    default = "https://testnet.gaskiller.xyz",
                    enum_values("https://testnet.gaskiller.xyz", "http://localhost:8080"),
                    description = "Base URL of the router ingress. Defaults to the public \
                                   testnet; select localhost for local development, or replace \
                                   with your own deployment."
                ))
            )
        )
    ),
    tags(
        (
            name = "Tasks",
            description = "Submitting compute tasks, polling their status, and retrieving the \
                           ready-to-sign payload."
        ),
        (
            name = "Metadata",
            description = "Public AVS identity and settlement addresses, consumed by the \
                           restaking indexer and by integrators wiring a target contract."
        ),
        (
            name = "Admin",
            description = "API key lifecycle, guarded by the operator's `ADMIN_KEY`. Operator \
                           surface, not part of the integrator API."
        ),
        (
            name = "Health",
            description = "Liveness probing."
        )
    ),
    paths(
        crate::ingress::submit_task_handler,
        crate::ingress::list_tasks_handler,
        crate::ingress::get_task_handler,
        crate::ingress::avs_metadata_handler,
        crate::operator_http::healthz_handler,
        crate::operator_http::readyz_handler,
        crate::operator_http::metrics_handler,
        crate::ingress::create_api_key_handler,
        crate::ingress::list_api_keys_handler,
        crate::ingress::revoke_api_key_handler,
    ),
    components(schemas(
        Address,
        HexBytes,
        HexUint256,
        CallData,
        TransitionIndex,
        GasKillerTaskRequest,
        GasKillerTaskRequestBody,
        TaskAcceptedResponse,
        TaskView,
        TaskStatus,
        PayloadView,
        AvsMetadata,
        AvsContracts,
        AvsOperatorSetMetadata,
        AvsOperatorSetSoftware,
        CreateApiKeyRequest,
        CreatedApiKey,
        ApiKeyMetadata,
        ApiErrorEnvelope,
        ApiErrorBody,
        ErrorCode,
    )),
    modifiers(&SecurityAddon)
)]
pub struct ApiDoc;

/// Tags whose operations describe the operator surface rather than the integrator API.
///
/// These are removed from the published document rather than merely left undocumented, so no
/// site-side configuration can publish them by accident. `Admin` mints and revokes every API key
/// the router honours, and the docs site renders the published document as an interactive
/// playground, which is the wrong place to invite someone to paste an `ADMIN_KEY`. `Health` is a
/// deployment concern: an integrator has no use for the router's liveness, and publishing it
/// invites it to be treated as an API to poll.
///
/// The handlers keep their annotations either way: `openapi.internal.json` is the whole API, and
/// the tests still require every served route to be documented there.
pub const PRIVATE_TAGS: &[&str] = &["Admin", "Health"];

/// The eight operation slots a path item can hold, as mutable references. `PathItem` models
/// methods as named fields rather than a map, so anything walking every operation has to spell
/// them out.
fn operation_slots(item: &mut utoipa::openapi::PathItem) -> [&mut Option<Operation>; 8] {
    [
        &mut item.get,
        &mut item.put,
        &mut item.post,
        &mut item.delete,
        &mut item.options,
        &mut item.head,
        &mut item.patch,
        &mut item.trace,
    ]
}

/// The operations a path item actually defines.
fn operations(item: &utoipa::openapi::PathItem) -> impl Iterator<Item = &Operation> {
    [
        &item.get,
        &item.put,
        &item.post,
        &item.delete,
        &item.options,
        &item.head,
        &item.patch,
        &item.trace,
    ]
    .into_iter()
    .flatten()
}

/// Whether an operation carries any of [`PRIVATE_TAGS`].
fn is_private(operation: &Operation) -> bool {
    operation
        .tags
        .as_ref()
        .is_some_and(|tags| tags.iter().any(|tag| PRIVATE_TAGS.contains(&tag.as_str())))
}

/// Names of the security schemes an operation requires.
///
/// Read back out of the serialized form because a `SecurityRequirement` keeps its scheme names in
/// a private map, and the requirement serializes to exactly `{"SchemeName": [scopes]}`.
fn required_schemes(operation: &Operation) -> Vec<String> {
    operation
        .security
        .iter()
        .flatten()
        .filter_map(|requirement| serde_json::to_value(requirement).ok())
        .filter_map(|value| match value {
            serde_json::Value::Object(map) => Some(map.into_iter().map(|(name, _)| name)),
            _ => None,
        })
        .flatten()
        .collect()
}

/// Every `#/components/schemas/<name>` reference anywhere within `value`.
fn schema_references(value: &serde_json::Value) -> Vec<String> {
    const PREFIX: &str = "#/components/schemas/";
    let mut found = Vec::new();
    let mut pending = vec![value];
    while let Some(node) = pending.pop() {
        match node {
            serde_json::Value::Object(map) => {
                for (key, child) in map {
                    if key == "$ref"
                        && let Some(name) = child.as_str().and_then(|r| r.strip_prefix(PREFIX))
                    {
                        found.push(name.to_string());
                    }
                    pending.push(child);
                }
            }
            serde_json::Value::Array(items) => pending.extend(items),
            _ => {}
        }
    }
    found
}

/// Component schemas reachable from the document's remaining paths, following references through
/// the schemas themselves so a type used only by a nested field is kept.
fn reachable_schemas(document: &utoipa::openapi::OpenApi) -> BTreeSet<String> {
    let schemas = document.components.as_ref().map(|c| &c.schemas);
    let mut reachable = BTreeSet::new();
    let mut pending: Vec<serde_json::Value> =
        serde_json::to_value(&document.paths).into_iter().collect();

    while let Some(value) = pending.pop() {
        for name in schema_references(&value) {
            if reachable.insert(name.clone())
                && let Some(schema) = schemas.and_then(|s| s.get(&name))
                && let Ok(value) = serde_json::to_value(schema)
            {
                pending.push(value);
            }
        }
    }
    reachable
}

/// Removes every [`PRIVATE_TAGS`] operation from `document`, along with what only they used: the
/// security schemes they required, the component schemas they alone referenced, and their tag
/// definitions.
///
/// Pruning by reachability rather than by a list means a type added to an admin endpoint later
/// stays out of the published document without anyone remembering to exclude it.
fn strip_private(document: &mut utoipa::openapi::OpenApi) {
    for item in document.paths.paths.values_mut() {
        for slot in operation_slots(item) {
            if slot.as_ref().is_some_and(is_private) {
                *slot = None;
            }
        }
    }
    document
        .paths
        .paths
        .retain(|_, item| operations(item).next().is_some());

    let kept_schemes: BTreeSet<String> = document
        .paths
        .paths
        .values()
        .flat_map(operations)
        .flat_map(required_schemes)
        .collect();
    let kept_tags: BTreeSet<String> = document
        .paths
        .paths
        .values()
        .flat_map(operations)
        .filter_map(|operation| operation.tags.as_ref())
        .flatten()
        .cloned()
        .collect();
    let kept_schemas = reachable_schemas(document);

    if let Some(components) = document.components.as_mut() {
        components
            .security_schemes
            .retain(|name, _| kept_schemes.contains(name));
        components
            .schemas
            .retain(|name, _| kept_schemas.contains(name));
    }
    if let Some(tags) = document.tags.as_mut() {
        tags.retain(|tag| kept_tags.contains(&tag.name));
    }
}

/// Renders the integrator-facing document, the bytes `router/docs/openapi.json` holds and the docs site
/// renders. Pretty-printed JSON with a trailing newline, so the committed file is a well-formed
/// text file and comparing the two is a plain string equality.
pub fn render() -> Result<String, serde_json::Error> {
    let mut document = ApiDoc::openapi();
    strip_private(&mut document);
    Ok(format!("{}\n", document.to_pretty_json()?))
}

/// Renders the whole API including the operator surface, the bytes `router/docs/openapi.internal.json`
/// holds. Not for publication; see [`PRIVATE_TAGS`].
pub fn render_internal() -> Result<String, serde_json::Error> {
    Ok(format!("{}\n", ApiDoc::openapi().to_pretty_json()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Path item keys that name an operation. A path item can also carry `summary`, `parameters`
    /// and friends, so anything walking operations has to filter for these.
    const METHODS: &[&str] = &[
        "get", "put", "post", "delete", "options", "head", "patch", "trace",
    ];

    /// Every `$ref` target anywhere in `value`. Collected rather than resolved in place so a
    /// caller can report each one against the document it belongs to.
    fn all_references(value: &serde_json::Value) -> Vec<String> {
        let mut found = Vec::new();
        let mut pending = vec![value];
        while let Some(node) = pending.pop() {
            match node {
                serde_json::Value::Object(map) => {
                    for (key, child) in map {
                        if key == "$ref"
                            && let Some(target) = child.as_str()
                        {
                            found.push(target.to_string());
                        }
                        pending.push(child);
                    }
                }
                serde_json::Value::Array(items) => pending.extend(items),
                _ => {}
            }
        }
        found
    }

    /// The operations a rendered document defines, as `(method, path, operation)`.
    fn operations_of(document: &serde_json::Value) -> Vec<(&str, &str, &serde_json::Value)> {
        document["paths"]
            .as_object()
            .expect("the document has paths")
            .iter()
            .flat_map(|(path, item)| {
                item.as_object()
                    .expect("a path item")
                    .iter()
                    .filter(|(key, _)| METHODS.contains(&key.as_str()))
                    .map(move |(method, operation)| (method.as_str(), path.as_str(), operation))
            })
            .collect()
    }

    /// Asserts a rendered document is internally consistent.
    ///
    /// utoipa serializes from typed structs, so well-formed JSON is guaranteed and there is no
    /// syntax left to get wrong. What is not guaranteed is that the document hangs together. A
    /// `value_type` naming a type missing from `components(schemas(...))` emits a reference to a
    /// schema that does not exist, and [`strip_private`] could prune one something still points
    /// at. Either parses cleanly and then breaks whatever consumes it, which is how a
    /// well-formed document still fails a renderer.
    fn assert_consistent(rendered: &str, label: &str) {
        let document: serde_json::Value = serde_json::from_str(rendered)
            .unwrap_or_else(|e| panic!("{label} is not valid JSON: {e}"));

        // Every reference resolves. `Value::pointer` reads the fragment as the JSON pointer it
        // is, so this covers references to anything, not just component schemas.
        let references = all_references(&document);
        for reference in &references {
            let pointer = reference
                .strip_prefix('#')
                .unwrap_or_else(|| panic!("{label} has a non-local reference `{reference}`"));
            assert!(
                document.pointer(pointer).is_some(),
                "{label} references `{reference}`, which the document does not define"
            );
        }

        // Nothing declared that nothing points at, so the component list cannot silently
        // accumulate types the API no longer exposes.
        let referenced: BTreeSet<&str> = references
            .iter()
            .filter_map(|reference| reference.rsplit('/').next())
            .collect();
        for name in document["components"]["schemas"]
            .as_object()
            .expect("the document has component schemas")
            .keys()
        {
            assert!(
                referenced.contains(name.as_str()),
                "{label} defines the schema `{name}`, which nothing references"
            );
        }

        let schemes: BTreeSet<&str> = document["components"]["securitySchemes"]
            .as_object()
            .map(|map| map.keys().map(String::as_str).collect())
            .unwrap_or_default();
        let mut operation_ids: BTreeSet<&str> = BTreeSet::new();

        for (method, path, operation) in operations_of(&document) {
            // Operation ids address a page in the generated reference, so a collision silently
            // drops one of the two.
            if let Some(id) = operation["operationId"].as_str() {
                assert!(
                    operation_ids.insert(id),
                    "{label}: `{id}` is the operation id of more than one operation"
                );
            }

            // A requirement naming a scheme the document does not define leaves a consumer with
            // no way to authenticate the call it is describing.
            for requirement in operation["security"].as_array().into_iter().flatten() {
                for name in requirement
                    .as_object()
                    .expect("a security requirement")
                    .keys()
                {
                    assert!(
                        schemes.contains(name.as_str()),
                        "{label}: {method} {path} requires the undefined security scheme `{name}`"
                    );
                }
            }

            // The path template and the declared path parameters have to agree in both
            // directions: a `{param}` with no parameter is undocumented, and a parameter with no
            // `{param}` is a typo in the annotation's path.
            let templated: BTreeSet<&str> = path
                .split('/')
                .filter_map(|segment| segment.strip_prefix('{')?.strip_suffix('}'))
                .collect();
            let declared: BTreeSet<&str> = operation["parameters"]
                .as_array()
                .into_iter()
                .flatten()
                .filter(|parameter| parameter["in"] == "path")
                .filter_map(|parameter| parameter["name"].as_str())
                .collect();
            assert_eq!(
                templated, declared,
                "{label}: {method} {path} templates {templated:?} but declares {declared:?}"
            );
        }
    }

    /// Both documents ship, so both are held to the same structural bar. The published one
    /// matters most: it is the only one that has been through [`strip_private`], so it is the
    /// only one where a reference can be left pointing at something that was pruned away.
    #[test]
    fn both_documents_are_internally_consistent() {
        assert_consistent(
            &render().expect("the published document serializes"),
            "router/docs/openapi.json",
        );
        assert_consistent(
            &render_internal().expect("the internal document serializes"),
            "router/docs/openapi.internal.json",
        );
    }

    /// The committed documents are what the docs site consumes, so they have to be regenerated
    /// whenever a handler annotation or a DTO changes. This is the gate that makes forgetting
    /// visible, in CI as well as locally.
    #[test]
    fn committed_documents_match_the_handlers() {
        for (committed, generated, path) in [
            (
                include_str!("../docs/openapi.json"),
                render().expect("the published document serializes"),
                "router/docs/openapi.json",
            ),
            (
                include_str!("../docs/openapi.internal.json"),
                render_internal().expect("the internal document serializes"),
                "router/docs/openapi.internal.json",
            ),
        ] {
            assert_eq!(
                committed, generated,
                "{path} is stale; regenerate it with `cargo run --bin openapi`"
            );
        }
    }

    /// The operator surface must not reach the published document, which the docs site renders as
    /// a playground. Checked on the rendered bytes rather than the in-memory document, since the
    /// bytes are what ships.
    #[test]
    fn the_published_document_excludes_the_operator_surface() {
        let published: serde_json::Value =
            serde_json::from_str(&render().expect("the published document serializes"))
                .expect("the published document is JSON");

        for path in published["paths"].as_object().expect("paths").keys() {
            for operator_route in ["/admin", "/healthz", "/readyz", "/metrics"] {
                assert!(
                    !path.starts_with(operator_route),
                    "{path} is an operator route and must not be published"
                );
            }
        }
        for operation in published["paths"]
            .as_object()
            .expect("paths")
            .values()
            .flat_map(|item| item.as_object().expect("a path item").values())
        {
            let tags = operation["tags"].as_array().cloned().unwrap_or_default();
            for tag in tags {
                assert!(
                    !PRIVATE_TAGS.contains(&tag.as_str().unwrap_or_default()),
                    "a {tag} operation reached the published document"
                );
            }
        }

        // What only the operator surface used goes with it, so the published document carries no
        // dangling credential or type an integrator can never use.
        let components = &published["components"];
        assert!(
            components["securitySchemes"]["AdminAuth"].is_null(),
            "the admin credential is described in the published document"
        );
        for schema in ["CreateApiKeyRequest", "CreatedApiKey", "ApiKeyMetadata"] {
            assert!(
                components["schemas"][schema].is_null(),
                "{schema} is only used by the operator surface but was published"
            );
        }
        assert!(
            !published["tags"]
                .as_array()
                .expect("tags")
                .iter()
                .any(|tag| tag["name"] == "Admin"),
            "the Admin tag is defined in the published document"
        );
        assert!(
            !published["tags"]
                .as_array()
                .expect("tags")
                .iter()
                .any(|tag| tag["name"] == "Health"),
            "the Health tag is defined in the published document"
        );

        // The integrator API is untouched by the pruning.
        assert!(published["paths"]["/tasks"]["post"].is_object());
        assert!(components["securitySchemes"]["ApiKeyAuth"].is_object());
        assert!(components["schemas"]["PayloadView"].is_object());
    }

    /// The internal document is the whole API, so the annotations on the admin handlers are still
    /// exercised rather than quietly rotting behind the exclusion.
    #[test]
    fn the_internal_document_keeps_the_operator_surface() {
        let internal: serde_json::Value =
            serde_json::from_str(&render_internal().expect("the internal document serializes"))
                .expect("the internal document is JSON");

        assert!(internal["paths"]["/healthz"]["get"].is_object());
        assert!(internal["paths"]["/readyz"]["get"].is_object());
        assert!(internal["paths"]["/metrics"]["get"].is_object());
        assert!(internal["paths"]["/admin/keys"]["post"].is_object());
        assert!(internal["paths"]["/admin/keys"]["get"].is_object());
        assert!(internal["paths"]["/admin/keys/{id}"]["delete"].is_object());
        assert!(internal["components"]["securitySchemes"]["AdminAuth"].is_object());
        assert!(internal["components"]["schemas"]["CreatedApiKey"].is_object());
    }

    /// Every response example has to deserialize into the type its `body` names.
    ///
    /// The examples are literal JSON in the annotations, which is exactly the kind of thing that
    /// goes stale when a response type changes. Parsing each one back into that type is what
    /// stops an example describing a shape the handler cannot return.
    #[test]
    fn response_examples_deserialize_into_their_body_type() {
        let document = serde_json::to_value(ApiDoc::openapi()).expect("the document serializes");
        let mut checked = 0;

        for (method, path, operation) in operations_of(&document) {
            for (status, response) in operation["responses"].as_object().into_iter().flatten() {
                for media in response["content"].as_object().into_iter().flatten() {
                    let (_, media) = media;
                    let Some(example) = media.get("example") else {
                        continue;
                    };

                    // A body is either a schema reference or an array of them.
                    let schema = &media["schema"];
                    let (body, is_list) = match schema["$ref"].as_str() {
                        Some(reference) => (reference, false),
                        None => (
                            schema["items"]["$ref"].as_str().unwrap_or_else(|| {
                                panic!("{method} {path} {status} has an example but no body schema")
                            }),
                            true,
                        ),
                    };
                    let body = body.rsplit('/').next().unwrap_or(body);
                    let label = format!("{method} {path} {status} ({body})");

                    // Each element of a list body is checked as one value of the item type, so
                    // the same arms serve both shapes.
                    let values: Vec<&serde_json::Value> = if is_list {
                        example
                            .as_array()
                            .unwrap_or_else(|| panic!("{label}: the example is not an array"))
                            .iter()
                            .collect()
                    } else {
                        vec![example]
                    };

                    for value in values {
                        let value = value.clone();
                        let outcome = match body {
                            "ApiErrorEnvelope" => {
                                serde_json::from_value::<ApiErrorEnvelope>(value).map(|_| ())
                            }
                            "TaskAcceptedResponse" => {
                                serde_json::from_value::<TaskAcceptedResponse>(value).map(|_| ())
                            }
                            "TaskView" => serde_json::from_value::<TaskView>(value).map(|_| ()),
                            "AvsMetadata" => {
                                serde_json::from_value::<AvsMetadata>(value).map(|_| ())
                            }
                            other => panic!(
                                "{label}: no check for body type `{other}`; add an arm here so \
                                 its examples are validated too"
                            ),
                        };
                        outcome.unwrap_or_else(|e| {
                            panic!(
                                "{label}: the example does not parse as \
                                                           {body}: {e}"
                            )
                        });
                        checked += 1;
                    }
                }
            }
        }

        assert!(
            checked >= 21,
            "only {checked} response examples were checked; the reference pages lost some"
        );
    }

    /// The reference pages are much less useful without a worked example on the success path, so
    /// the endpoints an integrator actually calls each carry one.
    #[test]
    fn the_integrator_endpoints_have_success_examples() {
        let published: serde_json::Value =
            serde_json::from_str(&render().expect("the published document serializes"))
                .expect("the published document is JSON");

        for (path, method, status) in [
            ("/tasks", "post", "202"),
            ("/tasks", "post", "200"),
            ("/tasks", "get", "200"),
            ("/tasks/{task_id}", "get", "200"),
        ] {
            let response = &published["paths"][path][method]["responses"][status];
            let example = &response["content"]["application/json"]["example"];
            assert!(
                !example.is_null(),
                "{method} {path} {status} has no example"
            );
        }
    }

    /// Validates real serialized DTOs against the schemas the document publishes.
    ///
    /// Everything else here checks the document against itself. This checks it against serde,
    /// which is the one thing generation does not guarantee: utoipa infers `required` and
    /// nullability from the Rust type and the serde attributes it can see, and if that inference
    /// ever diverges from what `Serialize` actually emits, the document is wrong in a way no
    /// amount of internal consistency would reveal.
    ///
    /// Each type is checked twice, fully populated and as sparsely as it can legally be, because
    /// the two catch opposite errors: a full instance catches a field the schema forbids or
    /// mistypes, and a sparse one catches a field wrongly marked required, or one the schema
    /// types as nullable that serde omits instead (or the reverse).
    #[test]
    fn serialized_dtos_validate_against_their_schemas() {
        use alloy_primitives::{Address, Bytes, U256};
        use gas_killer_common::avs_contracts::AvsContracts;
        use gas_killer_common::payload::PayloadView;

        let document = serde_json::to_value(ApiDoc::openapi()).expect("the document serializes");

        /// Validates one serialized instance against one named component schema.
        ///
        /// The schema root carries the whole component set alongside the reference, so the
        /// `#/components/schemas/...` pointers inside the schemas resolve locally and the
        /// validator never reaches for anything remote.
        fn assert_valid<T: serde::Serialize>(
            document: &serde_json::Value,
            schema: &str,
            instance: &T,
            label: &str,
        ) {
            let root = serde_json::json!({
                "$ref": format!("#/components/schemas/{schema}"),
                "components": document["components"],
            });
            let validator = jsonschema::validator_for(&root)
                .unwrap_or_else(|e| panic!("{schema} is not a usable schema: {e}"));
            let instance = serde_json::to_value(instance).expect("the instance serializes");

            let failures: Vec<String> = validator
                .iter_errors(&instance)
                .map(|error| format!("{} at `{}`", error, error.instance_path()))
                .collect();
            assert!(
                failures.is_empty(),
                "the {label} {schema} does not match the schema the document publishes:\n  {}\n\
                 serialized as: {}",
                failures.join("\n  "),
                serde_json::to_string(&instance).unwrap_or_default()
            );
        }

        let address = Address::with_last_byte(1);
        let contracts = AvsContracts {
            chain_id: 11155111,
            avs_address: address,
            bls_signature_checker: address,
            registry_coordinator: address,
            schnorr_stake_registry: Some(address),
            demo_target: Some(address),
            demo_factory: Some(address),
        };
        let sparse_contracts = AvsContracts {
            schnorr_stake_registry: None,
            demo_target: None,
            demo_factory: None,
            ..contracts.clone()
        };
        assert_valid(&document, "AvsContracts", &contracts, "fully populated");
        assert_valid(&document, "AvsContracts", &sparse_contracts, "sparse");

        // The field this most needs to hold for: `logo`, `twitter` and `operatorSets` are typed
        // as non-nullable optionals, which is only correct while they carry
        // `skip_serializing_if`. A sparse instance is what proves they are omitted, not nulled.
        let resolved = gas_killer_common::avs_contracts::ResolvedContracts::default();
        resolved.publish(contracts);
        let metadata = AvsMetadata {
            name: "Gas Killer".to_string(),
            website: "https://gaskiller.xyz".to_string(),
            description: "Verifiable off-chain compute".to_string(),
            logo: Some("https://gaskiller.xyz/logo.png".to_string()),
            twitter: Some("gaskiller".to_string()),
            operator_sets: Some(vec![AvsOperatorSetMetadata {
                name: "default".to_string(),
                id: "0".to_string(),
                description: "The only operator set".to_string(),
                software: vec![AvsOperatorSetSoftware {
                    name: "gas-killer-node".to_string(),
                    description: "Operator node".to_string(),
                    url: "https://github.com/gas-killer/service".to_string(),
                }],
                slashing_conditions: vec!["none".to_string()],
            }]),
            contracts: resolved,
        };
        assert_valid(&document, "AvsMetadata", &metadata, "fully populated");
        assert_valid(&document, "AvsMetadata", &AvsMetadata::default(), "sparse");

        let payload = PayloadView {
            to: address,
            data: Bytes::from_static(&[0x93, 0xde, 0x45, 0x31]),
            value: U256::ZERO,
            chain_id: 11155111,
            estimated_gas: 234_000,
            valid_until_block: 22_345_678,
        };
        assert_valid(&document, "PayloadView", &payload, "fully populated");

        // `TaskView` is the counterexample that keeps the check honest: `error` and `payload`
        // carry no `skip_serializing_if`, so a sparse one serializes them as null and the schema
        // has to allow it.
        assert_valid(
            &document,
            "TaskView",
            &TaskView {
                task_id: "3f8c1e02-9a4b-4c7d-8e1f-2b6a5c9d0e11".to_string(),
                status: TaskStatus::Ready,
                created_at: 1_753_180_800,
                updated_at: 1_753_180_812,
                error: None,
                payload: Some(payload),
            },
            "fully populated",
        );
        assert_valid(
            &document,
            "TaskView",
            &TaskView {
                task_id: "3f8c1e02-9a4b-4c7d-8e1f-2b6a5c9d0e11".to_string(),
                status: TaskStatus::Queued,
                created_at: 1_753_180_800,
                updated_at: 1_753_180_800,
                error: None,
                payload: None,
            },
            "sparse",
        );

        assert_valid(
            &document,
            "TaskAcceptedResponse",
            &TaskAcceptedResponse {
                task_id: "3f8c1e02-9a4b-4c7d-8e1f-2b6a5c9d0e11".to_string(),
                status: TaskStatus::Queued,
                deduplicated: false,
            },
            "fully populated",
        );

        assert_valid(
            &document,
            "ApiErrorEnvelope",
            &ApiErrorEnvelope {
                error: ApiErrorBody {
                    code: ErrorCode::PayloadExpired,
                    message: "re-request".to_string(),
                },
            },
            "fully populated",
        );

        let created = CreatedApiKey {
            id: "k_1".to_string(),
            key: "gk_example".to_string(),
            label: Some("integration".to_string()),
            created_at: 1_753_180_800,
            invalid_at: Some(1_893_456_000),
            rpm_limit: Some(600),
        };
        assert_valid(&document, "CreatedApiKey", &created, "fully populated");
        assert_valid(
            &document,
            "CreatedApiKey",
            &CreatedApiKey {
                label: None,
                invalid_at: None,
                rpm_limit: None,
                ..created
            },
            "sparse",
        );

        let listed = ApiKeyMetadata {
            id: "k_1".to_string(),
            label: Some("integration".to_string()),
            created_at: 1_753_180_800,
            last_used: Some(1_753_180_900),
            invalid_at: Some(1_893_456_000),
            rpm_limit: Some(600),
        };
        assert_valid(&document, "ApiKeyMetadata", &listed, "fully populated");
        assert_valid(
            &document,
            "ApiKeyMetadata",
            &ApiKeyMetadata {
                label: None,
                last_used: None,
                invalid_at: None,
                rpm_limit: None,
                ..listed
            },
            "sparse",
        );
    }

    /// Guards the representations a structural derive would get wrong, which is the whole reason
    /// the schemas above are hand-built. A regression here means the document promises a shape
    /// the handlers do not serve.
    #[test]
    fn wire_representations_survive_generation() {
        let doc = serde_json::to_value(ApiDoc::openapi()).expect("the document serializes");
        let schemas = &doc["components"]["schemas"];

        // Addresses and uint256s are hex strings, not byte arrays or JSON numbers.
        assert_eq!(schemas["Address"]["type"], "string");
        assert_eq!(schemas["HexUint256"]["type"], "string");
        assert_eq!(schemas["HexBytes"]["type"], "string");

        // `call_data` is the one byte sequence that is an array rather than a hex string.
        assert_eq!(schemas["CallData"]["type"], "array");
        assert_eq!(schemas["CallData"]["items"]["maximum"], 255);
        assert_eq!(schemas["CallData"]["minItems"], 4);

        // `transition_index` keeps all three accepted JSON types.
        let variants = schemas["TransitionIndex"]["anyOf"]
            .as_array()
            .expect("the union is rendered as anyOf");
        assert_eq!(variants.len(), 3);
        assert_eq!(variants[1]["enum"][0], "auto");
        assert_eq!(variants[2]["type"], "null");

        // Error codes stay SCREAMING_SNAKE_CASE, which integrators match on.
        let codes = schemas["ErrorCode"]["enum"]
            .as_array()
            .expect("the error codes are an enum");
        assert!(codes.contains(&serde_json::json!("TRANSITION_MISMATCH")));
        assert!(codes.contains(&serde_json::json!("PAYLOAD_EXPIRED")));

        // Task statuses stay snake_case.
        let statuses = schemas["TaskStatus"]["enum"]
            .as_array()
            .expect("the statuses are an enum");
        assert!(statuses.contains(&serde_json::json!("queued")));
        assert!(statuses.contains(&serde_json::json!("ready")));
    }

    /// The tables below mirror the router's two listeners by convention, and this asserts the
    /// document describes exactly their union: nothing annotated that the tables omit, and
    /// nothing in a table without an annotation.
    ///
    /// The mirror itself is maintained by hand, and that is the tradeoff of not using
    /// `utoipa-axum`: axum exposes no way to enumerate a `Router`'s routes, so a route added to
    /// one of the builders and to neither the annotations nor these tables would ship
    /// undocumented. Adding a route is therefore three edits, called out in a comment on each
    /// builder: the handler's `#[utoipa::path]`, the `paths(...)` list above, and the table here.
    #[test]
    fn the_documented_routes_are_exactly_the_route_table() {
        // Paths in OpenAPI's `{param}` form rather than axum's `:param`. `/healthz` appears in
        // both tables because both listeners serve the same handler on it, and the document
        // carries one entry naming both ports.
        let ingress_routes = [
            ("/healthz", &["get"][..]),
            ("/avs-metadata", &["get"][..]),
            ("/tasks", &["post", "get"][..]),
            ("/tasks/{task_id}", &["get"][..]),
            ("/admin/keys", &["post", "get"][..]),
            ("/admin/keys/{id}", &["delete"][..]),
        ];
        let operator_routes = [
            ("/healthz", &["get"][..]),
            ("/readyz", &["get"][..]),
            ("/metrics", &["get"][..]),
        ];
        let route_table: BTreeSet<String> = ingress_routes
            .into_iter()
            .chain(operator_routes)
            .flat_map(|(path, methods)| {
                methods.iter().map(move |method| format!("{method} {path}"))
            })
            .collect();

        let document = serde_json::to_value(ApiDoc::openapi()).expect("the document serializes");
        let documented: BTreeSet<String> = document["paths"]
            .as_object()
            .expect("the document has paths")
            .iter()
            .flat_map(|(path, item)| {
                item.as_object()
                    .expect("a path item")
                    .keys()
                    .filter(|key| METHODS.contains(&key.as_str()))
                    .map(move |method| format!("{method} {path}"))
            })
            .collect();

        assert_eq!(
            documented, route_table,
            "the document and the mirrors of the route builders disagree; annotate the handler, \
             list it in `paths(...)`, and update the table in this test"
        );
    }
}
