use axum::{
    Router,
    extract::State,
    http::header,
    response::{Html, IntoResponse, Response},
    routing::get,
};
use std::sync::Arc;

static INDEX_HTML_TEMPLATE: &str = include_str!("../static/index.html");

#[derive(Clone, Debug)]
pub struct UiState {
    pub api_port: u16,
}

pub async fn serve_index(State(state): State<Arc<UiState>>) -> Response {
    let html = INDEX_HTML_TEMPLATE.replace("{{API_PORT}}", &state.api_port.to_string());
    Response::builder()
        .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
        .body(axum::body::Body::from(html))
        .unwrap_or_else(|_| Html(INDEX_HTML_TEMPLATE.to_string()).into_response())
}

pub fn create_ui_router(api_port: u16) -> Router {
    let state = Arc::new(UiState { api_port });
    Router::new().route("/", get(serve_index)).with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use axum::http::Request;
    use tower::ServiceExt;

    #[tokio::test]
    async fn test_ui_serve_index() {
        let app = create_ui_router(8002);
        let request = Request::builder()
            .uri("/")
            .body(axum::body::Body::empty())
            .unwrap();
        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), axum::http::StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/html; charset=utf-8"
        );

        let body = to_bytes(response.into_body(), 100_000).await.unwrap();
        let body_str = String::from_utf8(body.to_vec()).unwrap();
        assert!(body_str.contains("ReadQL Studio"));
        assert!(body_str.contains("8002"));
    }
}
