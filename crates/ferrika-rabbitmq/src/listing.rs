//! Bounded listings of queues, exchanges and bindings.
//!
//! On a broker with thousands of queues one `GET /api/queues/{vhost}`
//! returns every queue with all its statistics in one response, which is
//! slow and can pass the HTTP client's 64 MiB cap. The listings here ask
//! the management API for pages instead (`page`, `page_size`), filter by
//! name on the broker (`name`, `use_regex`), select only the fields Ferrika
//! shows (`columns`), and stop at a limit:
//!
//! - [`ManagementClient::list_queues`] and
//!   [`ManagementClient::list_exchanges`]: paged and filtered by the broker.
//! - [`ManagementClient::list_bindings`]: the API does not page bindings, so
//!   they are fetched whole (they are small: no statistics) and filtered
//!   and cut here.
//!
//! The broker matches `name` as a case-insensitive substring, or as a
//! case-insensitive regular expression with `use_regex=true` (checked
//! against RabbitMQ 4.4); the bindings filter here does the same. Pages are fetched one after another, so a queue created or
//! deleted meanwhile can shift an entry between pages (it may be missed
//! or shown twice); the counts are the broker's at the last page.

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::client::{segment, ManagementClient};
use crate::error::{RabbitError, RabbitResult};
use crate::model::{Binding, Exchange, Queue};

/// The largest page the management API serves (`page_size` above it is
/// refused).
pub const MAX_PAGE_SIZE: usize = 500;

/// How many entries a listing returns unless told otherwise.
pub const DEFAULT_LIST_LIMIT: usize = 1000;

/// The queue fields [`Queue`] reads; the API leaves out the rest (message
/// rates, memory, garbage collection, backing queue status...).
const QUEUE_COLUMNS: &str = "name,vhost,type,durable,auto_delete,exclusive,state,messages,\
                             messages_ready,messages_unacknowledged,consumers,arguments,policy,\
                             effective_policy_definition";

/// The exchange fields [`Exchange`] reads.
const EXCHANGE_COLUMNS: &str = "name,vhost,type,durable,auto_delete,internal,arguments";

/// Which entries a listing returns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListQuery {
    /// Only entries whose name contains this text, or, with
    /// [`Self::regex`], matches it as a regular expression; both
    /// case-insensitive, as the broker matches. For
    /// bindings, the source or the destination. Blank means no filter.
    pub name: Option<String>,
    /// Treat [`Self::name`] as a regular expression.
    pub regex: bool,
    /// The most entries to fetch; 0 fetches all of them, page by page.
    pub limit: usize,
}

impl Default for ListQuery {
    fn default() -> Self {
        Self {
            name: None,
            regex: false,
            limit: DEFAULT_LIST_LIMIT,
        }
    }
}

impl ListQuery {
    /// Every entry, no filter (fetched page by page).
    pub fn all() -> Self {
        Self {
            limit: 0,
            ..Self::default()
        }
    }

    /// The name filter, trimmed; `None` when blank.
    pub fn name_filter(&self) -> Option<&str> {
        self.name
            .as_deref()
            .map(str::trim)
            .filter(|name| !name.is_empty())
    }

    /// Checks the pattern before anything is sent.
    pub fn validate(&self) -> RabbitResult<()> {
        self.matcher().map(|_| ())
    }

    /// The page size: the limit, at most [`MAX_PAGE_SIZE`].
    fn page_size(&self) -> usize {
        match self.limit {
            0 => MAX_PAGE_SIZE,
            limit => limit.min(MAX_PAGE_SIZE),
        }
    }

    /// The filter as the broker applies it, for matching here.
    fn matcher(&self) -> RabbitResult<Option<regex::Regex>> {
        let Some(name) = self.name_filter() else {
            return Ok(None);
        };
        let pattern = if self.regex {
            name.to_string()
        } else {
            regex::escape(name)
        };
        regex::RegexBuilder::new(&pattern)
            .case_insensitive(true)
            .size_limit(1 << 20)
            .build()
            .map(Some)
            .map_err(|e| RabbitError::config(format!("Invalid name pattern '{name}': {e}")))
    }

    /// The query string of page `page` (1-based).
    fn page_query(&self, page: usize, columns: &str) -> String {
        let mut query = format!(
            "page={page}&page_size={}&columns={}",
            self.page_size(),
            segment_list(columns)
        );
        if let Some(name) = self.name_filter() {
            query.push_str(&format!("&name={}&use_regex={}", segment(name), self.regex));
        }
        query
    }
}

/// Encodes a comma-separated column list, keeping the commas.
fn segment_list(columns: &str) -> String {
    columns
        .split(',')
        .map(|c| segment(c.trim()))
        .collect::<Vec<_>>()
        .join(",")
}

/// A listing cut at the query's limit.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Listing<T> {
    pub items: Vec<T>,
    /// Entries matching the filter on the broker, fetched or not.
    pub matched: u64,
    /// Entries in the vhost, matching or not.
    pub total: u64,
}

impl<T> Listing<T> {
    /// Whether more entries match than were fetched.
    pub fn truncated(&self) -> bool {
        (self.items.len() as u64) < self.matched
    }
}

/// One page as the management API returns it when `page` is given.
#[derive(Debug, Deserialize)]
struct Page<T> {
    #[serde(default = "Vec::new")]
    items: Vec<T>,
    #[serde(default)]
    filtered_count: u64,
    #[serde(default)]
    total_count: u64,
    #[serde(default)]
    page_count: u64,
}

impl ManagementClient {
    /// The queues of `vhost` that `query` selects, fetched in pages.
    pub async fn list_queues(
        &self,
        vhost: &str,
        query: &ListQuery,
    ) -> RabbitResult<Listing<Queue>> {
        self.list_paged(
            &format!("/api/queues/{}", segment(vhost)),
            query,
            QUEUE_COLUMNS,
        )
        .await
    }

    /// The exchanges of `vhost` that `query` selects, fetched in pages. The
    /// default exchange (empty name) is left out by any name filter.
    pub async fn list_exchanges(
        &self,
        vhost: &str,
        query: &ListQuery,
    ) -> RabbitResult<Listing<Exchange>> {
        self.list_paged(
            &format!("/api/exchanges/{}", segment(vhost)),
            query,
            EXCHANGE_COLUMNS,
        )
        .await
    }

    /// The bindings of `vhost` whose source or destination `query` selects.
    /// The management API does not page bindings, so all are fetched and
    /// the filter and limit apply here.
    pub async fn list_bindings(
        &self,
        vhost: &str,
        query: &ListQuery,
    ) -> RabbitResult<Listing<Binding>> {
        let matcher = query.matcher()?;
        let bindings = self.bindings(vhost).await?;
        Ok(cut_bindings(bindings, matcher.as_ref(), query.limit))
    }

    async fn list_paged<T: DeserializeOwned>(
        &self,
        path: &str,
        query: &ListQuery,
        columns: &str,
    ) -> RabbitResult<Listing<T>> {
        query.validate()?;
        let mut listing = Listing {
            items: Vec::new(),
            matched: 0,
            total: 0,
        };
        let mut page = 1;
        loop {
            let fetched: Page<T> = match self
                .get_json(&format!("{path}?{}", query.page_query(page, columns)))
                .await
            {
                Ok(fetched) => fetched,
                // Entries deleted since the previous page.
                Err(RabbitError::Api { status: 400, .. }) if page > 1 => break,
                Err(e) => return Err(e),
            };
            listing.matched = fetched.filtered_count;
            listing.total = fetched.total_count;
            let got = fetched.items.len();
            listing.items.extend(fetched.items);
            if query.limit > 0 && listing.items.len() >= query.limit {
                listing.items.truncate(query.limit);
                break;
            }
            if got == 0 || page as u64 >= fetched.page_count {
                break;
            }
            page += 1;
        }
        // Entries created since the first page can push the count up.
        listing.matched = listing.matched.max(listing.items.len() as u64);
        Ok(listing)
    }
}

/// Bindings whose source or destination `matcher` matches, at most
/// `limit` (0: all).
fn cut_bindings(
    bindings: Vec<Binding>,
    matcher: Option<&regex::Regex>,
    limit: usize,
) -> Listing<Binding> {
    let total = bindings.len() as u64;
    let kept: Vec<Binding> = bindings
        .into_iter()
        .filter(|b| matcher.is_none_or(|m| m.is_match(&b.source) || m.is_match(&b.destination)))
        .collect();
    let matched = kept.len() as u64;
    let mut items = kept;
    if limit > 0 {
        items.truncate(limit);
    }
    Listing {
        items,
        matched,
        total,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn query(name: Option<&str>, regex: bool, limit: usize) -> ListQuery {
        ListQuery {
            name: name.map(str::to_string),
            regex,
            limit,
        }
    }

    #[test]
    fn the_page_size_follows_the_limit_up_to_the_apis_maximum() {
        assert_eq!(query(None, false, 20).page_size(), 20);
        assert_eq!(query(None, false, 5000).page_size(), MAX_PAGE_SIZE);
        assert_eq!(ListQuery::all().page_size(), MAX_PAGE_SIZE);
        assert_eq!(ListQuery::default().limit, DEFAULT_LIST_LIMIT);
    }

    #[test]
    fn the_query_string_carries_paging_filter_and_columns() {
        let plain = query(None, false, 100).page_query(2, "name,vhost");
        assert_eq!(plain, "page=2&page_size=100&columns=name,vhost");
        let filtered = query(Some(" orders.dlq "), false, 0).page_query(1, "name");
        assert_eq!(
            filtered,
            "page=1&page_size=500&columns=name&name=orders.dlq&use_regex=false"
        );
        // The pattern is encoded so it cannot add parameters.
        let regex = query(Some("^orders&x=1|a b$"), true, 10).page_query(1, "name");
        assert!(
            regex.ends_with("&name=%5Eorders%26x%3D1%7Ca%20b%24&use_regex=true"),
            "{regex}"
        );
        assert!(query(Some("  "), true, 10).name_filter().is_none());
    }

    #[test]
    fn bad_patterns_are_refused_before_any_request() {
        let err = query(Some("orders["), true, 10).validate().unwrap_err();
        assert!(err.to_string().contains("Invalid name pattern"), "{err}");
        // Without --regex the same text is a plain substring.
        assert!(query(Some("orders["), false, 10).validate().is_ok());
    }

    fn binding(source: &str, destination: &str) -> Binding {
        serde_json::from_value(json!({
            "source": source,
            "vhost": "/",
            "destination": destination,
            "destination_type": "queue",
            "routing_key": "#",
        }))
        .unwrap()
    }

    #[test]
    fn bindings_are_filtered_like_the_broker_filters_names() {
        let bindings = vec![
            binding("", "orders"),
            binding("dlx", "Orders.DLQ"),
            binding("events", "audit"),
        ];
        let substring = query(Some("orders"), false, 0).matcher().unwrap();
        let listing = cut_bindings(bindings.clone(), substring.as_ref(), 0);
        assert_eq!(listing.matched, 2);
        assert_eq!(listing.total, 3);
        assert!(!listing.truncated());

        let regex = query(Some("^DL"), true, 0).matcher().unwrap();
        let listing = cut_bindings(bindings.clone(), regex.as_ref(), 0);
        assert_eq!(listing.items, vec![binding("dlx", "Orders.DLQ")]);

        let capped = cut_bindings(bindings, None, 2);
        assert_eq!(capped.items.len(), 2);
        assert_eq!(capped.matched, 3);
        assert!(capped.truncated());
    }

    #[test]
    fn pages_parse_with_and_without_items() {
        let page: Page<Queue> = serde_json::from_value(json!({
            "filtered_count": 9, "item_count": 1, "page": 1, "page_count": 9,
            "page_size": 1, "total_count": 1200,
            "items": [{ "name": "q", "vhost": "/", "type": "quorum" }]
        }))
        .unwrap();
        assert_eq!(page.items.len(), 1);
        assert_eq!(page.filtered_count, 9);
        assert_eq!(page.total_count, 1200);
        assert_eq!(page.page_count, 9);
        let empty: Page<Queue> = serde_json::from_value(json!({
            "filtered_count": 0, "page_count": 0, "total_count": 0, "items": []
        }))
        .unwrap();
        assert!(empty.items.is_empty());
    }

    /// A fake `/api/queues/%2F` with `count` queues that pages like the
    /// management API.
    fn paging_server(count: usize) -> crate::test_support::Handler {
        use crate::test_support::param;
        std::sync::Arc::new(move |path: &str| {
            let page: usize = param(path, "page").unwrap().parse().unwrap();
            let size: usize = param(path, "page_size").unwrap().parse().unwrap();
            if size > MAX_PAGE_SIZE {
                return (
                    400,
                    r#"{"error":"invalid_pagination_parameters"}"#.to_string(),
                );
            }
            let page_count = count.div_ceil(size);
            if page > page_count.max(1) {
                return (400, r#"{"error":"page_out_of_range"}"#.to_string());
            }
            let items: Vec<_> = ((page - 1) * size..(page * size).min(count))
                .map(|i| json!({ "name": format!("q-{i:04}"), "vhost": "/", "type": "classic" }))
                .collect();
            let body = json!({
                "filtered_count": count, "total_count": count, "page": page,
                "page_count": page_count, "page_size": size, "item_count": items.len(),
                "items": items,
            });
            (200, body.to_string())
        })
    }

    #[tokio::test]
    async fn listings_fetch_page_by_page_up_to_the_limit() {
        let (url, seen) = crate::test_support::serve(paging_server(1200)).await;
        let client = ManagementClient::new(&url).unwrap();

        let all = client.list_queues("/", &ListQuery::all()).await.unwrap();
        assert_eq!(all.items.len(), 1200);
        assert_eq!(all.items[1199].name, "q-1199");
        assert_eq!((all.matched, all.total), (1200, 1200));
        assert!(!all.truncated());
        {
            let seen = seen.lock().unwrap();
            assert_eq!(seen.len(), 3, "{seen:?}");
            assert!(seen[0].starts_with("/api/queues/%2F?page=1&page_size=500&columns=name,"));
            assert!(seen[2].contains("page=3&"));
            seen.iter()
                .for_each(|p| assert!(!p.contains("name="), "{p}"));
        }
        seen.lock().unwrap().clear();

        let some = client
            .list_queues("/", &query(None, false, 600))
            .await
            .unwrap();
        assert_eq!(some.items.len(), 600);
        assert_eq!(some.matched, 1200);
        assert!(some.truncated());
        assert_eq!(seen.lock().unwrap().len(), 2);
        seen.lock().unwrap().clear();

        let few = client
            .list_queues("/", &query(None, false, 3))
            .await
            .unwrap();
        assert_eq!(few.items.len(), 3);
        assert_eq!(seen.lock().unwrap().len(), 1);
        assert!(seen.lock().unwrap()[0].contains("page_size=3&"));

        let empty_server = crate::test_support::serve(paging_server(0)).await.0;
        let empty = ManagementClient::new(&empty_server)
            .unwrap()
            .list_queues("/", &ListQuery::default())
            .await
            .unwrap();
        assert!(empty.items.is_empty());
        assert!(!empty.truncated());
    }

    #[tokio::test]
    async fn a_bad_pattern_sends_nothing() {
        let (url, seen) = crate::test_support::serve(paging_server(10)).await;
        let client = ManagementClient::new(&url).unwrap();
        assert!(client
            .list_exchanges("/", &query(Some("("), true, 10))
            .await
            .is_err());
        assert!(seen.lock().unwrap().is_empty());
    }
}
